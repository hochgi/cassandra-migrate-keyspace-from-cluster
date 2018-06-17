package hochgi.devops.cassandra.backup

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.datastax.driver.core.{ResultSet, Row, Session, Statement}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

class ModifiedCassandraSourceStage(id: String, stmt: Statement, session: Session) extends GraphStage[SourceShape[Row]] with LazyLogging {
  val out: Outlet[Row] = Outlet("CassandraSource.out")
  override val shape: SourceShape[Row] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private[this] var maybeRs = Option.empty[ResultSet]
    private[this] var futFetchedCallback: Try[ResultSet] => Unit = _
    private[this] var failCount = 0
    private[this] var fetchedRowsCount = 0
    private[this] val minimumPrefetchedBufferSize = math.max(1,stmt.getFetchSize / 2)
    private[this] def executeInitialStatement(): Unit = Util.invokeTryCallback(session.executeAsync(stmt))(futFetchedCallback)

    def countAndPush(): Unit = maybeRs.foreach { rs =>
      val row = rs.one()
      fetchedRowsCount += 1
      push(out, row)
    }

    def printAndComplete(): Unit = {
      println(s"fetched rows count for [$id] is [$fetchedRowsCount]")
      completeStage()
    }

    override def preStart(): Unit = {
      futFetchedCallback = getAsyncCallback[Try[ResultSet]](tryPushAfterFetch).invoke
      executeInitialStatement()
    }

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        maybeRs.foreach { rs =>
          val rowsReady = rs.getAvailableWithoutFetching
          if (rowsReady > 0) {
            countAndPush()
            if(rowsReady < minimumPrefetchedBufferSize && !rs.isFullyFetched)
              rs.fetchMoreResults()
          }
          else Try(rs.isExhausted) match {
            case Success(true) => printAndComplete()
            case falseOrFailed =>
              falseOrFailed.failed.foreach { err =>
                logger.error(s"failed to query isExhausted for [$id] after fetching [$fetchedRowsCount] rows", err)
              }

              // fetch next page
              Util.invokeTryCallback(rs.fetchMoreResults())(futFetchedCallback)
          }
        }
      }
    })

    private[this] def tryPushAfterFetch(rsOrFailure: Try[ResultSet]): Unit = rsOrFailure match {
      case Success(rs) => {
        maybeRs = Some(rs)
        if (rs.getAvailableWithoutFetching == 0) printAndComplete()
        else if (isAvailable(out)) countAndPush()
      }
      case Failure(err) => {
        failCount += 1
        logger.error(s"failed [fail count #$failCount] to fetch [initial? ${maybeRs.isEmpty}] for [$id] after fetching [$fetchedRowsCount] rows (retrying)", err)
        maybeRs.fold(executeInitialStatement()) { rs =>
          materializer.scheduleOnce(2.seconds,() => Util.invokeTryCallback(rs.fetchMoreResults())(futFetchedCallback))
        }
      }
    }
  }
}