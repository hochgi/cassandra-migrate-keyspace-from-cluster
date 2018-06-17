package hochgi.devops.cassandra.backup

import java.util.concurrent.atomic.AtomicLong

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.datastax.driver.core._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.{global => ec}
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class LocalCassandraSink(conf: Config) extends LazyLogging {

  val parallelism: Int = conf.getInt("hochgi.devops.cassandra.local.parallelism")
  val statement: String = conf.getString("hochgi.devops.cassandra.local.insert-statement")
  val prefix: Option[String] = Some(statement.takeWhile(_ != '('))

  def cassandraSink(session: Session): Sink[Row, Future[Done]] = {
    val preparedStatement = session.prepare(statement)
    val statementBinder = (row: Row, stmt: PreparedStatement) => {
      var bs = stmt.bind()
      row.getColumnDefinitions
        .iterator()
        .forEachRemaining { coldef =>
          val colName = coldef.getName
          bs = coldef.getType.getName match {
            case DataType.Name.TEXT | DataType.Name.VARCHAR => bs.setString(colName, row.getString(colName))
            case DataType.Name.ASCII     => bs.setString(colName,    row.getString(colName))
            case DataType.Name.BIGINT    => bs.setLong(colName,      row.getLong(colName))
            case DataType.Name.BOOLEAN   => bs.setBool(colName,      row.getBool(colName))
            case DataType.Name.DECIMAL   => bs.setDecimal(colName,   row.getDecimal(colName))
            case DataType.Name.DOUBLE    => bs.setDouble(colName,    row.getDouble(colName))
            case DataType.Name.FLOAT     => bs.setFloat(colName,     row.getFloat(colName))
            case DataType.Name.INT       => bs.setInt(colName,       row.getInt(colName))
            case DataType.Name.TIMESTAMP => bs.setTimestamp(colName, row.getTimestamp(colName))
            case DataType.Name.UUID      => bs.setUUID(colName,      row.getUUID(colName))
            case DataType.Name.VARINT    => bs.setVarint(colName,    row.getVarint(colName))
            case DataType.Name.INET      => bs.setInet(colName,      row.getInet(colName))
            case DataType.Name.DATE      => bs.setDate(colName,      row.getDate(colName))
            case DataType.Name.TIME      => bs.setTime(colName,      row.getTime(colName))
            case DataType.Name.CUSTOM    => ???
            case DataType.Name.BLOB      => ???
            case DataType.Name.COUNTER   => ???
            case DataType.Name.TIMEUUID  => ???
            case DataType.Name.SMALLINT  => bs.setShort(colName,     row.getShort(colName))
            case DataType.Name.TINYINT   => bs.setByte(colName,      row.getByte(colName))
            case DataType.Name.DURATION  => ???
            case DataType.Name.LIST      => ???
            case DataType.Name.MAP       => ???
            case DataType.Name.SET       => ???
            case DataType.Name.UDT       => ???
            case DataType.Name.TUPLE     => ???
          }
        }
      bs
    }

    Flow[Row]
      .mapAsyncUnordered(parallelism) { row =>
        val stmt = statementBinder(row, preparedStatement)

        lazy val stmt4log = Util.rowStringRepr(row,prefix)
        Util.simpleRetry(16)(Util.listenableFutureToFuture(session.executeAsync(stmt))) {
          case err: Throwable => {
            //JMX metrics mark
            CassandraMirrorBackupper.ingestFailureRate.mark()

            logger.error(s"Failed to ingest bounded statement for row [$stmt4log]")
          }
        }(ec).transform { t =>
          t.failed.foreach { _ =>
            // Only when we fail completely after retrying
            // we'll log to failed-statements log.
            LocalCassandraSink.failedStatementsLogger.error(stmt4log)
          }
          Success(t)
        }(ec)
      }.toMat(Sink.foreach[Try[ResultSet]]{
      case Success(_) => {
        // JMX metrics mark
        CassandraMirrorBackupper.ingestedRowRate.mark()

        // printout reports
        val currentCount = LocalCassandraSink.ingestCounter.incrementAndGet()
        val pow = math.max(3, math.log10(currentCount).toInt)
        if (currentCount % math.pow(10, pow).toLong == 0)
          println(s"total ingest count is [$currentCount]")
      }
      case Failure(_) => {
        // JMX metrics mark
        CassandraMirrorBackupper.ingestTotalFailureRate.mark()

        // printout reports
        val currentCount = LocalCassandraSink.errorsCounter.incrementAndGet()
        val pow = math.log10(currentCount).toInt
        if (currentCount % math.pow(10, pow).toLong == 0 || pow == 0)
          println(s"total ingest count is [$currentCount]")
      }
    })(Keep.right)
  }
}

object LocalCassandraSink {
  val failedStatementsLogger: Logger = LoggerFactory.getLogger("failedStatementsLogger")
  val ingestCounter: AtomicLong = new AtomicLong(0L)
  val errorsCounter: AtomicLong = new AtomicLong(0L)
}
