package hochgi.devops.cassandra.backup

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}
import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core.policies.RetryPolicy
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object CassandraMirrorBackupper extends Instrumented(new com.codahale.metrics.MetricRegistry()) with App with LazyLogging {

  val conf = ConfigFactory.load()
  val retryPolicy = new RetryPolicy {
    override def onReadTimeout(statement: Statement, cl: ConsistencyLevel, requiredResponses: Int, receivedResponses: Int, dataRetrieved: Boolean, nbRetry: Int): RetryPolicy.RetryDecision = {
      if(nbRetry > 15) RetryPolicy.RetryDecision.rethrow()
      else RetryPolicy.RetryDecision.retry(ConsistencyLevel.ONE)
    }

    override def onWriteTimeout(statement: Statement, cl: ConsistencyLevel, writeType: WriteType, requiredAcks: Int, receivedAcks: Int, nbRetry: Int): RetryPolicy.RetryDecision = {
      if(nbRetry > 15) RetryPolicy.RetryDecision.rethrow()
      else RetryPolicy.RetryDecision.retry(ConsistencyLevel.ONE)
    }

    override def onUnavailable(statement: Statement, cl: ConsistencyLevel, requiredReplica: Int, aliveReplica: Int, nbRetry: Int): RetryPolicy.RetryDecision = {
      if(nbRetry > 15) RetryPolicy.RetryDecision.rethrow()
      else RetryPolicy.RetryDecision.retry(ConsistencyLevel.ONE)
    }

    override def onRequestError(statement: Statement, cl: ConsistencyLevel, e: DriverException, nbRetry: Int): RetryPolicy.RetryDecision = {
      logger.error(s"request error: [$statement] with [$cl] and try#[$nbRetry]",e)
      if(nbRetry > 15) RetryPolicy.RetryDecision.rethrow()
      else RetryPolicy.RetryDecision.retry(ConsistencyLevel.ONE)
    }

    override def init(cluster: Cluster): Unit = {}

    override def close(): Unit = {}
  }

  val remoteFuture = {
    val cluster = conf
      .getStringList("hochgi.devops.cassandra.remote.hosts")
      .asScala
      .foldLeft(Cluster.builder){ case (cb,host) => cb.addContactPoint(host) }
      .withPort(conf.getInt("hochgi.devops.cassandra.remote.cql-port"))
      .withRetryPolicy(retryPolicy)
      .withoutJMXReporting()
      .build()

    Util.simpleRetry(10)(Util.listenableFutureToFuture(cluster.connectAsync())) {
      case e => logger.error("failed to connect to remote cluster",e)
    }(ExecutionContext.global)
  }

  val localFuture = {
    val cluster = conf
      .getStringList("hochgi.devops.cassandra.local.hosts")
      .asScala
      .foldLeft(Cluster.builder){ case (cb,host) => cb.addContactPoint(host) }
      .withPort(conf.getInt("hochgi.devops.cassandra.local.cql-port"))
      .withRetryPolicy(retryPolicy)
      .withoutJMXReporting()
      .build()

    Util.simpleRetry(10)(Util.listenableFutureToFuture(cluster.connectAsync())) {
      case e => logger.error("failed to connect to local cluster",e)
    }(ExecutionContext.global)
  }

  val system = ActorSystem("CassandraMirrorBackupper")
  val mat = ActorMaterializer()(system)

  val done = remoteFuture.zip(localFuture).flatMap { case (remote,local) =>

    val remoteRowsSource: Source[Row, NotUsed] = RemoteCassandraSource.cassandraSource(conf, remote)
    val localRowsSink: Sink[Row, Future[Done]] = LocalCassandraSink.cassandraSink(conf, local)

    remoteRowsSource.runWith(localRowsSink)(mat).transformWith { t =>

      import scala.concurrent.ExecutionContext.Implicits.global

      t.fold[Unit](e => {
        e.printStackTrace(System.err)
        System.err.println("[FAILURE!]\n")
      }, _ => println("[SUCCESS!]\n"))

      mat.shutdown()
      val f1 = Util.listenableFutureToFuture(remote.closeAsync())
      val f2 = Util.listenableFutureToFuture(local.closeAsync())
      val f3 = system.terminate()

      (for {
        _ <- f1
        _ <- f2
        _ <- f3
      } yield ()).andThen {
        case Success(_) =>
          println("[termination succeeded]\n")
          // Can't use proper scheduling like:
          //   system.scheduler.scheduleOnce(10.seconds)(System.exit(0))
          // since system is already shutting down.
          // This will make sure we are exiting
          Future(blocking {
            Thread.sleep(10000)
            System.exit(0)
          })
        case Failure(e) =>
          e.printStackTrace(System.err)
          System.err.println("[termination failed]\n")
          System.exit(1)
      }
    }(ExecutionContext.global)
  }(ExecutionContext.global)
  Await.result(done, Duration.Inf)
}
