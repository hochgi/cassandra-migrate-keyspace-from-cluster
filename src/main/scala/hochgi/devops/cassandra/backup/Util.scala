package hochgi.devops.cassandra.backup

import java.util.Base64

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.datastax.driver.core.{ColumnDefinitions, DataType, Row}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

object Util {


  /**
    * Converts Guava's ListenableFuture to scala Future at the expense of the global ExecutionContext
    * @param listenableFuture ListenableFuture to convert
    * @return corresponding scala Future
    */
  def listenableFutureToFuture[T](listenableFuture: ListenableFuture[T]): Future[T] = {
    val p = Promise[T]

    Futures.addCallback(listenableFuture, new FutureCallback[T] {
      override def onSuccess(result: T): Unit = p.success(result)
      override def onFailure(t: Throwable): Unit = p.failure(t)
    }, ExecutionContext.global)

    p.future
  }

  def invokeTryCallback[T](listenableFuture: ListenableFuture[T])(callback: Try[T] => Unit): Unit = {
    Futures.addCallback(listenableFuture, new FutureCallback[T] {
      override def onSuccess(result: T): Unit = callback(Success(result))
      override def onFailure(t: Throwable): Unit = callback(Failure(t))
    }, ExecutionContext.global)
  }

  def simpleRetry[T](maxRetries: Int)
                    (task: => Future[T])
                    (onFail: Throwable => Unit)
                    (implicit ec: ExecutionContext): Future[T] = {
    if (maxRetries <= 1) task
    else task.recoverWith {
      case err: Throwable =>
        onFail(err)
        simpleRetry(maxRetries - 1)(task)(onFail)
    }
  }

  def cartesianProduct[T](factors: List[List[T]]): Iterator[List[T]] = factors match {
    case Nil => Iterator.empty
    case xs :: Nil => xs.iterator.map(any => List(any))
    case xs :: xss => xs.iterator.flatMap { elem =>
      new Iterator[List[T]] {

        private val inner = cartesianProduct(xss)

        override def hasNext: Boolean = inner.hasNext

        override def next(): List[T] = elem :: inner.next()
      }
    }
  }

  def wrap(s: String): String = "'" + s + "'"

  def genericColumnValue(coldef: ColumnDefinitions.Definition, row: Row): String = coldef.getType.getName match {
    case DataType.Name.CUSTOM    => ???
    case DataType.Name.ASCII     => wrap(row.getString(coldef.getName))
    case DataType.Name.BIGINT    => row.getLong(coldef.getName).toString
    case DataType.Name.BLOB      => wrap(Base64.getEncoder.encodeToString(row.getBytes(coldef.getName).array()))
    case DataType.Name.BOOLEAN   => row.getBool(coldef.getName).toString
    case DataType.Name.COUNTER   => ???
    case DataType.Name.DECIMAL   => row.getDecimal(coldef.getName).toPlainString
    case DataType.Name.DOUBLE    => row.getDouble(coldef.getName).toString
    case DataType.Name.FLOAT     => row.getFloat(coldef.getName).toString
    case DataType.Name.INT       => row.getInt(coldef.getName).toString
    case DataType.Name.TEXT      => wrap(row.getString(coldef.getName))
    case DataType.Name.TIMESTAMP => wrap(row.getTimestamp(coldef.getName).toString)
    case DataType.Name.UUID      => wrap(row.getUUID(coldef.getName).toString)
    case DataType.Name.VARCHAR   => wrap(row.getString(coldef.getName))
    case DataType.Name.VARINT    => row.getVarint(coldef.getName).toString
    case DataType.Name.TIMEUUID  => ???
    case DataType.Name.INET      => wrap(row.getInet(coldef.getName).toString)
    case DataType.Name.DATE      => wrap(row.getDate(coldef.getName).toString)
    case DataType.Name.TIME      => wrap(row.getTime(coldef.getName).toString)
    case DataType.Name.SMALLINT  => row.getShort(coldef.getName).toString
    case DataType.Name.TINYINT   => row.getByte(coldef.getName).toString
    case DataType.Name.DURATION  => ???
    case DataType.Name.LIST      => ???
    case DataType.Name.MAP       => ???
    case DataType.Name.SET       => ???
    case DataType.Name.UDT       => ???
    case DataType.Name.TUPLE     => ???
  }

  /**
    * Given a row, constructs a CQLSH statement ready to use.
    * Not all corner cases are dealt with, but this should be good enough for most use-cases
    *
    * For instance, if you have binary data (blob),
    * it is output as the base64 equivalent string.
    * So you'll have to patch your logs programmatically.
    */
  def rowStringRepr(row: Row, prefix: Option[String] = None): String = {
    val sbk = new StringBuilder
    val sbv = new StringBuilder
    prefix.foreach(sbk.++=)
    val coldDefs = row.getColumnDefinitions.asList()
    val last = coldDefs.get(coldDefs.size() - 1)
    sbk += '('
    sbv += '('
    row.getColumnDefinitions.asList().forEach { coldef =>
      sbk ++= coldef.getName
      sbv ++= genericColumnValue(coldef, row)
      if(coldef ne last) {
        sbk += ','
        sbv += ','
      } else {
        sbk ++= ") VALUES "
        sbv ++= ");"
      }
    }
    sbv.addString(sbk).result()
  }


  def mergeSourcesRec[T](sources: Seq[Source[T,NotUsed]]): Source[T,NotUsed] = {
    if(sources.isEmpty) Source.empty
    else if(sources.length == 1) sources.head
    else {
      val reduced = sources.sliding(2, 2).map { oneOrTwoSources =>
        if (oneOrTwoSources.length == 1) oneOrTwoSources.head
        else oneOrTwoSources.head.merge(oneOrTwoSources.last)
      }.toSeq
      mergeSourcesRec(reduced)
    }
  }
}
