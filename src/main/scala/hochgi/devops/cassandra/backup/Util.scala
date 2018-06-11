package hochgi.devops.cassandra.backup

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent.{ExecutionContext, Future, Promise}

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
}
