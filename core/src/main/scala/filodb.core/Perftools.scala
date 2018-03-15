package filodb.core

import scala.concurrent.{ExecutionContext, Future}

import kamon.Kamon
import monix.eval.Task

/**
 * Helpers for performance measurement and tracing.
 */
object Perftools {
  def timeMillis(f: => Unit): Long = {
    val start = System.currentTimeMillis
    f
    System.currentTimeMillis - start
  }

  def withTrace[A](source: Task[A], traceName: String): Task[A] =
    Task.defer {
      val span = Kamon.buildSpan(traceName).start()
      source.doOnFinish(_ => Task.eval(span.finish()))
    }

  /**
   * Starts a new Kamon tracer segment for sync code.  Note that if you use this within futures
   * or for-comprehensions that run in different threads, you need the kamon-scala module so that the proper
   * traceContext is passed amongst threads.
   */
  def subtrace[T](name: String,
                  category: String,
                  library: String = "filodb_core")
                 (code: => T): T = {
      val span = Kamon.buildSpan(s"$library.$category.$name").start()
      try { code } finally { span.finish() }
    }

  /**
   * Starts a new Kamon tracer segment for async code.  Note that if you use this within futures
   * or for-comprehensions that run in different threads, you need the kamon-scala module so that the proper
   * traceContext is passed amongst threads.
   */
  def asyncSubtrace[T](name: String,
                       category: String,
                       library: String = "filodb_core")
                      (code: => Future[T])
                      (implicit ec: ExecutionContext): Future[T] = {
    val span = Kamon.buildSpan(s"$library.$category.$name").start()
    code.onComplete { case _ => span.finish() }
    code
  }
}