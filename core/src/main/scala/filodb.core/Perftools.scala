package filodb.core

import kamon.trace.{TraceContext, Tracer}
import scala.concurrent.Future

/**
 * Helpers for performance measurement and tracing.
 */
object Perftools {
  def timeMillis(f: => Unit): Long = {
    val start = System.currentTimeMillis
    f
    System.currentTimeMillis - start
  }

  /**
   * Starts a new Kamon tracer segment for sync code.  Note that if you use this within futures
   * or for-comprehensions that run in different threads, you need the kamon-scala module so that the proper
   * traceContext is passed amongst threads.
   */
  def subtrace[T](name: String,
                  category: String,
                  ctx: Option[TraceContext] = None,
                  library: String = "filodb_core")
                 (code: => T): T =
    ctx.getOrElse(Tracer.currentContext).withNewSegment(name, category, library)(code)

  /**
   * Starts a new Kamon tracer segment for async code.  Note that if you use this within futures
   * or for-comprehensions that run in different threads, you need the kamon-scala module so that the proper
   * traceContext is passed amongst threads.
   */
  def asyncSubtrace[T](name: String,
                       category: String,
                       ctx: Option[TraceContext] = None,
                       library: String = "filodb_core")
                      (code: => Future[T]): Future[T] =
    ctx.getOrElse(Tracer.currentContext).withNewAsyncSegment(name, category, library)(code)
}