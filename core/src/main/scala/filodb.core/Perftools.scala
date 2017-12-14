package filodb.core

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import kamon.Kamon
import kamon.metric.instrument.Gauge
import kamon.trace.{TraceContext, Tracer}
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
      val ctx = Kamon.tracer.newContext(traceName)
      source.doOnFinish(_ => Task.eval(ctx.finish()))
    }

  /**
   * Shortcut to create a Kamon Gauge that polls every pollingInterval by calling gaugeFunc.
   * Polling gauges are good to use for tracking values which would be too expensive to update every time they change.
   * @param name the unique name of the Gauge metric
   * @param pollingInterval the interval at which gaugeFunc will be called to obtain the current value
   * @param tags a Map with tags for the metric, such as dataset or shard
   * @param gaugeFunc a function to return a Long for the current value, called at every pollingInterval
   */
  def pollingGauge(name: String,
                   pollingInterval: FiniteDuration,
                   tags: Map[String, String] = Map.empty)(gaugeFunc: => Long): Gauge = {
    val collector = new Gauge.CurrentValueCollector { def currentValue: Long = gaugeFunc }
    Kamon.metrics.registerGauge(name, collector, tags, refreshInterval = Some(pollingInterval))
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