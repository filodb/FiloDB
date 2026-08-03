package filodb.core.metrics

import java.util.concurrent.ExecutorService

import scala.concurrent.duration.DurationInt

import com.typesafe.scalalogging.StrictLogging
import monix.execution.CancelableFuture

/**
 * Instrumented wrapper for ExecutorService that tracks metrics
 */
private class InstrumentedExecutorService(underlying: ExecutorService,
                                          executorName: String,
                                          metrics: FilodbMetrics) extends ExecutorService with StrictLogging {

  import java.util.concurrent._
  import scala.jdk.CollectionConverters._

  // Metrics
  private val baseAttributes = Map("name" -> executorName, "type" -> underlying.getClass.getSimpleName)

  private val tasksSubmitted = metrics.counter("executor_tasks_submitted", isBytes = false, None, baseAttributes)
  private val tasksCompleted = metrics.counter("executor_tasks_completed", isBytes = false, None, baseAttributes)
  private val queueGauge = metrics.gauge("executor_queue_size", isBytes = false, None, baseAttributes)
  private val activeGauge = metrics.gauge("executor_threads_active", isBytes = false, None, baseAttributes)
  private val latencyHist = metrics.histogram("executor_task_wait_latency", Some(TimeUnit.NANOSECONDS), baseAttributes)

  private val scheduler = monix.execution.Scheduler.global
  private val fut = underlying match {
    case tpe: ThreadPoolExecutor =>
      monix.reactive.Observable.interval(metrics.otelConfig.exportIntervalSeconds.seconds).foreach { _ =>
        queueGauge.update(tpe.getQueue.size().toDouble)
        activeGauge.update(tpe.getActiveCount.toDouble)
      }(scheduler)

    case fjp: ForkJoinPool =>
      monix.reactive.Observable.interval(metrics.otelConfig.exportIntervalSeconds.seconds).foreach { _ =>
        queueGauge.update(fjp.getQueuedSubmissionCount.toDouble)
        activeGauge.update(fjp.getActiveThreadCount.toDouble)
      }(scheduler)

    case _ =>
      logger.warn(s"ExecutorService of type ${underlying.getClass.getName} is not instrumented " +
        s"for queue size and active threads")
      CancelableFuture.successful(())
  }

  // Wrap Runnable to track completion and latency
  private def wrapRunnable(r: Runnable): Runnable = {
    val submitTime = System.nanoTime()
    tasksSubmitted.increment()

    () => {
      try {
        val waitLatency = System.nanoTime() - submitTime
        latencyHist.record(waitLatency)
        r.run()
      } finally {
        tasksCompleted.increment()
      }
    }
  }

  // Wrap Callable to track completion and latency
  private def wrapCallable[T](c: Callable[T]): Callable[T] = {
    val submitTime = System.nanoTime()
    tasksSubmitted.increment()

    () => {
      try {
        val waitLatency = System.nanoTime() - submitTime
        latencyHist.record(waitLatency)
        c.call()
      } finally {
        tasksCompleted.increment()
      }
    }
  }

  override def execute(command: Runnable): Unit =
    underlying.execute(wrapRunnable(command))

  override def submit(task: Runnable): Future[_] =
    underlying.submit(wrapRunnable(task))

  override def submit[T](task: Runnable, result: T): Future[T] =
    underlying.submit(wrapRunnable(task), result)

  override def submit[T](task: Callable[T]): Future[T] =
    underlying.submit(wrapCallable(task))

  override def invokeAll[T](tasks: java.util.Collection[_ <: Callable[T]]): java.util.List[Future[T]] = {
    val wrappedTasks = new java.util.ArrayList[Callable[T]](tasks.size())
    tasks.asScala.foreach(task => wrappedTasks.add(wrapCallable(task)))
    underlying.invokeAll(wrappedTasks)
  }

  override def invokeAll[T](
                             tasks: java.util.Collection[_ <: Callable[T]],
                             timeout: Long,
                             unit: TimeUnit
                           ): java.util.List[Future[T]] = {
    val wrappedTasks = new java.util.ArrayList[Callable[T]](tasks.size())
    tasks.asScala.foreach(task => wrappedTasks.add(wrapCallable(task)))
    underlying.invokeAll(wrappedTasks, timeout, unit)
  }

  override def invokeAny[T](tasks: java.util.Collection[_ <: Callable[T]]): T = {
    val wrappedTasks = new java.util.ArrayList[Callable[T]](tasks.size())
    tasks.asScala.foreach(task => wrappedTasks.add(wrapCallable(task)))
    underlying.invokeAny(wrappedTasks)
  }

  override def invokeAny[T](tasks: java.util.Collection[_ <: Callable[T]],
                            timeout: Long,
                            unit: TimeUnit): T = {
    val wrappedTasks = new java.util.ArrayList[Callable[T]](tasks.size())
    tasks.asScala.foreach(task => wrappedTasks.add(wrapCallable(task)))
    underlying.invokeAny(wrappedTasks, timeout, unit)
  }

  override def shutdown(): Unit = {
    fut.cancel()
    underlying.shutdown()
  }

  override def shutdownNow(): java.util.List[Runnable] = {
    fut.cancel()
    underlying.shutdownNow()
  }

  override def isShutdown: Boolean = {
    underlying.isShutdown
  }

  override def isTerminated: Boolean = underlying.isTerminated

  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean =
    underlying.awaitTermination(timeout, unit)
}

