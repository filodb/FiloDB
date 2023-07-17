package filodb.coordinator


import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.{ForkJoinPool, ForkJoinWorkerThread}

import com.typesafe.scalalogging.StrictLogging
import kamon.instrumentation.executor.ExecutorInstrumentation
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService

import filodb.core.GlobalConfig
import filodb.core.memstore.FiloSchedulers.QuerySchedName
import filodb.memory.data.Shutdown

object QueryScheduler extends StrictLogging {


  val queryScheduler = createInstrumentedQueryScheduler()

  /**
   * Instrumentation adds following metrics on the Query Scheduler
   *
   * # Counter
   * executor_tasks_submitted_total{type="ThreadPoolExecutor",name="query-sched-prometheus"}
   * # Counter
   * executor_tasks_completed_total{type="ThreadPoolExecutor",name="query-sched-prometheus"}
   * # Histogram
   * executor_threads_active{type="ThreadPoolExecutor",name="query-sched-prometheus"}
   * # Histogram
   * executor_queue_size_count{type="ThreadPoolExecutor",name="query-sched-prometheus"}
   *
   */
  private def createInstrumentedQueryScheduler(): SchedulerService = {
    val numSchedThreads = Math.ceil(GlobalConfig.systemConfig.getDouble("filodb.query.threads-factor")
      * sys.runtime.availableProcessors).toInt
    val schedName = s"$QuerySchedName"
    val exceptionHandler = new UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = {
        logger.error("Uncaught Exception in Query Scheduler", e)
        e match {
          case ie: InternalError => Shutdown.haltAndCatchFire(ie)
          case _ => {
            /* Do nothing. */
          }
        }
      }
    }
    val threadFactory = new ForkJoinPool.ForkJoinWorkerThreadFactory {
      def newThread(pool: ForkJoinPool): ForkJoinWorkerThread = {
        val thread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool)
        thread.setDaemon(true)
        thread.setUncaughtExceptionHandler(exceptionHandler)
        thread.setName(s"$schedName-${thread.getPoolIndex}")
        thread
      }
    }
    val executor = new ForkJoinPool(numSchedThreads, threadFactory, exceptionHandler, true)

    Scheduler.apply(ExecutorInstrumentation.instrument(executor, schedName))
  }

}
