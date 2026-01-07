package filodb.coordinator


import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.{ForkJoinPool, ForkJoinWorkerThread}

import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService

import filodb.core.GlobalConfig
import filodb.core.memstore.FiloSchedulers.{FlightIoSchedName, QuerySchedName}
import filodb.core.metrics.FilodbMetrics
import filodb.memory.data.Shutdown

object QueryScheduler extends StrictLogging {

  private val exceptionHandler = new UncaughtExceptionHandler {
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
  val queryScheduler: SchedulerService = createInstrumentedQueryScheduler()

  val flightIoScheduler: SchedulerService = Scheduler.io(FlightIoSchedName, reporter = (ex: Throwable) => {
    logger.error("Uncaught Exception in Flight IO Scheduler", ex)
    ex match {
      case ie: InternalError => Shutdown.haltAndCatchFire(ie)
      case _ => {
        /* Do nothing. */
      }
    }
  })

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

    Scheduler.apply(FilodbMetrics.instrumentExecutor(executor, schedName))
  }

}
