package filodb.coordinator

import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.{Executors, ForkJoinPool, ForkJoinWorkerThread}

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

  val flightIoScheduler: SchedulerService = createFlightIoScheduler()

  /**
   * Creates a scheduler for Arrow Flight I/O.
   *
   * On Java 21+ this uses a virtual-thread-per-task executor (zero OS-thread overhead
   * per blocked I/O call).  On Java 11 it falls back to a plain `Scheduler.io` backed
   * by a cached thread pool, which is functionally equivalent for our purposes.
   *
   * Virtual-thread APIs are invoked via reflection so the source compiles on JDK 11
   * without needing a multi-release jar or compiler flags.
   */
  private def createFlightIoScheduler(): SchedulerService = {
    val majorVersion = {
      val v = System.getProperty("java.specification.version", "11")
      if (v.startsWith("1.")) v.split("\\.")(1).toInt else v.toInt
    }
    if (majorVersion >= 21) {
      try {
        // Reflectively equivalent to:
        //   Thread.ofVirtual()
        //     .name(FlightIoSchedName, 0L)
        //     .uncaughtExceptionHandler(exceptionHandler)
        //     .factory()
        // scalastyle:off null
        val ofVirtual  = classOf[Thread].getMethod("ofVirtual").invoke(null)
        val named      = ofVirtual.getClass
                           .getMethod("name", classOf[String], java.lang.Long.TYPE)
                           .invoke(ofVirtual, FlightIoSchedName, java.lang.Long.valueOf(0L))
        val withHandler = named.getClass
                            .getMethod("uncaughtExceptionHandler", classOf[UncaughtExceptionHandler])
                            .invoke(named, exceptionHandler)
        val factory    = withHandler.getClass
                           .getMethod("factory")
                           .invoke(withHandler)
                           .asInstanceOf[java.util.concurrent.ThreadFactory]
        // Reflectively equivalent to: Executors.newThreadPerTaskExecutor(factory)
        val executor   = classOf[Executors]
                           .getMethod("newThreadPerTaskExecutor", classOf[java.util.concurrent.ThreadFactory])
                           .invoke(null, factory)
                           .asInstanceOf[java.util.concurrent.ExecutorService]
        logger.info(s"$FlightIoSchedName: using virtual-thread-per-task executor (Java $majorVersion)")
        Scheduler.apply(executor)
      } catch {
        case e: Exception =>
          logger.warn(s"$FlightIoSchedName: failed to create virtual-thread executor, " +
            s"falling back to Scheduler.io", e)
          Scheduler.io(name = FlightIoSchedName)
      }
    } else {
      logger.info(s"$FlightIoSchedName: using Scheduler.io (Java $majorVersion < 21)")
      Scheduler.io(name = FlightIoSchedName)
    }
  }

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

    val threadFactory = new ForkJoinPool.ForkJoinWorkerThreadFactory {
      def newThread(pool: ForkJoinPool): ForkJoinWorkerThread = {
        val thread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool)
        thread.setDaemon(true)
        thread.setUncaughtExceptionHandler(exceptionHandler)
        thread.setName(QuerySchedName)
        thread
      }
    }
    val executor = new ForkJoinPool(numSchedThreads, threadFactory, exceptionHandler, true)

    Scheduler.apply(FilodbMetrics.instrumentExecutor(executor, QuerySchedName))
  }

}
