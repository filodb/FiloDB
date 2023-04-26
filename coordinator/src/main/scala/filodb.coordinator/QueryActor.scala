package filodb.coordinator

import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.{ForkJoinPool, ForkJoinWorkerThread}
import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success}
import scala.util.control.NonFatal
import akka.actor.{ActorRef, Props}
import akka.pattern.AskTimeoutException
import kamon.Kamon
import kamon.instrumentation.executor.ExecutorInstrumentation
import kamon.tag.TagSet
import monix.catnap.CircuitBreaker
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.exceptions.ExecutionRejectedException
import monix.execution.schedulers.SchedulerService
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader
import filodb.coordinator.queryplanner.SingleClusterPlanner
import filodb.core._
import filodb.core.memstore.{FiloSchedulers, TermInfo, TimeSeriesStore}
import filodb.core.memstore.ratelimit.CardinalityRecord
import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.query.{QueryConfig, QueryContext, QueryLimitException, QuerySession, QueryStats, SerializedRangeVector}
import filodb.core.store.CorruptVectorException
import filodb.memory.data.Shutdown
import filodb.query._
import filodb.query.exec.{ExecPlan, InProcessPlanDispatcher, PlanDispatcher}

object QueryActor {
  final case class ThrowException(dataset: DatasetRef)

  def props(memStore: TimeSeriesStore, dataset: Dataset,
            schemas: Schemas, shardMapFunc: => ShardMapper,
            earliestRawTimestampFn: => Long): Props =
    Props(new QueryActor(memStore, dataset, schemas,
                         shardMapFunc, earliestRawTimestampFn))
}

/**
 * Translates external query API calls into internal ColumnStore calls.
 *
 * The actual reading of data structures and aggregation is performed asynchronously by Observables,
 * so it is probably fine for there to be just one QueryActor per dataset.
 */
final class QueryActor(memStore: TimeSeriesStore,
                       dataset: Dataset,
                       schemas: Schemas,
                       shardMapFunc: => ShardMapper,
                       earliestRawTimestampFn: => Long) extends BaseActor {
  import QueryActor._
  import client.QueryCommands._
  import filodb.core.memstore.FiloSchedulers._

  val config = context.system.settings.config
  val dsOptions = schemas.part.options
  val dsRef = dataset.ref

  var filodbSpreadMap = new collection.mutable.HashMap[collection.Map[String, String], Int]
  val applicationShardKeyNames = dsOptions.nonMetricShardColumns
  val defaultSpread = config.getInt("filodb.spread-default")

  implicit val spreadOverrideReader: ValueReader[SpreadAssignment] = ValueReader.relative { spreadAssignmentConfig =>
    SpreadAssignment(
    shardKeysMap = dsOptions.nonMetricShardColumns.map(x =>
      (x, spreadAssignmentConfig.getString(x))).toMap[String, String],
      spread = spreadAssignmentConfig.getInt("_spread_")
    )
  }
  val spreadAssignment : List[SpreadAssignment]= config.as[List[SpreadAssignment]]("filodb.spread-assignment")
  spreadAssignment.foreach{ x => filodbSpreadMap.put(x.shardKeysMap, x.spread)}

  val spreadFunc = QueryContext.simpleMapSpreadFunc(applicationShardKeyNames, filodbSpreadMap, defaultSpread)
  val functionalSpreadProvider = FunctionalSpreadProvider(spreadFunc)

  logger.info(s"Starting QueryActor and QueryEngine for ds=$dsRef schemas=$schemas")
  val queryConfig = QueryConfig(config.getConfig("filodb.query"))
  val queryPlanner = new SingleClusterPlanner(dataset, schemas, shardMapFunc,
                                              earliestRawTimestampFn, queryConfig, "raw",
                                              functionalSpreadProvider)
  val queryScheduler = createInstrumentedQueryScheduler()

  private val tags = Map("dataset" -> dsRef.toString)
  private val lpRequests = Kamon.counter("queryactor-logicalPlan-requests").withTags(TagSet.from(tags))
  private val epRequests = Kamon.counter("queryactor-execplan-requests").withTags(TagSet.from(tags))
  private val queryErrors = Kamon.counter("queryactor-query-errors").withTags(TagSet.from(tags))
  private val uncaughtExceptions = Kamon.counter("queryactor-uncaught-exceptions").withTags(TagSet.from(tags))
  private val numRejectedPlans = Kamon.counter("circuit-breaker-num-rejected-plans").withTags(TagSet.from(tags))

  private val circuitBreakerEnabled = config.getBoolean("filodb.query.circuit-breaker.enabled")
  private val circuitBreakerNumFailures = config.getInt("filodb.query.circuit-breaker.open-when-num-failures")
  private val circuitBreakerResetTimeout = config.as[FiniteDuration]("filodb.query.circuit-breaker.reset-timeout")
  private val circuitBreakerExpBackOffFactor = config.getDouble("filodb.query.circuit-breaker.exp-backoff-factor")
  private val circuitBreakerMaxTimeout = config.as[FiniteDuration]("filodb.query.circuit-breaker.max-reset-timeout")
  private val circuitBreaker = CircuitBreaker[Task].unsafe(
    circuitBreakerNumFailures, circuitBreakerResetTimeout, circuitBreakerExpBackOffFactor, circuitBreakerMaxTimeout,
    onRejected = Task.eval(numRejectedPlans.increment()),
    onClosed = Task.eval(logger.info("Query CircuitBreaker closed")),
    onHalfOpen = Task.eval(logger.info("Query CircuitBreaker is now half-open")),
    onOpen = Task.eval(logger.info("Query CircuitBreaker is now open"))
  )

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
    val numSchedThreads = Math.ceil(config.getDouble("filodb.query.threads-factor")
                                      * sys.runtime.availableProcessors).toInt
    val schedName = s"$QuerySchedName-$dsRef"
    val exceptionHandler = new UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = {
        logger.error("Uncaught Exception in Query Scheduler", e)
        uncaughtExceptions.increment()
        e match {
          case ie: InternalError => Shutdown.haltAndCatchFire(ie)
          case _ => { /* Do nothing. */ }
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
    val executor = new ForkJoinPool( numSchedThreads, threadFactory, exceptionHandler, true)

    Scheduler.apply(ExecutorInstrumentation.instrument(executor, schedName))
  }

  // scalastyle:off method.length
  def execPhysicalPlan2(q: ExecPlan, replyTo: ActorRef): Unit = {
    logger.debug(s"Received request to run query $q")
    if (checkTimeoutBeforeQueryExec(q.queryContext, replyTo)) {
      epRequests.increment()
      val queryExecuteSpan = Kamon.spanBuilder(s"query-actor-exec-plan-execute-${q.getClass.getSimpleName}")
        .asChildOf(Kamon.currentSpan())
        .start()
      // Dont finish span since we finish it asynchronously when response is received
      Kamon.runWithSpan(queryExecuteSpan, false) {
        queryExecuteSpan.tag("query", q.getClass.getSimpleName)
        queryExecuteSpan.tag("query-id", q.queryContext.queryId)
        val querySession = QuerySession(q.queryContext,
                                        queryConfig,
                                        streamingDispatch = PlanDispatcher.streamingResultsEnabled,
                                        catchMultipleLockSetErrors = true)
        queryExecuteSpan.mark("query-actor-received-execute-start")

        val execTask = if (querySession.streamingDispatch) {
          q.executeStreaming(memStore, querySession)(queryScheduler)
            .onErrorHandle { t =>
              StreamQueryError(q.queryContext.queryId, querySession.queryStats, t)
            }.map { resp =>
              // Avoiding the assert when the InProcessPlanDispatcher is used. As it runs
              // the query on the current/Actor thread instead of the scheduler
              if (!q.dispatcher.isInstanceOf[InProcessPlanDispatcher]) {
                FiloSchedulers.assertThreadName(QuerySchedName)
              }
              replyTo ! resp
              resp match {
                case e: StreamQueryError =>
                  logQueryErrors(e.t)
                  queryErrors.increment()
                  queryExecuteSpan.fail(e.t.getMessage)
                  // rethrow so circuit beaker can block queries when there is a surge of such exceptions
                  if (e.t.isInstanceOf[QueryTimeoutException] || e.t.isInstanceOf[AskTimeoutException]) throw e.t
                case _ =>
              }
            }.guarantee(Task.eval {
              SerializedRangeVector.queryCpuTime.increment(querySession.queryStats.totalCpuNanos)
              querySession.close()
              queryExecuteSpan.finish()
            }).completedL
        } else { // TODO remove this block when query streaming is enabled and working well
          q.execute(memStore, querySession)(queryScheduler)
            .onErrorHandle { t =>
              QueryError(q.queryContext.queryId, querySession.queryStats, t)
            }.map { res =>
              // Avoiding the assert when the InProcessPlanDispatcher is used. As it runs
              // the query on the current/Actor thread instead of the scheduler
              if (!q.dispatcher.isInstanceOf[InProcessPlanDispatcher]) {
                FiloSchedulers.assertThreadName(QuerySchedName)
              }
              replyTo ! res
              res match {
                case e: QueryError =>
                  logQueryErrors(e.t)
                  queryErrors.increment()
                  queryExecuteSpan.fail(e.t.getMessage)
                  // rethrow so circuit beaker can block queries when there is a surge of such exceptions
                  if (e.t.isInstanceOf[QueryTimeoutException] || e.t.isInstanceOf[AskTimeoutException]) throw e.t
                case _ =>
              }
            }.guarantee(Task.eval {
              SerializedRangeVector.queryCpuTime.increment(querySession.queryStats.totalCpuNanos)
              queryExecuteSpan.finish()
              querySession.close()
            })
        }

        val execTask2 = if (circuitBreakerEnabled) {
          circuitBreaker.protect(execTask)
            .onErrorRecover {
              case t: ExecutionRejectedException =>
                logQueryErrors(t)
                if (querySession.streamingDispatch)
                  replyTo ! StreamQueryError(q.queryContext.queryId, querySession.queryStats, t)
                else
                  replyTo ! QueryError(q.queryContext.queryId, querySession.queryStats, t)
              case _ => // all other errors are already handled
            }
        } else execTask
        execTask2.runToFuture(queryScheduler)
      }
    }

    def logQueryErrors(t: Throwable): Unit = {
      // error logging
      t match {
        case _: BadQueryException => // dont log user errors
        case _: AskTimeoutException => // dont log ask timeouts. useless - let it simply flow up
        case _: QueryTimeoutException | _: ExecutionRejectedException => // log just message, no need for stacktrace
          logger.error(s"Query Error ${t.getClass.getSimpleName} queryId=${q.queryContext.queryId} " +
            s"${q.queryContext.origQueryParams} ${t.getMessage}")
        case _: QueryLimitException =>
          logger.warn(s"Query Limit Breached " +
            s"${q.queryContext.origQueryParams} ${t.getMessage}")
        case e: Throwable =>
          logger.error(s"Query Error queryId=${q.queryContext.queryId} " +
            s"${q.queryContext.origQueryParams}", e)
      }
      // debug logging
      t match {
        case cve: CorruptVectorException => memStore.analyzeAndLogCorruptPtr(dsRef, cve)
        case t: Throwable =>
      }
    }
  }

  private def processLogicalPlan2Query(q: LogicalPlan2Query, replyTo: ActorRef): Unit = {
    if (checkTimeoutBeforeQueryExec(q.qContext, replyTo)) {
      // This is for CLI use only. Always prefer clients to materialize logical plan
      lpRequests.increment()
      try {
        val execPlan = queryPlanner.materialize(q.logicalPlan, q.qContext)
        if (PlanDispatcher.streamingResultsEnabled) {
          val res = queryPlanner.dispatchStreamingExecPlan(execPlan, Kamon.currentSpan())(queryScheduler, 30.seconds)
          queryengine.Utils.streamToFatQueryResponse(q.qContext, res).runToFuture(queryScheduler).onComplete {
            case Success(resp) => replyTo ! resp
            case Failure(e) => replyTo ! QueryError(q.qContext.queryId, QueryStats(), e)
          }(queryScheduler)
        } else {
          self forward execPlan
        }
      } catch {
        case NonFatal(ex) =>
          if (!ex.isInstanceOf[BadQueryException]) // dont log user errors
            logger.error(s"Exception while materializing logical plan", ex)
          replyTo ! QueryError("unknown", QueryStats(), ex)
      }
    }
  }



  private def processExplainPlanQuery(q: ExplainPlan2Query, replyTo: ActorRef): Unit = {
    if (checkTimeoutBeforeQueryExec(q.qContext, replyTo)) {
      try {
        val execPlan = queryPlanner.materialize(q.logicalPlan, q.qContext)
        replyTo ! execPlan
      } catch {
        case NonFatal(ex) =>
          if (!ex.isInstanceOf[BadQueryException]) // dont log user errors
            logger.error(s"Exception while materializing logical plan", ex)
          replyTo ! QueryError("unknown", QueryStats(), ex)
      }
    }
  }

  private def processIndexValues(g: GetIndexValues, originator: ActorRef): Unit = {
    val localShards = memStore.activeShards(g.dataset)
    if (localShards contains g.shard) {
      originator ! memStore.labelValues(g.dataset, g.shard, g.indexName, g.limit)
                           .map { case TermInfo(term, freq) => (term.toString, freq) }
    } else {
      val destNode = shardMapFunc.coordForShard(g.shard)
      if (destNode != ActorRef.noSender) { destNode.forward(g) }
      else                               { originator ! BadArgument(s"Shard ${g.shard} is not assigned") }
    }
  }

  private def execTopkCardinalityQuery(q: GetTopkCardinality, sender: ActorRef): Unit = {
    implicit val ord = new Ordering[CardinalityRecord]() {
      override def compare(x: CardinalityRecord, y: CardinalityRecord): Int = {
        if (q.addInactive) x.tsCount - y.tsCount
          else x.activeTsCount - y.activeTsCount
        }
      }.reverse
    try {
      val cards = memStore.scanTsCardinalities(q.dataset, q.shards, q.shardKeyPrefix, q.depth)
      val heap = mutable.PriorityQueue[CardinalityRecord]()
      cards.foreach { card =>
          heap.enqueue(card)
          if (heap.size > q.k) heap.dequeue()
        }
      sender ! heap.toSeq
    } catch { case e: Exception =>
      sender ! QueryError(s"Error Occurred", QueryStats(), e)
    }
  }

  def checkTimeoutBeforeQueryExec(queryContext: QueryContext, replyTo: ActorRef): Boolean = {
    val ex = queryContext.checkQueryTimeout(this.getClass.getName, false)
    ex match {
      case Some(qte) => replyTo ! QueryError(queryContext.queryId, QueryStats(), qte)
                        false
      case None =>      true
    }
  }

  def receive: Receive = {
    case q: LogicalPlan2Query      => val replyTo = sender()
                                      processLogicalPlan2Query(q, replyTo)
    case q: ExplainPlan2Query      => val replyTo = sender()
                                      processExplainPlanQuery(q, replyTo)
    case q: ExecPlan              =>  execPhysicalPlan2(q, sender())
    case q: GetTopkCardinality     => execTopkCardinalityQuery(q, sender())

    case GetIndexNames(ref, limit, _) =>
      sender() ! memStore.indexNames(ref, limit).map(_._1).toBuffer
    case g: GetIndexValues         => processIndexValues(g, sender())

    case ThrowException(dataset) =>
      logger.warn(s"Throwing exception for dataset $dataset. QueryActor will be killed")
      throw new RuntimeException
    case msg                     =>
      logger.error(s"Unhandled message $msg in QueryActor")
  }

}
