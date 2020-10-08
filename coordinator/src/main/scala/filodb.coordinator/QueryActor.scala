package filodb.coordinator

import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.{ForkJoinPool, ForkJoinWorkerThread}

import scala.util.control.NonFatal

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.dispatch.{Envelope, UnboundedStablePriorityMailbox}
import com.typesafe.config.Config
import kamon.Kamon
import kamon.instrumentation.executor.ExecutorInstrumentation
import kamon.tag.TagSet
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader

import filodb.coordinator.queryplanner.SingleClusterPlanner
import filodb.core._
import filodb.core.memstore.{FiloSchedulers, MemStore, TermInfo}
import filodb.core.metadata.Schemas
import filodb.core.query.{QueryConfig, QueryContext, QuerySession}
import filodb.core.store.CorruptVectorException
import filodb.query._
import filodb.query.exec.ExecPlan

object QueryCommandPriority extends java.util.Comparator[Envelope] {
  override def compare(o1: Envelope, o2: Envelope): Int = {
    (o1.message, o2.message) match {
      case (q1: QueryCommand, q2: QueryCommand) => q1.submitTime.compareTo(q2.submitTime)
      case (_, _: QueryCommand) => -1 // non-query commands are admin and have higher priority
      case (_: QueryCommand, _) => 1 // non-query commands are admin and have higher priority
      case _ => 0
    }
  }
}

class QueryActorMailbox(settings: ActorSystem.Settings, config: Config)
  extends UnboundedStablePriorityMailbox(QueryCommandPriority)

object QueryActor {
  final case class ThrowException(dataset: DatasetRef)

  def props(memStore: MemStore, dsRef: DatasetRef,
            schemas: Schemas, shardMapFunc: => ShardMapper,
            earliestRawTimestampFn: => Long): Props =
    Props(new QueryActor(memStore, dsRef, schemas,
                         shardMapFunc, earliestRawTimestampFn)).withMailbox("query-actor-mailbox")
}

/**
 * Translates external query API calls into internal ColumnStore calls.
 *
 * The actual reading of data structures and aggregation is performed asynchronously by Observables,
 * so it is probably fine for there to be just one QueryActor per dataset.
 */
final class QueryActor(memStore: MemStore,
                       dsRef: DatasetRef,
                       schemas: Schemas,
                       shardMapFunc: => ShardMapper,
                       earliestRawTimestampFn: => Long) extends BaseActor {
  import QueryActor._
  import client.QueryCommands._
  import filodb.core.memstore.FiloSchedulers._

  val config = context.system.settings.config
  val dsOptions = schemas.part.options

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
  val queryConfig = new QueryConfig(config.getConfig("filodb.query"))
  val queryPlanner = new SingleClusterPlanner(dsRef, schemas, shardMapFunc,
                                              earliestRawTimestampFn, queryConfig, functionalSpreadProvider)
  val queryScheduler = createInstrumentedQueryScheduler()

  private val tags = Map("dataset" -> dsRef.toString)
  private val lpRequests = Kamon.counter("queryactor-logicalPlan-requests").withTags(TagSet.from(tags))
  private val epRequests = Kamon.counter("queryactor-execplan-requests").withTags(TagSet.from(tags))
  private val resultVectors = Kamon.histogram("queryactor-result-num-rvs").withTags(TagSet.from(tags))
  private val queryErrors = Kamon.counter("queryactor-query-errors").withTags(TagSet.from(tags))

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
      override def uncaughtException(t: Thread, e: Throwable): Unit =
        logger.error("Uncaught Exception in Query Scheduler", e)
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

  def execPhysicalPlan2(q: ExecPlan, replyTo: ActorRef): Unit = {
    if (checkTimeout(q.queryContext, replyTo)) {
      epRequests.increment()
      Kamon.currentSpan().tag("query", q.getClass.getSimpleName)
      Kamon.currentSpan().tag("query-id", q.queryContext.queryId)
      val querySession = QuerySession(q.queryContext, queryConfig)
      q.execute(memStore, querySession)(queryScheduler)
        .foreach { res =>
          FiloSchedulers.assertThreadName(QuerySchedName)
          querySession.close()
          replyTo ! res
          res match {
            case QueryResult(_, _, vectors) => resultVectors.record(vectors.length)
            case e: QueryError =>
              queryErrors.increment()
              logger.debug(s"queryId ${q.queryContext.queryId} Normal QueryError returned from query execution: $e")
              e.t match {
                case cve: CorruptVectorException => memStore.analyzeAndLogCorruptPtr(dsRef, cve)
                case t: Throwable =>
              }
          }
        }(queryScheduler).recover { case ex =>
          querySession.close()
          // Unhandled exception in query, should be rare
          logger.error(s"queryId ${q.queryContext.queryId} Unhandled Query Error: ", ex)
          replyTo ! QueryError(q.queryContext.queryId, ex)
        }(queryScheduler)
    }
  }

  private def processLogicalPlan2Query(q: LogicalPlan2Query, replyTo: ActorRef) = {
    if (checkTimeout(q.qContext, replyTo)) {
      // This is for CLI use only. Always prefer clients to materialize logical plan
      lpRequests.increment()
      try {
        val execPlan = queryPlanner.materialize(q.logicalPlan, q.qContext)
        self forward execPlan
      } catch {
        case NonFatal(ex) =>
          if (!ex.isInstanceOf[BadQueryException]) // dont log user errors
            logger.error(s"Exception while materializing logical plan", ex)
          replyTo ! QueryError("unknown", ex)
      }
    }
  }

  private def processExplainPlanQuery(q: ExplainPlan2Query, replyTo: ActorRef): Unit = {
    if (checkTimeout(q.qContext, replyTo)) {
      try {
        val execPlan = queryPlanner.materialize(q.logicalPlan, q.qContext)
        replyTo ! execPlan
      } catch {
        case NonFatal(ex) =>
          if (!ex.isInstanceOf[BadQueryException]) // dont log user errors
            logger.error(s"Exception while materializing logical plan", ex)
          replyTo ! QueryError("unknown", ex)
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

  def checkTimeout(queryContext: QueryContext, replyTo: ActorRef): Boolean = {
    // timeout can occur here if there is a build up in actor mailbox queue and delayed delivery
    val queryTimeElapsed = System.currentTimeMillis() - queryContext.submitTime
    if (queryTimeElapsed >= queryContext.plannerParams.queryTimeoutMillis) {
      replyTo ! QueryError(s"Query timeout, $queryTimeElapsed ms > ${queryContext.plannerParams.queryTimeoutMillis}",
                           QueryTimeoutException(queryTimeElapsed, this.getClass.getName))
      false
    } else true
  }

  def receive: Receive = {
    case q: LogicalPlan2Query      => val replyTo = sender()
                                      processLogicalPlan2Query(q, replyTo)
    case q: ExplainPlan2Query      => val replyTo = sender()
                                      processExplainPlanQuery(q, replyTo)
    case q: ExecPlan              =>  execPhysicalPlan2(q, sender())

    case GetIndexNames(ref, limit, _) =>
      sender() ! memStore.indexNames(ref, limit).map(_._1).toBuffer
    case g: GetIndexValues         => processIndexValues(g, sender())

    case ThrowException(dataset) =>
      logger.warn(s"Throwing exception for dataset $dataset. QueryActor will be killed")
      throw new RuntimeException
  }

}
