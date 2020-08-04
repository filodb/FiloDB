package filodb.coordinator

import scala.collection.mutable.{HashMap => mHashMap, Queue => mQueue}
import scala.concurrent.Future
import scala.util.control.NonFatal

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.dispatch.{Envelope, UnboundedStablePriorityMailbox}
import com.typesafe.config.Config
import kamon.Kamon
import kamon.tag.TagSet
import monix.execution.Scheduler
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


// A class to manage a queue and a set of currently executing queries, allowing no more than concurrentQueries
// at a time.  When the query and execplans finishes execution, then they are removed.  This has multiple benefits
// including limiting memory usage and tracking running queries.
// concurrentQueries defines the number of current queries where all execPlans belonging to the same queryID
// is counted as a single query.
// A queue keeps track of queries for which there is no space.  It is not expected for there to be more than
// one query in the queue belonging to the same queryID, this is because only high-level ExecPlans spawn more
// low-level ones, and spawned ones arrive in the Actor mailbox and will go striaght to executing if the parent
// plan is already executing.
// A single threaded scheduler is created for each query (all ExecPlans with the same query ID).  This reduces
// thread contention significantly and reduces task switching overhead, boosting query throughput especially
// when the tasks are small.
class QueryScheduler(concurrentQueries: Int, maxQueueLen: Int, tags: Map[String, String]) {
  //  Currently executing query IDs -> number of execPlans in that query ID.  Used to track when all plans finish
  //  so that cleanup can be done and more queries can be popped off the stack.
  val currentIDs = new mHashMap[String, Int]().withDefaultValue(0)
  //  Queue of queries waiting to be added to currently executing ones
  val queue = new mQueue[(ExecPlan, ActorRef)]
  //  Mapping of queryID to scheduler.  Each queryID gets its own scheduler.
  val idToSched = new mHashMap[String, Scheduler]()
  //  Queue of schedulers available for future queries
  val schedulers = new mQueue[Scheduler]()

  // Immediate ExecPlan execution, direct on current pool, no Queue, vs from queue
  private val countExecImmed = Kamon.counter("query-scheduler-execute-immediate").withTags(TagSet.from(tags))
  private val countExecQueue = Kamon.counter("query-scheduler-execute-queue").withTags(TagSet.from(tags))
  private val countAddQueue  = Kamon.counter("query-scheduler-add-queue").withTags(TagSet.from(tags))
  private val countQueueFull = Kamon.counter("query-scheduler-reject-queue-full").withTags(TagSet.from(tags))
  private val queueLen = Kamon.gauge("query-scheduler-queue-length").withTags(TagSet.from(tags))

  // Given a plan and replyTo, executes the plan if there is room or it is a currently executing query.
  // If there isn't room, add it to the queue.  If the queue has no room, return false; otherwise true.
  def execOrWait(plan: ExecPlan,
                 replyTo: ActorRef,
                 newPlanFunc: (ExecPlan, ActorRef, Scheduler) => Future[Unit]): Boolean = synchronized {
    val id = plan.queryContext.queryId
    if ((currentIDs contains id) || currentIDs.size < concurrentQueries) {
      countExecImmed.increment()
      execute(plan, replyTo, newPlanFunc)
      true
    } else if (queueSize < maxQueueLen) {
      countAddQueue.increment()
      queue.enqueue((plan, replyTo))
      true
    } else {
      countQueueFull.increment()
      false
    }
  }

  // Length of queue - not synchronized, so data might not be consistent.
  def queueSize: Int = queue.length

  // Pop next item(s?) off top of queue so long as there is room to execute another query.
  def tryPop(newPlanFunc: (ExecPlan, ActorRef, Scheduler) => Future[Unit]): Unit = synchronized {
    if (currentIDs.size < concurrentQueries && queue.nonEmpty) {
      val (nextPlan, replyTo) = queue.dequeue
      countExecQueue.increment()
      execute(nextPlan, replyTo, newPlanFunc)
    }
  }

  // Executes newPlanFunc, updating currentIDs/plans as needed
  // The closure is called in the current thread, but the callback when Future/plan is completed will not be.
  // Note that an appropriate single threaded scheduler is pulled off the stack and used.
  private def execute(newPlan: ExecPlan,
                      replyTo: ActorRef,
                      newPlanFunc: (ExecPlan, ActorRef, Scheduler) => Future[Unit]): Unit = {
    val queryId = newPlan.queryContext.queryId

    // Add plan to data structures and obtain scheduler
    val sched = synchronized {
      currentIDs(queryId) += 1
      idToSched.getOrElseUpdate(queryId, {
        if (schedulers.nonEmpty) schedulers.dequeue
        else Scheduler.singleThread(s"query-sched-${currentIDs.size}")
      })
    }

    val planFut = newPlanFunc(newPlan, replyTo, sched)

    // Remove plan on Future complete.  If a slot opens up, recursively call myself to open up a new slot
    planFut.onComplete {
      case _ => synchronized {
                  currentIDs(queryId) -= 1
                  if (currentIDs(queryId) == 0) {
                    currentIDs.remove(queryId)
                    schedulers.enqueue(idToSched.remove(queryId).get)
                    queueLen.update(queueSize)
                    tryPop(newPlanFunc)
                  }
                }
    }(sched)
  }

  // Currently active query IDs.  Not synchronized, so data might not be consistent.
  def currentQueryIDs: Set[String] = currentIDs.keySet.toSet
}

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

  private val tags = Map("dataset" -> dsRef.toString)
  private val lpRequests = Kamon.counter("queryactor-logicalPlan-requests").withTags(TagSet.from(tags))
  private val epRequests = Kamon.counter("queryactor-execplan-requests").withTags(TagSet.from(tags))
  private val resultVectors = Kamon.histogram("queryactor-result-num-rvs").withTags(TagSet.from(tags))
  private val queryErrors = Kamon.counter("queryactor-query-errors").withTags(TagSet.from(tags))

  val numSchedThreads = Math.ceil(queryConfig.threadsFactor * sys.runtime.availableProcessors).toInt
  val sched = new QueryScheduler(numSchedThreads, queryConfig.maxQueueLen, tags)

  def execPhysicalPlan2(q: ExecPlan, replyTo: ActorRef, sched: Scheduler): Future[Unit] = {
    if (checkTimeout(q.queryContext, replyTo)) {
      epRequests.increment()
      Kamon.currentSpan().tag("query", q.getClass.getSimpleName)
      Kamon.currentSpan().tag("query-id", q.queryContext.queryId)
      val querySession = QuerySession(q.queryContext, queryConfig)
      q.execute(memStore, querySession)(sched)
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
        }(sched).recover { case ex =>
          querySession.close()
          // Unhandled exception in query, should be rare
          logger.error(s"queryId ${q.queryContext.queryId} Unhandled Query Error: ", ex)
          replyTo ! QueryError(q.queryContext.queryId, ex)
        }(sched)
    } else {
      Future.successful(())
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
    if (queryTimeElapsed >= queryContext.queryTimeoutMillis) {
      replyTo ! QueryError(s"Query timeout, $queryTimeElapsed ms > ${queryContext.queryTimeoutMillis}",
                           QueryTimeoutException(queryTimeElapsed, this.getClass.getName))
      false
    } else true
  }

  def receive: Receive = {
    case q: LogicalPlan2Query      => val replyTo = sender()
                                      processLogicalPlan2Query(q, replyTo)
    case q: ExplainPlan2Query      => val replyTo = sender()
                                      processExplainPlanQuery(q, replyTo)
    case q: ExecPlan               => if (!sched.execOrWait(q, sender(), execPhysicalPlan2))
                                        sender() ! QueryQueueFull

    case GetIndexNames(ref, limit, _) =>
      sender() ! memStore.indexNames(ref, limit).map(_._1).toBuffer
    case g: GetIndexValues         => processIndexValues(g, sender())

    case ThrowException(dataset) =>
      logger.warn(s"Throwing exception for dataset $dataset. QueryActor will be killed")
      throw new RuntimeException
  }

}
