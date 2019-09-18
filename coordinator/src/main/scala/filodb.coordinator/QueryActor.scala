package filodb.coordinator

import java.util.concurrent.atomic.AtomicLong

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.dispatch.{Envelope, UnboundedStablePriorityMailbox}
import com.typesafe.config.Config
import kamon.Kamon
import monix.execution.Scheduler
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader
import scala.util.control.NonFatal

import filodb.coordinator.queryengine2.{EmptyFailureProvider, QueryEngine}
import filodb.core._
import filodb.core.memstore.{FiloSchedulers, MemStore, TermInfo}
import filodb.core.metadata.Dataset
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
  private val nextId = new AtomicLong()
  def nextQueryId: Long = nextId.getAndIncrement

  final case class ThrowException(dataset: DatasetRef)

  def props(memStore: MemStore, dataset: Dataset, shardMapFunc: => ShardMapper): Props =
    Props(new QueryActor(memStore, dataset, shardMapFunc)).withMailbox("query-actor-mailbox")
}

/**
 * Translates external query API calls into internal ColumnStore calls.
 *
 * The actual reading of data structures and aggregation is performed asynchronously by Observables,
 * so it is probably fine for there to be just one QueryActor per dataset.
 */
final class QueryActor(memStore: MemStore,
                       dataset: Dataset,
                       shardMapFunc: => ShardMapper) extends BaseActor {
  import QueryActor._
  import client.QueryCommands._
  import filodb.core.memstore.FiloSchedulers._

  val config = context.system.settings.config

  var filodbSpreadMap = new collection.mutable.HashMap[collection.Map[String, String], Int]
  val applicationShardKeyNames = dataset.options.nonMetricShardColumns
  val defaultSpread = config.getInt("filodb.spread-default")

  implicit val spreadOverrideReader: ValueReader[SpreadAssignment] = ValueReader.relative { spreadAssignmentConfig =>
    SpreadAssignment(
    shardKeysMap = dataset.options.nonMetricShardColumns.map(x =>
      (x, spreadAssignmentConfig.getString(x))).toMap[String, String],
      spread = spreadAssignmentConfig.getInt("_spread_")
    )
  }
  val spreadAssignment : List[SpreadAssignment]= config.as[List[SpreadAssignment]]("filodb.spread-assignment")
  spreadAssignment.foreach{ x => filodbSpreadMap.put(x.shardKeysMap, x.spread)}

  val spreadFunc = QueryOptions.simpleMapSpreadFunc(applicationShardKeyNames, filodbSpreadMap, defaultSpread)
  val functionalSpreadProvider = FunctionalSpreadProvider(spreadFunc)

  val queryEngine2 = new QueryEngine(dataset, shardMapFunc,
    EmptyFailureProvider, functionalSpreadProvider)
  val queryConfig = new QueryConfig(config.getConfig("filodb.query"))
  val numSchedThreads = Math.ceil(config.getDouble("filodb.query.threads-factor") * sys.runtime.availableProcessors)
  val queryScheduler = Scheduler.fixedPool(s"$QuerySchedName-${dataset.ref}", numSchedThreads.toInt)

  private val tags = Map("dataset" -> dataset.ref.toString)
  private val lpRequests = Kamon.counter("queryactor-logicalPlan-requests").refine(tags)
  private val epRequests = Kamon.counter("queryactor-execplan-requests").refine(tags)
  private val resultVectors = Kamon.histogram("queryactor-result-num-rvs").refine(tags)
  private val queryErrors = Kamon.counter("queryactor-query-errors").refine(tags)

  def execPhysicalPlan2(q: ExecPlan, replyTo: ActorRef): Unit = {
    epRequests.increment
    Kamon.currentSpan().tag("query", q.getClass.getSimpleName)
    val span = Kamon.buildSpan(s"execplan2-${q.getClass.getSimpleName}")
      .withTag("query-id", q.id)
      .start()
    q.execute(memStore, dataset, queryConfig)(queryScheduler, queryConfig.askTimeout)
     .foreach { res =>
       FiloSchedulers.assertThreadName(QuerySchedName)
       replyTo ! res
       res match {
         case QueryResult(_, _, vectors) => resultVectors.record(vectors.length)
         case e: QueryError =>
           queryErrors.increment
           logger.debug(s"queryId ${q.id} Normal QueryError returned from query execution: $e")
           e.t match {
             case cve: CorruptVectorException => memStore.analyzeAndLogCorruptPtr(dataset.ref, cve)
             case t: Throwable =>
           }
       }
       span.finish()
     }(queryScheduler).recover { case ex =>
       // Unhandled exception in query, should be rare
       logger.error(s"queryId ${q.id} Unhandled Query Error: ", ex)
       replyTo ! QueryError(q.id, ex)
       span.finish()
     }(queryScheduler)
  }

  private def getSpreadProvider(queryOptions: QueryOptions): SpreadProvider = {
    return queryOptions.spreadProvider.getOrElse(functionalSpreadProvider)
  }

  private def processLogicalPlan2Query(q: LogicalPlan2Query, replyTo: ActorRef) = {
    // This is for CLI use only. Always prefer clients to materialize logical plan
    lpRequests.increment
    try {
      val execPlan = queryEngine2.materialize(q.logicalPlan, q.queryOptions, q.tsdbQueryParams)
      self forward execPlan
    } catch {
      case NonFatal(ex) =>
        if (!ex.isInstanceOf[BadQueryException]) // dont log user errors
          logger.error(s"Exception while materializing logical plan", ex)
        replyTo ! QueryError("unknown", ex)
    }
  }

  private def processExplainPlanQuery(q: ExplainPlan2Query, replyTo: ActorRef) = {
    try {
      val execPlan = queryEngine2.materialize(q.logicalPlan, q.queryOptions, q.tsdbQueryParams)
      replyTo ! execPlan
    } catch {
      case NonFatal(ex) =>
        if (!ex.isInstanceOf[BadQueryException]) // dont log user errors
          logger.error(s"Exception while materializing logical plan", ex)
        replyTo ! QueryError("unknown", ex)
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