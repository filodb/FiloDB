package filodb.coordinator

import java.util.concurrent.atomic.AtomicLong

import scala.util.control.NonFatal

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.dispatch.{Envelope, UnboundedStablePriorityMailbox}
import com.typesafe.config.Config
import kamon.Kamon

import filodb.coordinator.queryengine2.QueryEngine
import filodb.core._
import filodb.core.memstore.{MemStore, TermInfo}
import filodb.core.metadata.Dataset
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

  implicit val scheduler = monix.execution.Scheduler(context.dispatcher)
  val config = context.system.settings.config

  val queryEngine2 = new QueryEngine(dataset, shardMapFunc)
  val queryConfig = new QueryConfig(config.getConfig("filodb.query"))

  private val tags = Map("dataset" -> dataset.ref.toString)
  private val lpRequests = Kamon.counter("queryactor-logicalPlan-requests").refine(tags)
  private val epRequests = Kamon.counter("queryactor-execplan-requests").refine(tags)
  private val resultVectors = Kamon.histogram("queryactor-result-num-rvs").refine(tags)
  private val queryErrors = Kamon.counter("queryactor-query-errors").refine(tags)

  def execPhysicalPlan2(q: ExecPlan, replyTo: ActorRef): Unit = {
    epRequests.increment
    Kamon.currentSpan().tag("query", q.getClass.getSimpleName)
    val span = Kamon.buildSpan(s"execplan2-${q.getClass.getSimpleName}").start()
    implicit val _ = queryConfig.askTimeout
    q.execute(memStore, dataset, queryConfig)
     .foreach { res =>
       replyTo ! res
       res match {
         case QueryResult(_, _, vectors) => resultVectors.record(vectors.length)
         case e: QueryError =>
           queryErrors.increment
           logger.debug(s"queryId ${q.id} Normal QueryError returned from query execution: $e")
       }
       span.finish()
     }.recover { case ex =>
       // Unhandled exception in query, should be rare
       logger.error(s"queryId ${q.id} Unhandled Query Error: ", ex)
       replyTo ! QueryError(q.id, ex)
       span.finish()
     }
  }

  private def processLogicalPlan2Query(q: LogicalPlan2Query, replyTo: ActorRef) = {
    // This is for CLI use only. Always prefer clients to materialize logical plan
    lpRequests.increment
    try {
      val execPlan = queryEngine2.materialize(q.logicalPlan, q.queryOptions)
      self forward execPlan
    } catch {
      case NonFatal(ex) =>
        logger.error(s"Exception while materializing logical plan", ex)
        replyTo ! QueryError("unknown", ex)
    }
  }

  private def processIndexValues(g: GetIndexValues, originator: ActorRef): Unit = {
    val localShards = memStore.activeShards(g.dataset)
    if (localShards contains g.shard) {
      originator ! memStore.indexValues(g.dataset, g.shard, g.indexName, g.limit)
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

    case q: ExecPlan              => execPhysicalPlan2(q, sender())

    case GetIndexNames(ref, limit, _) =>
      sender() ! memStore.indexNames(ref).take(limit).map(_._1).toBuffer
    case g: GetIndexValues         => processIndexValues(g, sender())

    case ThrowException(dataset) =>
      logger.warn(s"Throwing exception for dataset $dataset. QueryActor will be killed")
      throw new RuntimeException
  }

}