package filodb.coordinator

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorRef
import akka.pattern.AskTimeoutException
import java.util.concurrent.TimeUnit
import kamon.Kamon
import kamon.tag.TagSet
import monix.execution.Scheduler.{global => scheduler}

import filodb.coordinator.client.Client
import filodb.coordinator.client.QueryCommands.LogicalPlan2Query
import filodb.core.DatasetRef
import filodb.query.{QueryError, QueryResult, TopkCardinalities}
import filodb.query.QueryResponse

/**
 * TODO(a_theimer): update
 * Periodically queries a node for all namespace cardinalities.
 * Kamon gauges are updated with the response data.
 *
 * The intent is to publish a low-cardinality metric such that namespace
 * cardinality queries can be efficiently answered.
 */
case class ShardMetricAggregator(dsIterProducer: () => Iterator[DatasetRef],
                                 coordProducer: () => ActorRef) {
  // All "_TU" constants quantify units of TIME_UNIT.
  private val TIME_UNIT = TimeUnit.SECONDS
  private val SCHED_INIT_DELAY_TU = 10
  private val SCHED_DELAY_TU = 10
  private val ASK_TIMEOUT_TU = 30

  private val METRIC_NAME = "shard_metric_agg"
  private val NS_VALUE = "metadata"
  private val WS_VALUE = "shard"
  private val K: Int = 250

  // TODO(a_theimer): can this be static in a function? Weird to have here...
  private val ASK_EXTRACTOR: PartialFunction[Any, QueryResponse] = {
    case qres: QueryResponse => qres
  }

  // TODO(a_theimer): just query RocksDB once

  def scheduleAggregatorJob() : Unit = {
    scheduler.scheduleWithFixedDelay(
      SCHED_INIT_DELAY_TU,
      SCHED_DELAY_TU,
      TIME_UNIT,
      () => executeAggregateQuery())
  }

  private def executeAggregateQuery() : Unit = {
    // foreach addInactive / dataset pair...
    Seq(true, false).foreach{ addInactive =>
      dsIterProducer().foreach { dsRef =>  // TODO(a_theimer): combine all datasets
        // get the topk workspace values
        val wsResponse = askTopkBlocking(dsRef, Nil, addInactive)
        val wsValues = new mutable.ArrayBuffer[String]
        wsResponse match {
          case qres: QueryResult => {
            qres.result.foreach(_.rows().foreach{ rr =>
              wsValues.append(rr.getString(0))
            }
            )}
          case qerr: QueryError => ???
        }
        // for each workspace, get the topk namespaces
        wsValues.foreach{ ws =>
          val nsRes = askTopkBlocking(dsRef, Seq(ws), addInactive)
          nsRes match {
            case qres: QueryResult => {
              qres.result.foreach(_.rows().foreach{ rr =>
                // publish a cardinality metric for each namespace
                val ns = rr.getString(0)
                val count = rr.getLong(1)
                val tags = Map("_ws_" -> WS_VALUE, "_ns_" -> NS_VALUE,
                  "ds" -> dsRef.dataset, "ws" -> ws, "ns" -> ns,
                  "addInactive" -> addInactive.toString)
                Kamon.gauge(METRIC_NAME).withTags(TagSet.from(tags)).update(count.toDouble)
              })
            }
            case qerr: QueryError => ???
          }
        }
      }
    }
  }

  private def askTopkBlocking(dsRef: DatasetRef,
                              shardKeyPrefix: Seq[String],
                              addInactive: Boolean): QueryResponse = {
    try {
      Client.actorAsk(
        coordProducer(),
        LogicalPlan2Query(dsRef, TopkCardinalities(shardKeyPrefix, K, addInactive)),
        FiniteDuration(ASK_TIMEOUT_TU, TIME_UNIT))(ASK_EXTRACTOR)
    } catch {
      case e: AskTimeoutException => ???
    }

  }
}
