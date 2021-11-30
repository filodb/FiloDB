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
import filodb.query.{QueryError, QueryResponse, QueryResult, TopkCardinalities, TsCardinalities}

/**
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

  private val METRIC_ACTIVE = "active"
  private val METRIC_TOTAL = "total"
  private val NS = "ns_agg"
  private val WS = "ns_agg"

  def scheduleAggregatorJob() : Unit = {
    scheduler.scheduleWithFixedDelay(
      SCHED_INIT_DELAY_TU,
      SCHED_DELAY_TU,
      TIME_UNIT,
      () => executeAggregateQuery())
  }

  private def executeAggregateQuery() : Unit = {
    import filodb.query.exec.TsCardExec.RowData
    dsIterProducer().foreach { dsRef =>
      val res = askTopkBlocking(dsRef)
      res match {
        case qres: QueryResult =>
          qres.result.foreach(_.rows().foreach{ rr =>
            // publish a cardinality metric for each namespace
            val data = RowData.fromRowReader(rr)
            val tags = Map("_ws_" -> WS, "_ns_" -> NS, "ds" -> dsRef.dataset)
            Kamon.gauge(METRIC_ACTIVE).withTags(TagSet.from(tags)).update(data.counts.active.toDouble)
            Kamon.gauge(METRIC_TOTAL).withTags(TagSet.from(tags)).update(data.counts.total.toDouble)
          })
        case qerr: QueryError => ???
      }
    }
  }

  private def askTopkBlocking(dsRef: DatasetRef): QueryResponse = {
    val groupDepth = 1
    val prefix = Nil
    val askExtractor: PartialFunction[Any, QueryResponse] = {
      case qres: QueryResponse => qres
    }
    try {
      Client.actorAsk(
        coordProducer(),
        LogicalPlan2Query(dsRef, TsCardinalities(prefix, groupDepth)),
        FiniteDuration(ASK_TIMEOUT_TU, TIME_UNIT))(askExtractor)
    } catch {
      case e: AskTimeoutException => ???
    }

  }
}
