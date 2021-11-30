package filodb.coordinator

import java.util.concurrent.TimeUnit

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Success

import akka.actor.ActorRef
import kamon.Kamon
import kamon.tag.TagSet
import monix.execution.Scheduler.{global => scheduler}

import filodb.coordinator.client.Client
import filodb.coordinator.client.QueryCommands.LogicalPlan2Query
import filodb.core.DatasetRef
import filodb.query.{QueryResult, TsCardinalities}

/**
 * Periodically queries a node for all namespace cardinalities.
 * Kamon gauges are updated with the response data.
 *
 * The intent is to publish a low-cardinality metric such that namespace
 * cardinality queries can be efficiently answered.
 */
case class NamespaceCardinalityPublisher(dsIterProducer: () => Iterator[DatasetRef],
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

  private val futQueue_ = new java.util.concurrent.ConcurrentLinkedQueue[(Future[Any], String)]()

  def schedulePeriodicPublishJob() : Unit = {
    scheduler.scheduleWithFixedDelay(
      SCHED_INIT_DELAY_TU,
      SCHED_DELAY_TU,
      TIME_UNIT,
      () => queryAndSchedulePublish())
  }

  private def publishFutureData() : Unit = {
    import filodb.query.exec.TsCardExec.RowData

    while (true) {
      var futOpt : Option[(Future[Any], String)] = None
      futQueue_.synchronized{
        if (!futQueue_.isEmpty && futQueue_.peek()._1.isCompleted) {
          futOpt = Some(futQueue_.poll())
        }
      }

      if (futOpt.isEmpty) {
        return
      }

      futOpt.get._1.value match {
        case Some(Success(QueryResult(_, _, rv, _, _, _))) =>
          rv.foreach(_.rows().foreach{ rr =>
            // publish a cardinality metric for each namespace
            val data = RowData.fromRowReader(rr)
            val dataset = futOpt.get._2
            val tags = Map("_ws_" -> WS, "_ns_" -> NS, "ds" -> dataset, "prefix" -> data.group)
            Kamon.gauge(METRIC_ACTIVE).withTags(TagSet.from(tags)).update(data.counts.active.toDouble)
            Kamon.gauge(METRIC_TOTAL).withTags(TagSet.from(tags)).update(data.counts.total.toDouble)
          })
        case _ => ???
      }
    }
  }

  private def queryAndSchedulePublish() : Unit = {
    val groupDepth = 1
    val prefix = Nil
    dsIterProducer().foreach { dsRef =>
      val fut = Client.asyncAsk(
        coordProducer(),
        LogicalPlan2Query(dsRef, TsCardinalities(prefix, groupDepth)),
        FiniteDuration(ASK_TIMEOUT_TU, TIME_UNIT))
      futQueue_.add((fut, dsRef.dataset))
    }
    scheduler.scheduleOnce(ASK_TIMEOUT_TU, TIME_UNIT, () => publishFutureData)
  }
}
