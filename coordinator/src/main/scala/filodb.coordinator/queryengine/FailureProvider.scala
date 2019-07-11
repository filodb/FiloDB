package filodb.coordinator.queryengine

import filodb.query.LogicalPlan
import filodb.query.exec.PlanDispatcher

/**
  * A provider to get failure ranges. Query engine can use failure ranges while preparing physical
  * plan to reroute or skip failure ranges. Ranges are based on dataset and over all clusters.
  */
trait FailureProvider {
  def getFailures(dataset: String, queryTimeRange: TimeRange): Seq[FailureTimeRange]
}

/**
  * Time range.
  * @param start epoch time in seconds.
  * @param end epoch time in seconds.
  */
case class TimeRange(start: Long, end: Long)

/**
  * Failure details.
  * @param pod cluster name.
  * @param dataset dataset name
  * @param timeRange time range.
  * @param dispatcher dispatcher implementation for given pod.
  */
case class FailureTimeRange(pod: String, dataset: String, timeRange: TimeRange, dispatcher: PlanDispatcher)

/**
  * For rerouting queries for failure ranges, Route trait will offer more context in the form of corrective
  * ranges for queries or alternative dispatchers.
  * A local route indicates a non-failure range on local cluster. A remote route indicates a non-failure
  * range on remote cluster.
  */
trait Route
case class LocalRoute(tr: Option[TimeRange]) extends Route
case class RemoteRoute(tr: Option[TimeRange], dispatcher: PlanDispatcher) extends Route

/**
  * Planner for routing based on failure ranges for a given LogicalPlan.
  */
trait RoutingPlanner {
  def plan(lp: LogicalPlan, failure: Seq[FailureTimeRange]): Seq[Route]
}
