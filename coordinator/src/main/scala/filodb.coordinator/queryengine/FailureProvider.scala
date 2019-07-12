package filodb.coordinator.queryengine

import filodb.core.DatasetRef
import filodb.query.LogicalPlan
import filodb.query.exec.PlanDispatcher

/**
  * A provider to get failure ranges. Query engine can use failure ranges while preparing physical
  * plan to reroute or skip failure ranges. Ranges are based on dataset and over all clusters.
  * Provider will filter failure ranges by current cluster and its replicas. Failures which do not
  * belong to current cluster or its replica, will be skipped.
  */
trait FailureProvider {
  def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange]
}

/**
  * Time range.
  * @param startInMillis epoch time in millis.
  * @param endInMillis epoch time in millis.
  */
case class TimeRange(startInMillis: Long, endInMillis: Long)

/**
  * Failure details.
  * @param clusterName cluster name.
  * @param datasetRef Dataset reference for database and dataset.
  * @param timeRange time range.
  * @param dispatcher dispatcher implementation for given cluster.
  */
case class FailureTimeRange(clusterName: String, datasetRef: DatasetRef, timeRange: TimeRange,
                            dispatcher: Option[PlanDispatcher])

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
