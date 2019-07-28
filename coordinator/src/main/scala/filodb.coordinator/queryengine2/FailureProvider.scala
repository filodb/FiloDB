package filodb.coordinator.queryengine2

import filodb.core.DatasetRef

/**
  * A provider to get failure ranges. Query engine can use failure ranges while preparing physical
  * plan to reroute or skip failure ranges. Ranges are based on dataset and over all clusters.
  * Provider will filter failure ranges by current cluster and its replicas. Failures which do not
  * belong to current cluster or its replica, will be skipped.
  */
trait FailureProvider {
  def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange]
}

object EmptyFailureProvider extends FailureProvider {
  override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
    Seq[FailureTimeRange]()
  }
}

/**
  * Time range.
  *
  * @param startInMillis epoch time in millis.
  * @param endInMillis   epoch time in millis.
  */
case class TimeRange(startInMillis: Long, endInMillis: Long)

/**
  * Failure details.
  *
  * @param clusterName cluster name.
  * @param datasetRef  Dataset reference for database and dataset.
  * @param timeRange   time range.
  */
case class FailureTimeRange(clusterName: String, datasetRef: DatasetRef, timeRange: TimeRange,
                            isRemote: Boolean)

/**
  * For rerouting queries for failure ranges, Route trait will offer more context in the form of corrective
  * ranges for queries or alternative dispatchers.
  * A local route indicates a non-failure range on local cluster. A remote route indicates a non-failure
  * range on remote cluster.
  */
trait Route

case class LocalRoute(timeRange: Option[TimeRange]) extends Route

case class RemoteRoute(timeRange: Option[TimeRange]) extends Route