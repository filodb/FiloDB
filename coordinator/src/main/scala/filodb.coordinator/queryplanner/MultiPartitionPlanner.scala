package filodb.coordinator.queryplanner

import filodb.coordinator.queryplanner.LogicalPlanUtils._
import filodb.core.metadata.Dataset
import filodb.core.query.{PromQlQueryParams, QueryContext}
import filodb.query.{BinaryJoin, LabelValues, LogicalPlan, RawSeriesLikePlan, SeriesKeysByFilters}
import filodb.query.exec._

case class PartitionAssignment(partitionName: String, endPoint: String, timeRange: TimeRange)

trait PartitionLocationProvider {

  def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment]
  def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment]
}

class MultiPartitionPlanner(partitionLocationProvider: PartitionLocationProvider,
                            localPartitionPlanner: QueryPlanner,
                            localPartitionName: String,
                            dataset: Dataset) extends QueryPlanner {

  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val tsdbQueryParams = qContext.origQueryParams

    if(!tsdbQueryParams.isInstanceOf[PromQlQueryParams] || // We don't know the promql issued (unusual)
      (tsdbQueryParams.isInstanceOf[PromQlQueryParams]
        && !tsdbQueryParams.asInstanceOf[PromQlQueryParams].processMultiPartition)) // Query was part of routing
      localPartitionPlanner.materialize(logicalPlan, qContext)

    else logicalPlan match {
      case lp: BinaryJoin          => materializeBinaryJoin(lp, qContext)
      case lp: LabelValues         => materializeLabelValues(lp, qContext)
      case lp: SeriesKeysByFilters => materializeSeriesKeysFilters(lp, qContext)
      case _                       => materializeSimpleQuery(logicalPlan, qContext)

    }
  }

  private def getRoutingKeys(logicalPlan: LogicalPlan) = dataset.options.nonMetricShardColumns
    .map(x => (x, LogicalPlan.getLabelValueFromLogicalPlan(logicalPlan, x)))

  private def generateRemoteExecParams(queryParams: PromQlQueryParams, endPoint: String, startMs: Long, endMs: Long) = {
    PromQlQueryParams(queryParams.promQl, startMs / 1000, queryParams.stepSecs, endMs / 1000, queryParams.spread,
      Option(endPoint), queryParams.remoteQueryPath, queryParams.httpRequestTimeoutMs)
  }

  def materializeSimpleQuery(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val routingKeys = getRoutingKeys(logicalPlan)
    if (routingKeys.forall(_._2.isEmpty)) localPartitionPlanner.materialize(logicalPlan, qContext)
    else {
      val routingKeyMap = routingKeys.map(x => (x._1, x._2.get.head)).toMap
      val offsetMs = LogicalPlanUtils.getOffsetMillis(logicalPlan)
      val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
      val periodicSeriesTimeWithOffset = TimeRange((queryParams.startSecs * 1000) - offsetMs,
        (queryParams.endSecs * 1000) - offsetMs)
      val lookBackMs = getLookBackMillis(logicalPlan)
      val stepMs = queryParams.stepSecs * 1000
      // Time at which raw data would be retrieved which is used to get partition assignments.
      // It should have time with offset and lookback as we need raw data at time including offset and lookback.
      val queryTimeRange = TimeRange(periodicSeriesTimeWithOffset.startMs - lookBackMs,
        periodicSeriesTimeWithOffset.endMs)

      val partitions = partitionLocationProvider.getPartitions(routingKeyMap, queryTimeRange).
        sortBy(_.timeRange.startMs)
      var prevPartitionStart = periodicSeriesTimeWithOffset.startMs
      val execPlans = partitions.zipWithIndex.map { case (p, i) =>
        // First partition should start from query start time
        val startMs = if (i == 0) queryParams.startSecs * 1000
                      else {
                        // Lookback not supported across partitions
                        val numStepsInPrevPartition = (p.timeRange.startMs - prevPartitionStart + lookBackMs) / stepMs
                        val lastPartitionInstant = prevPartitionStart + numStepsInPrevPartition * stepMs
                        lastPartitionInstant + stepMs
                      }
        prevPartitionStart = startMs
        val endMs = p.timeRange.endMs + offsetMs
        if (p.partitionName.equals(localPartitionName))
          localPartitionPlanner.materialize(
            copyLogicalPlanWithUpdatedTimeRange(logicalPlan, TimeRange(startMs, endMs)), qContext)
        else {
          PromQlRemoteExec(qContext, InProcessPlanDispatcher, dataset.ref,
            generateRemoteExecParams(queryParams, p.endPoint, startMs, endMs), logicalPlan.isInstanceOf[RawSeriesLikePlan])
        }
      }
      if (execPlans.size == 1) execPlans.head
      else StitchRvsExec(qContext, InProcessPlanDispatcher, execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlRemoteExec]))
      // ^^ Stitch RemoteExec plan results with local using InProcessPlanDispatcher
      // Sort to move RemoteExec in end as it does not have schema
    }
  }

  def materializeBinaryJoin(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val routingKeys = getRoutingKeys(logicalPlan)
    if (routingKeys.forall(_._2.isEmpty)) localPartitionPlanner.materialize(logicalPlan, qContext)
    else {
      val offsetMillis = LogicalPlanUtils.getOffsetMillis(logicalPlan)
      val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
      val periodicSeriesTimeWithOffset = TimeRange((queryParams.startSecs * 1000) - offsetMillis,
        (queryParams.endSecs * 1000) - offsetMillis)
      val lookBackTimeMs = getLookBackMillis(logicalPlan)
      // Time at which raw data would be retrieved which is used to get partition assignments.
      // It should have time with offset and lookback as we need raw data at time including offset and lookback.
      val queryTimeRange = TimeRange(periodicSeriesTimeWithOffset.startMs - lookBackTimeMs,
        periodicSeriesTimeWithOffset.endMs)
      // Lhs & Rhs in Binary Join can have different values
      val routingKeyMap = routingKeys.flatMap(x => x._2.get.map(y => (x._1, y))).toMap
      val partitions = partitionLocationProvider.getPartitions(routingKeyMap, queryTimeRange).
        sortBy(_.timeRange.startMs)
      val partitionName = partitions.head.partitionName

      // Binary Join supported only fro single partition now
      if (partitions.forall(_.partitionName.equals((partitionName)))) {
        if (partitionName.equals(localPartitionName)) localPartitionPlanner.materialize(logicalPlan, qContext)
        else PromQlRemoteExec(qContext, InProcessPlanDispatcher, dataset.ref,
          generateRemoteExecParams(queryParams, partitions.head.endPoint, queryParams.startSecs * 1000, queryParams.endSecs * 1000))
      }
      else throw new UnsupportedOperationException("Binary Join across multiple partitions not supported")
    }
  }

  def materializeSeriesKeysFilters(lp: SeriesKeysByFilters, qContext: QueryContext): ExecPlan = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val partitions = partitionLocationProvider.getAuthorizedPartitions(TimeRange(queryParams.startSecs * 1000, queryParams.endSecs * 1000))
    val execPlans = partitions.map { p =>
      if (p.partitionName.equals(localPartitionName))
        localPartitionPlanner.materialize(lp.copy(startMs = p.timeRange.startMs, endMs = p.timeRange.endMs), qContext)
      else PromSeriesMatchMetadataExec(qContext, InProcessPlanDispatcher, dataset.ref,
        generateRemoteExecParams(queryParams, p.endPoint, p.timeRange.startMs, p.timeRange.endMs))
    }
    if (execPlans.size == 1) execPlans.head
    else PartKeysDistConcatExec(qContext, InProcessPlanDispatcher,
      execPlans.sortWith((x, y) => !x.isInstanceOf[PromSeriesMatchMetadataExec]))
  }

  def materializeLabelValues(lp: LabelValues, qContext: QueryContext): ExecPlan = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val partitions = partitionLocationProvider.getAuthorizedPartitions(TimeRange(queryParams.startSecs * 1000, queryParams.endSecs * 1000))
    val execPlans = partitions.map { p =>
      if (p.partitionName.equals(localPartitionName))
        localPartitionPlanner.materialize(lp.copy(startMs = p.timeRange.startMs, endMs = p.timeRange.endMs), qContext)
      else PromLabelValuesRemoteExec(qContext, InProcessPlanDispatcher, dataset.ref,
        generateRemoteExecParams(queryParams, p.endPoint, p.timeRange.startMs, p.timeRange.endMs),
        lp.labelNames, lp.labelConstraints)
    }
    if (execPlans.size == 1) execPlans.head
    else LabelValuesDistConcatExec(qContext, InProcessPlanDispatcher,
      execPlans.sortWith((x, y) => !x.isInstanceOf[PromLabelValuesRemoteExec]))
  }

}
