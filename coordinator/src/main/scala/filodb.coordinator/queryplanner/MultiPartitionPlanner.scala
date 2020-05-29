package filodb.coordinator.queryplanner

import com.typesafe.config.ConfigFactory

import filodb.coordinator.queryplanner.LogicalPlanUtils._
import filodb.core.metadata.Dataset
import filodb.core.query.{PromQlQueryParams, QueryContext}
import filodb.query.{BinaryJoin, LabelValues, LogicalPlan, SeriesKeysByFilters}
import filodb.query.exec._

case class PartitionAssignment(partitionName: String, endPoint: String, timeRange: TimeRange)

trait PartitionLocationProvider {

  def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment]
  def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment]
}

class MultiPartitionPlanner(partitionLocationProvider: PartitionLocationProvider,
                            localPlanner: QueryPlanner,
                            localPartition: String,
                            dataset: Dataset) extends QueryPlanner {

  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val tsdbQueryParams = qContext.origQueryParams

    if(!tsdbQueryParams.isInstanceOf[PromQlQueryParams] || // We don't know the promql issued (unusual)
      (tsdbQueryParams.isInstanceOf[PromQlQueryParams]
        && !tsdbQueryParams.asInstanceOf[PromQlQueryParams].processRouting)) // Query was part of routing
      localPlanner.materialize(logicalPlan, qContext)

    else logicalPlan match {
      case lp: BinaryJoin          => materializeBinaryJoin(lp, qContext)
      case lp: LabelValues         => materializeLabelValues(lp, qContext)
      case lp: SeriesKeysByFilters => materializeSeriesKeysFilters(lp, qContext)
      case _                       => materializeSimpleQuery(logicalPlan, qContext)

    }
  }

  private def getRoutingKeys(logicalPlan: LogicalPlan) = dataset.options.nonMetricShardColumns
    .map(x => (x, LogicalPlan.getLabelValueFromLogicalPlan(logicalPlan, x)))

  private def generateRemoteExec(queryContext: QueryContext, startMs: Long,
                                 endMs: Long, endPoint: String): PromQlExec = {
    val queryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val promQlParams = PromQlQueryParams(ConfigFactory.parseString(s"""endpoint = "${endPoint}""""),
      queryParams.promQl, startMs / 1000, queryParams.stepSecs, endMs / 1000, queryParams.spread, processFailure = true,
      processRouting = false)
    PromQlExec(queryContext, InProcessPlanDispatcher, dataset.ref, promQlParams)
  }

  def materializeSimpleQuery(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val routingKeys = getRoutingKeys(logicalPlan)
    if (routingKeys.forall(_._2.isEmpty)) localPlanner.materialize(logicalPlan, qContext)
    else {
      val routingKeyMap = routingKeys.map(x => (x._1, x._2.get.head)).toMap
      val offsetMs = LogicalPlanUtils.getOffsetMillis(logicalPlan)
      val periodicSeriesTime = getPeriodicSeriesTimeFromLogicalPlan(logicalPlan)
      val periodicSeriesTimeWithOffset = TimeRange(periodicSeriesTime.startMs - offsetMs,
        periodicSeriesTime.endMs - offsetMs)
      val lookBackTimeMs = getLookBackMillis(logicalPlan)
      // Time at which raw data would be retrieved which is used to get partition assignments.
      // It should have time with offset and lookback as we need raw data at time including offset and lookback.
      val queryTimeRange = TimeRange(periodicSeriesTimeWithOffset.startMs - lookBackTimeMs,
        periodicSeriesTimeWithOffset.endMs)

      val partitions = partitionLocationProvider.getPartitions(routingKeyMap, queryTimeRange).
        sortBy(_.timeRange.startMs)
      val execPlans = partitions.map { p =>
        val startMs = p.timeRange.startMs + offsetMs + lookBackTimeMs
        val endMs = p.timeRange.endMs + offsetMs
        if (p.partitionName.equals(localPartition)) localPlanner.materialize(
          copyWithUpdatedTimeRange(logicalPlan, TimeRange(startMs, endMs)), qContext)
        else generateRemoteExec(qContext, startMs, endMs, p.endPoint)
      }
      if (execPlans.size == 1) execPlans.head
      else StitchRvsExec(qContext,
        InProcessPlanDispatcher,
        execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlExec]))
      // ^^ Stitch RemoteExec plan results with local using InProcessPlanDispatcher
      // Sort to move RemoteExec in end as it does not have schema
    }
  }


  def materializeBinaryJoin(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val routingKeys = getRoutingKeys(logicalPlan)
    if (routingKeys.forall(_._2.isEmpty)) localPlanner.materialize(logicalPlan, qContext)
    else {
      val offsetMillis = LogicalPlanUtils.getOffsetMillis(logicalPlan)
      val periodicSeriesTime = getPeriodicSeriesTimeFromLogicalPlan(logicalPlan)
      val periodicSeriesTimeWithOffset = TimeRange(periodicSeriesTime.startMs - offsetMillis,
        periodicSeriesTime.endMs - offsetMillis)
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
        if (partitionName.equals(localPartition)) localPlanner.materialize(logicalPlan, qContext)
        else generateRemoteExec(qContext, periodicSeriesTime.startMs / 1000, periodicSeriesTime.endMs / 1000,
          partitions.head.endPoint)
      }
      else throw new UnsupportedOperationException("Binary Join across multiple partitions not supported")
    }
  }

  def materializeSeriesKeysFilters(lp: SeriesKeysByFilters, qContext: QueryContext): ExecPlan = {
   val partitions = partitionLocationProvider.getAuthorizedPartitions(TimeRange(lp.startMs, lp.endMs))
    val execPlans = partitions.map { p =>
      if (p.partitionName.equals(localPartition))
        localPlanner.materialize(lp.copy(startMs = p.timeRange.startMs, endMs = p.timeRange.endMs), qContext)
      else generateRemoteExec(qContext, p.timeRange.startMs, p.timeRange.endMs, p.endPoint)
    }
    if (execPlans.size == 1) execPlans.head
    else PartKeysDistConcatExec(qContext,
      InProcessPlanDispatcher,
      execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlExec]))
  }

  def materializeLabelValues(lp: LabelValues, qContext: QueryContext): ExecPlan = {
    val partitions = partitionLocationProvider.getAuthorizedPartitions(TimeRange(lp.startMs, lp.endMs))
    val execPlans = partitions.map { p =>
      if (p.partitionName.equals(localPartition))
        localPlanner.materialize(lp.copy(startMs = p.timeRange.startMs, endMs = p.timeRange.endMs), qContext)
      else generateRemoteExec(qContext, p.timeRange.startMs, p.timeRange.endMs, p.endPoint)
    }
    if (execPlans.size == 1) execPlans.head
    else LabelValuesDistConcatExec(qContext,
      InProcessPlanDispatcher,
      execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlExec]))
  }

}
