package filodb.coordinator.queryplanner

import com.typesafe.config.ConfigFactory

import filodb.coordinator.queryplanner.LogicalPlanUtils._
import filodb.core.DatasetRef
import filodb.core.metadata.DatasetOptions
import filodb.core.query.{PromQlQueryParams, QueryContext}
import filodb.query.{BinaryJoin, LogicalPlan, SeriesKeysByFilters}
import filodb.query.exec.{ExecPlan, InProcessPlanDispatcher, PartKeysDistConcatExec, PromQlExec, StitchRvsExec}


case class PartitionAssignment(partitionName: String, endPoint: String, timeRange: TimeRange)

trait PartitionLocationProvider{

  def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment]
  def getAuthorizedPartitions(timeRange: TimeRange): Seq[PartitionAssignment]
}

class MultiPartitionPlanner(partitionLocationProvider: PartitionLocationProvider,
                            localPlanner: QueryPlanner,
                            localPartition: String,
                            datasetOptions: DatasetOptions,
                            datasetRef: DatasetRef) extends QueryPlanner {

  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val tsdbQueryParams = qContext.origQueryParams

    if(!tsdbQueryParams.isInstanceOf[PromQlQueryParams] || // We don't know the promql issued (unusual)
      (tsdbQueryParams.isInstanceOf[PromQlQueryParams]
        && !tsdbQueryParams.asInstanceOf[PromQlQueryParams].processRouting) || // Query was part of routing
      !hasSingleTimeRange(logicalPlan)) // Sub queries have different time ranges (unusual)
      localPlanner.materialize(logicalPlan, qContext)

    else logicalPlan match {
      case lp: BinaryJoin          => materializeBinaryJoin(lp, qContext)
     // case lp: LabelValues         => materializeLabelValues(lp, qContext)
      case lp: SeriesKeysByFilters => materializeSeriesKeysFilters(lp, qContext)
      case _                       => materializeSimpleQuery(logicalPlan, qContext)

    }
  }

  private def getRoutingKeys(logicalPlan: LogicalPlan) = datasetOptions.nonMetricShardColumns
    .map(x => (x, LogicalPlanUtils.getLabelValueFromLogicalPlan(logicalPlan, x)))

  def materializeSimpleQuery(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val routingKeys = getRoutingKeys(logicalPlan)
    if (routingKeys.forall(_._2.isEmpty)) localPlanner.materialize(logicalPlan, qContext)
    else {
      val routingKeyMap = routingKeys.map(x => (x._1, x._2.get.head)).toMap
      lazy val offsetMillis = LogicalPlanUtils.getOffsetMillis(logicalPlan)
      lazy val periodicSeriesTime = getPeriodicSeriesTimeFromLogicalPlan(logicalPlan)
      lazy val periodicSeriesTimeWithOffset = TimeRange(periodicSeriesTime.startMs - offsetMillis,
        periodicSeriesTime.endMs - offsetMillis)
      lazy val lookBackTimeMs = getLookBackMillis(logicalPlan)
      // Time at which raw data would be retrieved which is used to get partition assignments.
      // It should have time with offset and lookback as we need raw data at time including offset and lookback.
      lazy val queryTimeRange = TimeRange(periodicSeriesTimeWithOffset.startMs - lookBackTimeMs,
        periodicSeriesTimeWithOffset.endMs)
      lazy val partitions = partitionLocationProvider.getPartitions(routingKeyMap, queryTimeRange).
        sortBy(_.timeRange.startMs)
      val offsetMs = LogicalPlanUtils.getOffsetMillis(logicalPlan)
      val execPlans = partitions.map { p =>
        if (p.equals(localPartition)) localPlanner.materialize(logicalPlan, qContext)
        else {
          val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
          val promQlParams = PromQlQueryParams(ConfigFactory.parseString(s"""buddy.http.endpoint ="${p.endPoint}""""),
            queryParams.promQl, (p.timeRange.startMs + offsetMs + lookBackTimeMs) / 1000, queryParams.stepSecs,
            (p.timeRange.endMs + offsetMs) / 1000, queryParams.spread, processFailure = true, processRouting = false)
          PromQlExec(qContext, InProcessPlanDispatcher, datasetRef, promQlParams)
        }
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
      lazy val offsetMillis = LogicalPlanUtils.getOffsetMillis(logicalPlan)
      lazy val periodicSeriesTime = getPeriodicSeriesTimeFromLogicalPlan(logicalPlan)
      lazy val periodicSeriesTimeWithOffset = TimeRange(periodicSeriesTime.startMs - offsetMillis,
        periodicSeriesTime.endMs - offsetMillis)
      lazy val lookBackTimeMs = getLookBackMillis(logicalPlan)
      // Time at which raw data would be retrieved which is used to get partition assignments.
      // It should have time with offset and lookback as we need raw data at time including offset and lookback.
      lazy val queryTimeRange = TimeRange(periodicSeriesTimeWithOffset.startMs - lookBackTimeMs,
        periodicSeriesTimeWithOffset.endMs)
      val routingKeyMap = routingKeys.map(x => (x._1, x._2.get.head)).toMap
      lazy val partitions = partitionLocationProvider.getPartitions(routingKeyMap, queryTimeRange).
        sortBy(_.timeRange.startMs)
      if (partitions.forall(_.partitionName.equals((localPartition)))) localPlanner.materialize(logicalPlan, qContext)
      else throw new UnsupportedOperationException("Binary Join across multiple partitions not supported")
    }
  }
//
//  def materializeLabelValues(lp: LogicalPlan, qContext: QueryContext): ExecPlan = {
//  partitionLocationProvider.getAuthorizedPartitions(T)
//  }

  def materializeSeriesKeysFilters(lp: SeriesKeysByFilters, qContext: QueryContext): ExecPlan = {
   val partitions = partitionLocationProvider.getAuthorizedPartitions(TimeRange(lp.startMs, lp.endMs))
    val execPlans = partitions.map { p =>
      if (p.equals(localPartition)) localPlanner.materialize(lp, qContext)
      else {
        val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
        val promQlParams = PromQlQueryParams(ConfigFactory.parseString(s"""buddy.http.endpoint ="${p.endPoint}""""),
          queryParams.promQl, p.timeRange.startMs / 1000, queryParams.stepSecs,
          p.timeRange.endMs / 1000, queryParams.spread, processFailure = true, processRouting = false)
        PromQlExec(qContext, InProcessPlanDispatcher, datasetRef, promQlParams)
      }
    }
    if (execPlans.size == 1) execPlans.head
    else PartKeysDistConcatExec(qContext,
      InProcessPlanDispatcher,
      execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlExec]))

  }

}
