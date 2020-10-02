package filodb.coordinator.queryplanner

import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.queryplanner.LogicalPlanUtils._
import filodb.core.metadata.Dataset
import filodb.core.query.{PromQlQueryParams, QueryConfig, QueryContext}
import filodb.query.{BinaryJoin, LabelValues, LogicalPlan, SeriesKeysByFilters}
import filodb.query.exec._

case class PartitionAssignment(partitionName: String, endPoint: String, timeRange: TimeRange)

trait PartitionLocationProvider {

  def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment]
  def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment]
}

class MultiPartitionPlanner(partitionLocationProvider: PartitionLocationProvider,
                            localPartitionPlanner: QueryPlanner,
                            localPartitionName: String,
                            dataset: Dataset,
                            queryConfig: QueryConfig) extends QueryPlanner with StrictLogging {

  import net.ceedubs.ficus.Ficus._

  val remoteHttpTimeoutMs: Long =
    queryConfig.routingConfig.config.as[Option[Long]]("remote.http.timeout").getOrElse(60000)

  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val tsdbQueryParams = qContext.origQueryParams

    if(!tsdbQueryParams.isInstanceOf[PromQlQueryParams] || // We don't know the promql issued (unusual)
      (tsdbQueryParams.isInstanceOf[PromQlQueryParams]
        && !qContext.plannerParam.processMultiPartition)) // Query was part of routing
      localPartitionPlanner.materialize(logicalPlan, qContext)

    else logicalPlan match {
      case lp: BinaryJoin          => materializeBinaryJoin(lp, qContext)
      case lp: LabelValues         => materializeLabelValues(lp, qContext)
      case lp: SeriesKeysByFilters => materializeSeriesKeysFilters(lp, qContext)
      case _                       => materializeSimpleQuery(logicalPlan, qContext)

    }
  }

  private def getRoutingKeys(logicalPlan: LogicalPlan) = {
    val columnFilterGroup = LogicalPlan.getColumnFilterGroup(logicalPlan)
    dataset.options.nonMetricShardColumns
      .map(x => (x, LogicalPlan.getColumnValues(columnFilterGroup, x)))
  }

  private def generateRemoteExecParams(queryContext: QueryContext, startMs: Long, endMs: Long) = {
    val queryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryContext.copy(origQueryParams = queryParams.copy(startSecs = startMs/1000, endSecs = endMs / 1000), plannerParam
      = queryContext.plannerParam.copy(processMultiPartition = false))
  }

  /**
    *
    * @param routingKeys Non Metric ShardColumns of dataset and value in logicalPlan
    * @param queryParams PromQlQueryParams having query details
    * @param logicalPlan Logical plan
    */
  private def partitionUtil(routingKeys: Seq[(String, Set[String])], queryParams: PromQlQueryParams,
                            logicalPlan: LogicalPlan) = {
    val routingKeyMap = routingKeys.map(x => (x._1, x._2.head)).toMap
    val offsetMs = LogicalPlanUtils.getOffsetMillis(logicalPlan)
    val periodicSeriesTimeWithOffset = TimeRange((queryParams.startSecs * 1000) - offsetMs,
      (queryParams.endSecs * 1000) - offsetMs)
    val lookBackMs = getLookBackMillis(logicalPlan)

    // Time at which raw data would be retrieved which is used to get partition assignments.
    // It should have time with offset and lookback as we need raw data at time including offset and lookback.
    val queryTimeRange = TimeRange(periodicSeriesTimeWithOffset.startMs - lookBackMs,
      periodicSeriesTimeWithOffset.endMs)

    val partitions = partitionLocationProvider.getPartitions(routingKeyMap, queryTimeRange).
      sortBy(_.timeRange.startMs)
    if (partitions.isEmpty) new UnsupportedOperationException("No partitions found for routing keys: " + routingKeyMap)

    (partitions, lookBackMs, offsetMs)
  }

  /**
    * Materialize all queries except Binary Join and Metadata
    */
  def materializeSimpleQuery(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val routingKeys = getRoutingKeys(logicalPlan)
    if (routingKeys.forall(_._2.isEmpty)) localPartitionPlanner.materialize(logicalPlan, qContext)
    else {
      val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
      val stepMs = queryParams.stepSecs * 1000
      val isInstantQuery: Boolean = if (queryParams.startSecs == queryParams.endSecs) true else false

      val (partitions, lookBackMs, offsetMs) = partitionUtil(routingKeys, queryParams, logicalPlan)
      var prevPartitionStart = queryParams.startSecs * 1000
      val execPlans = partitions.zipWithIndex.map { case (p, i) =>
        // First partition should start from query start time
        // No need to calculate time according to step for instant queries
        val startMs = if (i == 0 || isInstantQuery) queryParams.startSecs * 1000
                      else {
                        // Lookback not supported across partitions
                        val numStepsInPrevPartition = (p.timeRange.startMs - prevPartitionStart + lookBackMs) / stepMs
                        val lastPartitionInstant = prevPartitionStart + numStepsInPrevPartition * stepMs
                        val start = lastPartitionInstant + stepMs
                        // If query duration is less than or equal to lookback start will be greater than query end time
                        if (start > (queryParams.endSecs * 1000)) queryParams.endSecs * 1000 else start
                      }
        prevPartitionStart = startMs
        val endMs = if (isInstantQuery) queryParams.endSecs * 1000 else p.timeRange.endMs + offsetMs
        logger.debug(s"partitionInfo=$p; updated startMs=$startMs, endMs=$endMs")
        if (p.partitionName.equals(localPartitionName))
          localPartitionPlanner.materialize(
            copyLogicalPlanWithUpdatedTimeRange(logicalPlan, TimeRange(startMs, endMs)), qContext)
        else {
          val httpEndpoint = p.endPoint + queryParams.remoteQueryPath.getOrElse("")
          PromQlRemoteExec(httpEndpoint, remoteHttpTimeoutMs, generateRemoteExecParams(qContext, startMs, endMs),
            InProcessPlanDispatcher, dataset.ref)
        }
      }
      if (execPlans.size == 1) execPlans.head
      else StitchRvsExec(qContext, InProcessPlanDispatcher,
        execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlRemoteExec]))
      // ^^ Stitch RemoteExec plan results with local using InProcessPlanDispatcher
      // Sort to move RemoteExec in end as it does not have schema
    }
  }

  def materializeBinaryJoin(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val routingKeys = getRoutingKeys(logicalPlan)
    if (routingKeys.forall(_._2.isEmpty)) localPartitionPlanner.materialize(logicalPlan, qContext)
    else {
      val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
      val partitions = partitionUtil(routingKeys, queryParams, logicalPlan)._1
      val partitionName = partitions.head.partitionName

      // Binary Join supported only for single partition now
      if (partitions.forall(_.partitionName.equals((partitionName)))) {
        if (partitionName.equals(localPartitionName)) localPartitionPlanner.materialize(logicalPlan, qContext)
        else {
          val httpEndpoint = partitions.head.endPoint + queryParams.remoteQueryPath.getOrElse("")
          PromQlRemoteExec(httpEndpoint, remoteHttpTimeoutMs, generateRemoteExecParams(qContext,
            queryParams.startSecs * 1000, queryParams.endSecs * 1000), InProcessPlanDispatcher, dataset.ref
            )
        }
      }
      else throw new UnsupportedOperationException("Binary Join across multiple partitions not supported")
    }
  }

  def materializeSeriesKeysFilters(lp: SeriesKeysByFilters, qContext: QueryContext): ExecPlan = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val partitions = partitionLocationProvider.getAuthorizedPartitions(
      TimeRange(queryParams.startSecs * 1000, queryParams.endSecs * 1000))
    val execPlans = partitions.map { p =>
      logger.debug(s"partitionInfo=$p; queryParams=$queryParams")
      if (p.partitionName.equals(localPartitionName))
        localPartitionPlanner.materialize(lp.copy(startMs = p.timeRange.startMs, endMs = p.timeRange.endMs), qContext)
      else
        createMetadataRemoteExec(qContext, queryParams, p, Map("match[]" -> queryParams.promQl))
    }
    if (execPlans.size == 1) execPlans.head
    else PartKeysDistConcatExec(qContext, InProcessPlanDispatcher,
      execPlans.sortWith((x, y) => !x.isInstanceOf[MetadataRemoteExec]))
  }

  def materializeLabelValues(lp: LabelValues, qContext: QueryContext): ExecPlan = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val partitions = partitionLocationProvider.getAuthorizedPartitions(
      TimeRange(queryParams.startSecs * 1000, queryParams.endSecs * 1000))
    val execPlans = partitions.map { p =>
      logger.debug(s"partitionInfo=$p; queryParams=$queryParams")
      if (p.partitionName.equals(localPartitionName))
        localPartitionPlanner.materialize(lp.copy(startMs = p.timeRange.startMs, endMs = p.timeRange.endMs), qContext)
      else
        createMetadataRemoteExec(qContext, queryParams, p,
          Map("filter" -> lp.filters.map{f => f.column + f.filter.operatorString + f.filter.valuesStrings.head}.
            mkString(","), "labels" -> lp.labelNames.mkString(",")))
    }
    if (execPlans.size == 1) execPlans.head
    else LabelValuesDistConcatExec(qContext, InProcessPlanDispatcher,
      execPlans.sortWith((x, y) => !x.isInstanceOf[MetadataRemoteExec]))
  }

  private def createMetadataRemoteExec(qContext: QueryContext, queryParams: PromQlQueryParams,
                                       partitionAssignment: PartitionAssignment, urlParams: Map[String, String]) = {
    val finalQueryContext = generateRemoteExecParams(
      qContext, partitionAssignment.timeRange.startMs, partitionAssignment.timeRange.endMs)
    val httpEndpoint = partitionAssignment.endPoint + finalQueryContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      remoteQueryPath.getOrElse("")
    MetadataRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
      urlParams, finalQueryContext, InProcessPlanDispatcher, dataset.ref)
  }
}
