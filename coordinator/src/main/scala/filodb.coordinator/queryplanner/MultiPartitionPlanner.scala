package filodb.coordinator.queryplanner

import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.queryplanner.LogicalPlanUtils._
import filodb.core.metadata.Dataset
import filodb.core.query.{PromQlQueryParams, QueryConfig, QueryContext}
import filodb.query.{BinaryJoin, LabelValues, LogicalPlan, SeriesKeysByFilters, SetOperator}
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
                            queryConfig: QueryConfig,
                            remoteExecHttpClient: RemoteExecHttpClient = RemoteHttpClient.defaultClient)
  extends QueryPlanner with StrictLogging {

  import net.ceedubs.ficus.Ficus._

  val remoteHttpTimeoutMs: Long =
    queryConfig.routingConfig.config.as[Option[Long]]("remote.http.timeout").getOrElse(60000)

  val datasetMetricColumn: String = dataset.options.metricColumn

  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val tsdbQueryParams = qContext.origQueryParams

    if(!tsdbQueryParams.isInstanceOf[PromQlQueryParams] || // We don't know the promql issued (unusual)
      (tsdbQueryParams.isInstanceOf[PromQlQueryParams]
        && !qContext.plannerParams.processMultiPartition)) // Query was part of routing
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
    val routingKeys = dataset.options.nonMetricShardColumns
      .map(x => (x, LogicalPlan.getColumnValues(columnFilterGroup, x)))
    if (routingKeys.flatMap(_._2).isEmpty) Seq.empty else routingKeys
  }

  private def generateRemoteExecParams(queryContext: QueryContext, startMs: Long, endMs: Long) = {
    val queryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryContext.copy(origQueryParams = queryParams.copy(startSecs = startMs/1000, endSecs = endMs / 1000),
      plannerParams = queryContext.plannerParams.copy(processMultiPartition = false))
  }

  private def partitionUtilNonBinaryJoin(logicalPlan: LogicalPlan, queryParams: PromQlQueryParams) = {

    val routingKeys = getRoutingKeys(logicalPlan)

    val offsetMs = LogicalPlanUtils.getOffsetMillis(logicalPlan)
    val periodicSeriesTimeWithOffset = TimeRange((queryParams.startSecs * 1000) - offsetMs,
      (queryParams.endSecs * 1000) - offsetMs)
    val lookBackMs = getLookBackMillis(logicalPlan)

    // Time at which raw data would be retrieved which is used to get partition assignments.
    // It should have time with offset and lookback as we need raw data at time including offset and lookback.
    val queryTimeRange = TimeRange(periodicSeriesTimeWithOffset.startMs - lookBackMs,
      periodicSeriesTimeWithOffset.endMs)

    val partitions = if (routingKeys.isEmpty) List.empty
    else {
      val routingKeyMap = routingKeys.map(x => (x._1, x._2.head)).toMap
      partitionLocationProvider.getPartitions(routingKeyMap, queryTimeRange).
        sortBy(_.timeRange.startMs)
    }
    if (partitions.isEmpty && !routingKeys.isEmpty)
      new UnsupportedOperationException("No partitions found for routing keys: " + routingKeys)

    (partitions, lookBackMs, offsetMs, routingKeys)
  }
  /**
    * @param queryParams PromQlQueryParams having query details
    * @param logicalPlan Logical plan
    */
  private def partitionUtil(queryParams: PromQlQueryParams,
                            logicalPlan: BinaryJoin): (List[PartitionAssignment], Seq[(String, Set[String])]) = {

  val lhsPartitionsAndRoutingKeys = logicalPlan.lhs match {
    case b: BinaryJoin => partitionUtil(queryParams, b)
    case _             => val p = partitionUtilNonBinaryJoin(logicalPlan.lhs, queryParams)
                          (p._1, p._4)
  }

    val rhsPartitionsAndRoutingKeys = logicalPlan.rhs match {
      case b: BinaryJoin => partitionUtil(queryParams, b)
      case _             => val p = partitionUtilNonBinaryJoin(logicalPlan.rhs, queryParams)
                            (p._1, p._4)
    }
    (lhsPartitionsAndRoutingKeys._1 ++ lhsPartitionsAndRoutingKeys._1,
      rhsPartitionsAndRoutingKeys._2 ++ rhsPartitionsAndRoutingKeys._2)
  }


  /**
    * Materialize all queries except Binary Join and Metadata
    */
  def materializeSimpleQuery(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val (partitions, lookBackMs, offsetMs, routingKeys) = partitionUtilNonBinaryJoin(logicalPlan, queryParams)
    if (routingKeys.forall(_._2.isEmpty)) localPartitionPlanner.materialize(logicalPlan, qContext)
    else {
      val stepMs = queryParams.stepSecs * 1000
      val isInstantQuery: Boolean = if (queryParams.startSecs == queryParams.endSecs) true else false

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
            InProcessPlanDispatcher, dataset.ref, remoteExecHttpClient)
        }
      }
      if (execPlans.size == 1) execPlans.head
      else StitchRvsExec(qContext, InProcessPlanDispatcher,
        execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlRemoteExec]))
      // ^^ Stitch RemoteExec plan results with local using InProcessPlanDispatcher
      // Sort to move RemoteExec in end as it does not have schema
    }
  }

  def materializeMultiPartitionBinaryJoin(logicalPlan: BinaryJoin, qContext: QueryContext): ExecPlan = {
    val lhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.lhs)))
    val rhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.rhs)))

    val lhsExec = logicalPlan.lhs match {
      case b: BinaryJoin   => materializeBinaryJoin(b, lhsQueryContext)
      case               _ => materializeSimpleQuery(logicalPlan.lhs, lhsQueryContext)
    }

    val rhsExec = logicalPlan.rhs match {
      case b: BinaryJoin => materializeBinaryJoin(b, rhsQueryContext)
      case _             => materializeSimpleQuery(logicalPlan.rhs, rhsQueryContext)
    }

    val onKeysReal = ExtraOnByKeysUtil.getRealOnLabels(logicalPlan, queryConfig.addExtraOnByKeysTimeRanges)

    if (logicalPlan.operator.isInstanceOf[SetOperator])
      SetOperatorExec(qContext, InProcessPlanDispatcher, Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
        LogicalPlanUtils.renameLabels(onKeysReal, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn), datasetMetricColumn)
    else
      BinaryJoinExec(qContext, InProcessPlanDispatcher, Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
        logicalPlan.cardinality, LogicalPlanUtils.renameLabels(onKeysReal, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.include, datasetMetricColumn), datasetMetricColumn)

  }

  def materializeBinaryJoin(logicalPlan: BinaryJoin, qContext: QueryContext): ExecPlan = {

    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val (partitions, routingKeys) = partitionUtil(queryParams, logicalPlan)
    if (routingKeys.forall(_._2.isEmpty)) localPartitionPlanner.materialize(logicalPlan, qContext)
    else {
      val partitionName = partitions.head.partitionName
      // Binary Join for single partition
      if (partitions.forall(_.partitionName.equals((partitionName)))) {
        if (partitionName.equals(localPartitionName)) localPartitionPlanner.materialize(logicalPlan, qContext)
        else {
          val httpEndpoint = partitions.head.endPoint + queryParams.remoteQueryPath.getOrElse("")
          PromQlRemoteExec(httpEndpoint, remoteHttpTimeoutMs, generateRemoteExecParams(qContext,
            queryParams.startSecs * 1000, queryParams.endSecs * 1000),
            InProcessPlanDispatcher, dataset.ref, remoteExecHttpClient)
        }
      }
      else materializeMultiPartitionBinaryJoin(logicalPlan, qContext)
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
        createMetadataRemoteExec(qContext, queryParams, p, PlannerUtil.getLabelValuesUrlParams(lp, queryParams))
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
      urlParams, finalQueryContext, InProcessPlanDispatcher, dataset.ref, remoteExecHttpClient)
  }
}
