package filodb.coordinator.queryplanner

import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.queryplanner.LogicalPlanUtils._
import filodb.core.metadata.{Dataset, DatasetOptions, Schemas}
import filodb.core.query.{PromQlQueryParams, QueryConfig, QueryContext}
import filodb.query._
import filodb.query.exec._

case class PartitionAssignment(partitionName: String, endPoint: String, timeRange: TimeRange)

trait PartitionLocationProvider {

  def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment]
  def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment]
}

/**
 * The MultiPartitionPlanner  is responsible for planning queries that span one or more deployment partitions.
 * Data for each shard-key (ws/ns) combination can be ingested into a different partition. The partitionLocationProvider
 * param provides the locality mapping of fully specified ws/ns to a single partition. Note that this planner DOES NOT
 * handle regex in ws/ns labels in the queries as that is handled by ShardKeyRegexPlanner. Planners  are
 * Hierarchical and ShardKeyRegexPlanner wraps MultiPartitionPlanner, thus all regex on namespace (_ns_) in the
 * queries are already replaced with equals when the materialize of this class is invoked.
 *
 * @param partitionLocationProvider The implementation is responsible to get the partition assignments based on the
 *                                  shard keys and an additional time dimension to handle assignments of partitions
 * @param localPartitionPlanner     The planner instance to use of the data is available locally
 * @param localPartitionName        Unique name for the local partition
 * @param dataset                   The dataset instance, see Dataset documentation for more details
 * @param queryConfig               Configuration for the query planner
 * @param remoteExecHttpClient      If the partition is not local, a remote call is made to the correct partition to
 *                                  query and retrieve the data.
 */
class MultiPartitionPlanner(partitionLocationProvider: PartitionLocationProvider,
                            localPartitionPlanner: QueryPlanner,
                            localPartitionName: String,
                            val dataset: Dataset,
                            val queryConfig: QueryConfig,
                            remoteExecHttpClient: RemoteExecHttpClient = RemoteHttpClient.defaultClient)
  extends QueryPlanner with StrictLogging with PlannerHelper {

  override val schemas: Schemas = Schemas(dataset.schema)
  override val dsOptions: DatasetOptions = schemas.part.options

  import net.ceedubs.ficus.Ficus._

  val remoteHttpTimeoutMs: Long =
    queryConfig.routingConfig.config.as[Option[Long]]("remote.http.timeout").getOrElse(60000)

  val datasetMetricColumn: String = dataset.options.metricColumn

  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val tsdbQueryParams = qContext.origQueryParams

    if(!tsdbQueryParams.isInstanceOf[PromQlQueryParams] || // We don't know the promql issued (unusual)
      (tsdbQueryParams.isInstanceOf[PromQlQueryParams]
        && !qContext.plannerParams.processMultiPartition)) { // Query was part of routing
      localPartitionPlanner.materialize(logicalPlan, qContext)
    }  else if (LogicalPlan.hasSubqueryWithWindowing(logicalPlan) || logicalPlan.isInstanceOf[TopLevelSubquery]) {
      materializeSubquery(logicalPlan, qContext)
    } else {
        walkLogicalPlanTree(logicalPlan, qContext).plans.head
    }
  }

  override def walkLogicalPlanTree(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = logicalPlan match {
    case lp: BinaryJoin                  => materializeBinaryJoin(lp, qContext)
    case mdq: MetadataQueryPlan          => materializeMetadataQueryPlan(mdq, qContext)
    case lp: ApplyInstantFunction        => super.materializeApplyInstantFunction(qContext, lp)
    case lp: ApplyInstantFunctionRaw     => super.materializeApplyInstantFunctionRaw(qContext, lp)
    case lp: Aggregate                   => super.materializeAggregate(qContext, lp)
    case lp: ScalarVectorBinaryOperation => super.materializeScalarVectorBinOp(qContext, lp)
    case lp: ApplyMiscellaneousFunction  => super.materializeApplyMiscellaneousFunction(qContext, lp)
    case lp: ApplySortFunction           => super.materializeApplySortFunction(qContext, lp)
    case lp: ScalarVaryingDoublePlan     => super.materializeScalarPlan(qContext, lp)
    case lp: ScalarTimeBasedPlan         => super.materializeScalarTimeBased(qContext, lp)
    case lp: VectorPlan                  => super.materializeVectorPlan(qContext, lp)
    case lp: ScalarFixedDoublePlan       => super.materializeFixedScalar(qContext, lp)
    case lp: ApplyAbsentFunction         => super.materializeAbsentFunction(qContext, lp)
    case lp: ScalarBinaryOperation       => super.materializeScalarBinaryOperation(qContext, lp)
    case lp: ApplyLimitFunction          => super.materializeLimitFunction(qContext, lp)

    // Imp: At the moment, these two cases for subquery will not get executed, materialize is already
    // Checking if the plan is a TopLevelSubQuery or any of the descendant is a SubqueryWithWindowing and
    // will invoke materializeSubquery, currently these placeholders are added to make compiler happy as LogicalPlan
    // is a sealed trait. Ideally, everything, including subqueries need to be walked and materialized by the
    // Planner. Once we identify the test cases for subqueries that need to walk the tree, remove this block of
    // comment, remove the special handling from materialize method and fix the next case handling SubqueryWithWindowing
    // and TopLevelSubquery
    case _: SubqueryWithWindowing |
         _: TopLevelSubquery             => PlanResult(materializeSubquery(logicalPlan, qContext)::Nil)
    case _: PeriodicSeries |
         _: PeriodicSeriesWithWindowing |
         _: RawChunkMeta |
         _: RawSeries                    => materializePeriodicAndRawSeries(logicalPlan, qContext)
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

  /**
   *
   * @param logicalPlan Logical plan
   * @param queryParams PromQlQueryParams having query details
   * @return Returns PartitionAssignment, lookback, offset and routing keys
   */
  private def partitionUtilNonBinaryJoin(logicalPlan: LogicalPlan, queryParams: PromQlQueryParams) = {

    val routingKeys = getRoutingKeys(logicalPlan)

    val offsetMs = LogicalPlanUtils.getOffsetMillis(logicalPlan)
    // To cover entire time range for queries like sum(foo offset 2d) - sum(foo)
    val periodicSeriesTimeWithOffset = TimeRange((queryParams.startSecs * 1000) - offsetMs.max,
      (queryParams.endSecs * 1000) - offsetMs.min)
    val lookBackMs = getLookBackMillis(logicalPlan).max

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
    if (partitions.isEmpty && routingKeys.nonEmpty)
      logger.warn(s"No partitions found for routing keys: $routingKeys")

    (partitions, lookBackMs, offsetMs, routingKeys)
  }

  /**
   *
   * @param logicalPlan Logical plan
   * @param queryParams PromQlQueryParams having query details
   * @return Returns PartitionAssignment and routing keys
   */
  private def partitionUtilSubquery(logicalPlan: LogicalPlan, queryParams: PromQlQueryParams) = {
    val routingKeys = getRoutingKeys(logicalPlan)
    val lastPoint = TimeRange(queryParams.endSecs * 1000, queryParams.endSecs * 1000)
    val partitions =
      if (routingKeys.isEmpty) {
        List.empty
      } else {
        val routingKeyMap = routingKeys.map(x => (x._1, x._2.head)).toMap
        partitionLocationProvider.getPartitions(routingKeyMap, lastPoint).sortBy(_.timeRange.startMs)
      }
    if (partitions.isEmpty && routingKeys.nonEmpty)
      logger.warn(s"No partitions found for routing keys: $routingKeys")
    (partitions, routingKeys)
  }
  /**
    * @param queryParams PromQlQueryParams having query details
    * @param logicalPlan Logical plan
   *  @return Returns PartitionAssignment and routing keys
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
    (lhsPartitionsAndRoutingKeys._1 ++ rhsPartitionsAndRoutingKeys._1,
      lhsPartitionsAndRoutingKeys._2 ++ rhsPartitionsAndRoutingKeys._2)
  }


  /**
    * Materialize all queries except Binary Join and Metadata
    */
  def materializePeriodicAndRawSeries(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {

    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val (partitions, lookBackMs, offsetMs, routingKeys) = partitionUtilNonBinaryJoin(logicalPlan, queryParams)
    val execPlan = if (partitions.isEmpty || routingKeys.forall(_._2.isEmpty))
      localPartitionPlanner.materialize(logicalPlan, qContext)
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
        val endMs = if (isInstantQuery) queryParams.endSecs * 1000 else p.timeRange.endMs + offsetMs.min
        logger.debug(s"partitionInfo=$p; updated startMs=$startMs, endMs=$endMs")
        if (p.partitionName.equals(localPartitionName))
          localPartitionPlanner.materialize(
            copyLogicalPlanWithUpdatedTimeRange(logicalPlan, TimeRange(startMs, endMs)), qContext)
        else {
          val httpEndpoint = p.endPoint + queryParams.remoteQueryPath.getOrElse("")
          PromQlRemoteExec(httpEndpoint, remoteHttpTimeoutMs, generateRemoteExecParams(qContext, startMs, endMs),
            inProcessPlanDispatcher, dataset.ref, remoteExecHttpClient)
        }
      }
      if (execPlans.size == 1) execPlans.head
      else StitchRvsExec(qContext, inProcessPlanDispatcher,
        execPlans.sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec]))
      // ^^ Stitch RemoteExec plan results with local using InProcessPlanDispatcher
      // Sort to move RemoteExec in end as it does not have schema
    }
    PlanResult(execPlan:: Nil)
  }


  /**
   * Materialize queries having subqueries in them.
   * Here, we don't stitch across partitions and we assume for a given
   * set of routingKeys we will have only ONE partition. We do not try to find multiple
   * partitions that cover our time range or find one that has the best overlap, we just
   * choose one partition that has the end of our interval.
   */
  def materializeSubquery(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val (partitions, routingKeys) = partitionUtilSubquery(logicalPlan, queryParams)
    if (partitions.isEmpty || routingKeys.forall(_._2.isEmpty)) {
      localPartitionPlanner.materialize(logicalPlan, qContext)
    } else {
      val execPlans = partitions.map { p =>
        val startMs = queryParams.startSecs * 1000
        val endMs = queryParams.endSecs * 1000
        logger.debug(s"partitionInfo=$p; updated startMs=$startMs, endMs=$endMs")
        if (p.partitionName.equals(localPartitionName))
          localPartitionPlanner.materialize(logicalPlan, qContext)
        else {
          val httpEndpoint = p.endPoint + queryParams.remoteQueryPath.getOrElse("")
          PromQlRemoteExec(
            httpEndpoint, remoteHttpTimeoutMs, generateRemoteExecParams(qContext, startMs, endMs),
            inProcessPlanDispatcher, dataset.ref, remoteExecHttpClient
          )
        }
      }
      if (execPlans.size == 1) {
        execPlans.head
      } else {
        StitchRvsExec(
          qContext,
          inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec])
        )
      }
      // ^^ Stitch RemoteExec plan results with local using InProcessPlanDispatcher
      // Sort to move RemoteExec in end as it does not have schema
    }
  }

  private def materializeMultiPartitionBinaryJoin(logicalPlan: BinaryJoin, qContext: QueryContext): ExecPlan = {
    val lhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.lhs)))
    val rhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.rhs)))

    val lhsExec = this.materialize(logicalPlan.lhs, lhsQueryContext)
    val rhsExec = this.materialize(logicalPlan.rhs, rhsQueryContext)

    val onKeysReal = ExtraOnByKeysUtil.getRealOnLabels(logicalPlan, queryConfig.addExtraOnByKeysTimeRanges)

    if (logicalPlan.operator.isInstanceOf[SetOperator])
      SetOperatorExec(qContext, InProcessPlanDispatcher(queryConfig), Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
        LogicalPlanUtils.renameLabels(onKeysReal, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn), datasetMetricColumn)
    else
      BinaryJoinExec(qContext, inProcessPlanDispatcher, Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
        logicalPlan.cardinality, LogicalPlanUtils.renameLabels(onKeysReal, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.include, datasetMetricColumn), datasetMetricColumn)
  }

  def materializeBinaryJoin(logicalPlan: BinaryJoin, qContext: QueryContext): PlanResult = {

    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val (partitions, routingKeys) = partitionUtil(queryParams, logicalPlan)
    val execPlan = if (partitions.isEmpty) {
      logger.warn(s"No partitions found for routingKeys: $routingKeys")
      localPartitionPlanner.materialize(logicalPlan, qContext)
    } else if (routingKeys.forall(_._2.isEmpty)) localPartitionPlanner.materialize(logicalPlan, qContext)
    else {
      val partitionName = partitions.head.partitionName
      // Binary Join for single partition
      if (partitions.forall(_.partitionName.equals(partitionName))) {
        if (partitionName.equals(localPartitionName)) localPartitionPlanner.materialize(logicalPlan, qContext)
        else {
          val httpEndpoint = partitions.head.endPoint + queryParams.remoteQueryPath.getOrElse("")
          PromQlRemoteExec(httpEndpoint, remoteHttpTimeoutMs, generateRemoteExecParams(qContext,
            queryParams.startSecs * 1000, queryParams.endSecs * 1000),
            inProcessPlanDispatcher, dataset.ref, remoteExecHttpClient)
        }
      }
      else materializeMultiPartitionBinaryJoin(logicalPlan, qContext)
    }
    PlanResult(execPlan::Nil)
  }

  private def copy(lp: MetadataQueryPlan, startMs: Long, endMs: Long): MetadataQueryPlan = lp match {
    case sk: SeriesKeysByFilters  => sk.copy(startMs = startMs, endMs = endMs)
    case lv: LabelValues          => lv.copy(startMs = startMs, endMs = endMs)
    case ln: LabelNames           => ln.copy(startMs = startMs, endMs = endMs)
    case lc: LabelCardinality     => lc.copy(startMs = startMs, endMs = endMs)
  }

  def materializeMetadataQueryPlan(lp: MetadataQueryPlan, qContext: QueryContext): PlanResult = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val partitions = partitionLocationProvider.getAuthorizedPartitions(
      TimeRange(queryParams.startSecs * 1000, queryParams.endSecs * 1000))
    val execPlan = if (partitions.isEmpty) {
      logger.warn(s"No partitions found for ${queryParams.startSecs}, ${queryParams.endSecs}")
      localPartitionPlanner.materialize(lp, qContext)
    }
    else {
      val execPlans = partitions.map { p =>
        logger.debug(s"partitionInfo=$p; queryParams=$queryParams")
        if (p.partitionName.equals(localPartitionName))
          localPartitionPlanner.materialize(
            copy(lp, startMs = p.timeRange.startMs, endMs = p.timeRange.endMs), qContext)
        else {
          val params: Map[String, String] = lp match {
            case _: SeriesKeysByFilters |
                 _: LabelNames |
                 _: LabelCardinality          => Map("match[]" -> queryParams.promQl)
            case lv: LabelValues              => PlannerUtil.getLabelValuesUrlParams(lv, queryParams)
          }
          createMetadataRemoteExec(qContext, p, params)
        }
      }
      if (execPlans.size == 1) execPlans.head
      else lp match {
        case _: SeriesKeysByFilters => PartKeysDistConcatExec(qContext, inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[MetadataRemoteExec]))
        case _: LabelValues => LabelValuesDistConcatExec(qContext, inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[MetadataRemoteExec]))
        case _: LabelNames => LabelNamesDistConcatExec(qContext, inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[MetadataRemoteExec]))
        case _: LabelCardinality => LabelCardinalityDistConcatExec(qContext, inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[MetadataRemoteExec]))
      }
    }
    PlanResult(execPlan::Nil)
  }

  private def createMetadataRemoteExec(qContext: QueryContext, partitionAssignment: PartitionAssignment,
                                       urlParams: Map[String, String]) = {
    val finalQueryContext = generateRemoteExecParams(
      qContext, partitionAssignment.timeRange.startMs, partitionAssignment.timeRange.endMs)
    val httpEndpoint = partitionAssignment.endPoint + finalQueryContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      remoteQueryPath.getOrElse("")
    MetadataRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
      urlParams, finalQueryContext, inProcessPlanDispatcher, dataset.ref, remoteExecHttpClient)
  }
}
