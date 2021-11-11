package filodb.coordinator.queryplanner

import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.queryplanner.LogicalPlanUtils._
import filodb.core.metadata.{Dataset, DatasetOptions, Schemas}
import filodb.core.query.{PromQlQueryParams, QueryConfig, QueryContext}
import filodb.query.{MetadataQueryPlan, _}
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
      // Pseudo code for the materialize
      //
      // def materialize(lp) {
      //   walk(lp)
      // }
      //
      // def walk(lp) {
      //   if lp.isLocalPlan() {
      //     localPlanner.materialize(lp)
      //   } else if partitions(lp).size == 1 && partitions(lp.partitions).head.name != "localPartition" {
      //     materializeRemoteExecPlan(lp)
      //   } else {
      //     case BinaryJoin:
      //       lhs, rhs = walk(lp.lhs), walk(lp.rhs)
      //       BinaryJoinExec(lhs, rhs, inProcess)
      //     case x: _:
      //       // X represents appropriate handler based on type of x
      //       plannerHelper.materializeX(x)
      //    }
      // }
    val tsdbQueryParams = qContext.origQueryParams

    if(!tsdbQueryParams.isInstanceOf[PromQlQueryParams] || // We don't know the promql issued (unusual)
      (tsdbQueryParams.isInstanceOf[PromQlQueryParams]
        && !qContext.plannerParams.processMultiPartition)) { // Query was part of routing
      localPartitionPlanner.materialize(logicalPlan, qContext)
    }  else if (LogicalPlan.hasSubqueryWithWindowing(logicalPlan) || logicalPlan.isInstanceOf[TopLevelSubquery]) {
      materializeSubquery(logicalPlan, qContext)
    } else logicalPlan match {
      case mqp: MetadataQueryPlan               => materializeMetadataQueryPlan(mqp, qContext).plans.head
      case _                                    => walkLogicalPlanTree(logicalPlan, qContext).plans.head
    }
  }

  override def walkLogicalPlanTree(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {
    // Should avoid this asInstanceOf, far many places where we do this now.
    val params = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val partitions = getPartitions(logicalPlan, params)


    if (isSinglePartition(partitions)) {

        val (partitionName, startMs, endMs) =
          ( partitions.headOption.map(_.partitionName).getOrElse(localPartitionName),
            params.startSecs * 1000L,
            params.endSecs * 1000L)

        // If the plan is on a single partition, then depending on partition name we either delegate to local or
        // remote planner
        val execPlan = if (partitionName.equals(localPartitionName)) {
            localPartitionPlanner.materialize(logicalPlan, qContext)
        } else {
          // Single partition but remote, send the entire plan remotely
          val remotePartitionEndpoint = partitions.head.endPoint
          val httpEndpoint = remotePartitionEndpoint + params.remoteQueryPath.getOrElse("")
          PromQlRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
            generateRemoteExecParams(qContext, startMs, endMs),
            inProcessPlanDispatcher, dataset.ref, remoteExecHttpClient)
        }
        PlanResult(Seq(execPlan))
    } else walkMultiPartitionPlan(logicalPlan, qContext)
  }

  /**
   * Invoked when the plan tree spans multiple plans
   *
   * @param logicalPlan The multi partition LogicalPlan tree
   * @param qContext the QueryContext object
   * @return
   */
  private def walkMultiPartitionPlan(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {
    logicalPlan match {
      case lp: BinaryJoin                 => materializeMultiPartitionBinaryJoin(lp, qContext)
      case _: MetadataQueryPlan           => throw new IllegalArgumentException("MetadataQueryPlan unexpected here")
      case lp: ApplyInstantFunction       => super.materializeApplyInstantFunction(qContext, lp)
      case lp: ApplyInstantFunctionRaw    => super.materializeApplyInstantFunctionRaw(qContext, lp)
      case lp: Aggregate                  => super.materializeAggregate(qContext, lp)
      case lp: ScalarVectorBinaryOperation=> super.materializeScalarVectorBinOp(qContext, lp)
      case lp: ApplyMiscellaneousFunction => super.materializeApplyMiscellaneousFunction(qContext, lp)
      case lp: ApplySortFunction          => super.materializeApplySortFunction(qContext, lp)
      case lp: ScalarVaryingDoublePlan    => super.materializeScalarPlan(qContext, lp)
      case _: ScalarTimeBasedPlan         => throw new IllegalArgumentException("ScalarTimeBasedPlan unexpected here")
      case lp: VectorPlan                 => super.materializeVectorPlan(qContext, lp)
      case _: ScalarFixedDoublePlan       => throw new IllegalArgumentException("ScalarFixedDoublePlan unexpected here")
      case lp: ApplyAbsentFunction        => super.materializeAbsentFunction(qContext, lp)
      case lp: ScalarBinaryOperation      => super.materializeScalarBinaryOperation(qContext, lp)
      case lp: ApplyLimitFunction         => super.materializeLimitFunction(qContext, lp)
      case _: TsCardinalities             => throw new IllegalArgumentException("TsCardinalities unexpected here")

      // Imp: At the moment, these two cases for subquery will not get executed, materialize is already
      // Checking if the plan is a TopLevelSubQuery or any of the descendant is a SubqueryWithWindowing and
      // will invoke materializeSubquery, currently these placeholders are added to make compiler happy as LogicalPlan
      // is a sealed trait. Ideally, everything, including subqueries need to be walked and materialized by the
      // Planner. Once we identify the test cases for subqueries that need to walk the tree, remove this block of
      // comment, remove the special handling from materialize method and fix the next case handling
      // SubqueryWithWindowing and TopLevelSubquery
      case _: SubqueryWithWindowing |
           _: TopLevelSubquery            => PlanResult(materializeSubquery(logicalPlan, qContext) :: Nil)
      case _: PeriodicSeries |
           _: PeriodicSeriesWithWindowing |
           _: RawChunkMeta |
           _: RawSeries                   => materializePeriodicAndRawSeries(logicalPlan, qContext)
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

  /**
   * Gets the partition Assignment for the given plan
   */
  private def getPartitions(logicalPlan: LogicalPlan, queryParams: PromQlQueryParams) : Seq[PartitionAssignment] = {

    //1.  Get a Seq of all Leaf node filters
    val leafFilters = LogicalPlan.getColumnFilterGroup(logicalPlan)
    val nonMetricColumnSet = dataset.options.nonMetricShardColumns.toSet
    //2. Filter from each leaf node filters to keep only nonShardKeyColumns and convert them to key value map
    val routingKeyMap = leafFilters.map(cf => {
      cf.filter(col => nonMetricColumnSet.contains(col.column)).map(
        x => (x.column, x.filter.valuesStrings.head.toString)).toMap
    })
    // 3. Get the start and end time is ms based on the lookback, offset and the user provided start and end time
    val (maxOffsetMs, minOffsetMs) = LogicalPlanUtils.getOffsetMillis(logicalPlan)
      .foldLeft((Long.MinValue, Long.MaxValue)) {
        case ((accMax, accMin), currValue) => (accMax.max(currValue), accMin.min(currValue))
      }

    val periodicSeriesTimeWithOffset = TimeRange((queryParams.startSecs * 1000) - maxOffsetMs,
      (queryParams.endSecs * 1000) - minOffsetMs)
    val lookBackMs = getLookBackMillis(logicalPlan).max

    //4. Get the Query time range based on user provided range, offsets in previous steps and lookback
    val queryTimeRange = TimeRange(periodicSeriesTimeWithOffset.startMs - lookBackMs,
      periodicSeriesTimeWithOffset.endMs)

    //5. Based on the map in 2 and time range in 5, get the partitions to query
    routingKeyMap.flatMap(metricMap =>
      partitionLocationProvider.getPartitions(metricMap, queryTimeRange))
  }

  /**
   * Checks if all the PartitionAssignments belong to same partition
   */
  private def isSinglePartition(partitions: Seq[PartitionAssignment]) : Boolean = {
    if (partitions.isEmpty)
      true
    else {
      val partName = partitions.head.partitionName
      partitions.forall(_.partitionName.equals(partName))
    }
  }


  /**
   *
   * @param logicalPlan Logical plan
   * @param queryParams PromQlQueryParams having query details
   * @return Returns PartitionAssignment, lookback, offset and routing keys
   */
  private def resolvePartitionsAndRoutingKeys(logicalPlan: LogicalPlan, queryParams: PromQlQueryParams) = {

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
    * Materialize all queries except Binary Join and Metadata
    */
  def materializePeriodicAndRawSeries(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {

    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val (partitions, lookBackMs, offsetMs, routingKeys) = resolvePartitionsAndRoutingKeys(logicalPlan, queryParams)
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

  private def materializeMultiPartitionBinaryJoin(logicalPlan: BinaryJoin, qContext: QueryContext): PlanResult = {
    val lhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.lhs)))
    val rhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.rhs)))

    val lhsExec = this.materialize(logicalPlan.lhs, lhsQueryContext)
    val rhsExec = this.materialize(logicalPlan.rhs, rhsQueryContext)

    val onKeysReal = ExtraOnByKeysUtil.getRealOnLabels(logicalPlan, queryConfig.addExtraOnByKeysTimeRanges)

    val execPlan = if (logicalPlan.operator.isInstanceOf[SetOperator])
      SetOperatorExec(qContext, InProcessPlanDispatcher(queryConfig), Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
        LogicalPlanUtils.renameLabels(onKeysReal, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn), datasetMetricColumn)
    else
      BinaryJoinExec(qContext, inProcessPlanDispatcher, Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
        logicalPlan.cardinality, LogicalPlanUtils.renameLabels(onKeysReal, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.include, datasetMetricColumn), datasetMetricColumn)
    PlanResult(execPlan :: Nil)
  }

  private def copy(lp: MetadataQueryPlan, startMs: Long, endMs: Long): MetadataQueryPlan = lp match {
    case sk: SeriesKeysByFilters       => sk.copy(startMs = startMs, endMs = endMs)
    case lv: LabelValues               => lv.copy(startMs = startMs, endMs = endMs)
    case ln: LabelNames                => ln.copy(startMs = startMs, endMs = endMs)
    case lc: LabelCardinality          => lc.copy(startMs = startMs, endMs = endMs)
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
                 _: LabelCardinality    => Map("match[]" -> queryParams.promQl)
            case lv: LabelValues        => PlannerUtil.getLabelValuesUrlParams(lv, queryParams)
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
        case _: LabelCardinality => LabelCardinalityReduceExec(qContext, inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[MetadataRemoteExec]))
      }
    }
    PlanResult(execPlan::Nil)
  }

  private def materializeTsCardinalities(qContext: QueryContext, lp: TsCardinalities) : PlanResult = {
    // TODO(a_theimer): outdated?
    // This code is nearly identical to materializeMetadataQueryPlan, but it prevents some
    //   boilerplate code clutter that results when TopkCardinalities extends MetadataQueryPlan.
    //   If this code's maintenance isn't worth some extra stand-in lines of code (i.e. at matchers that
    //   take MetadataQueryPlan instances), then just extend TopkCardinalities from MetadataQueryPlan.
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val partitions = partitionLocationProvider.getAuthorizedPartitions(
      TimeRange(queryParams.startSecs * 1000, queryParams.endSecs * 1000))
    val execPlan = if (partitions.isEmpty) {
      logger.warn(s"No partitions found for ${queryParams.startSecs}, ${queryParams.endSecs}")
      localPartitionPlanner.materialize(lp, qContext)
    } else {
      val leafPlans = partitions.map { p =>
        logger.debug(s"partitionInfo=$p; queryParams=$queryParams")
        if (p.partitionName.equals(localPartitionName))
          localPartitionPlanner.materialize(lp.copy(), qContext)
        else ??? // TODO: handle remote
      }.toSeq
      val reducer = TsCardReduceExec(qContext, inProcessPlanDispatcher, leafPlans)
      reducer.addRangeVectorTransformer(TsCardPresenter())
      reducer
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
