package filodb.coordinator.queryplanner

import java.util.concurrent.ConcurrentHashMap

import scala.collection.concurrent.{Map => ConcurrentMap}
import scala.jdk.CollectionConverters._

import com.typesafe.scalalogging.StrictLogging
import io.grpc.ManagedChannel

import filodb.coordinator.queryplanner.LogicalPlanUtils._
import filodb.core.metadata.{Dataset, DatasetOptions, Schemas}
import filodb.core.query.{ColumnFilter, PromQlQueryParams, QueryConfig, QueryContext, RangeParams, RvRange}
import filodb.grpc.GrpcCommonUtils
import filodb.query._
import filodb.query.LogicalPlan._
import filodb.query.exec._

case class PartitionAssignment(partitionName: String, httpEndPoint: String, timeRange: TimeRange,
                               grpcEndPoint: Option[String] = None)

trait PartitionLocationProvider {

  def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment]
  def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter],
                            timeRange: TimeRange): List[PartitionAssignment]
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
                            shardKeyMatcher: Seq[ColumnFilter] => Seq[Seq[ColumnFilter]] =
                                PartitionLocationPlanner.equalsOnlyShardKeyMatcher,
                            remoteExecHttpClient: RemoteExecHttpClient = RemoteHttpClient.defaultClient,
                            channels: ConcurrentMap[String, ManagedChannel] =
                            new ConcurrentHashMap[String, ManagedChannel]().asScala)
  extends PartitionLocationPlanner(dataset, partitionLocationProvider, shardKeyMatcher) with StrictLogging {

  override val schemas: Schemas = Schemas(dataset.schema)
  override val dsOptions: DatasetOptions = schemas.part.options

  val plannerSelector: String = queryConfig.plannerSelector
    .getOrElse(throw new IllegalArgumentException("plannerSelector is mandatory"))

  val remoteHttpTimeoutMs: Long = queryConfig.remoteHttpTimeoutMs.getOrElse(60000)

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

    if(
      !tsdbQueryParams.isInstanceOf[PromQlQueryParams] || // We don't know the promql issued (unusual)
      (tsdbQueryParams.isInstanceOf[PromQlQueryParams] && !qContext.plannerParams.processMultiPartition)
    ) { // Query was part of routing
      localPartitionPlanner.materialize(logicalPlan, qContext)
    } else logicalPlan match {
      case mqp: MetadataQueryPlan             => materializeMetadataQueryPlan(mqp, qContext).plans.head
      case lp: TsCardinalities                => materializeTsCardinalities(lp, qContext).plans.head
      case _                                  =>
        val result = walkLogicalPlanTree(logicalPlan, qContext)
        if (result.plans.size > 1) {
          val dispatcher = PlannerUtil.pickDispatcher(result.plans)
          MultiPartitionDistConcatExec(qContext, dispatcher, result.plans)
        } else result.plans.head
    }
  }

  // scalastyle:off method.length
  override def walkLogicalPlanTree(logicalPlan: LogicalPlan,
                                   qContext: QueryContext,
                                   forceInProcess: Boolean = false): PlanResult = {
    // Should avoid this asInstanceOf, far many places where we do this now.
    val params = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    // MultiPartitionPlanner has capability to stitch across time partitions, however, the logic is mostly broken
    // and not well tested. The logic below would not work well for any kind of subquery since their actual
    // start and ends are different from the start/end parameter of the query context. If we are to implement
    // stitching across time, we need to to pass proper parameters to getPartitions() call
    val paramToCheckPartitions = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val partitions = getPartitions(logicalPlan, paramToCheckPartitions)

    if (isSinglePartition(partitions)) {

      val (partitionName, startMs, endMs, grpcEndpoint) = partitions.headOption match {
          case Some(pa: PartitionAssignment)
                                         => (pa.partitionName, params.startSecs * 1000L,
                                                params.endSecs * 1000L, pa.grpcEndPoint)
          case None                      => (localPartitionName, params.startSecs * 1000L, params.endSecs * 1000L, None)
        }

        // If the plan is on a single partition, then depending on partition name we either delegate to local or
        // remote planner
        val execPlan = if (partitionName.equals(localPartitionName)) {
            localPartitionPlanner.materialize(logicalPlan, qContext)
        } else {
          val promQl = LogicalPlanParser.convertToQuery(logicalPlan)
          val remoteContext = logicalPlan match {
            case tls: TopLevelSubquery =>
              val instantTime = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].startSecs
              val stepSecs = tls.stepMs / 1000
              generateRemoteExecParamsWithStep(qContext, promQl, instantTime, stepSecs, instantTime)
            case psp: PeriodicSeriesPlan =>
              val startSecs = psp.startMs / 1000
              val stepSecs = psp.stepMs / 1000
              val endSecs = psp.endMs / 1000
              generateRemoteExecParamsWithStep(qContext, promQl, startSecs, stepSecs, endSecs)
            case _ =>
              generateRemoteExecParams(qContext, promQl, startMs, endMs)
          }
          // Single partition but remote, send the entire plan remotely
          if (grpcEndpoint.isDefined && !(queryConfig.grpcPartitionsDenyList.contains("*") ||
            queryConfig.grpcPartitionsDenyList.contains(partitionName.toLowerCase))) {
            val endpoint = grpcEndpoint.get
            val channel = channels.getOrElseUpdate(endpoint, GrpcCommonUtils.buildChannelFromEndpoint(endpoint))
            PromQLGrpcRemoteExec(channel, remoteHttpTimeoutMs, remoteContext, inProcessPlanDispatcher,
              dataset.ref, plannerSelector)
          } else {
            val remotePartitionEndpoint = partitions.head.httpEndPoint
            val httpEndpoint = remotePartitionEndpoint + params.remoteQueryPath.getOrElse("")
            PromQlRemoteExec(httpEndpoint, remoteHttpTimeoutMs, remoteContext, inProcessPlanDispatcher,
              dataset.ref, remoteExecHttpClient)
          }
        }
        PlanResult(Seq(execPlan))
    } else walkMultiPartitionPlan(logicalPlan, qContext)
  }
  // scalastyle:on method.length

  // scalastyle:off cyclomatic.complexity
  /**
   * Invoked when the plan tree spans multiple plans
   *
   * @param logicalPlan The multi partition LogicalPlan tree
   * @param qContext the QueryContext object
   * @return
   */
  private def walkMultiPartitionPlan(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {
    logicalPlan match {
      case _: MetadataQueryPlan            => throw new IllegalArgumentException(
                                                          "MetadataQueryPlan unexpected here")
      case lp: ApplyInstantFunctionRaw     => super.materializeApplyInstantFunctionRaw(qContext, lp)
      case lp: ApplyMiscellaneousFunction  => super.materializeApplyMiscellaneousFunction(qContext, lp)
      case lp: ApplySortFunction           => super.materializeApplySortFunction(qContext, lp)
      case _: ScalarTimeBasedPlan          => throw new IllegalArgumentException(
                                                          "ScalarTimeBasedPlan unexpected here")
      case lp: VectorPlan                  => super.materializeVectorPlan(qContext, lp)
      case _: ScalarFixedDoublePlan        => throw new IllegalArgumentException(
                                                          "ScalarFixedDoublePlan unexpected here")
      case lp: ScalarBinaryOperation       => super.materializeScalarBinaryOperation(qContext, lp)
      case lp: ApplyLimitFunction          => super.materializeLimitFunction(qContext, lp)
      case lp: TsCardinalities             => materializeTsCardinalities(lp, qContext)
      case lp: TopLevelSubquery            => super.materializeTopLevelSubquery(qContext, lp)
      case _: BinaryJoin |
           _: ApplyInstantFunction |
           _: Aggregate |
           _: ScalarVectorBinaryOperation |
           _: ScalarVaryingDoublePlan |
           _: ApplyAbsentFunction |
           _: SubqueryWithWindowing |
           _: PeriodicSeriesWithWindowing |
           _: PeriodicSeries |
           _: RawChunkMeta |
           _: RawSeries                    => materializePlanHandleSplitLeaf(logicalPlan, qContext)
    }
  }
  // scalastyle:on cyclomatic.complexity

  private def getRoutingKeys(logicalPlan: LogicalPlan) = {
    LogicalPlan.getNonMetricShardKeyFilters(logicalPlan, dataset.options.nonMetricShardColumns)
        .flatMap(LogicalPlanUtils.resolveShardKeyFilters(_, shardKeyMatcher))
  }

  private def generateRemoteExecParams(queryContext: QueryContext, promQl: String, startMs: Long, endMs: Long) = {
    val queryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryContext.copy(
      origQueryParams = queryParams.copy(promQl = promQl, startSecs = startMs / 1000, endSecs = endMs / 1000),
      plannerParams = queryContext.plannerParams.copy(processMultiPartition = false))
  }

  private def generateRemoteExecParamsWithStep(
    queryContext: QueryContext, promQl: String, startSecs: Long, stepSecs: Long, endSecs: Long
  ) = {
    val queryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryContext.copy(
      origQueryParams =
        queryParams.copy(promQl = promQl, startSecs = startSecs, stepSecs = stepSecs, endSecs = endSecs),
      plannerParams = queryContext.plannerParams.copy(processMultiPartition = false)
    )
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

    val partitions = routingKeys
      .map{ key =>
        // Convert Seq[ColumnFilter] to Map[String, String]
        key.map(filter => (filter.column, filter.filter.valuesStrings.head.toString)).toMap
      }
      .flatMap(shardKey => partitionLocationProvider.getPartitions(shardKey, queryTimeRange))
      .distinct
      .sortBy(_.timeRange.startMs)
    if (partitions.isEmpty && routingKeys.nonEmpty)
      logger.warn(s"No partitions found for routing keys: $routingKeys")

    (partitions, lookBackMs, offsetMs, routingKeys)
  }

  /**
   * Materialize plans with leaves that are not split across time.
   */
  def materializeNonSplitPeriodicAndRawSeries(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val (partitions, lookBackMs, offsetMs, routingKeys) = resolvePartitionsAndRoutingKeys(logicalPlan, queryParams)
    val execPlan = if (partitions.isEmpty || routingKeys.isEmpty)
      localPartitionPlanner.materialize(logicalPlan, qContext)
    else {
      val execPlans = partitions.map(materializeForPartition(logicalPlan, _, qContext))
      if (execPlans.size == 1) execPlans.head
      else {
        // TODO: Do we pass in QueryContext in LogicalPlan's helper rvRangeForPlan?
        MultiPartitionDistConcatExec(
          qContext, inProcessPlanDispatcher, execPlans.sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec]))
      }
      // Sort to move RemoteExec in end as it does not have schema
    }
    PlanResult(execPlan:: Nil)
  }

  /**
   * If the argument partition is local, materialize the LogicalPlan with the local planner.
   *   Otherwise, create a PromQlRemoteExec.
   * @param timeRangeOverride: if given, the plan will be materialized to this range. Otherwise, the
   *                           range is computed from the PromQlQueryParams.
   */
  private def materializeForPartition(logicalPlan: LogicalPlan,
                                      partition: PartitionAssignment,
                                      queryContext: QueryContext,
                                      timeRangeOverride: Option[TimeRange] = None): ExecPlan = {
    val queryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val timeRange = timeRangeOverride.getOrElse(TimeRange(1000 * queryParams.startSecs, 1000 * queryParams.endSecs))
    val (partitionName, grpcEndpoint) = (partition.partitionName, partition.grpcEndPoint)
    if (partitionName.equals(localPartitionName)) {
      // FIXME: subquery tests fail when their time-ranges are updated
      //   with the original query params
      val lpWithUpdatedTime = if (timeRangeOverride.isDefined) {
        copyLogicalPlanWithUpdatedTimeRange(logicalPlan, timeRange)
      } else logicalPlan
      localPartitionPlanner.materialize(lpWithUpdatedTime, queryContext)
    } else {
      val promQL = LogicalPlanParser.convertToQuery(logicalPlan)
      val ctx = generateRemoteExecParams(queryContext, promQL, timeRange.startMs, timeRange.endMs)
      if (grpcEndpoint.isDefined &&
        !(queryConfig.grpcPartitionsDenyList.contains("*") ||
          queryConfig.grpcPartitionsDenyList.contains(partitionName.toLowerCase))) {
        val channel = channels.getOrElseUpdate(grpcEndpoint.get,
          GrpcCommonUtils.buildChannelFromEndpoint(grpcEndpoint.get))
        PromQLGrpcRemoteExec(channel, remoteHttpTimeoutMs, ctx, inProcessPlanDispatcher,
          dataset.ref, plannerSelector)
      } else {
        val httpEndpoint = partition.httpEndPoint + queryParams.remoteQueryPath.getOrElse("")
        PromQlRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
          ctx, inProcessPlanDispatcher, dataset.ref, remoteExecHttpClient)
      }
    }
  }

  /**
   * Given a sequence of assignments and a time-range to query, returns (assignment, range)
   *   pairs that describe the time-ranges to be queried for each assignment such that:
   *     (a) the returned ranges lie within the argument time-range, and
   *     (b) lookbacks do not cross partition splits (where the "lookback" is defined only by the argument)
   * @param assignments must be sorted and time-disjoint
   * @param queryRange the complete time-range. Does not include the offset.
   * @param lookbackMs the time to skip immediately after a partition split.
   * @param stepMsOpt occupied iff the returned ranges should describe periodic steps
   *                  (i.e. all range start times (except the first) should be snapped to a step)
   */
  def getAssignmentQueryRanges(assignments: Seq[PartitionAssignment], queryRange: TimeRange,
                               lookbackMs: Long = 0L, offsetMs: Long = 0L,
                               stepMsOpt: Option[Long] = None): Seq[(PartitionAssignment, TimeRange)] = {
    // Construct a sequence of Option[TimeRange]; the ith range is None iff the ith partition has no range to query.
    // First partition doesn't need its start snapped to a periodic step, so deal with it separately.
    val filteredAssignments = assignments.dropWhile(_.timeRange.endMs < queryRange.startMs)
    if (filteredAssignments.isEmpty || filteredAssignments.head.timeRange.startMs > queryRange.endMs) {
      return Nil
    }
    val headRange = {
      val range = filteredAssignments.head.timeRange
      Some(TimeRange(math.max(queryRange.startMs, range.startMs),
                     math.min(range.endMs + offsetMs, queryRange.endMs)))
    }
    // Snap remaining range starts to a step (if a step is provided).
    val tailRanges = filteredAssignments.tail.takeWhile(_.timeRange.startMs < queryRange.endMs).map { assign =>
      val startMs = if (stepMsOpt.nonEmpty) {
        snapToStep(timestamp = assign.timeRange.startMs + lookbackMs + offsetMs,
                   step = stepMsOpt.get,
                   origin = queryRange.startMs)
      } else {
        assign.timeRange.startMs + lookbackMs + offsetMs
      }
      val endMs = math.min(queryRange.endMs, assign.timeRange.endMs + offsetMs)
      if (startMs <= endMs) {
        Some(TimeRange(startMs, endMs))
      } else None
    }
    // Filter out the Nones and flatten the Somes.
    (Seq(headRange) ++ tailRanges).zip(filteredAssignments).filter(_._1.nonEmpty).map{ case (rangeOpt, part) =>
      (part, rangeOpt.get)
    }
  }

  // FIXME: this is a near-exact copy-paste of a method in the ShardKeyRegexPlanner --
  //  more evidence that these two classes should be merged.
  private def materializeAggregate(aggregate: Aggregate, queryContext: QueryContext): PlanResult = {
    val plan = if (LogicalPlanUtils.hasDescendantAggregateOrJoin(aggregate.vectors)) {
      val childPlan = materialize(aggregate.vectors, queryContext)
      addAggregator(aggregate, queryContext, PlanResult(Seq(childPlan)))
    } else {
      val queryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
      val (partitions, _, _, _) = resolvePartitionsAndRoutingKeys(aggregate, queryParams)
      val childQueryContext = queryContext.copy(
        plannerParams = queryContext.plannerParams.copy(skipAggregatePresent = true))
      val execPlans = partitions.map(p => materializeForPartition(aggregate, p, childQueryContext))
      val exec = if (execPlans.size == 1) execPlans.head
      else {
        if ((aggregate.operator.equals(AggregationOperator.TopK)
          || aggregate.operator.equals(AggregationOperator.BottomK)
          || aggregate.operator.equals(AggregationOperator.CountValues)
          ) && !canSupportMultiPartitionCalls(execPlans))
          throw new UnsupportedOperationException(s"Shard Key regex not supported for ${aggregate.operator}")
        else {
          val reducer = MultiPartitionReduceAggregateExec(queryContext, inProcessPlanDispatcher,
            execPlans.sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec]), aggregate.operator, aggregate.params)
          reducer.addRangeVectorTransformer(AggregatePresenter(aggregate.operator, aggregate.params,
            RangeParams(queryParams.startSecs, queryParams.stepSecs, queryParams.endSecs)))
          reducer
        }
      }
      exec
    }
    PlanResult(Seq(plan))
  }

  /**
   * Materialize any plan whose materialization strategy is governed by whether-or-not it
   *   contains leaves that individually span partitions.
   */
  private def materializePlanHandleSplitLeaf(logicalPlan: LogicalPlan,
                                             qContext: QueryContext): PlanResult = {
    val qParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    // (1) Find the set of shard-key filter groups for each plan.
    // (2) Resolve each group into a set of groups (e.g. if any regex filters exist).
    // (3) For each filter group, replace the filters of the source RawSeries plan.
    // (4) Identify whether-or-not any of the RawSeries plans would source data from multiple partitions.
    val hasMultiPartitionLeaves =
      LogicalPlan.findLeafLogicalPlans(logicalPlan)
        .filter(_.isInstanceOf[RawSeries])
        .flatMap { rs =>
          val rawFilters = LogicalPlan.getNonMetricShardKeyFilters(rs, dataset.options.nonMetricShardColumns)
          val filters = rawFilters.flatMap(shardKeyMatcher(_))
          filters.map(rs.replaceFilters)
        }
        .exists(getPartitions(_, qParams).size > 1)
    if (hasMultiPartitionLeaves) {
      materializeSplitLeafPlan(logicalPlan, qContext)
    } else { logicalPlan match {
      case agg: Aggregate => materializeAggregate(agg, qContext)
      case psw: PeriodicSeriesWithWindowing => materializeNonSplitPeriodicAndRawSeries(psw, qContext)
      case sqw: SubqueryWithWindowing => super.materializeSubqueryWithWindowing(qContext, sqw)
      case bj: BinaryJoin => materializeMultiPartitionBinaryJoinNoSplitLeaf(bj, qContext)
      case sv: ScalarVectorBinaryOperation => super.materializeScalarVectorBinOp(qContext, sv)
      case aif: ApplyInstantFunction => super.materializeApplyInstantFunction(qContext, aif)
      case svdp: ScalarVaryingDoublePlan => super.materializeScalarPlan(qContext, svdp)
      case aaf: ApplyAbsentFunction => super.materializeAbsentFunction(qContext, aaf)
      case ps: PeriodicSeries => materializeNonSplitPeriodicAndRawSeries(ps, qContext)
      case rcm: RawChunkMeta => materializeNonSplitPeriodicAndRawSeries(rcm, qContext)
      case rs: RawSeries => materializeNonSplitPeriodicAndRawSeries(rs, qContext)
      case x => throw new IllegalArgumentException(s"unhandled type: ${x.getClass}")
    }}
  }

  /**
   * Throws a BadQueryException if any of the following conditions hold:
   *     (1) the plan spans more than one non-metric shard key prefix.
   *     (2) the plan contains at least one BinaryJoin, and any of its BinaryJoins contain an offset.
   * @param splitLeafPlan must contain leaf plans that individually span multiple partitions.
   */
  private def validateSplitLeafPlan(splitLeafPlan: LogicalPlan): Unit = {
    val baseErrorMessage = "This query contains selectors that individually read data from multiple partitions. " +
                           "This is likely because a selector's data was migrated between partitions. "
    if (hasBinaryJoin(splitLeafPlan) && getOffsetMillis(splitLeafPlan).exists(_ > 0)) {
      throw new BadQueryException( baseErrorMessage +
          "These \"split\" queries cannot contain binary joins with offsets."
      )
    }
    lazy val hasMoreThanOneNonMetricShardKey =
      getNonMetricShardKeyFilters(splitLeafPlan, dataset.options.nonMetricShardColumns)
        .filter(_.nonEmpty).distinct.size > 1
    if (hasMoreThanOneNonMetricShardKey) {
      throw new BadQueryException( baseErrorMessage +
          "These \"split\" queries are not supported if they contain multiple non-metric shard keys."
      )
    }
  }

  /**
   * Materializes a LogicalPlan with leaves that individually span multiple partitions.
   * All "split-leaf" plans will fail to materialize (throw a BadQueryException) if they
   *   span more than one non-metric shard key prefix.
   * Split-leaf plans that contain at least one BinaryJoin will additionally fail to materialize
   *   if any of the plan's BinaryJoins contain an offset.
   */
  private def materializeSplitLeafPlan(logicalPlan: LogicalPlan,
                                       qContext: QueryContext): PlanResult = {
    validateSplitLeafPlan(logicalPlan)
    val qParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    // get a mapping of assignments to time-ranges to query
    val assignmentRanges = {
      // "distinct" in case this is a BinaryJoin
      val partitions = getPartitions(logicalPlan, qParams).distinct.sortBy(_.timeRange.startMs)
      val timeRange = TimeRange(1000 * qParams.startSecs, 1000 * qParams.endSecs)
      val lookbackMs = getLookBackMillis(logicalPlan).max
      val offsetMs = getOffsetMillis(logicalPlan).max
      val stepMsOpt = if (qParams.startSecs == qParams.endSecs) None else Some(1000 * qParams.stepSecs)
      getAssignmentQueryRanges(partitions, timeRange,
        lookbackMs = lookbackMs, offsetMs = offsetMs, stepMsOpt = stepMsOpt)
    }
    // materialize a plan for each range/assignment pair
    val plans = assignmentRanges.map { case (part, range) =>
      val newParams = qParams.copy(startSecs = range.startMs / 1000,
                                   endSecs = range.endMs / 1000)
      val newContext = qContext.copy(origQueryParams = newParams)
      materializeForPartition(logicalPlan, part, newContext)
    }
    // stitch if necessary
    val resPlan = if (plans.size == 1) {
      plans.head
    } else {
      // returns NaNs for missing timestamps
      val rvRange = RvRange(1000 * qParams.startSecs,
                            1000 * qParams.stepSecs,
                            1000 * qParams.endSecs)
      StitchRvsExec(qContext, inProcessPlanDispatcher, Some(rvRange), plans)
    }
    PlanResult(Seq(resPlan))
  }

  /**
   * Materialize a BinaryJoin whose individual leaf plans do not span partitions.
   */
  private def materializeMultiPartitionBinaryJoinNoSplitLeaf(logicalPlan: BinaryJoin,
                                                             qContext: QueryContext): PlanResult = {
    val lhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.lhs)))
    val rhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.rhs)))

    val lhsExec = this.materialize(logicalPlan.lhs, lhsQueryContext)
    val rhsExec = this.materialize(logicalPlan.rhs, rhsQueryContext)

    val execPlan = if (logicalPlan.operator.isInstanceOf[SetOperator])
      SetOperatorExec(qContext, InProcessPlanDispatcher(queryConfig), Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
        LogicalPlanUtils.renameLabels(logicalPlan.on, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn), datasetMetricColumn,
        rvRangeFromPlan(logicalPlan))
    else
      BinaryJoinExec(qContext, inProcessPlanDispatcher, Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
        logicalPlan.cardinality, LogicalPlanUtils.renameLabels(logicalPlan.on, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.include, datasetMetricColumn), datasetMetricColumn,
        rvRangeFromPlan(logicalPlan))
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


    // LabelCardinality is a special case, here the partitions to send this query to is not  the authorized partition
    // but the actual one where data resides, similar to how non metadata plans work, however, getting label cardinality
    // is a metadata operation and shares common components with other metadata endpoints.
    val partitions = lp match {
      case lc: LabelCardinality       => getPartitions(lc, qContext.origQueryParams.asInstanceOf[PromQlQueryParams])
      case _                          => getMetadataPartitions(lp.filters,
        TimeRange(queryParams.startSecs * 1000, queryParams.endSecs * 1000))
    }

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
                 _: LabelCardinality    => Map("match[]" -> LogicalPlanParser.metadataMatchToQuery(lp))
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

  def materializeTsCardinalities(lp: TsCardinalities, qContext: QueryContext): PlanResult = {

    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val partitions = if (lp.shardKeyPrefix.size >= 2) {
      // At least a ws/ns pair is required to select specific partitions.
      getPartitions(lp, queryParams, infiniteTimeRange = true)
    } else {
      logger.info(s"(ws, ns) pair not provided in prefix=${lp.shardKeyPrefix};" +
                  s"dispatching to all authorized partitions")
      getMetadataPartitions(lp.filters(), TimeRange(0, Long.MaxValue))
    }
    val execPlan = if (partitions.isEmpty) {
      logger.warn(s"no partitions found for $lp; defaulting to local planner")
      localPartitionPlanner.materialize(lp, qContext)
    } else {
      val execPlans = partitions.map { p =>
        logger.debug(s"partition=$p; plan=$lp")
        if (p.partitionName.equals(localPartitionName))
          localPartitionPlanner.materialize(lp, qContext)
        else {
          val newQueryContext = qContext.copy(origQueryParams = queryParams.copy(verbose = true))
          createMetadataRemoteExec(newQueryContext, p, lp.queryParams())
        }
      }
      if (execPlans.size == 1) {
        execPlans.head
      } else {
        TsCardReduceExec(qContext, inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[MetadataRemoteExec]))
      }
    }
    PlanResult(execPlan::Nil)
  }

  def getMetadataPartitions(filters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] = {
    val nonMetricShardKeyFilters = filters.filter(f => dataset.options.nonMetricShardColumns.contains(f.column))
    partitionLocationProvider.getMetadataPartitions(nonMetricShardKeyFilters, timeRange)
  }

  private def createMetadataRemoteExec(qContext: QueryContext, partitionAssignment: PartitionAssignment,
                                       urlParams: Map[String, String]) = {
    val finalQueryContext = generateRemoteExecParams(
      qContext, "<metadata>", partitionAssignment.timeRange.startMs, partitionAssignment.timeRange.endMs)
    val httpEndpoint = partitionAssignment.httpEndPoint +
      finalQueryContext.origQueryParams.asInstanceOf[PromQlQueryParams].remoteQueryPath.getOrElse("")
    MetadataRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
      urlParams, finalQueryContext, inProcessPlanDispatcher, dataset.ref, remoteExecHttpClient, queryConfig)
  }
}
