package filodb.coordinator.queryplanner

import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.queryplanner.LogicalPlanUtils._
import filodb.core.metadata.{Dataset, DatasetOptions, Schemas}
import filodb.core.query.{QueryConfig, QueryContext}
import filodb.query._
import filodb.query.exec._

/**
  * LongTimeRangePlanner knows about limited retention of raw data, and existence of downsampled data.
  * For any query that arrives beyond the retention period of raw data, it splits the query into
  * two time ranges - latest time range from raw data, and old time range from downsampled data.
  * It then stitches the subquery results to present top level query results for entire time range.
  *
  * @param rawClusterPlanner this planner (typically a SingleClusterPlanner) abstracts planning for raw cluster data
  * @param downsampleClusterPlanner this planner (typically a SingleClusterPlanner)
  *                                 abstracts planning for downsample cluster data
  * @param earliestRawTimestampFn the function that will provide millis timestamp of earliest sample that
  *                             would be available in the raw cluster
  * @param latestDownsampleTimestampFn the function that will provide millis timestamp of newest sample
  *                                    that would be available in the downsample cluster. This typically
  *                                    is not "now" because of the delay in population of downsampled data
  *                                    via spark job. If job is run every 6 hours,
  *                                    `(System.currentTimeMillis - 12.hours.toMillis)`
  *                                    may a function that could be passed. 12 hours to account for failures/reruns etc.
  * @param stitchDispatcher function to get the dispatcher for the stitch exec plan node
  */
  class LongTimeRangePlanner(rawClusterPlanner: QueryPlanner,
                             downsampleClusterPlanner: QueryPlanner,
                             earliestRawTimestampFn: => Long,
                             latestDownsampleTimestampFn: => Long,
                             stitchDispatcher: => PlanDispatcher,
                             val queryConfig: QueryConfig,
                             val dataset: Dataset) extends QueryPlanner
                             with DefaultPlanner with StrictLogging {
  override val schemas: Schemas = Schemas(dataset.schema)
  override val dsOptions: DatasetOptions = schemas.part.options


  private def materializePeriodicSeriesPlan(qContext: QueryContext, periodicSeriesPlan: PeriodicSeriesPlan) = {
    val execPlan = if (!periodicSeriesPlan.isRoutable)
      rawClusterPlanner.materialize(periodicSeriesPlan, qContext)
    else
      materializeRoutablePlan(qContext, periodicSeriesPlan)
    PlanResult(Seq(execPlan))
  }

  // scalastyle:off method.length
  private def materializeRoutablePlan(qContext: QueryContext, periodicSeriesPlan: PeriodicSeriesPlan): ExecPlan = {
    import LogicalPlan._
    val earliestRawTime = earliestRawTimestampFn
    val offsetMillis = LogicalPlanUtils.getOffsetMillis(periodicSeriesPlan)
    val (maxOffset, minOffset) = (offsetMillis.max, offsetMillis.min)
    require(!periodicSeriesPlan.hasAtModifier || periodicSeriesPlan.getDataSource(earliestRawTime) != 2,
      s"$periodicSeriesPlan @modifier and query range should be all greater or less than $earliestRawTime")

    val lookbackMs = LogicalPlanUtils.getLookBackMillis(periodicSeriesPlan).max
    val startWithOffsetMs = periodicSeriesPlan.startMs - maxOffset
    // For scalar binary operation queries like sum(rate(foo{job = "app"}[5m] offset 8d)) * 0.5
    val endWithOffsetMs = periodicSeriesPlan.endMs - minOffset
    if (maxOffset != minOffset
          && startWithOffsetMs - lookbackMs < earliestRawTime
          && endWithOffsetMs >= earliestRawTime) {
          // If we get maxOffset != minOffset it means we have some Binary join in our LogicalPlan that
          // has different offsets, for example sum(foo{} offset 8d + bar{}), here the top level operation
          // is aggregation on a binary joined data with offset. Here we fall back to the default implementations in
          // DefaultPlanner
          // Also, consider the case sum(foo{}) + sum(bar{} offset 8d), its a PeriodicSeriesPlan with two offset values
          // This can be done most efficiently find a point in time that can be pushed down completely to LT,
          // completely in Raw and minimal required data in QS and stitch all together. However currently will we
          // delegate to the default implementation of this logical plan, which would perform the operations
          // in-process in DefaultPlanner
          // TODO: Operations like sum(foo{} offset 8d) + 10 should be entirely pushed down despite the offset
          //  however, right now this will just push then aggregation depending on offset or in worst case perform the
          //  operation InProcess. This needs to be addressed in subsequent enhancements.
         super.defaultWalkLogicalPlanTree(periodicSeriesPlan, qContext).plans.head
    }
    else if (endWithOffsetMs < earliestRawTime) { // full time range in downsampled cluster
      logger.debug("materializing against downsample cluster:: {}", qContext.origQueryParams)
      downsampleClusterPlanner.materialize(periodicSeriesPlan, qContext)
    } else if (startWithOffsetMs - lookbackMs >= earliestRawTime) { // full time range in raw cluster
      rawClusterPlanner.materialize(periodicSeriesPlan, qContext)
      // "(endWithOffsetMs - lookbackMs) < earliestRawTime" check is erroneous, we claim that we have
      // a long lookback only if the last lookback window overlaps with earliestRawTime, however, we
      // should check for ANY interval overalapping earliestRawTime. We
      // can happen with ANY lookback interval, not just the last one.
    } else if (
      endWithOffsetMs - lookbackMs < earliestRawTime || //TODO lookbacks can overlap in the middle intervals too
        LogicalPlan.hasSubqueryWithWindowing(periodicSeriesPlan)
    ) {
      // For subqueries and long lookback queries, we keep things simple by routing to
      // downsample cluster since dealing with lookback windows across raw/downsample
      // clusters is quite complex and is not in scope now. We omit recent instants for which downsample
      // cluster may not have complete data
      val lastDownsampleSampleTime = latestDownsampleTimestampFn
      val downsampleLp = if (endWithOffsetMs < lastDownsampleSampleTime) {
        periodicSeriesPlan
      } else {
        // TODO: can be a bug, suppose start=end and this is a query like:
        // avg_over_time(foo[7d]) or avg_over_time(foo[7d:1d])
        // these queries are not splittable by design and should keep their start and end the same
        // the queestion is only from which cluster: raw or downsample to pull potentially partial data
        // how does copyLogicalPlan with updated end changes the meanings of these queries entirely
        // Why not get rid of this else all together and just send original logical plan to downsample cluster?

        // latestDownsampleTimestampFn + offsetMillis.min gives time in milliseconds and some of our plans like
        // ScalarFixedDoublePlan accept times in seconds. Thus cases like sum(rate(foo{}[longtime])) or vector(0)
        // this mismatch in end time for LHS and RHS causes the Binary join object creation to fail and even plan
        // materialize to fail.
        copyLogicalPlanWithUpdatedTimeRange(periodicSeriesPlan,
          TimeRange(periodicSeriesPlan.startMs, (latestDownsampleTimestampFn + offsetMillis.min) / 1000 * 1000))
      }
      logger.debug("materializing against downsample cluster:: {}", qContext.origQueryParams)
      downsampleClusterPlanner.materialize(downsampleLp, qContext)
    } else { // raw/downsample overlapping query without long lookback
      // Split the query between raw and downsample planners
      // Note - should never arrive here when start == end (so step never 0)
      require(periodicSeriesPlan.stepMs > 0,
        "Step was 0 when trying to split query between raw and downsample cluster")
      val numStepsInDownsample = (earliestRawTime - startWithOffsetMs + lookbackMs) / periodicSeriesPlan.stepMs
      val lastDownsampleInstant = periodicSeriesPlan.startMs + numStepsInDownsample * periodicSeriesPlan.stepMs
      val firstInstantInRaw = lastDownsampleInstant + periodicSeriesPlan.stepMs

      val downsampleLp = copyLogicalPlanWithUpdatedTimeRange(periodicSeriesPlan,
        TimeRange(periodicSeriesPlan.startMs, lastDownsampleInstant))
      val downsampleEp = downsampleClusterPlanner.materialize(downsampleLp, qContext)
      logger.debug("materializing against downsample cluster:: {}", qContext.origQueryParams)

      val rawLp = copyLogicalPlanWithUpdatedTimeRange(periodicSeriesPlan, TimeRange(firstInstantInRaw,
        periodicSeriesPlan.endMs))
      val rawEp = rawClusterPlanner.materialize(rawLp, qContext)
      StitchRvsExec(qContext, stitchDispatcher, rvRangeFromPlan(periodicSeriesPlan),
        Seq(rawEp, downsampleEp))
    }
  }
  // scalastyle:on method.length

  private def rawClusterMaterialize(qContext: QueryContext, logicalPlan: LogicalPlan): PlanResult = {
    PlanResult(Seq(rawClusterPlanner.materialize(logicalPlan, qContext)))
  }

  /**
   * Materializes Label cardinality plan, the entire plan is split in long term cluster and raw cluster
   * The split will be performed in the following fashion if the user provided start and end time spans across
   * earliestRawTs and latestDSTS
   *
   *                  |------------------------|-----------------|-------------------------------|
   *                  ^                        ^                 ^                               ^
   *    User provided start time          earliestRawTS      latestDSTS                User provided end time
   *
   *
   *                           <merge sketches to get total cardinality>
   *                                  |                           \
   *                        <Sketch from DS Cluster>          <Sketch from Raw Cluster>
   *                             |                                   |
   *                 |------------------------|             |------------------------|
   *                 ^                        ^             ^                        ^
   *   User provided start time           earliestRawTS   earliestRawTS            User provided end time
   *
   * @param logicalPlan           The LabelCardinality logical plan to materialize
   * @param queryContext          The QueryContext object
   * @return
   */
  private def materializeLabelCardinalityPlan(logicalPlan: LabelCardinality, queryContext: QueryContext): PlanResult = {
    val (startTime, endTime) = (logicalPlan.startMs, logicalPlan.endMs)
    val execPlan = if (startTime > latestDownsampleTimestampFn) {
      // This is the case where no data cardinality will be retrieved from DownsampleStore, simply push down to
      // raw planner
      rawClusterPlanner.materialize(logicalPlan, queryContext)
    } else if (endTime < earliestRawTimestampFn) {
      // This is the case where no data cardinality will be retrieved from RawStore, simply push down to
      // DS planner
      downsampleClusterPlanner.materialize(logicalPlan, queryContext)
    } else {
      // There is a split, send to DS and Raw planners and get sketches from them without the presenter,
      // perform a local reduction to merge the sketches and add a presenter
      val dsLogicalPlan = logicalPlan.copy(endMs = earliestRawTimestampFn, clusterType = "downsample")
      val rawLogicalPlan = logicalPlan.copy(startMs = earliestRawTimestampFn)
      val qCtx = queryContext.copy(plannerParams = queryContext.plannerParams.copy(skipAggregatePresent = true))

      val dsPlan = downsampleClusterPlanner.materialize(dsLogicalPlan, qCtx)
      val rawPlan = rawClusterPlanner.materialize(rawLogicalPlan, qCtx)

      val reduceExec = LabelCardinalityReduceExec(queryContext, stitchDispatcher, Seq(dsPlan, rawPlan))
      if (!queryContext.plannerParams.skipAggregatePresent) {
        reduceExec.addRangeVectorTransformer(new LabelCardinalityPresenter())
      }
      reduceExec
    }
    PlanResult(Seq(execPlan))
  }

  /**
   * Materialize Ts cardinality plan. For v1 version, we only go to raw cluster for back compatibility. For v2 versions,
   * we would go to both downsample and raw cluster
   *
   * @param logicalPlan  The TsCardinalities logical plan to materialize
   * @param queryContext The QueryContext object
   * @return
   */
  private def materializeTSCardinalityPlan(queryContext: QueryContext, logicalPlan: TsCardinalities): PlanResult = {
    logicalPlan.version match {
      case 2 => {
        val rawPlan = rawClusterPlanner.materialize(logicalPlan, queryContext)
        val dsPlan = downsampleClusterPlanner.materialize(logicalPlan, queryContext)
        val stitchedPlan = TsCardReduceExec(queryContext, stitchDispatcher, Seq(rawPlan, dsPlan))
        PlanResult(Seq(stitchedPlan))
      }
      // version 1 defaults to raw as done before
      case 1 => rawClusterMaterialize(queryContext, logicalPlan)
      case _ => throw new UnsupportedOperationException(s"version ${logicalPlan.version} not supported!")
    }
  }

  // scalastyle:off cyclomatic.complexity
  override def walkLogicalPlanTree(logicalPlan: LogicalPlan,
                                   qContext: QueryContext,
                                   forceInProcess: Boolean = false): PlanResult = {

    if (!LogicalPlanUtils.hasBinaryJoin(logicalPlan)) {
      logicalPlan match {
        case p: PeriodicSeriesPlan         => materializePeriodicSeriesPlan(qContext, p)
        case lc: LabelCardinality          => materializeLabelCardinalityPlan(lc, qContext)
        case ts: TsCardinalities           => materializeTSCardinalityPlan(qContext, ts)
        case _: LabelValues |
             _: ApplyLimitFunction |
             _: SeriesKeysByFilters |
             _: ApplyInstantFunctionRaw |
             _: RawSeries |
             _: LabelNames                 => rawClusterMaterialize(qContext, logicalPlan)
      }
    }
    else logicalPlan match {
      case lp: RawSeries                   => rawClusterMaterialize(qContext, lp)
      case lp: RawChunkMeta                => rawClusterMaterialize(qContext, lp)
      case lp: PeriodicSeries              => materializePeriodicSeriesPlan(qContext, lp)
      case lp: PeriodicSeriesWithWindowing => materializePeriodicSeriesPlan(qContext, lp)
      case lp: ApplyInstantFunction        => super.materializeApplyInstantFunction(qContext, lp)
      case lp: ApplyInstantFunctionRaw     => super.materializeApplyInstantFunctionRaw(qContext, lp)
      case lp: Aggregate                   => materializePeriodicSeriesPlan(qContext, lp)
      case lp: BinaryJoin                  => materializePeriodicSeriesPlan(qContext, lp)
      case lp: ScalarVectorBinaryOperation => super.materializeScalarVectorBinOp(qContext, lp)
      case lp: LabelValues                 => rawClusterMaterialize(qContext, lp)
      case lp: TsCardinalities             => materializeTSCardinalityPlan(qContext, lp)
      case lp: SeriesKeysByFilters         => rawClusterMaterialize(qContext, lp)
      case lp: ApplyMiscellaneousFunction  => super.materializeApplyMiscellaneousFunction(qContext, lp)
      case lp: ApplySortFunction           => super.materializeApplySortFunction(qContext, lp)
      case lp: ScalarVaryingDoublePlan     => super.materializeScalarPlan(qContext, lp)
      case lp: ScalarTimeBasedPlan         => super.materializeScalarTimeBased(qContext, lp)
      case lp: VectorPlan                  => super.materializeVectorPlan(qContext, lp)
      case lp: ScalarFixedDoublePlan       => super.materializeFixedScalar(qContext, lp)
      case lp: ApplyAbsentFunction         => super.materializeAbsentFunction(qContext, lp)
      case lp: ScalarBinaryOperation       => super.materializeScalarBinaryOperation(qContext, lp)
      case lp: SubqueryWithWindowing       => materializePeriodicSeriesPlan(qContext, lp)
      case lp: TopLevelSubquery            => super.materializeTopLevelSubquery(qContext, lp)
      case lp: ApplyLimitFunction          => rawClusterMaterialize(qContext, lp)
      case lp: LabelNames                  => rawClusterMaterialize(qContext, lp)
      case lp: LabelCardinality            => materializeLabelCardinalityPlan(lp, qContext)
    }
    // scalastyle:on cyclomatic.complexity
  }

  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    walkLogicalPlanTree(logicalPlan, qContext).plans.head
  }
}
