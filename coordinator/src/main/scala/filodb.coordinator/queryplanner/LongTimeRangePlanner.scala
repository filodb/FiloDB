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
                             with PlannerHelper with StrictLogging {
  override val schemas: Schemas = Schemas(dataset.schema)
  override val dsOptions: DatasetOptions = schemas.part.options

  // scalastyle:off method.length
  private def materializePeriodicSeriesPlan(qContext: QueryContext, periodicSeriesPlan: PeriodicSeriesPlan) = {
    val earliestRawTime = earliestRawTimestampFn
    lazy val offsetMillis = LogicalPlanUtils.getOffsetMillis(periodicSeriesPlan)
    lazy val lookbackMs = LogicalPlanUtils.getLookBackMillis(periodicSeriesPlan).max
    lazy val startWithOffsetMs = periodicSeriesPlan.startMs - offsetMillis.max
    // For scalar binary operation queries like sum(rate(foo{job = "app"}[5m] offset 8d)) * 0.5
    lazy val endWithOffsetMs = periodicSeriesPlan.endMs - offsetMillis.max
    val execPlan = if (!periodicSeriesPlan.isRoutable)
      rawClusterPlanner.materialize(periodicSeriesPlan, qContext)
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
        copyLogicalPlanWithUpdatedTimeRange(periodicSeriesPlan,
          TimeRange(periodicSeriesPlan.startMs, latestDownsampleTimestampFn + offsetMillis.min))
      }
      logger.debug("materializing against downsample cluster:: {}", qContext.origQueryParams)
      downsampleClusterPlanner.materialize(downsampleLp, qContext)
    } else { // raw/downsample overlapping query without long lookback
      // Split the query between raw and downsample planners
      // Note - should never arrive here when start == end (so step never 0)
      require(periodicSeriesPlan.stepMs > 0, "Step was 0 when trying to split query between raw and downsample cluster")
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
      StitchRvsExec(qContext, stitchDispatcher, Seq(rawEp, downsampleEp))
    }
    PlanResult(Seq(execPlan))
  }
  // scalastyle:on method.length

  def rawClusterMaterialize(qContext: QueryContext, logicalPlan: LogicalPlan): PlanResult = {
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

  // scalastyle:off cyclomatic.complexity
  override def walkLogicalPlanTree(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {

    if (!LogicalPlanUtils.hasBinaryJoin(logicalPlan)) {
      logicalPlan match {
        case p: PeriodicSeriesPlan         => materializePeriodicSeriesPlan(qContext, p)
        case lc: LabelCardinality          => materializeLabelCardinalityPlan(lc, qContext)
        case _: LabelValues |
             _: ApplyLimitFunction |
             _: SeriesKeysByFilters |
             _: ApplyInstantFunctionRaw |
             _: RawSeries |
             _: LabelNames |
             _: TsCardinalities            => rawClusterMaterialize(qContext, logicalPlan)
      }
    }
    else logicalPlan match {
      case lp: RawSeries                   => rawClusterMaterialize(qContext, lp)
      case lp: RawChunkMeta                => rawClusterMaterialize(qContext, lp)
      case lp: PeriodicSeries              => materializePeriodicSeriesPlan(qContext, lp)
      case lp: PeriodicSeriesWithWindowing => materializePeriodicSeriesPlan(qContext, lp)
      case lp: ApplyInstantFunction        => materializeApplyInstantFunction(qContext, lp)
      case lp: ApplyInstantFunctionRaw     => materializeApplyInstantFunctionRaw(qContext, lp)
      case lp: Aggregate                   => materializeAggregate(qContext, lp)
      case lp: BinaryJoin                  => materializeBinaryJoin(qContext, lp)
      case lp: ScalarVectorBinaryOperation => materializeScalarVectorBinOp(qContext, lp)
      case lp: LabelValues                 => rawClusterMaterialize(qContext, lp)
      case lp: TsCardinalities             => rawClusterMaterialize(qContext, lp)
      case lp: SeriesKeysByFilters         => rawClusterMaterialize(qContext, lp)
      case lp: ApplyMiscellaneousFunction  => materializeApplyMiscellaneousFunction(qContext, lp)
      case lp: ApplySortFunction           => materializeApplySortFunction(qContext, lp)
      case lp: ScalarVaryingDoublePlan     => materializeScalarPlan(qContext, lp)
      case lp: ScalarTimeBasedPlan         => materializeScalarTimeBased(qContext, lp)
      case lp: VectorPlan                  => materializeVectorPlan(qContext, lp)
      case lp: ScalarFixedDoublePlan       => materializeFixedScalar(qContext, lp)
      case lp: ApplyAbsentFunction         => materializeAbsentFunction(qContext, lp)
      case lp: ScalarBinaryOperation       => materializeScalarBinaryOperation(qContext, lp)
      case lp: SubqueryWithWindowing       => materializePeriodicSeriesPlan(qContext, lp)
      case lp: TopLevelSubquery            => materializeTopLevelSubquery(qContext, lp)
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
