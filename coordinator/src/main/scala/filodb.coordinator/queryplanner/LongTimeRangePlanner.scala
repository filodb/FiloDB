package filodb.coordinator.queryplanner

import com.typesafe.scalalogging.StrictLogging
import filodb.coordinator.queryplanner.LogicalPlanUtils._
import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.query.{PromQlQueryParams, QueryConfig, QueryContext}
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
                           config: QueryConfig,
                           dataset1: Dataset) extends QueryPlanner with
  PlannerMaterializer with StrictLogging {

  override def queryConfig: QueryConfig = config
  val inProcessPlanDispatcher = InProcessPlanDispatcher(queryConfig)
  override def dataset: Dataset = dataset1
  override def schemas: Schemas = Schemas(dataset.schema)
  private val datasetMetricColumn = dsOptions.metricColumn


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
      logger.info("materializing against downsample cluster:: {}", qContext.origQueryParams)
      downsampleClusterPlanner.materialize(periodicSeriesPlan, qContext)
    } else if (startWithOffsetMs - lookbackMs >= earliestRawTime) // full time range in raw cluster
      rawClusterPlanner.materialize(periodicSeriesPlan, qContext)
    // the below looks like a bug, probably should be "<=", check case in LongTimeRangePlannerSpec
    // "TODO here we have raw plan start bigger than the actual end of top plan"
    // in the last "else" firstInstantInRaw can end up to be bigger than lastDownsampleInstant
    else if (endWithOffsetMs - lookbackMs < earliestRawTime) { // raw/downsample overlapping query with long lookback
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
      logger.info("materializing against downsample cluster:: {}", qContext.origQueryParams)
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
      logger.info("materializing against downsample cluster:: {}", qContext.origQueryParams)

      val rawLp = copyLogicalPlanWithUpdatedTimeRange(periodicSeriesPlan, TimeRange(firstInstantInRaw,
        periodicSeriesPlan.endMs))
      val rawEp = rawClusterPlanner.materialize(rawLp, qContext)
      StitchRvsExec(qContext, stitchDispatcher, Seq(rawEp, downsampleEp))
    }
    PlanResult(Seq(execPlan), false)
  }


  def materializeBinaryJoin(qContext: QueryContext, logicalPlan: BinaryJoin): PlanResult = {

    val lhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.lhs)))
    val rhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.rhs)))

    val lhs = walkLogicalPlanTree(logicalPlan.lhs, lhsQueryContext)
    val rhs = walkLogicalPlanTree(logicalPlan.rhs, rhsQueryContext)

//    val lhsExec = logicalPlan.lhs match {
//      case b: BinaryJoin   => materializeBinaryJoin(b, lhsQueryContext)
//      case               _ => materializePeriodicSeriesPlan(logicalPlan.lhs, lhsQueryContext)
//    }
//
//    val rhsExec = logicalPlan.rhs match {
//      case b: BinaryJoin => materializeBinaryJoin(b, rhsQueryContext)
//      case _             => materializePeriodicSeriesPlan(logicalPlan.rhs, rhsQueryContext)
//    }

    val onKeysReal = ExtraOnByKeysUtil.getRealOnLabels(logicalPlan, queryConfig.addExtraOnByKeysTimeRanges)

    val dispatcher = if (!lhs.plans.head.dispatcher.isLocalCall && !rhs.plans.head.dispatcher.isLocalCall) {
      val lhsCluster = lhs.plans.head.dispatcher.clusterName
      val rhsCluster = rhs.plans.head.dispatcher.clusterName
      if (rhsCluster.equals(lhsCluster)) PlannerUtil.pickDispatcher(lhs.plans ++ rhs.plans)
      else inProcessPlanDispatcher
    } else inProcessPlanDispatcher

    val stitchedLhs = if (lhs.needsStitch) Seq(StitchRvsExec(qContext,
      dispatcher, lhs.plans))
    else lhs.plans

    val stitchedRhs = if (rhs.needsStitch) Seq(StitchRvsExec(qContext,
      dispatcher, rhs.plans))
    else rhs.plans
  val execPlan =
  if (logicalPlan.operator.isInstanceOf[SetOperator])
      SetOperatorExec(qContext, dispatcher, stitchedLhs, stitchedRhs, logicalPlan.operator,
        LogicalPlanUtils.renameLabels(onKeysReal, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn), datasetMetricColumn)
    else
      BinaryJoinExec(qContext, dispatcher, stitchedLhs, stitchedRhs, logicalPlan.operator,
        logicalPlan.cardinality, LogicalPlanUtils.renameLabels(onKeysReal, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn), logicalPlan.include,
        datasetMetricColumn)

   PlanResult(Seq(execPlan), false)
  }
   ApplyInstantFunction

//  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
//    logicalPlan match {
//      case b: BinaryJoin         => materializeBinaryJoin(b, qContext)
//      case p: PeriodicSeriesPlan => materializePeriodicSeriesPlan(p, qContext)
//       // Metadata query not supported for downsample cluster
//      case _                     => rawClusterPlanner.materialize(logicalPlan, qContext)
//    }
//  }

  override def walkLogicalPlanTree(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {
    logicalPlan match {
      //        case lp: RawSeries                   => materializeRawSeries(qContext, lp)
      //        case lp: RawChunkMeta                => materializeRawChunkMeta(qContext, lp)
      case lp: PeriodicSeries              => materializePeriodicSeriesPlan(qContext, lp)
      case lp: PeriodicSeriesWithWindowing => materializePeriodicSeriesPlan(qContext, lp)
      case lp: ApplyInstantFunction        => materializeApplyInstantFunction(qContext, lp)
      case lp: ApplyInstantFunctionRaw     => materializeApplyInstantFunctionRaw(qContext, lp)
      case lp: Aggregate                   => materializeAggregate(qContext, lp)
      case lp: BinaryJoin                  => materializeBinaryJoin(qContext, lp)
      case lp: ScalarVectorBinaryOperation => materializeScalarVectorBinOp(qContext, lp)
//      case lp: LabelValues                 => materializeLabelValues(qContext, lp)
//      case lp: SeriesKeysByFilters         => materializeSeriesKeysByFilters(qContext, lp)
      case lp: ApplyMiscellaneousFunction  => materializeApplyMiscellaneousFunction(qContext, lp)
      case lp: ApplySortFunction           => materializeApplySortFunction(qContext, lp)
      case lp: ScalarVaryingDoublePlan     => materializeScalarPlan(qContext, lp)
      //case lp: ScalarTimeBasedPlan         => materializeScalarTimeBased(qContext, lp)
      case lp: VectorPlan                  => materializeVectorPlan(qContext, lp)
    //  case lp: ScalarFixedDoublePlan       =>  materializeFixedScalar(qContext, lp)
      case lp: ApplyAbsentFunction         => materializeAbsentFunction(qContext, lp)
      //case lp: ScalarBinaryOperation       => materializeScalarBinaryOperation(qContext, lp)
      case lp: SubqueryWithWindowing       => materializeSubqueryWithWindowing(qContext, lp)
      case lp: TopLevelSubquery            => materializeTopLevelSubquery(qContext, lp)
      case _                               => PlanResult(Seq(rawClusterPlanner.materialize(logicalPlan, qContext)),
        false)
    }
  }

  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
  //TODO like materializesplitplans in walk
    walkLogicalPlanTree(logicalPlan, qContext).plans.head
  }
}
