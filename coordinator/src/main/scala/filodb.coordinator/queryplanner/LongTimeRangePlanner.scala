package filodb.coordinator.queryplanner

import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.queryplanner.LogicalPlanUtils._
import filodb.core.query.{PromQlQueryParams, QueryConfig, QueryContext}
import filodb.query.{BinaryJoin, LogicalPlan, PeriodicSeriesPlan, SetOperator}
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
                           queryConfig: QueryConfig,
                           datasetMetricColumn: String) extends QueryPlanner with StrictLogging {

  val inProcessPlanDispatcher = InProcessPlanDispatcher(queryConfig)

  //scalastyle:off method.length
  private def materializePeriodicSeriesPlan(periodicSeriesPlan: PeriodicSeriesPlan, qContext: QueryContext) = {
    val earliestRawTime = earliestRawTimestampFn
    lazy val offsetMillis = LogicalPlanUtils.getOffsetMillis(periodicSeriesPlan)
    lazy val lookbackMs = LogicalPlanUtils.getLookBackMillis(periodicSeriesPlan).max
    lazy val startWithOffsetMs = periodicSeriesPlan.startMs - offsetMillis.max
    // For scalar binary operation queries like sum(rate(foo{job = "app"}[5m] offset 8d)) * 0.5
    lazy val endWithOffsetMs = periodicSeriesPlan.endMs - offsetMillis.max
    if (!periodicSeriesPlan.isRoutable)
      rawClusterPlanner.materialize(periodicSeriesPlan, qContext)
    else if (endWithOffsetMs < earliestRawTime) { // full time range in downsampled cluster
      logger.info("materializing against downsample cluster:: {}", qContext.origQueryParams)
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
  }

  def materializeBinaryJoin(logicalPlan: BinaryJoin, qContext: QueryContext): ExecPlan = {
    val lhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.lhs)))
    val rhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.rhs)))

    val lhsExec = logicalPlan.lhs match {
      case b: BinaryJoin   => materializeBinaryJoin(b, lhsQueryContext)
      case               _ => materializePeriodicSeriesPlan(logicalPlan.lhs, lhsQueryContext)
    }

    val rhsExec = logicalPlan.rhs match {
      case b: BinaryJoin => materializeBinaryJoin(b, rhsQueryContext)
      case _             => materializePeriodicSeriesPlan(logicalPlan.rhs, rhsQueryContext)
    }

    val onKeysReal = ExtraOnByKeysUtil.getRealOnLabels(logicalPlan, queryConfig.addExtraOnByKeysTimeRanges)

    val dispatcher = if (!lhsExec.dispatcher.isLocalCall && !rhsExec.dispatcher.isLocalCall) {
      val lhsCluster = lhsExec.dispatcher.clusterName
      val rhsCluster = rhsExec.dispatcher.clusterName
      if (rhsCluster.equals(lhsCluster)) PlannerUtil.pickDispatcher(lhsExec.children ++ rhsExec.children)
      else inProcessPlanDispatcher
    } else inProcessPlanDispatcher

    if (logicalPlan.operator.isInstanceOf[SetOperator])
      SetOperatorExec(qContext, dispatcher, Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
        LogicalPlanUtils.renameLabels(onKeysReal, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn), datasetMetricColumn)
    else
      BinaryJoinExec(qContext, dispatcher, Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
        logicalPlan.cardinality, LogicalPlanUtils.renameLabels(onKeysReal, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn), logicalPlan.include,
        datasetMetricColumn)

  }
  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    logicalPlan match {
      case b: BinaryJoin         => materializeBinaryJoin(b, qContext)
      case p: PeriodicSeriesPlan => materializePeriodicSeriesPlan(p, qContext)
       // Metadata query not supported for downsample cluster
      case _                     => rawClusterPlanner.materialize(logicalPlan, qContext)
    }
  }
}
