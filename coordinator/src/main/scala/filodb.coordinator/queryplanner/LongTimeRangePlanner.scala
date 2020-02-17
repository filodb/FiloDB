package filodb.coordinator.queryplanner

import filodb.coordinator.queryplanner.LogicalPlanUtils._
import filodb.query.{LogicalPlan, PeriodicSeriesPlan, QueryContext}
import filodb.query.exec.{ExecPlan, PlanDispatcher, StitchRvsExec}

/**
  * LongTimeRangePlanner knows about limited retention of raw data, and existence of downsampled data.
  * For any query that arrives beyond the retention period of raw data, it splits the query into
  * two time ranges - latest time range from raw data, and old time range from downsampled data.
  * It then stitches the subquery results to present top level query results for entire time range.
  *
  * @param rawClusterPlanner this planner (typically a SingleClusterPlanner) abstracts planning for raw cluster data
  * @param downsampleClusterPlanner this planner (typically a SingleClusterPlanner)
  *                                 abstracts planning for downsample cluster data
  * @param earliestRawTimestamp the function that will provide millis timestamp of earliest sample that
  *                             would be available in the raw cluster
  * @param stitchDispatcher function to get the dispatcher for the stitch exec plan node
  */
class LongTimeRangePlanner(rawClusterPlanner: QueryPlanner,
                           downsampleClusterPlanner: QueryPlanner,
                           earliestRawTimestamp: => Long,
                           stitchDispatcher: => PlanDispatcher) extends QueryPlanner {

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    logicalPlan match {
      case p: PeriodicSeriesPlan =>
        val earliestRawTime = earliestRawTimestamp
        if (p.endMs < earliestRawTime) downsampleClusterPlanner.materialize(logicalPlan, qContext)
        else if (p.startMs >= earliestRawTime) rawClusterPlanner.materialize(logicalPlan, qContext)
        else {
          // Split the query between raw and downsample planners
          // TODO incorporate lookback window into this calculation
          val numStepsDownsample = (earliestRawTime - p.startMs) / p.stepMs
          val lastInstantInDownsample = p.startMs + numStepsDownsample * p.stepMs
          val firstInstantInRaw = lastInstantInDownsample + p.stepMs

          val lookBackTime = getRawSeriesStartTime(logicalPlan).map(p.startMs - _).get
          val downsampleLp = copyWithUpdatedTimeRange(logicalPlan,
                                           TimeRange(p.startMs, lastInstantInDownsample), lookBackTime)
          val downsampleEp = downsampleClusterPlanner.materialize(downsampleLp, qContext)

          val rawLp = copyWithUpdatedTimeRange(logicalPlan,
                                                      TimeRange(firstInstantInRaw, p.endMs),
                                                      lookBackTime)
          val rawEp = rawClusterPlanner.materialize(rawLp, qContext)
          StitchRvsExec(qContext.queryId, stitchDispatcher, Seq(rawEp, downsampleEp))
        }
      case _ =>
        // for now send everything else to raw cluster. Metadata queries are TODO
        rawClusterPlanner.materialize(logicalPlan, qContext)
    }
  }
}
