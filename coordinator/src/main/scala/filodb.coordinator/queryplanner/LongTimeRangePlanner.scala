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
  */
class LongTimeRangePlanner(rawClusterPlanner: QueryPlanner,
                           downsampleClusterPlanner: QueryPlanner,
                           rawDataRetentionMillis: Long,
                           stitchDispatcher: => PlanDispatcher) extends QueryPlanner {

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    logicalPlan match {
      case p: PeriodicSeriesPlan =>
        val lastRawTime = System.currentTimeMillis() - rawDataRetentionMillis
        if (p.end < lastRawTime) downsampleClusterPlanner.materialize(logicalPlan, qContext)
        else if (p.start >= lastRawTime) rawClusterPlanner.materialize(logicalPlan, qContext)
        else {

          val numStepsDownsample = (lastRawTime - p.start) / p.step
          val lastInstantInDownsample = p.start + numStepsDownsample * p.step
          val firstInstantInRaw = lastInstantInDownsample + p.step

          val lookBackTime = getRawSeriesStartTime(logicalPlan).map(p.start - _).get
          val downsampleLp = copyWithUpdatedTimeRange(logicalPlan,
                                           TimeRange(p.start, lastInstantInDownsample), lookBackTime)
          val downsampleEp = downsampleClusterPlanner.materialize(downsampleLp, qContext)

          val rawLp = copyWithUpdatedTimeRange(logicalPlan,
                                                      TimeRange(firstInstantInRaw, p.end),
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
