package filodb.coordinator.queryplanner

import filodb.coordinator.queryplanner.LogicalPlanUtils._
import filodb.core.query.QueryContext
import filodb.query.{LogicalPlan, PeriodicSeriesPlan}
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
                           stitchDispatcher: => PlanDispatcher) extends QueryPlanner {

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    logicalPlan match {
      case p: PeriodicSeriesPlan =>
        val earliestRawTime = earliestRawTimestampFn
        lazy val offsetMillis = LogicalPlanUtils.getOffsetMillis(logicalPlan)
        lazy val lookbackMs = LogicalPlanUtils.getLookBackMillis(logicalPlan)
        lazy val startWithOffsetMs = p.startMs - offsetMillis
        lazy val endWithOffsetMs = p.endMs - offsetMillis
        if (!logicalPlan.isRoutable) rawClusterPlanner.materialize(logicalPlan, qContext)
        else if (endWithOffsetMs < earliestRawTime) downsampleClusterPlanner.materialize(logicalPlan, qContext)
        else if (startWithOffsetMs - lookbackMs >= earliestRawTime)
          rawClusterPlanner.materialize(logicalPlan, qContext)
        else if (endWithOffsetMs - lookbackMs < earliestRawTime) {
          val lastDownsampleSampleTime = latestDownsampleTimestampFn
          val downsampleLp = if (endWithOffsetMs < lastDownsampleSampleTime) {
            logicalPlan
          } else {
            copyWithUpdatedTimeRange(logicalPlan,
              TimeRange(p.startMs, latestDownsampleTimestampFn + offsetMillis), lookbackMs)
          }
          downsampleClusterPlanner.materialize(downsampleLp, qContext)
        } else {
          // Split the query between raw and downsample planners
          val numStepsInDownsample = (earliestRawTime - startWithOffsetMs + lookbackMs) / p.stepMs
          val lastDownsampleInstant = p.startMs + numStepsInDownsample * p.stepMs
          val firstInstantInRaw = lastDownsampleInstant + p.stepMs

          val downsampleLp = copyWithUpdatedTimeRange(logicalPlan,
                                                      TimeRange(p.startMs, lastDownsampleInstant),
                                                      lookbackMs)
          val downsampleEp = downsampleClusterPlanner.materialize(downsampleLp, qContext)

          val rawLp = copyWithUpdatedTimeRange(logicalPlan, TimeRange(firstInstantInRaw, p.endMs), lookbackMs)
          val rawEp = rawClusterPlanner.materialize(rawLp, qContext)
          StitchRvsExec(qContext, stitchDispatcher, Seq(rawEp, downsampleEp))
        }
      case _ => rawClusterPlanner.materialize(logicalPlan, qContext)
    }
  }
}
