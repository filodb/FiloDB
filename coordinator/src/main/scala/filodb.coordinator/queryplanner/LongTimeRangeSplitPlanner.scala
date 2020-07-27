package filodb.coordinator.queryplanner

import com.typesafe.scalalogging.StrictLogging
import debox.Buffer

import filodb.coordinator.queryplanner.LogicalPlanUtils._
import filodb.core.query.QueryContext
import filodb.query.{LogicalPlan, PeriodicSeriesPlan}
import filodb.query.exec.{ExecPlan, PlanDispatcher, StitchRvsExec}

/**
  * LongTimeRangeSplitPlanner splits query along the time dimension, based on a configured threshold (splitThresholdMs).
  * It knows about limited retention of raw data, and existence of downsampled data.
  * For any query that arrives beyond the retention period of raw data, it splits the query into
  * two time ranges - latest time range from raw data, and old time range from downsampled data.
  * It then splits each of the query ranges into multiple smaller ranges (splitSizeMs).
  * And finally stitches the subquery results to present top level query results for entire time range.
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
  * @param splitThresholdMs the function that will provide the threshold in milliseconds. If the query time ranges is
  *                         beyond this threshold, then the query will be split into smaller time range queries.
  * @param splitSizeMs  If the query is to be broken then it will be of this size
  * @param stitchDispatcher function to get the dispatcher for the stitch exec plan node
  */
class LongTimeRangeSplitPlanner(rawClusterPlanner: QueryPlanner,
                                downsampleClusterPlanner: QueryPlanner,
                                earliestRawTimestampFn: => Long,
                                latestDownsampleTimestampFn: => Long,
                                splitThresholdMs: => Long,
                                splitSizeMs: => Long,
                                stitchDispatcher: => PlanDispatcher) extends QueryPlanner with StrictLogging {

  //Most of the logic is derived from LongTimeRangePlanner
  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    logicalPlan match {
      case p: PeriodicSeriesPlan =>
        val earliestRawTime = earliestRawTimestampFn
        val offsetMillis = LogicalPlanUtils.getOffsetMillis(logicalPlan)
        val lookbackMs = LogicalPlanUtils.getLookBackMillis(logicalPlan)
        val startWithOffsetMs = p.startMs - offsetMillis
        val endWithOffsetMs = p.endMs - offsetMillis
        if (!logicalPlan.isRoutable) //can be answered wholly by raw cluster
          splitPlan(p, qContext, p1 => rawClusterPlanner.materialize(p1, qContext))
        else if (endWithOffsetMs < earliestRawTime) // can be answered wholly by downsample cluster
          splitPlan(p, qContext, p1 => downsampleClusterPlanner.materialize(p1, qContext))
        else if (startWithOffsetMs - lookbackMs >= earliestRawTime) //can be answered wholly by raw cluster
          splitPlan(p, qContext, p1 => rawClusterPlanner.materialize(p1, qContext))
        else if (endWithOffsetMs - lookbackMs < earliestRawTime) { //can be answered wholly by downsample cluster
          val lastDownsampleSampleTime = latestDownsampleTimestampFn
          val downsampleLp = if (endWithOffsetMs < lastDownsampleSampleTime) {
            logicalPlan
          } else {
            copyWithUpdatedTimeRange(logicalPlan,
              TimeRange(p.startMs, latestDownsampleTimestampFn + offsetMillis))
          }
          splitPlan(downsampleLp, qContext, p1 =>  downsampleClusterPlanner.materialize(p1, qContext))
        } else {
          // Split the query between raw and downsample planners
          val numStepsInDownsample = (earliestRawTime - startWithOffsetMs + lookbackMs) / p.stepMs
          val lastDownsampleInstant = p.startMs + numStepsInDownsample * p.stepMs
          val firstInstantInRaw = lastDownsampleInstant + p.stepMs

          val downsampleLp = copyWithUpdatedTimeRange(logicalPlan,
            TimeRange(p.startMs, lastDownsampleInstant))
          val downsampleEp = splitPlan(downsampleLp, qContext,
            p1 => downsampleClusterPlanner.materialize(p1, qContext))

          val rawLp = copyWithUpdatedTimeRange(logicalPlan, TimeRange(firstInstantInRaw, p.endMs))
          val rawEp = splitPlan(rawLp, qContext, p1 => rawClusterPlanner.materialize(p1, qContext))
          StitchRvsExec(qContext, stitchDispatcher, Seq(rawEp, downsampleEp))
        }
      // Metadata query not supported for downsample cluster
      case _ => rawClusterPlanner.materialize(logicalPlan, qContext)
    }
  }

  /**
    * Split query if the time range is greater than the threshold. It clones the given LogicalPlan with the smaller
    * time ranges, creates an execPlan using the provided planner and finally returns Stitch ExecPlan
    * @param lPlan LogicalPlan to be split
    * @param qContext QueryContext
    * @param planner provided function which takes input as LogicalPlan and creates an ExecPlan
    */
  private def splitPlan(lPlan: LogicalPlan, qContext: QueryContext,
                        planner: PeriodicSeriesPlan => ExecPlan): ExecPlan = {
    val windowSplits: Buffer[(Long, Long)] = Buffer.empty
    val lp = lPlan.asInstanceOf[PeriodicSeriesPlan]
    if (lp.isSplittable && lp.endMs - lp.startMs > splitThresholdMs) {
      logger.info(s"Splitting query queryId=${qContext.queryId} start=${lp.startMs}" +
        s" end=${lp.endMs} step=${lp.stepMs} splitThresholdMs=$splitThresholdMs splitSizeMs=$splitSizeMs")
      val numStepsPerSplit = splitSizeMs/lp.stepMs
      var startTime = lp.startMs
      var endTime = Math.min(lp.startMs + numStepsPerSplit * lp.stepMs, lp.endMs)
      val splitPlans: Buffer[ExecPlan] = Buffer.empty
      while (endTime < lp.endMs ) {
        splitPlans += planner(copyWithUpdatedTimeRange(lp, TimeRange(startTime, endTime)))
        startTime = endTime + lp.stepMs
        endTime = Math.min(startTime + numStepsPerSplit*lp.stepMs, lp.endMs)
      }
      // when endTime == lp.endMs - exit condition
      splitPlans += planner(copyWithUpdatedTimeRange(lp, TimeRange(startTime, endTime)))
      logger.info(s"splitsize queryId=${qContext.queryId} numWindows=${windowSplits.length}")
      StitchRvsExec(qContext, stitchDispatcher, splitPlans.toList, parallelChildTasks = false) // Sequential execution
    } else {
      planner(lp)
    }
  }
}
