package filodb.coordinator.queryplanner

import scala.collection.mutable.ArrayBuffer

import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.queryplanner.LogicalPlanUtils.{getLookBackMillis, getTimeFromLogicalPlan}
import filodb.core.query.{QueryContext, RangeParams}
import filodb.prometheus.ast.Vectors.PromMetricLabel
import filodb.prometheus.ast.WindowConstants
import filodb.query._

object LogicalPlanUtils extends StrictLogging {

  /**
    * Check whether all child logical plans have same start and end time
    */
  def hasSingleTimeRange(logicalPlan: LogicalPlan): Boolean = {
    logicalPlan match {
      case binaryJoin: BinaryJoin =>
        val lhsTime = getTimeFromLogicalPlan(binaryJoin.lhs)
        val rhsTime = getTimeFromLogicalPlan(binaryJoin.rhs)
        (lhsTime.startMs == rhsTime.startMs) && (lhsTime.endMs == rhsTime.endMs)
      case _ => true
    }
  }

  /**
    * Retrieve start and end time from LogicalPlan
    */
  // scalastyle:off cyclomatic.complexity
  def getTimeFromLogicalPlan(logicalPlan: LogicalPlan): TimeRange = {
    logicalPlan match {
      case lp: PeriodicSeries              => TimeRange(lp.startMs, lp.endMs)
      case lp: PeriodicSeriesWithWindowing => TimeRange(lp.startMs, lp.endMs)
      case lp: ApplyInstantFunction        => getTimeFromLogicalPlan(lp.vectors)
      case lp: Aggregate                   => getTimeFromLogicalPlan(lp.vectors)
      case lp: BinaryJoin                  => val lhsTime = getTimeFromLogicalPlan(lp.lhs)
                                              val rhsTime = getTimeFromLogicalPlan(lp.rhs)
                                              if (lhsTime != rhsTime) throw new UnsupportedOperationException(
                                                "Binary Join has different LHS and RHS times")
                                              else lhsTime
      case lp: ScalarVectorBinaryOperation => getTimeFromLogicalPlan(lp.vector)
      case lp: ApplyMiscellaneousFunction  => getTimeFromLogicalPlan(lp.vectors)
      case lp: ApplySortFunction           => getTimeFromLogicalPlan(lp.vectors)
      case lp: ScalarVaryingDoublePlan     => getTimeFromLogicalPlan(lp.vectors)
      case lp: ScalarTimeBasedPlan         => TimeRange(lp.rangeParams.startSecs, lp.rangeParams.endSecs)
      case lp: VectorPlan                  => getTimeFromLogicalPlan(lp.scalars)
      case lp: ApplyAbsentFunction         => getTimeFromLogicalPlan(lp.vectors)
      case lp: RawSeries                   => lp.rangeSelector match {
                                                case i: IntervalSelector => TimeRange(i.from, i.to)
                                                case _ => throw new BadQueryException(s"Invalid logical plan")
                                              }
      case lp: LabelValues                 => TimeRange(lp.startMs, lp.endMs)
      case lp: SeriesKeysByFilters         => TimeRange(lp.startMs, lp.endMs)
      case lp: ApplyInstantFunctionRaw     => getTimeFromLogicalPlan(lp.vectors)
      case lp: ScalarBinaryOperation       => TimeRange(lp.rangeParams.startSecs * 1000, lp.rangeParams.endSecs * 1000)
      case lp: ScalarFixedDoublePlan       => TimeRange(lp.timeStepParams.startSecs * 1000,
                                              lp.timeStepParams.endSecs * 1000)
      case lp: RawChunkMeta                => throw new UnsupportedOperationException(s"RawChunkMeta does not have " +
                                              s"time")
    }
  }
  // scalastyle:on cyclomatic.complexity

  /**
   * Used to change start and end time(TimeRange) of LogicalPlan
   * NOTE: Plan should be PeriodicSeriesPlan
   */
  def copyLogicalPlanWithUpdatedTimeRange(logicalPlan: LogicalPlan,
                                          timeRange: TimeRange): LogicalPlan = {
    logicalPlan match {
      case lp: PeriodicSeriesPlan  => copyWithUpdatedTimeRange(lp, timeRange)
      case lp: RawSeriesLikePlan   => copyNonPeriodicWithUpdatedTimeRange(lp, timeRange)
      case lp: LabelValues         => lp.copy(startMs = timeRange.startMs, endMs = timeRange.endMs)
      case lp: SeriesKeysByFilters => lp.copy(startMs = timeRange.startMs, endMs = timeRange.endMs)
    }
  }

  /**
    * Used to change start and end time(TimeRange) of LogicalPlan
    * NOTE: Plan should be PeriodicSeriesPlan
    */
  //scalastyle:off cyclomatic.complexity
  def copyWithUpdatedTimeRange(logicalPlan: PeriodicSeriesPlan,
                               timeRange: TimeRange): PeriodicSeriesPlan = {
    logicalPlan match {
      case lp: PeriodicSeries              => lp.copy(startMs = timeRange.startMs,
                                                      endMs = timeRange.endMs,
                                                      rawSeries = copyNonPeriodicWithUpdatedTimeRange(lp.rawSeries,
                                                       timeRange).asInstanceOf[RawSeries])
      case lp: PeriodicSeriesWithWindowing => lp.copy(startMs = timeRange.startMs,
                                                      endMs = timeRange.endMs,
                                                      series = copyNonPeriodicWithUpdatedTimeRange(lp.series,
                                                               timeRange))
      case lp: ApplyInstantFunction        => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange))

      case lp: Aggregate                   => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange))

      case lp: BinaryJoin                  => lp.copy(lhs = copyWithUpdatedTimeRange(lp.lhs, timeRange),
                                               rhs = copyWithUpdatedTimeRange(lp.rhs, timeRange))
      case lp: ScalarVectorBinaryOperation => lp.copy(vector = copyWithUpdatedTimeRange(lp.vector, timeRange))

      case lp: ApplyMiscellaneousFunction  => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange))

      case lp: ApplySortFunction           => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange))
      case lp: ApplyAbsentFunction         => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange))
      case lp: ScalarVaryingDoublePlan     => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange))
      case lp: RawChunkMeta                => lp.rangeSelector match {
                                              case is: IntervalSelector  => lp.copy(rangeSelector = is.copy(
                                                                            timeRange.startMs, timeRange.endMs))
                                              case AllChunksSelector |
                                                   EncodedChunksSelector |
                                                   InMemoryChunksSelector |
                                                   WriteBufferSelector     => throw new UnsupportedOperationException(
                                                                             "Copy supported only for IntervalSelector")
                                            }
      case lp: VectorPlan                  => lp.copy(scalars = copyWithUpdatedTimeRange(lp.scalars, timeRange).
                                            asInstanceOf[ScalarPlan])
      case lp: ScalarTimeBasedPlan         => lp.copy(rangeParams = RangeParams(timeRange.startMs * 1000,
                                              lp.rangeParams.stepSecs, timeRange.endMs * 1000))
      case lp: ScalarFixedDoublePlan       => lp.copy(timeStepParams = RangeParams(timeRange.startMs * 1000,
                                              lp.timeStepParams.stepSecs, timeRange.endMs * 1000))
      case lp: ScalarBinaryOperation       =>  val updatedLhs = if (lp.lhs.isRight) Right(copyWithUpdatedTimeRange
                                              (lp.lhs.right.get, timeRange).asInstanceOf[ScalarBinaryOperation]) else
                                              Left(lp.lhs.left.get)
                                              val updatedRhs = if (lp.rhs.isRight) Right(copyWithUpdatedTimeRange(
                                                lp.rhs.right.get, timeRange).asInstanceOf[ScalarBinaryOperation])
                                              else Left(lp.rhs.left.get)
                                              lp.copy(lhs = updatedLhs, rhs = updatedRhs, rangeParams =
                                                RangeParams(timeRange.startMs * 1000, lp.rangeParams.stepSecs,
                                                  timeRange.endMs * 1000))
    }
  }

  /**
    * Used to change rangeSelector of RawSeriesLikePlan
    */
  private def copyNonPeriodicWithUpdatedTimeRange(plan: LogicalPlan,
                                                  timeRange: TimeRange): RawSeriesLikePlan = {
    plan match {
      case rs: RawSeries => rs.rangeSelector match {
        case is: IntervalSelector => rs.copy(rangeSelector = is.copy(timeRange.startMs, timeRange.endMs))
        case _ => throw new UnsupportedOperationException("Copy supported only for IntervalSelector")
      }
      case p: ApplyInstantFunctionRaw =>
        p.copy(vectors = copyNonPeriodicWithUpdatedTimeRange(p.vectors, timeRange)
          .asInstanceOf[RawSeries])
      case _ => throw new UnsupportedOperationException("Copy supported only for RawSeries")
    }
  }

  /**
    * Retrieve start time of Raw Series
    * NOTE: Plan should be PeriodicSeriesPlan
    */
  def getRawSeriesStartTime(logicalPlan: LogicalPlan): Option[Long] = {
    val leaf = LogicalPlan.findLeafLogicalPlans(logicalPlan)
    if (leaf.isEmpty) None else {
      leaf.head match {
        case lp: RawSeries => lp.rangeSelector match {
          case rs: IntervalSelector => Some(rs.from)
          case _ => None
        }
        case _ => throw new BadQueryException(s"Invalid logical plan $logicalPlan")
      }
    }
  }

  def getOffsetMillis(logicalPlan: LogicalPlan): Long = {
    val leaf = LogicalPlan.findLeafLogicalPlans(logicalPlan)
    if (leaf.isEmpty) 0 else {
      leaf.head match {
        case lp: RawSeries => lp.offsetMs.getOrElse(0)
        case _ => 0
      }
    }
  }

  def getLookBackMillis(logicalPlan: LogicalPlan): Long = {
    val staleDataLookbackMillis = WindowConstants.staleDataLookbackMillis
    val leaf = LogicalPlan.findLeafLogicalPlans(logicalPlan)
    if (leaf.isEmpty) 0 else {
      leaf.head match {
        case lp: RawSeries => lp.lookbackMs.getOrElse(staleDataLookbackMillis)
        case _ => 0
      }
    }
  }

  def getMetricName(logicalPlan: LogicalPlan, datasetMetricColumn: String): Set[String] = {
    val columnFilterGroup = LogicalPlan.getColumnFilterGroup(logicalPlan)
    val metricName = LogicalPlan.getColumnValues(columnFilterGroup, PromMetricLabel)
    if (metricName.isEmpty) LogicalPlan.getColumnValues(columnFilterGroup, datasetMetricColumn)
    else metricName
  }

  /**
    * Renames Prom AST __name__ label to one based on the actual metric column of the dataset,
    * if it is not the prometheus standard
    */
   def renameLabels(labels: Seq[String], datasetMetricColumn: String): Seq[String] =
    if (datasetMetricColumn != PromMetricLabel) {
      labels map {
        case PromMetricLabel     => datasetMetricColumn
        case other: String       => other
      }
    } else {
      labels
    }

  /**
    * Split query if the time range is greater than the threshold. It clones the given LogicalPlan with the smaller
    * time ranges, creates an execPlan using the provided planner and finally returns Stitch ExecPlan.
    * @param lPlan LogicalPlan to be split
    * @param qContext QueryContext
    * @param timeSplitEnabled split based on longer time range
    * @param minTimeRangeForSplitMs if time range is longer than this, plan will be split into multiple plans
    * @param splitSizeMs time range for each split, if plan needed to be split
    */
  def splitPlans(lPlan: LogicalPlan,
                 qContext: QueryContext,
                 timeSplitEnabled: Boolean,
                 minTimeRangeForSplitMs: Long,
                 splitSizeMs: Long): Seq[LogicalPlan] = {
    val lp = lPlan.asInstanceOf[PeriodicSeriesPlan]
    if (timeSplitEnabled && lp.isTimeSplittable && lp.endMs - lp.startMs > minTimeRangeForSplitMs
        && lp.stepMs <= splitSizeMs) {
      logger.info(s"Splitting query queryId=${qContext.queryId} start=${lp.startMs}" +
        s" end=${lp.endMs} step=${lp.stepMs} splitThresholdMs=$minTimeRangeForSplitMs splitSizeMs=$splitSizeMs")
      val numStepsPerSplit = splitSizeMs/lp.stepMs
      var startTime = lp.startMs
      var endTime = Math.min(lp.startMs + numStepsPerSplit * lp.stepMs, lp.endMs)
      val splitPlans: ArrayBuffer[LogicalPlan] = ArrayBuffer.empty
      while (endTime < lp.endMs ) {
        splitPlans += copyWithUpdatedTimeRange(lp, TimeRange(startTime, endTime))
        startTime = endTime + lp.stepMs
        endTime = Math.min(startTime + numStepsPerSplit*lp.stepMs, lp.endMs)
      }
      // when endTime == lp.endMs - exit condition
      splitPlans += copyWithUpdatedTimeRange(lp, TimeRange(startTime, endTime))
      logger.info(s"splitsize queryId=${qContext.queryId} numWindows=${splitPlans.length}")
      splitPlans
    } else {
      Seq(lp)
    }
  }
}

/**
 * Temporary utility to modify plan to add extra join-on keys or group-by keys
 * for specific time ranges.
 */
object ExtraOnByKeysUtil {

  def getRealOnLabels(lp: BinaryJoin, addStepKeyTimeRanges: Seq[Seq[Long]]): Seq[String] = {
    if (shouldAddExtraKeys(lp.lhs, addStepKeyTimeRanges: Seq[Seq[Long]]) ||
        shouldAddExtraKeys(lp.rhs, addStepKeyTimeRanges: Seq[Seq[Long]])) {
      // add extra keys if ignoring clause is not specified and on is specified
      if (lp.on.nonEmpty) lp.on ++ extraByOnKeys
      else lp.on
    } else {
      lp.on
    }
  }

  def getRealByLabels(lp: Aggregate, addStepKeyTimeRanges: Seq[Seq[Long]]): Seq[String] = {
    if (shouldAddExtraKeys(lp, addStepKeyTimeRanges)) {
      // add extra keys if without clause is not specified
      if (lp.without.isEmpty) lp.by ++ extraByOnKeys
      else lp.by
    } else {
      lp.by
    }
  }

  private def shouldAddExtraKeys(lp: LogicalPlan, addStepKeyTimeRanges: Seq[Seq[Long]]): Boolean = {
    // need to check if raw time range in query overlaps with configured addStepKeyTimeRanges
    val range = getTimeFromLogicalPlan(lp)
    val lookback = getLookBackMillis(lp)
    queryTimeRangeRequiresExtraKeys(range.startMs - lookback, range.endMs, addStepKeyTimeRanges)
  }

  val extraByOnKeys = Seq("_pi_", "_step_")
  /**
   * Returns true if two time ranges (x1, x2) and (y1, y2) overlap
   */
  private def rangeOverlaps(x1: Long, x2: Long, y1: Long, y2: Long): Boolean = {
    Math.max(x1, y1) <= Math.min(x2, y2)
  }

  private def queryTimeRangeRequiresExtraKeys(rawStartMs: Long,
                                              rawEndMs: Long,
                                              addStepKeyTimeRanges: Seq[Seq[Long]]): Boolean = {
    addStepKeyTimeRanges.exists { r => rangeOverlaps(rawStartMs, rawEndMs, r(0), r(1)) }
  }
}
