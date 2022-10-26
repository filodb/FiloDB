package filodb.coordinator.queryplanner

import scala.collection.mutable.ArrayBuffer

import com.typesafe.scalalogging.StrictLogging

import filodb.core.query.{QueryContext, RangeParams}
import filodb.prometheus.ast.SubqueryUtils
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
   * Given a LogicalPlan check if any descendent of plan is an aggregate operation
   * @param lp the LogicalPlan instance
   * @return true if a descendent is an aggregate else false
   */
  def hasDescendantAggregate(lp: LogicalPlan): Boolean = lp match {
    case _: Aggregate                 => true
    case nonLeaf: NonLeafLogicalPlan  => nonLeaf.children.exists(hasDescendantAggregate(_))
    case _                            => false
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
      case lp: ApplyLimitFunction          => getTimeFromLogicalPlan(lp.vectors)
      case lp: RawSeries                   => lp.rangeSelector match {
                                                case i: IntervalSelector => TimeRange(i.from, i.to)
                                                case _ => throw new BadQueryException(s"Invalid logical plan")
                                              }
      case lp: LabelValues                 => TimeRange(lp.startMs, lp.endMs)
      case lp: LabelCardinality            => TimeRange(lp.startMs, lp.endMs)
      case lp: LabelNames                  => TimeRange(lp.startMs, lp.endMs)
      case lp: TsCardinalities             => throw new UnsupportedOperationException(
                                                          "TsCardinalities does not have times")
      case lp: SeriesKeysByFilters         => TimeRange(lp.startMs, lp.endMs)
      case lp: ApplyInstantFunctionRaw     => getTimeFromLogicalPlan(lp.vectors)
      case lp: ScalarBinaryOperation       => TimeRange(lp.rangeParams.startSecs * 1000, lp.rangeParams.endSecs * 1000)
      case lp: ScalarFixedDoublePlan       => TimeRange(lp.timeStepParams.startSecs * 1000,
                                              lp.timeStepParams.endSecs * 1000)
      case sq: SubqueryWithWindowing       => TimeRange(sq.startMs, sq.endMs)
      case tlsq: TopLevelSubquery          => TimeRange(tlsq.startMs, tlsq.endMs)
      case lp: RawChunkMeta                => throw new UnsupportedOperationException(
                                                          "RawChunkMeta does not have times")
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
      case lp: PeriodicSeriesPlan       => copyWithUpdatedTimeRange(lp, timeRange)
      case lp: RawSeriesLikePlan        => copyNonPeriodicWithUpdatedTimeRange(lp, timeRange)
      case lp: LabelValues              => lp.copy(startMs = timeRange.startMs, endMs = timeRange.endMs)
      case lp: LabelNames               => lp.copy(startMs = timeRange.startMs, endMs = timeRange.endMs)
      case lp: LabelCardinality         => lp.copy(startMs = timeRange.startMs, endMs = timeRange.endMs)
      case lp: TsCardinalities          => lp  // immutable & no members need to be updated
      case lp: SeriesKeysByFilters      => lp.copy(startMs = timeRange.startMs, endMs = timeRange.endMs)
    }
  }

  /**
    * Used to change start and end time(TimeRange) of LogicalPlan
    * NOTE: Plan should be PeriodicSeriesPlan
    */
  //scalastyle:off cyclomatic.complexity
  //scalastyle:off method.length
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
      case lp: ApplyLimitFunction          => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange))
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
      case lp: ScalarTimeBasedPlan         => lp.copy(rangeParams = RangeParams(timeRange.startMs / 1000,
                                              lp.rangeParams.stepSecs, timeRange.endMs / 1000))
      case lp: ScalarFixedDoublePlan       => lp.copy(timeStepParams = RangeParams(timeRange.startMs / 1000,
                                              lp.timeStepParams.stepSecs, timeRange.endMs / 1000))
      case lp: ScalarBinaryOperation       =>  val updatedLhs = if (lp.lhs.isRight) Right(copyWithUpdatedTimeRange
                                              (lp.lhs.right.get, timeRange).asInstanceOf[ScalarBinaryOperation]) else
                                              Left(lp.lhs.left.get)
                                              val updatedRhs = if (lp.rhs.isRight) Right(copyWithUpdatedTimeRange(
                                                lp.rhs.right.get, timeRange).asInstanceOf[ScalarBinaryOperation])
                                              else Left(lp.rhs.left.get)
                                              lp.copy(lhs = updatedLhs, rhs = updatedRhs, rangeParams =
                                                RangeParams(timeRange.startMs / 1000, lp.rangeParams.stepSecs,
                                                  timeRange.endMs / 1000))
      case sq: SubqueryWithWindowing       => copySubqueryWithWindowingWithUpdatedTimeRange(timeRange, sq)
      case tlsq: TopLevelSubquery          => copyTopLevelSubqueryWithUpdatedTimeRange(timeRange, tlsq)
    }
  }

  private def copyTopLevelSubqueryWithUpdatedTimeRange(
    timeRange: TimeRange, topLevelSubquery: TopLevelSubquery
  ): TopLevelSubquery = {
    val newInner = copyWithUpdatedTimeRange(topLevelSubquery.innerPeriodicSeries, timeRange)
    topLevelSubquery.copy(innerPeriodicSeries = newInner, startMs = timeRange.startMs, endMs = timeRange.endMs)
  }

  def copySubqueryWithWindowingWithUpdatedTimeRange(
    timeRange: TimeRange, sqww: SubqueryWithWindowing
  ) : PeriodicSeriesPlan = {
    val offsetMs = sqww.offsetMs match {
      case None => 0
      case Some(ofMs) => ofMs
    }
    val preciseStartForInnerMs = timeRange.startMs - sqww.subqueryWindowMs - offsetMs
    val startForInnerMs = SubqueryUtils.getStartForFastSubquery(preciseStartForInnerMs, sqww.subqueryStepMs)
    val preciseEndForInnerMs = timeRange.endMs - offsetMs
    val endForInnerMs = SubqueryUtils.getEndForFastSubquery(preciseEndForInnerMs, sqww.subqueryStepMs)
    val innerTimeRange = TimeRange(startForInnerMs, endForInnerMs)
    val updatedInner = copyWithUpdatedTimeRange(sqww.innerPeriodicSeries, innerTimeRange)
    sqww.copy(innerPeriodicSeries = updatedInner, startMs = timeRange.startMs, endMs = timeRange.endMs)

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

  def getOffsetMillis(logicalPlan: LogicalPlan): Seq[Long] = {
    logicalPlan match {
      // Offset/Lookback aren't propagated to nested subquery plans;
      //   the subquery's offset needs to be added to child offsets.
      case sww: SubqueryWithWindowing => getOffsetMillis(sww.innerPeriodicSeries).map(_ + sww.offsetMs.getOrElse(0L))
      case nl: NonLeafLogicalPlan => nl.children.flatMap(getOffsetMillis)
      case rs: RawSeries => Seq(rs.offsetMs.getOrElse(0))
      case _             => Seq(0)
    }
  }

  def getLookBackMillis(logicalPlan: LogicalPlan): Seq[Long] = {
    // getLookBackMillis is used primarily for LongTimeRangePlanner,
    // SubqueryWtihWindowing returns lookback but TopLevelSubquery does not. The conceptual meaning of the lookback
    // is an interval that we want to process on one machine (primarily to compute range functions).
    // SubqueryWithWindowing has such a lookback while TopLevelSubquery does not.
    logicalPlan match {
      case sww: SubqueryWithWindowing => getLookBackMillis(sww.innerPeriodicSeries).map(lb => lb + sww.subqueryWindowMs)
      case nl: NonLeafLogicalPlan => nl.children.flatMap(getLookBackMillis)
      case rs: RawSeries => Seq(rs.lookbackMs.getOrElse(WindowConstants.staleDataLookbackMillis))
      case _             => Seq(0)
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

  /**
   * Returns PeriodicSeries or PeriodicSeriesWithWindowing plan present in LogicalPlan
   */
  def getPeriodicSeriesPlan(logicalPlan: LogicalPlan): Option[Seq[LogicalPlan]] = {
    logicalPlan match {
      case lp: PeriodicSeries              => Some(Seq(lp))
      case lp: PeriodicSeriesWithWindowing => Some(Seq(lp))
      case lp: ApplyInstantFunction        => getPeriodicSeriesPlan(lp.vectors)
      case lp: Aggregate                   => getPeriodicSeriesPlan(lp.vectors)
      case lp: BinaryJoin                  => val lhs = getPeriodicSeriesPlan(lp.lhs)
                                              val rhs = getPeriodicSeriesPlan(lp.rhs)
                                              if (lhs.isEmpty) rhs
                                              else if (rhs.isEmpty) lhs
                                              else Some(lhs.get ++ rhs.get)
      case lp: ScalarVectorBinaryOperation => getPeriodicSeriesPlan(lp.vector)
      case lp: ApplyMiscellaneousFunction  => getPeriodicSeriesPlan(lp.vectors)
      case lp: ApplySortFunction           => getPeriodicSeriesPlan(lp.vectors)
      case lp: ScalarVaryingDoublePlan     => getPeriodicSeriesPlan(lp.vectors)
      case lp: ScalarTimeBasedPlan         => None
      case lp: VectorPlan                  => getPeriodicSeriesPlan(lp.scalars)
      case lp: ApplyAbsentFunction         => getPeriodicSeriesPlan(lp.vectors)
      case lp: ApplyLimitFunction          => getPeriodicSeriesPlan(lp.vectors)
      case lp: RawSeries                   => None
      case lp: LabelValues                 => None
      case lp: LabelNames                  => None
      case lp: LabelCardinality            => None
      case lp: SeriesKeysByFilters         => None
      case lp: ApplyInstantFunctionRaw     => getPeriodicSeriesPlan(lp.vectors)
      case lp: ScalarBinaryOperation       =>  if (lp.lhs.isRight) getPeriodicSeriesPlan(lp.lhs.right.get)
                                               else if (lp.rhs.isRight) getPeriodicSeriesPlan(lp.rhs.right.get)
                                               else None
      case lp: ScalarFixedDoublePlan       => None
      case lp: RawChunkMeta                => None
      case sq: SubqueryWithWindowing       => getPeriodicSeriesPlan(sq.innerPeriodicSeries)
      case tlsq: TopLevelSubquery          => getPeriodicSeriesPlan(tlsq.innerPeriodicSeries)
      case lp: TsCardinalities             => None
    }
  }

  def hasBinaryJoin(lp: LogicalPlan): Boolean = {
   lp match {
     case b: BinaryJoin => true
     case n: NonLeafLogicalPlan => n.children.exists(hasBinaryJoin(_))
     case _ => false
   }
  }

  /**
   * Snap a timestamp to the next periodic step.
   * @param timestamp the timestamp to snap.
   * @param step the size of each periodic step.
   * @param origin where steps began.
   */
  def snapToStep(timestamp: Long, step: Long, origin: Long): Long = {
    val totalDiff = timestamp - origin
    val partialStep = totalDiff % step
    val diffToNextStep = if (partialStep > 0) step - partialStep else 0
    timestamp + diffToNextStep
  }
}
