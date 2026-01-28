package filodb.coordinator.queryplanner

import scala.collection.{mutable, Seq}
import scala.collection.mutable.ArrayBuffer

import com.typesafe.scalalogging.StrictLogging

import filodb.core.TargetSchemaProvider
import filodb.core.query.{ColumnFilter, QueryContext, QueryUtils, RangeParams}
import filodb.core.query.Filter.{Equals, EqualsRegex}
import filodb.prometheus.ast.SubqueryUtils
import filodb.prometheus.ast.Vectors.PromMetricLabel
import filodb.prometheus.ast.WindowConstants
import filodb.query._
import filodb.query.AggregateClause.ClauseType

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
   * Given a LogicalPlan check if any descendent of plan is either an aggregate or join operation
   * @param lp the LogicalPlan instance
   * @return true if a descendent is an aggregate or join else false
   */
  def hasDescendantAggregateOrJoin(lp: LogicalPlan): Boolean = lp match {
    case _: Aggregate                 => true
    // consider this BinaryJoin example foo + on(h) + bar.
    // partition1 has foo{h=1}, bar{h1=2} and partition2 has foo{h=2}, bar{h1=1}
    // the binary join cannot happen on a partition locally. InProcessPlanDispatcher is required.
    case _: BinaryJoin               => true
    case nonLeaf: NonLeafLogicalPlan  => nonLeaf.children.exists(hasDescendantAggregateOrJoin(_))
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
   * Used to change start and end time (with 'second' precision) of LogicalPlan
   * NOTE: Plan should be PeriodicSeriesPlan
   */
  def copyLogicalPlanWithUpdatedSeconds(logicalPlan: LogicalPlan,
                                        startSec: Long,
                                        endSec: Long): LogicalPlan = {
    // Snap the range endpoints to the previous second.
    // Plans sometimes require (correctly) that their children share the same start/stop times,
    //   but some plans do not support millisecond precision. A recursive time-range update might
    //   cause two child plan time-ranges to mismatch if the range is not second-aligned.
    // Given that millisecond precision is never necessary, this method accepts timestamps in
    //   'seconds' to avoid the hassle.
    val timeRange = TimeRange(
      1000 * startSec,
      1000 * endSec
    )
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
                                              (lp.lhs.toOption.get, timeRange).asInstanceOf[ScalarBinaryOperation]) else
                                              Left(lp.lhs.swap.toOption.get)
                                              val updatedRhs = if (lp.rhs.isRight) Right(copyWithUpdatedTimeRange(
                                                lp.rhs.toOption.get, timeRange).asInstanceOf[ScalarBinaryOperation])
                                              else Left(lp.rhs.swap.toOption.get)
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

  /**
   * @param logicalPlan LogicalPlan
   * @return maximum offset in milliseconds
   */
  def getMaxOffetInMillis(logicalPlan: LogicalPlan): Long = {
    getOffsetMillis(logicalPlan).max
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

  /**
   * @param logicalPlan LogicalPlan
   * @return maximum lookback in milliseconds
   */
  def getMaxLookBackInMillis(logicalPlan: LogicalPlan): Long = {
    getLookBackMillis(logicalPlan).max
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
      case lp: ScalarBinaryOperation       =>  if (lp.lhs.isRight) getPeriodicSeriesPlan(lp.lhs.toOption.get)
                                               else if (lp.rhs.isRight) getPeriodicSeriesPlan(lp.rhs.toOption.get)
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

  /**
   * Returns the target-schema for the argument filters and interval
   *   as long as the schema does not change during the interval.
   * @return an occupied Option iff a target-schema definition meets the above requirements.
   */
  def getTargetSchemaIfUnchanging(targetSchemaProvider: TargetSchemaProvider,
                                  filters: Seq[ColumnFilter],
                                  intervalSelector: IntervalSelector): Option[Seq[String]] = {
    val tsChanges = targetSchemaProvider.targetSchemaFunc(filters)
    val ichange = tsChanges.lastIndexWhere(change => change.time < intervalSelector.from)
    if (ichange > -1) {
      // Is this the final change?
      // Does the next change occur beyond the end of the interval?
      if (ichange == tsChanges.size - 1 || tsChanges(ichange + 1).time > intervalSelector.to) {
        return Some(tsChanges(ichange).schema)
      }
    }
    None
  }

  /**
   * Returns an IntervalSelector with a from/to that spans all data read by a RawSeries.
   *   This includes all lookbacks/offsets.
   */
  def getSpanningIntervalSelector(rs: RawSeries): IntervalSelector = {
    val offsetMs = LogicalPlanUtils.getOffsetMillis(rs).headOption.getOrElse(0L)
    val lookbackMs = LogicalPlanUtils.getLookBackMillis(rs).headOption.getOrElse(0L)
    val oldSelector = rs.rangeSelector.asInstanceOf[IntervalSelector]
    IntervalSelector(oldSelector.from - offsetMs - lookbackMs, oldSelector.to - offsetMs)
  }

  /**
   * Returns a sequence that contains all filters of "dest" unless a filter for the same
   *   label exists in "src"; in this case, the "src" filter is included.
   */
  def upsertFilters(dest: Seq[ColumnFilter], src: Seq[ColumnFilter]): Seq[ColumnFilter] = {
    val columnMap = new mutable.HashMap[String, ColumnFilter]
    dest.foreach(filter => columnMap(filter.column) = filter)
    src.foreach(filter => columnMap(filter.column) = filter)
    columnMap.values.toSeq
  }

  /**
   * Returns a set of target-schema columns iff all of:
   *   - all plan RawSeries share the same target-schema columns.
   *   - no target-schema definition changes during the query.
   */
  def sameRawSeriesTargetSchemaColumns(plan: LogicalPlan,
                                       targetSchemaProvider: TargetSchemaProvider,
                                       getShardKeyFilters: RawSeries => Seq[Seq[ColumnFilter]])
  : Option[Seq[String]] = {
    // compose a stream of Options for each RawSeries--
    //   the options contain a target-schema iff it is defined and unchanging.
    val rawSeries = LogicalPlan.findLeafLogicalPlans(plan)
      .filter(_.isInstanceOf[RawSeries])
      .map(_.asInstanceOf[RawSeries])
    if (rawSeries.exists(!_.rangeSelector.isInstanceOf[IntervalSelector])) {
      // Cannot handle RawSeries without IntervalSelector.
      return None
    }
    val rsTschemaOpts = rawSeries.flatMap{ rs =>
        val interval = LogicalPlanUtils.getSpanningIntervalSelector(rs)
        val rawShardKeyFilters = getShardKeyFilters(rs)
        // The filters might contain pipe-concatenated EqualsRegex values.
        // Convert these into sets of single-valued Equals filters.
        val resolvedShardKeyFilters = rawShardKeyFilters.flatMap { filters =>
          val equalsFilters: Seq[Seq[ColumnFilter]] = filters.map { filter =>
            filter.filter match {
              case EqualsRegex(values: String) if QueryUtils.containsPipeOnlyRegex(values) =>
                QueryUtils.splitAtUnescapedPipes(values).map(value => ColumnFilter(filter.column, Equals(value)))
              case _ => Seq(filter)
            }
          }
          // E.g. foo{key1=~"baz|bat",key2=~"bar|bak"} would give the following combos:
          // [[baz,bar], [baz,bak], [bat,bar], [bat,bak]]
          QueryUtils.combinations(equalsFilters)
        }.map(_.toSet).distinct.map(_.toSeq)  // make sure keys are distinct
      resolvedShardKeyFilters.map{ shardKey =>
        val filters = LogicalPlanUtils.upsertFilters(rs.filters, shardKey)
        LogicalPlanUtils.getTargetSchemaIfUnchanging(targetSchemaProvider, filters, interval)
      }
    }
    if (rsTschemaOpts.isEmpty) {
      return None
    }
    // make sure all tschemas are defined, and they all match
    val referenceSchema = rsTschemaOpts.head
    if (referenceSchema.isDefined
      && rsTschemaOpts.forall(tschema => tschema.isDefined && tschema == referenceSchema)) {
      Some(referenceSchema.get)
    } else {
      None
    }
  }

  /**
   * Returns true iff a plan can be pushdown-optimized.
   * See [[LogicalPlanUtils.getPushdownKeys]] for more info.
   */
  private def canPushdown(plan: CandidatePushdownPlan,
                          targetSchemaProvider: TargetSchemaProvider,
                          nonMetricShardKeyCols: Seq[String],
                          getShardKeyFilters: RawSeries => Seq[Seq[ColumnFilter]]): Boolean = {
    val hasPushdownableClause = plan match {
      case Aggregate(_, _, _, clauseOpt) =>
        clauseOpt.nonEmpty && clauseOpt.get.clauseType == AggregateClause.ClauseType.By
      case BinaryJoin(_, _, _, _, on, ignoring, _) =>
        on.nonEmpty && ignoring.isEmpty
    }
    if (!hasPushdownableClause) {
      return false
    }

    val clauseLabels = plan match {
      case Aggregate(_, _, _, clauseOpt) => clauseOpt.get.labels
      // TODO: Validate this is indeed the expected behavior as on clause with empty () vs no on clause are not same
      case BinaryJoin(_, _, _, _, on, _, _) => on.getOrElse(Nil)
    }

    val tschema = sameRawSeriesTargetSchemaColumns(plan, targetSchemaProvider, getShardKeyFilters)
    if (tschema.isEmpty) {
      return false
    }
    // make sure non-shard-key (i.e. non-implicit) target-schema columns are included in the "on" clause.
    val tschemaNoImplicit = tschema.get.filter(!nonMetricShardKeyCols.contains(_))
    tschemaNoImplicit.forall(clauseLabels.contains(_))
  }

  // scalastyle:off method.length
  // scalastyle:off cyclomatic.complexity
  /**
   * Returns the set of "keys" to which the LogicalPlan can be pushed down.
   * A "key" is generic-- it may mean "shards" (e.g. as in the [[SingleClusterPlanner]])
   *   or "shard-keys" (as in the [[ShardKeyRegexPlanner]]).
   * When this function returns a Some, the argument plan can be materialized for each key
   *   in the result, and the concatenation of these "key-local" plans will, on execute(), yield the
   *   same RangeVectors as the non-pushed-down plan.
   * @return an occupied Option iff it is valid to perform the a pushdown optimization on the argument plan.
   */
  def getPushdownKeys[T](lp: LogicalPlan,
                         targetSchemaProvider: TargetSchemaProvider,
                         nonMetricShardKeyCols: Seq[String],
                         getRawPushdownKeys: RawSeries => Set[T],
                         getShardKeyFilters: RawSeries => Seq[Seq[ColumnFilter]]): Option[Set[T]] = {
    def helper(lp: LogicalPlan): Option[Set[T]] = lp match {
      case vec: VectorPlan =>
        val keys = helper(vec.scalars)
        // pushdown only to a single key
        if (keys.isDefined && keys.get.size == 1) keys else None
      case svdp: ScalarVaryingDoublePlan =>
        val keys = helper(svdp.vectors)
        // pushdown only to a single key"
        if (keys.isDefined && keys.get.size == 1) keys else None
      case svbo: ScalarVectorBinaryOperation =>
        val lhsKeys = helper(svbo.scalarArg)
        val rhsKeys = helper(svbo.vector)
        val canPushdown =
          lhsKeys.isDefined && rhsKeys.isDefined &&
            // either the lhs/rhs keys are equal, or at least one of lhs/rhs includes only scalars.
            (lhsKeys.get.isEmpty || rhsKeys.get.isEmpty || lhsKeys == rhsKeys)
        // union lhs/rhs keys, since one might be empty (if it's a scalar)
        if (canPushdown) Some(lhsKeys.get.union(rhsKeys.get)) else None
      case ps: PeriodicSeries => helper(ps.rawSeries)
      case psw: PeriodicSeriesWithWindowing => helper(psw.series)
      case aif: ApplyInstantFunction => helper(aif.vectors)
      case bj: BinaryJoin =>
        val lhsKeys = helper(bj.lhs)
        val rhsKeys = helper(bj.rhs)
        val canPushdownBj =
          lhsKeys.isDefined && rhsKeys.isDefined &&
            // either the lhs/rhs keys are equal, or at least one of lhs/rhs includes only scalars.
            (lhsKeys.get.isEmpty || rhsKeys.get.isEmpty || lhsKeys == rhsKeys) &&
            canPushdown(bj, targetSchemaProvider, nonMetricShardKeyCols, getShardKeyFilters)
        // union lhs/rhs keys, since one might be empty (if it's a scalar)
        if (canPushdownBj) Some(lhsKeys.get.union(rhsKeys.get)) else None
      case agg: Aggregate =>
        val keys = helper(agg.vectors)
        val canPushdownAgg = canPushdown(agg, targetSchemaProvider, nonMetricShardKeyCols, getShardKeyFilters)
        if (keys.isDefined && canPushdownAgg) keys else None
      case nl: NonLeafLogicalPlan =>
        // return the set of all child keys iff all child plans can be pushdown-optimized
        val keys = nl.children.map(helper)
        if (keys.forall(_.isDefined)) Some(keys.flatMap(_.get).toSet) else None
      case rs: RawSeries => Some(getRawPushdownKeys(rs))
      case sc: ScalarPlan => Some(Set.empty)  // since "None" will prevent optimization for all parent plans
      case _ => None
    }
    // Require that all raw-series selectors are configured with the same target-schema columns.
    if (sameRawSeriesTargetSchemaColumns(lp, targetSchemaProvider, getShardKeyFilters).isDefined) helper(lp) else None
  }
  // scalastyle:on method.length
  // scalastyle:on cyclomatic.complexity

  /**
   * Returns true iff the entire plan tree preserves the argument label set.
   * In other words:
   *   - no aggregation "aggregates away" any of the argument labels.
   *   - no scalar() wraps any selector.
   */
  def treePreservesLabels(lp: LogicalPlan, labels: Seq[String]): Boolean = lp match {
    case agg: Aggregate =>
      val clausePreservesLabels = agg.clauseOpt match {
        case Some(AggregateClause(ClauseType.By, clauseLabels)) =>
          labels.forall(clauseLabels.contains(_))
        case Some(AggregateClause(ClauseType.Without, clauseLabels)) =>
          labels.forall(!clauseLabels.contains(_))
        case _ => labels.isEmpty
      }
      if (!clausePreservesLabels) {
        return false
      }
      treePreservesLabels(agg.vectors, labels)
    case _: ScalarVaryingDoublePlan => false
    case nl: NonLeafLogicalPlan => nl.children.forall(treePreservesLabels(_, labels))
    case _ => true
  }

  /**
   * Returns the concrete sets of Equals non-metric shard-key filters defined by the
   *   argument plan's Equals and pipe-concatenated EqualsRegex filters.
   *
   * Example: [{A=~1|2, B=1}, {A=1, B=2}] -> [{A=1, B=1}, {A=2, B=1}, {A=1, B=2}]
   *
   * @param lp should contain only Equals and pipe-concatenated EqualsRegex filters.
   */
  def resolvePipeConcatenatedShardKeyFilters(lp: LogicalPlan,
                                             nonMetricShardKeyCols: Seq[String]): Seq[Seq[ColumnFilter]] = {
    LogicalPlan.getNonMetricShardKeyFilters(lp, nonMetricShardKeyCols)
      .flatMap { group =>
        val keyToValues = group.map { filter =>
          val values = filter match {
            case ColumnFilter(col, regex: EqualsRegex) if QueryUtils.containsPipeOnlyRegex(regex.value.toString) =>
              QueryUtils.splitAtUnescapedPipes(regex.value.toString).distinct
            case ColumnFilter(col, equals: Equals) =>
              Seq(equals.value.toString)
            case _ => throw new IllegalArgumentException("unexpected filter: " + filter)
          }
          (filter.column, values)
        }.toMap
        QueryUtils.makeAllKeyValueCombos(keyToValues).map { shardKeyMap =>
          // Convert to set of Equals-only ColumnFilters.
          shardKeyMap.map(entry => ColumnFilter(entry._1, Equals(entry._2))).toSet
        }
      }
      .distinct
      .map(_.toSeq)
  }

  def getLogicalPlanTreeStringRepresentation(logicalPlan: LogicalPlan) : String = {
    // iterate over all the children of the logical plan and get the string representation
    val children = logicalPlan match {
      case nl: NonLeafLogicalPlan => nl.children.map(getLogicalPlanTreeStringRepresentation)
      case _ => Seq()
    }
    if (children.isEmpty) {
      logicalPlan.getClass.getSimpleName
    } else {
      s"${logicalPlan.getClass.getSimpleName}(${children.mkString(",")})"
    }
  }
}
