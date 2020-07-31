package filodb.coordinator.queryplanner

import filodb.prometheus.ast.Vectors.PromMetricLabel
import filodb.prometheus.ast.WindowConstants
import filodb.query._
import filodb.query.InstantFunctionId.{ClampMax, ClampMin}
import filodb.query.RangeFunctionId.QuantileOverTime

object QueryConstants {
  val Quotes = "\""
  val OpeningCurlyBraces = "{"
  val ClosingCurlyBraces = "}"
  val Space = " "
  val Offset = "offset"
  val OpeningRoundBracket = "("
  val ClosingRoundBracket = ")"
  val OpeningSquareBracket = "["
  val ClosingSquareBracket = "]"
  val Comma = ","
}

object LogicalPlanUtils {

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
  def getTimeFromLogicalPlan(logicalPlan: LogicalPlan): TimeRange = {
    logicalPlan match {
      case lp: PeriodicSeries              => TimeRange(lp.startMs, lp.endMs)
      case lp: PeriodicSeriesWithWindowing => TimeRange(lp.startMs, lp.endMs)
      case lp: ApplyInstantFunction        => getTimeFromLogicalPlan(lp.vectors)
      case lp: Aggregate                   => getTimeFromLogicalPlan(lp.vectors)
      case lp: BinaryJoin                  => // can assume lhs & rhs have same time
                                              getTimeFromLogicalPlan(lp.lhs)
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
      case _                               => throw new BadQueryException(s"Invalid logical plan ${logicalPlan}")
    }
  }

  /**
   * Used to change start and end time(TimeRange) of LogicalPlan
   * NOTE: Plan should be PeriodicSeriesPlan
   */
  def copyLogicalPlanWithUpdatedTimeRange(logicalPlan: LogicalPlan,
                                          timeRange: TimeRange): LogicalPlan = {
    logicalPlan match {
      case lp: PeriodicSeriesPlan => copyWithUpdatedTimeRange(lp, timeRange)
      case lp: RawSeriesLikePlan => copyNonPeriodicWithUpdatedTimeRange(lp, timeRange)
      case _ => throw new UnsupportedOperationException("Logical plan not supported for copy")
    }
  }

  /**
    * Used to change start and end time(TimeRange) of LogicalPlan
    * NOTE: Plan should be PeriodicSeriesPlan
    */
  def copyWithUpdatedTimeRange(logicalPlan: LogicalPlan,
                               timeRange: TimeRange): PeriodicSeriesPlan = {
    logicalPlan match {
      case lp: PeriodicSeries => lp.copy(startMs = timeRange.startMs,
                                         endMs = timeRange.endMs,
                                         rawSeries = copyNonPeriodicWithUpdatedTimeRange(lp.rawSeries, timeRange)
                                                     .asInstanceOf[RawSeries])
      case lp: PeriodicSeriesWithWindowing => lp.copy(startMs = timeRange.startMs,
                                                      endMs = timeRange.endMs,
                                                      series = copyNonPeriodicWithUpdatedTimeRange(lp.series,
                                                               timeRange))
      case lp: ApplyInstantFunction => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange))

      case lp: Aggregate  => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange))

      case lp: BinaryJoin => lp.copy(lhs = copyWithUpdatedTimeRange(lp.lhs, timeRange),
                                     rhs = copyWithUpdatedTimeRange(lp.rhs, timeRange))
      case lp: ScalarVectorBinaryOperation =>
                      lp.copy(vector = copyWithUpdatedTimeRange(lp.vector, timeRange))

      case lp: ApplyMiscellaneousFunction =>
                      lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange))

      case lp: ApplySortFunction => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange))

      case _ => throw new UnsupportedOperationException("Logical plan not supported for copy")
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
    LogicalPlan.findLeafLogicalPlans(logicalPlan).head match {
      case lp: RawSeries => lp.rangeSelector match {
        case rs: IntervalSelector => Some(rs.from)
        case _ => None
      }
      case _                      => throw new BadQueryException(s"Invalid logical plan $logicalPlan")
    }
  }

  def getOffsetMillis(logicalPlan: LogicalPlan): Long = {
    LogicalPlan.findLeafLogicalPlans(logicalPlan).head match {
      case lp: RawSeries => lp.offsetMs.getOrElse(0)
      case _             => 0
    }
  }

  def getLookBackMillis(logicalPlan: LogicalPlan): Long = {
    val staleDataLookbackMillis = WindowConstants.staleDataLookbackMillis
    LogicalPlan.findLeafLogicalPlans(logicalPlan).head match {
      case lp: RawSeries => lp.lookbackMs.getOrElse(staleDataLookbackMillis)
      case _             => 0
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

  import QueryConstants._

  private def getFiltersFromRawSeries(lp: RawSeries)= lp.filters.map(f => (f.column, f.filter.operatorString,
    Quotes + f.filter.valuesStrings.head.toString + Quotes))

  private def filtersToQuery(filters: Seq[(String, String, String)]): String = {
    // Get metric name from filters and remove quotes from start and end
    val name = filters.find(x => x._1.equals(PromMetricLabel)).head._3.replaceAll("^\"|\"$", "")
    // When only metric name is present
    if (filters.size == 1) name
    else name + OpeningCurlyBraces + filters.filterNot(x => x._1.equals(PromMetricLabel)).
      map(f => f._1 + f._2 + f._3).mkString(Comma) + ClosingCurlyBraces
  }

  private def rawSeriesLikeToQuery(lp: RawSeriesLikePlan): String = {
    lp match {
      case r: RawSeries               => filtersToQuery(getFiltersFromRawSeries(r))
      case a: ApplyInstantFunctionRaw => val filters = getFiltersFromRawSeries(a.vectors)
        val bucketFilter = ("_bucket_", "=", Quotes + functionArgsToQuery(a.functionArgs.head) + Quotes)
        filtersToQuery(filters :+ bucketFilter)
      case _            => throw new UnsupportedOperationException(s"$lp can't be converted to Query")
    }
  }

  private def periodicSeriesToQuery(periodicSeries: PeriodicSeries): String = {
    rawSeriesLikeToQuery(periodicSeries.rawSeries) +
      periodicSeries.offsetMs.map(o => Space + Offset + Space + (o / 1000).toString + "s" ).getOrElse("")
  }


  private def aggregateToQuery(lp: Aggregate): String = {
    val periodicSeriesQuery = logicalPlanToQuery(lp.vectors)
    val byString = if (lp.by.isEmpty) "" else Space + "by" + Space + OpeningRoundBracket + lp.by.mkString(Comma) +
      ClosingRoundBracket
    val withoutString = if (lp.without.isEmpty) "" else Space + "without" + Space + OpeningRoundBracket +
      lp.without.mkString(Comma) + ClosingRoundBracket

    lp.operator.toString.toLowerCase + OpeningRoundBracket + periodicSeriesQuery + ClosingRoundBracket + byString +
      withoutString
  }

  private def absentFnToQuery(lp: ApplyAbsentFunction): String = {
    val periodicSeriesQuery = logicalPlanToQuery(lp.vectors)
    "absent" + OpeningRoundBracket + periodicSeriesQuery + ClosingRoundBracket
  }

  private def functionArgsToQuery(f: FunctionArgsPlan): String = {
   f match {
     case d: ScalarFixedDoublePlan   => d.scalar.toString
     case t: ScalarTimeBasedPlan     => t.function.entryName + OpeningRoundBracket + ClosingRoundBracket
     case s: ScalarVaryingDoublePlan => logicalPlanToQuery(s.vectors)
   }
  }

  private def instantFnToQuery(lp: ApplyInstantFunction): String = {
    val periodicSeriesQuery = logicalPlanToQuery(lp.vectors)
    val prefix = lp.function.entryName + OpeningRoundBracket
    if (lp.functionArgs.isEmpty) prefix + periodicSeriesQuery + ClosingRoundBracket
    else {
      if (lp.function.equals(ClampMax) || lp.function.equals(ClampMin))
        prefix + periodicSeriesQuery + Comma + functionArgsToQuery(lp.functionArgs.head) + ClosingRoundBracket
      else prefix + functionArgsToQuery(lp.functionArgs.head) + Comma +
        periodicSeriesQuery + ClosingRoundBracket
    }
  }

  private def scalarBinaryOperationToQuery(lp: ScalarBinaryOperation): String = {
    val rhs = if (lp.rhs.isLeft) lp.rhs.left.get.toString else logicalPlanToQuery(lp.rhs.right.get)
    val lhs = if (lp.lhs.isLeft) lp.lhs.left.get.toString else logicalPlanToQuery(lp.lhs.right.get)
    lhs + Space + lp.operator.operatorString + Space + rhs
  }

  private def scalarVectorBinaryOperationToQuery(lp: ScalarVectorBinaryOperation): String = {
    val periodicSeriesQuery = logicalPlanToQuery(lp.vector)
    if (lp.scalarIsLhs) functionArgsToQuery(lp.scalarArg) + Space + lp.operator.operatorString + Space +
      periodicSeriesQuery
    else periodicSeriesQuery + Space + lp.operator.operatorString + Space + functionArgsToQuery(lp.scalarArg)
  }

  private def periodicSeriesWithWindowingToQuery(lp: PeriodicSeriesWithWindowing): String = {
    val rawSeriesQueryWithWindow = rawSeriesLikeToQuery(lp.series) + OpeningSquareBracket + lp.window/1000 + "s" +
      ClosingSquareBracket + lp.offsetMs.map(o => Space + Offset + Space + (o / 1000).toString + "s" ).getOrElse("")
    val prefix = lp.function.entryName + OpeningRoundBracket
    if (lp.functionArgs.isEmpty) prefix + rawSeriesQueryWithWindow + ClosingRoundBracket
    else {
      if (lp.function.equals(QuantileOverTime))
        prefix + functionArgsToQuery(lp.functionArgs.head) + Comma + rawSeriesQueryWithWindow + ClosingRoundBracket
      else
        prefix + rawSeriesQueryWithWindow + Comma + lp.functionArgs.map(functionArgsToQuery(_)).mkString(Comma) +
          ClosingRoundBracket
    }
  }

  private def sortToQuery(lp: ApplySortFunction): String = {
    val vector = logicalPlanToQuery(lp.vectors)
    lp.function.entryName + OpeningRoundBracket + vector + ClosingRoundBracket
  }

  private def binaryJoinToQuery(lp: BinaryJoin): String = {
    val lhs = logicalPlanToQuery(lp.lhs)
    val rhs = logicalPlanToQuery(lp.rhs)
    val on = if (lp.on.isEmpty) "" else Space + "on" + OpeningRoundBracket + lp.on.mkString(Comma) +
      ClosingRoundBracket
    val ignoring = if (lp.ignoring.isEmpty) "" else Space + "ignoring" + OpeningRoundBracket +
      lp.ignoring.mkString(Comma) + ClosingRoundBracket
    val groupingType = lp.cardinality match {
      case Cardinality.OneToMany  => Space + "group_right"
      case Cardinality.ManyToOne  => Space + "group_left"
      case _                      => ""
    }
    val grouping = if (lp.include.isEmpty) groupingType else groupingType + OpeningRoundBracket +
      lp.include.mkString(Comma) + ClosingRoundBracket
    lhs + Space + lp.operator.operatorString + on + ignoring + grouping + Space + rhs
  }

  def miscellaneousFnToQuery(lp: ApplyMiscellaneousFunction): String = {
   val prefix = lp.function.entryName + OpeningRoundBracket + logicalPlanToQuery(lp.vectors)
   if(lp.stringArgs.isEmpty) prefix + ClosingRoundBracket
   else prefix + Comma + lp.stringArgs.map(Quotes + _ + Quotes).mkString(Comma) + ClosingRoundBracket
  }

  def scalarVaryingDoubleToQuery(lp: ScalarVaryingDoublePlan): String = {
    lp.function.entryName + OpeningRoundBracket + logicalPlanToQuery(lp.vectors) + ClosingRoundBracket
  }

  def scalarTimeToQuery(lp: ScalarTimeBasedPlan): String = lp.function.entryName + OpeningRoundBracket +
    ClosingRoundBracket

  def vectorToQuery(lp: VectorPlan): String = "vector" + OpeningRoundBracket + logicalPlanToQuery(lp.scalars) +
    ClosingRoundBracket

  def logicalPlanToQuery(logicalPlan: LogicalPlan): String = {
    logicalPlan match {
      case lp: RawSeries                   => rawSeriesLikeToQuery(lp)
      case lp: PeriodicSeries              => periodicSeriesToQuery(lp)
      case lp: Aggregate                   => aggregateToQuery(lp)
      case lp: ApplyAbsentFunction         => absentFnToQuery(lp)
      case lp: ApplyInstantFunction        => instantFnToQuery(lp)
      case lp: ScalarVectorBinaryOperation => scalarVectorBinaryOperationToQuery(lp)
      case lp: ScalarBinaryOperation       => scalarBinaryOperationToQuery(lp)
      case lp: PeriodicSeriesWithWindowing => periodicSeriesWithWindowingToQuery(lp)
      case lp: ApplySortFunction           => sortToQuery(lp)
      case lp: BinaryJoin                  => binaryJoinToQuery(lp)
      case lp: ApplyMiscellaneousFunction  => miscellaneousFnToQuery(lp)
      case lp: ScalarVaryingDoublePlan     => scalarVaryingDoubleToQuery(lp)
      case lp: ScalarTimeBasedPlan         => scalarTimeToQuery(lp)
      case lp: ScalarFixedDoublePlan       => lp.scalar.toString
      case lp: VectorPlan                  => vectorToQuery(lp)
      case _                               => throw new UnsupportedOperationException(s"Logical plan to query not " +
                                              s"supported for ${logicalPlan}")
    }
  }
}
