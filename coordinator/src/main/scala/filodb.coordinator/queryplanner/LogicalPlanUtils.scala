package filodb.coordinator.queryplanner

import filodb.prometheus.ast.Vectors.PromMetricLabel
import filodb.prometheus.ast.WindowConstants
import filodb.query.InstantFunctionId.{ClampMax, ClampMin}
import filodb.query.RangeFunctionId.QuantileOverTime
import filodb.query._

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

  private val QUOTES = "\""
  private val OPENING_CURLY_BRACES = "{"
  private val CLOSING_CURLY_BRACES = "}"
  private val SPACE = " "
  private val OFFSET = "offset"
  private val OPENING_ROUND_BRACKET = "("
  private val CLOSING_ROUND_BRACKET = ")"
  private val OPENING_SQUARE_BRACKET = "["
  private val CLOSING_SQUARE_BRACKET = "]"
  private val COMMA = ","


  private def getFiltersFromRawSeries(lp: RawSeries)= lp.filters.map(f => (f.column, f.filter.operatorString,
    QUOTES + f.filter.valuesStrings.head.toString + QUOTES))

  private def filersToQuery(filters: Seq[(String, String, String)]): String = {
    val name = filters.find(x => x._1.equals(PromMetricLabel)).head._3 replaceAll(QUOTES, "")
    name + OPENING_CURLY_BRACES + filters.filterNot(x => x._1.equals(PromMetricLabel)).
      map(f => f._1 + f._2 + f._3).mkString(",") + CLOSING_CURLY_BRACES
  }

  private def rawSeriesLikeToQuery(lp: RawSeriesLikePlan): String = {
    lp match {
      case r: RawSeries               => filersToQuery(getFiltersFromRawSeries(r))
      case a: ApplyInstantFunctionRaw => val filters = getFiltersFromRawSeries(a.vectors)
        val bucketFilter = ("_bucket_", "=", QUOTES + functionArgsToQuery(a.functionArgs.head) + QUOTES)
        filersToQuery(filters :+ bucketFilter)
      case _            => throw new UnsupportedOperationException(s"$lp can't be converted to Query")
    }
  }

  private def periodicSeriesToQuery(periodicSeries: PeriodicSeries): String = {
    rawSeriesLikeToQuery(periodicSeries.rawSeries) +
      periodicSeries.offsetMs.map(o => SPACE + OFFSET + SPACE + (o / 1000).toString + "s" ).getOrElse("")
  }


  private def aggregateToQuery(lp: Aggregate): String = {
    val periodicSeriesQuery = periodicSeriesPlanToQuery(lp.vectors)
    val byString = if (lp.by.isEmpty) "" else SPACE + "by" + SPACE + OPENING_ROUND_BRACKET + lp.by.mkString(",") +
      CLOSING_ROUND_BRACKET
    val withoutString = if (lp.without.isEmpty) "" else SPACE + "without" + SPACE + OPENING_ROUND_BRACKET +
      lp.without.mkString(",") + CLOSING_ROUND_BRACKET

    lp.operator.toString.toLowerCase + OPENING_ROUND_BRACKET + periodicSeriesQuery + CLOSING_ROUND_BRACKET + byString +
      withoutString


  }

  private def absentFnToQuery(lp: ApplyAbsentFunction): String = {
    val periodicSeriesQuery = periodicSeriesPlanToQuery(lp.vectors)
    "absent" + OPENING_ROUND_BRACKET + periodicSeriesQuery + CLOSING_ROUND_BRACKET
  }

  private def functionArgsToQuery(f: FunctionArgsPlan): String = {
   f match {
     case d: ScalarFixedDoublePlan => d.scalar.toString
     case t: ScalarTimeBasedPlan => t.function.entryName + OPENING_ROUND_BRACKET + CLOSING_ROUND_BRACKET
     case s: ScalarVaryingDoublePlan => periodicSeriesPlanToQuery(s.vectors)
   }
  }

  private def instantFnToQuery(lp: ApplyInstantFunction): String = {
    val periodicSeriesQuery = periodicSeriesPlanToQuery(lp.vectors)
    val prefix = lp.function.entryName + OPENING_ROUND_BRACKET
    if (lp.functionArgs.isEmpty) prefix + periodicSeriesQuery + CLOSING_ROUND_BRACKET
    else {
      if (lp.function.equals(ClampMax) || lp.function.equals(ClampMin))
        prefix + periodicSeriesQuery + COMMA + functionArgsToQuery(lp.functionArgs.head) + CLOSING_ROUND_BRACKET
      else prefix + functionArgsToQuery(lp.functionArgs.head) + COMMA +
        periodicSeriesQuery + CLOSING_ROUND_BRACKET
    }
  }

  private def scalarBinaryOperationToQuery(lp: ScalarBinaryOperation): String = {
    val rhs = if (lp.rhs.isLeft) lp.rhs.left.get.toString else periodicSeriesPlanToQuery(lp.rhs.right.get)
    val lhs = if (lp.lhs.isLeft) lp.lhs.left.get.toString else periodicSeriesPlanToQuery(lp.lhs.right.get)
    lhs + SPACE + lp.operator.operatorString + SPACE + rhs

  }

  private def scalarVectorBinaryOperationToQuery(lp: ScalarVectorBinaryOperation): String = {
    val periodicSeriesQuery = periodicSeriesPlanToQuery(lp.vector)

    if (lp.scalarIsLhs) functionArgsToQuery(lp.scalarArg) + SPACE + lp.operator.operatorString + SPACE +
      periodicSeriesQuery
    else periodicSeriesQuery + SPACE + lp.operator.operatorString + SPACE + functionArgsToQuery(lp.scalarArg)
  }

  private def periodicSeriesWithWindowingToQuery(lp: PeriodicSeriesWithWindowing): String = {

    val rawSeriesQueryWithWindow = rawSeriesLikeToQuery(lp.series) + OPENING_SQUARE_BRACKET + lp.window/1000 + "s" +
      CLOSING_SQUARE_BRACKET
    val prefix = lp.function.entryName + OPENING_ROUND_BRACKET
    if (lp.functionArgs.isEmpty) prefix + rawSeriesQueryWithWindow + CLOSING_ROUND_BRACKET
    else {
      if (lp.function.equals(QuantileOverTime))
        prefix + functionArgsToQuery(lp.functionArgs.head) + COMMA + rawSeriesQueryWithWindow + CLOSING_ROUND_BRACKET
      else
        prefix + rawSeriesQueryWithWindow + COMMA + lp.functionArgs.map(functionArgsToQuery(_)).mkString(",") +
          CLOSING_ROUND_BRACKET
    }

  }

  private def periodicSeriesPlanToQuery(lp: PeriodicSeriesPlan): String = {
    lp match {
      case lp: PeriodicSeries              => periodicSeriesToQuery(lp)
      case lp: Aggregate                   => aggregateToQuery(lp)
      case lp: ApplyAbsentFunction         => absentFnToQuery(lp)
      case lp: ApplyInstantFunction        => instantFnToQuery(lp)
      case lp: ScalarVectorBinaryOperation      => scalarVectorBinaryOperationToQuery(lp)
      case lp: ScalarBinaryOperation      => scalarBinaryOperationToQuery(lp)
      case lp: PeriodicSeriesWithWindowing =>periodicSeriesWithWindowingToQuery(lp)
      case _                               => throw new UnsupportedOperationException(s"$lp not supported")
    }
  }

  def logicalPlanToQuery(logicalPlan: LogicalPlan): String = {
    logicalPlan match {
      case lp: PeriodicSeriesPlan         =>  periodicSeriesPlanToQuery(lp)

//      case lp: PeriodicSeriesWithWindowing => TimeRange(lp.startMs, lp.endMs)
//      case lp: ApplyInstantFunction        => getTimeFromLogicalPlan(lp.vectors)
//      case lp: Aggregate                   => getTimeFromLogicalPlan(lp.vectors)
//      case lp: BinaryJoin                  => // can assume lhs & rhs have same time
//        getTimeFromLogicalPlan(lp.lhs)
//      case lp: ScalarVectorBinaryOperation => getTimeFromLogicalPlan(lp.vector)
//      case lp: ApplyMiscellaneousFunction  => getTimeFromLogicalPlan(lp.vectors)
//      case lp: ApplySortFunction           => getTimeFromLogicalPlan(lp.vectors)
//      case lp: ScalarVaryingDoublePlan     => getTimeFromLogicalPlan(lp.vectors)
//      case lp: ScalarTimeBasedPlan         => TimeRange(lp.rangeParams.startSecs, lp.rangeParams.endSecs)
//      case lp: VectorPlan                  => getTimeFromLogicalPlan(lp.scalars)
//      case lp: ApplyAbsentFunction         => getTimeFromLogicalPlan(lp.vectors)
//      case lp: RawSeries                   => lp.rangeSelector match {
//        case i: IntervalSelector => TimeRange(i.from, i.to)
//        case _ => throw new BadQueryException(s"Invalid logical plan")
//      }
      case _                               => throw new BadQueryException(s"Invalid logical plan ${logicalPlan}")
    }
  }

}
