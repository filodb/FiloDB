package filodb.coordinator.queryplanner

import org.apache.commons.text.StringEscapeUtils

import filodb.core.query.ColumnFilter
import filodb.prometheus.ast.Vectors.PromMetricLabel
import filodb.prometheus.ast.WindowConstants
import filodb.query._
import filodb.query.AggregationOperator.CountValues
import filodb.query.InstantFunctionId.{ClampMax, ClampMin}
import filodb.query.RangeFunctionId.{QuantileOverTime, Timestamp}

object QueryConstants {
  val Quotes = "\""
  val OpeningCurlyBraces = "{"
  val ClosingCurlyBraces = "}"
  val Space = " "
  val Offset = "offset"
  val At = "@"
  val OpeningRoundBracket = "("
  val ClosingRoundBracket = ")"
  val OpeningSquareBracket = "["
  val ClosingSquareBracket = "]"
  val Comma = ","
}

object LogicalPlanParser {
  import QueryConstants._

  private def getFiltersFromRawSeries(lp: RawSeries)= getFiltersFromColumnFilters(lp.filters)

  private def getFiltersFromColumnFilters(filters: Seq[ColumnFilter]) = {
    filters.map(f => (f.column, f.filter.operatorString,
      Quotes + StringEscapeUtils.escapeJava(f.filter.valuesStrings.head.toString) + Quotes))
  }

  private def filtersToQuery(filters: Seq[(String, String, String)], columns: Seq[String],
                             lookback: Option[Long], offset: Option[Long], addWindow: Boolean): String = {
    val columnString = if (columns.isEmpty) "" else s"::${columns.head}"
    // Get metric name from filters and remove quotes from start and end
    val name = s"${filters.find(x => x._1.equals(PromMetricLabel)).head._3.
      replaceAll("^\"|\"$", "")}$columnString"
    val window = if (lookback.isDefined) {
      // Window should not be added for queries like sum(foo) even though they have lookback
      if (lookback.get.equals(WindowConstants.staleDataLookbackMillis) && !addWindow) ""
      else s"$OpeningSquareBracket${lookback.get / 1000}s$ClosingSquareBracket"
    } else ""
    val offsetString = offset.map(o => s"$Space$Offset$Space${(o / 1000).toString}s").getOrElse("")
    // When only metric name is present
    if (filters.size == 1) name + window + offsetString
    else s"$name$OpeningCurlyBraces${filters.filterNot(x => x._1.equals(PromMetricLabel)).
      map(f => f._1 + f._2 + f._3).mkString(Comma)}$ClosingCurlyBraces" + window + offsetString
  }

  private def rawSeriesLikeToQuery(lp: RawSeriesLikePlan, addWindow: Boolean): String = {
    lp match {
      case r: RawSeries               => filtersToQuery(getFiltersFromRawSeries(r), r.columns, r.lookbackMs,
                                         r.offsetMs, addWindow)
      case a: ApplyInstantFunctionRaw => val filters = getFiltersFromRawSeries(a.vectors)
        val bucketFilter = ("_bucket_", "=", s"$Quotes${functionArgsToQuery(a.functionArgs.head)}$Quotes")
        filtersToQuery(filters :+ bucketFilter, a.vectors.columns, a.vectors.lookbackMs, a.vectors.offsetMs, addWindow)
      case _                          => throw new UnsupportedOperationException(s"$lp can't be converted to Query")
    }
  }

  private def periodicSeriesToQuery(periodicSeries: PeriodicSeries): String = {
    // Queries like sum(foo) should not have window even though stale lookback is present
    val at = periodicSeries.atMs.fold("")(atMs => s" $At${atMs / 1000}")
    s"${rawSeriesLikeToQuery(periodicSeries.rawSeries, false)}$at"
  }

  private def aggregateToQuery(lp: Aggregate): String = {
    import filodb.query.AggregateClause.ClauseType
    val periodicSeriesQuery = convertToQuery(lp.vectors)
    val clauseString = lp.clauseOpt.map { clause =>
      val typeString = clause.clauseType match {
        case ClauseType.By => "by"
        case ClauseType.Without => "without"
      }
      val labelString = clause.labels.mkString(Comma)
      s"${Space}${typeString}${Space}$OpeningRoundBracket${labelString}$ClosingRoundBracket"
    }.getOrElse("")
    val params = if (lp.params.isEmpty) "" else {
      lp.params.map(p => if (p.isInstanceOf[String]) {Quotes + p + Quotes} else p).mkString(Comma) + Comma
    }

    val function = if (lp.operator.equals(CountValues)) "count_values" else lp.operator.toString.toLowerCase
    s"$function$OpeningRoundBracket$params$periodicSeriesQuery$ClosingRoundBracket$clauseString"
  }

  private def absentFnToQuery(lp: ApplyAbsentFunction): String = {
    val periodicSeriesQuery = convertToQuery(lp.vectors)
    s"absent$OpeningRoundBracket$periodicSeriesQuery$ClosingRoundBracket"
  }

  private def functionArgsToQuery(f: FunctionArgsPlan): String = {
    f match {
      case d: ScalarFixedDoublePlan   => d.scalar.toString
      case t: ScalarTimeBasedPlan     => s"${t.function.entryName}$OpeningRoundBracket$ClosingRoundBracket"
      case s: ScalarVaryingDoublePlan =>
        val functionName = s.function.entryName
        val vectorsQuery = convertToQuery(s.vectors)
        s"$functionName$OpeningRoundBracket$vectorsQuery$ClosingRoundBracket"
      case s: ScalarBinaryOperation   => scalarBinaryOperationToQuery(s)
    }
  }

  private def instantFnToQuery(lp: ApplyInstantFunction): String = {
    val periodicSeriesQuery = convertToQuery(lp.vectors)
    val prefix = lp.function.entryName + OpeningRoundBracket
    if (lp.functionArgs.isEmpty) s"$prefix$periodicSeriesQuery$ClosingRoundBracket"
    else {
      if (lp.function.equals(ClampMax) || lp.function.equals(ClampMin))
        s"$prefix$periodicSeriesQuery$Comma${functionArgsToQuery(lp.functionArgs.head)}$ClosingRoundBracket"
      else s"$prefix${functionArgsToQuery(lp.functionArgs.head)}$Comma$periodicSeriesQuery$ClosingRoundBracket"
    }
  }

  private def scalarBinaryOperationToQuery(lp: ScalarBinaryOperation): String = {
    val rhs = if (lp.rhs.isLeft) lp.rhs.left.get.toString else convertToQuery(lp.rhs.right.get)
    val lhs = if (lp.lhs.isLeft) lp.lhs.left.get.toString else convertToQuery(lp.lhs.right.get)
    s"($lhs$Space${lp.operator.operatorString}$Space$rhs)"
  }

  private def scalarVectorBinaryOperationToQuery(lp: ScalarVectorBinaryOperation): String = {
    val periodicSeriesQuery = convertToQuery(lp.vector)
    if (lp.scalarIsLhs) s"(${functionArgsToQuery(lp.scalarArg)}$Space${lp.operator.operatorString}$Space" +
      s"$periodicSeriesQuery)"
    else s"($periodicSeriesQuery ${lp.operator.operatorString} ${functionArgsToQuery(lp.scalarArg)})"
  }

  private def periodicSeriesWithWindowingToQuery(lp: PeriodicSeriesWithWindowing): String = {
    // Except Timestamp function all functions have window
    val addWindow = if (lp.function.equals(Timestamp)) false else true
    val rawSeries = rawSeriesLikeToQuery(lp.series, addWindow)
    val prefix = lp.function.entryName + OpeningRoundBracket
    val at = lp.atMs.fold("")(atMs => s" $At${atMs / 1000}")
    if (lp.functionArgs.isEmpty) s"$prefix$rawSeries$at$ClosingRoundBracket"
    else {
      if (lp.function.equals(QuantileOverTime))
        s"$prefix${functionArgsToQuery(lp.functionArgs.head)}$Comma$rawSeries$ClosingRoundBracket"
      else
        s"$prefix$rawSeries$at$Comma${lp.functionArgs.map(functionArgsToQuery(_)).mkString(Comma)}" +
          s"$ClosingRoundBracket"
    }
  }

  private def sortToQuery(lp: ApplySortFunction): String = {
    val vector = convertToQuery(lp.vectors)
    s"${lp.function.entryName}$OpeningRoundBracket$vector$ClosingRoundBracket"
  }

  private def binaryJoinToQuery(lp: BinaryJoin): String = {
    val lhs = convertToQuery(lp.lhs)
    val rhs = convertToQuery(lp.rhs)
    val on = lp.on.map(l => s"${Space}on$OpeningRoundBracket${l.mkString(Comma)}$ClosingRoundBracket").getOrElse("")
    val ignoring = if (lp.ignoring.isEmpty) "" else s"${Space}ignoring$OpeningRoundBracket" +
      s"${lp.ignoring.mkString(Comma)}$ClosingRoundBracket"

    val groupingType = lp.cardinality match {
      case Cardinality.OneToMany  => s"${Space}group_right"
      case Cardinality.ManyToOne  => s"${Space}group_left"
      case _                      => ""
    }
    val grouping = if (lp.include.isEmpty) groupingType else s"$groupingType$OpeningRoundBracket" +
      s"${lp.include.mkString(Comma)}$ClosingRoundBracket"
    s"($lhs$Space${lp.operator.operatorString}$on$ignoring$grouping$Space$rhs)"
  }

  def miscellaneousFnToQuery(lp: ApplyMiscellaneousFunction): String = {
    val prefix = s"${lp.function.entryName}$OpeningRoundBracket${convertToQuery(lp.vectors)}"
    if(lp.stringArgs.isEmpty) s"$prefix$ClosingRoundBracket"
    else s"$prefix$Comma${lp.stringArgs.map(Quotes + _ + Quotes).mkString(Comma)}$ClosingRoundBracket"
  }

  def scalarVaryingDoubleToQuery(lp: ScalarVaryingDoublePlan): String = {
    s"${lp.function.entryName}$OpeningRoundBracket${convertToQuery(lp.vectors)}$ClosingRoundBracket"
  }

  def scalarTimeToQuery(lp: ScalarTimeBasedPlan): String = s"${lp.function.entryName}$OpeningRoundBracket" +
    ClosingRoundBracket

  def vectorToQuery(lp: VectorPlan): String = s"vector$OpeningRoundBracket${convertToQuery(lp.scalars)}" +
    ClosingRoundBracket

  private def subqueryWithWindowingToQuery(sqww: SubqueryWithWindowing): String = {
    val periodicSeriesQuery = convertToQuery(sqww.innerPeriodicSeries)
    val prefix = sqww.functionId.entryName + OpeningRoundBracket
    val sqClause =
      s"${OpeningSquareBracket}${sqww.subqueryWindowMs/1000}s:${sqww.subqueryStepMs/1000}s$ClosingSquareBracket"
    val offset = sqww.offsetMs.fold("")(offsetMs => s" offset ${offsetMs/1000}s")
    val at = sqww.atMs.fold("")(atMs => s" $At${atMs / 1000}")
    val suffix = s"$sqClause$offset$at$ClosingRoundBracket"
    if (sqww.functionArgs.isEmpty) s"$prefix$periodicSeriesQuery$suffix"
    else if (sqww.functionArgs.size == 1) {
      s"$prefix${functionArgsToQuery(sqww.functionArgs.head)}$Comma$periodicSeriesQuery$suffix"
    } else if (sqww.functionArgs.size == 2) {
      s"$prefix${functionArgsToQuery(sqww.functionArgs.head)}$Comma" +
        s"${functionArgsToQuery(sqww.functionArgs(1))}$Comma$periodicSeriesQuery$suffix"
    } else {
      throw new UnsupportedOperationException(s"Unable to write query for $sqww")
    }
  }

  private def topLevelSubqueryToQuery(tlsq: TopLevelSubquery): String = {
    val periodicSeriesQuery = convertToQuery(tlsq.innerPeriodicSeries)
    val offset = tlsq.originalOffsetMs.fold("")(offsetMs => s" offset ${offsetMs/1000}s")
    val at = tlsq.atMs.fold("")(atMs => s" $At${atMs / 1000}")
    val sqClause =
      s"${OpeningSquareBracket}${(tlsq.orginalLookbackMs)/1000}s:${tlsq.stepMs/1000}s$ClosingSquareBracket"
    s"$periodicSeriesQuery$sqClause$offset$at"
  }

  def metadataMatchToQuery(lp: MetadataQueryPlan): String = {
    s"$OpeningCurlyBraces${
      getFiltersFromColumnFilters(lp.filters).map(f => f._1 + f._2 + f._3).mkString(Comma)
    }$ClosingCurlyBraces"
  }

  private def applyLimitToQuery(lp: ApplyLimitFunction): String = {
    val periodicSeriesQuery = convertToQuery(lp.vectors)
    s"$periodicSeriesQuery limit ${lp.limit}"
  }

  //scalastyle:off cyclomatic.complexity
  def convertToQuery(logicalPlan: LogicalPlan): String = {
    logicalPlan match {
      case lp: RawSeries                   => rawSeriesLikeToQuery(lp, true)
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
      case lp: SubqueryWithWindowing       => subqueryWithWindowingToQuery(lp)
      case lp: TopLevelSubquery            => topLevelSubqueryToQuery(lp)
      case lp: MetadataQueryPlan           => metadataMatchToQuery(lp)
      case lp: ApplyLimitFunction          => applyLimitToQuery(lp)
      case lp: ApplyInstantFunctionRaw     => throw new UnsupportedOperationException(s"Logical plan to PromQL not " +
                                                                                      s"supported for $lp")
      case lp: RawChunkMeta                => throw new UnsupportedOperationException(s"Logical plan to PromQL not " +
                                                                                      s"supported for $lp")
      case lp: TsCardinalities             => throw new UnsupportedOperationException(s"Logical plan to PromQL not " +
                                                                                      s"supported for $lp")
      // do not add default case here as we want to generate compile error when a new logical plan is added
    }
  }
}
