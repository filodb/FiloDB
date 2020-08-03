package filodb.coordinator.queryplanner

import filodb.prometheus.ast.Vectors.PromMetricLabel
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

object LogicalPlanParser {
  import QueryConstants._

  private def getFiltersFromRawSeries(lp: RawSeries)= lp.filters.map(f => (f.column, f.filter.operatorString,
    Quotes + f.filter.valuesStrings.head.toString + Quotes))

  private def filtersToQuery(filters: Seq[(String, String, String)]): String = {
    // Get metric name from filters and remove quotes from start and end
    val name = filters.find(x => x._1.equals(PromMetricLabel)).head._3.replaceAll("^\"|\"$", "")
    // When only metric name is present
    if (filters.size == 1) name
    else s"$name$OpeningCurlyBraces${filters.filterNot(x => x._1.equals(PromMetricLabel)).
      map(f => f._1 + f._2 + f._3).mkString(Comma)}$ClosingCurlyBraces"
  }

  private def rawSeriesLikeToQuery(lp: RawSeriesLikePlan): String = {
    lp match {
      case r: RawSeries               => filtersToQuery(getFiltersFromRawSeries(r))
      case a: ApplyInstantFunctionRaw => val filters = getFiltersFromRawSeries(a.vectors)
        val bucketFilter = ("_bucket_", "=", s"$Quotes${functionArgsToQuery(a.functionArgs.head)}$Quotes")
        filtersToQuery(filters :+ bucketFilter)
      case _            => throw new UnsupportedOperationException(s"$lp can't be converted to Query")
    }
  }

  private def periodicSeriesToQuery(periodicSeries: PeriodicSeries): String = {
    s"${rawSeriesLikeToQuery(periodicSeries.rawSeries)}${periodicSeries.offsetMs.map(o => Space + Offset + Space +
      (o / 1000).toString + "s" ).getOrElse("")}"
  }

  private def aggregateToQuery(lp: Aggregate): String = {
    val periodicSeriesQuery = convertToQuery(lp.vectors)
    val byString = if (lp.by.isEmpty) "" else Space + "by" + Space + OpeningRoundBracket + lp.by.mkString(Comma) +
      ClosingRoundBracket
    val withoutString = if (lp.without.isEmpty) "" else s"${Space}without$Space$OpeningRoundBracket" +
      s"${lp.without.mkString(Comma)}$ClosingRoundBracket"

    s"${lp.operator.toString.toLowerCase}$OpeningRoundBracket$periodicSeriesQuery$ClosingRoundBracket$byString" +
      s"$withoutString"
  }

  private def absentFnToQuery(lp: ApplyAbsentFunction): String = {
    val periodicSeriesQuery = convertToQuery(lp.vectors)
    s"absent$OpeningRoundBracket$periodicSeriesQuery$ClosingRoundBracket"
  }

  private def functionArgsToQuery(f: FunctionArgsPlan): String = {
    f match {
      case d: ScalarFixedDoublePlan   => d.scalar.toString
      case t: ScalarTimeBasedPlan     => s"${t.function.entryName}$OpeningRoundBracket$ClosingRoundBracket"
      case s: ScalarVaryingDoublePlan => convertToQuery(s.vectors)
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
    s"$lhs$Space${lp.operator.operatorString}$Space$rhs"
  }

  private def scalarVectorBinaryOperationToQuery(lp: ScalarVectorBinaryOperation): String = {
    val periodicSeriesQuery = convertToQuery(lp.vector)
    if (lp.scalarIsLhs) s"${functionArgsToQuery(lp.scalarArg)}$Space${lp.operator.operatorString}$Space" +
      s"$periodicSeriesQuery"
    else s"$periodicSeriesQuery ${lp.operator.operatorString} ${functionArgsToQuery(lp.scalarArg)}"
  }

  private def periodicSeriesWithWindowingToQuery(lp: PeriodicSeriesWithWindowing): String = {
    val rawSeriesQueryWithWindow = s"${rawSeriesLikeToQuery(lp.series)}$OpeningSquareBracket${lp.window/1000}s" +
      s"$ClosingSquareBracket${lp.offsetMs.map(o => Space + Offset + Space + (o / 1000).toString + "s").getOrElse("")}"
    val prefix = lp.function.entryName + OpeningRoundBracket
    if (lp.functionArgs.isEmpty) s"$prefix$rawSeriesQueryWithWindow$ClosingRoundBracket"
    else {
      if (lp.function.equals(QuantileOverTime))
        s"$prefix${functionArgsToQuery(lp.functionArgs.head)}$Comma$rawSeriesQueryWithWindow$ClosingRoundBracket"
      else
        s"$prefix$rawSeriesQueryWithWindow$Comma${lp.functionArgs.map(functionArgsToQuery(_)).mkString(Comma)}" +
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
    val on = if (lp.on.isEmpty) "" else s"${Space}on$OpeningRoundBracket${lp.on.mkString(Comma)}$ClosingRoundBracket"
    val ignoring = if (lp.ignoring.isEmpty) "" else s"${Space}ignoring$OpeningRoundBracket" +
      s"${lp.ignoring.mkString(Comma)}$ClosingRoundBracket"

    val groupingType = lp.cardinality match {
      case Cardinality.OneToMany  => s"${Space}group_right"
      case Cardinality.ManyToOne  => s"${Space}group_left"
      case _                      => ""
    }
    val grouping = if (lp.include.isEmpty) groupingType else s"$groupingType$OpeningRoundBracket" +
      s"${lp.include.mkString(Comma)}$ClosingRoundBracket"
    s"$lhs$Space${lp.operator.operatorString}$on$ignoring$grouping$Space$rhs"
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

  def convertToQuery(logicalPlan: LogicalPlan): String = {
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
