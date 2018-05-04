package filodb.query

import enumeratum.{Enum, EnumEntry}

//scalastyle:off
sealed abstract class InstantFunctionId(entryName: String) extends EnumEntry

object InstantFunctionId extends Enum[InstantFunctionId] {
  val values = findValues

  case object Abs extends InstantFunctionId("abs")

  case object Absent extends InstantFunctionId("absent")

  case object Ceil extends InstantFunctionId("ceil")

  case object ClampMax extends InstantFunctionId("clamp_max")

  case object ClampMin extends InstantFunctionId("clamp_min")

  case object DaysInMonth extends InstantFunctionId("days_in_month")

  case object DaysOfMonth extends InstantFunctionId("day_of_month")

  case object Exp extends InstantFunctionId("exp")

  case object Floor extends InstantFunctionId("floor")

  case object HistogramQuantile extends InstantFunctionId("histogram_quantile")

  case object Hour extends InstantFunctionId("hour")

  case object LabelReplace extends InstantFunctionId("label_replace")

  case object LabelJoin extends InstantFunctionId("label_join")

  case object Ln extends InstantFunctionId("ln")

  case object Log10 extends InstantFunctionId("log10")

  case object Log2 extends InstantFunctionId("log2")

  case object Minute extends InstantFunctionId("minute")

  case object Month extends InstantFunctionId("month")

  case object Round extends InstantFunctionId("round")

  case object Sort extends InstantFunctionId("sort")

  case object SortDesc extends InstantFunctionId("sort_desc")

  case object Sqrt extends InstantFunctionId("sqrt")

  case object Timestamp extends InstantFunctionId("timestamp")

  case object Year extends InstantFunctionId("year")

  // TODO time, vector, scalar
}

sealed abstract class RangeFunctionId(name: String) extends EnumEntry

object RangeFunctionId extends Enum[RangeFunctionId] {
  val values = findValues

  case object AvgOverTime extends RangeFunctionId("avg_over_time")

  case object Changes extends RangeFunctionId("changes")

  case object CountOverTime extends RangeFunctionId("count_over_time")

  case object Delta extends RangeFunctionId("delta")

  case object Deriv extends RangeFunctionId("deriv")

  case object HoltWinters extends RangeFunctionId("holt_winters")

  case object Idelta extends RangeFunctionId("idelta")

  case object Increase extends RangeFunctionId("increase")

  case object Irate extends RangeFunctionId("irate")

  case object MaxOverTime extends RangeFunctionId("max_over_time")

  case object MinOverTime extends RangeFunctionId("min_over_time")

  case object PredictLinear extends RangeFunctionId("predict_linear")

  case object QuantileOverTime extends RangeFunctionId("quantile_over_time")

  case object Rate extends RangeFunctionId("rate")

  case object Resets extends RangeFunctionId("resets")

  case object StdDevOverTime extends RangeFunctionId("stddev_over_time")

  case object StdVarOverTime extends RangeFunctionId("stdvar_over_time")

  case object SumOverTime extends RangeFunctionId("sum_over_time")

}

sealed trait AggregationOperator extends EnumEntry

object AggregationOperator extends Enum[AggregationOperator] {
  val values = findValues

  case object Avg extends AggregationOperator

  case object Count extends AggregationOperator

  case object Sum extends AggregationOperator

  case object Min extends AggregationOperator

  case object Max extends AggregationOperator

  case object Stddev extends AggregationOperator

  case object Stdvar extends AggregationOperator

  case object TopK extends AggregationOperator

  case object BottomK extends AggregationOperator

  case object CountValues extends AggregationOperator

  case object Quantile extends AggregationOperator

}

sealed trait BinaryOperator extends EnumEntry

object BinaryOperator extends Enum[BinaryOperator] {
  val values = findValues

  case object SUB extends BinaryOperator

  case object ADD extends BinaryOperator

  case object MUL extends BinaryOperator

  case object MOD extends BinaryOperator

  case object DIV extends BinaryOperator

  case object POW extends BinaryOperator

  //  case object LAND extends BinaryOperator
  //
  //  case object LOR extends BinaryOperator
  //
  //  case object LUnless extends BinaryOperator

  //  case object EQL extends BinaryOperator
  //
  //  case object NEQ extends BinaryOperator
  //
  //  case object LTE extends BinaryOperator
  //
  //  case object LSS extends BinaryOperator
  //
  //  case object GTE extends BinaryOperator
  //
  //  case object GTR extends BinaryOperator

  //  case object EQLRegex extends BinaryOperator
  //
  //  case object NEQRegex extends BinaryOperator


}

//scalastyle:on

