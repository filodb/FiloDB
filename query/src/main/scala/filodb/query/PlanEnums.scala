package filodb.query

import enumeratum.{Enum, EnumEntry}

//scalastyle:off
sealed abstract class InstantFunctionId(override val entryName: String) extends EnumEntry

object InstantFunctionId extends Enum[InstantFunctionId] {
  val values = findValues

  case object Abs extends InstantFunctionId("abs")

  case object Absent extends InstantFunctionId("absent")

  case object Ceil extends InstantFunctionId("ceil")

  case object ClampMax extends InstantFunctionId("clamp_max")

  case object ClampMin extends InstantFunctionId("clamp_min")

  case object Exp extends InstantFunctionId("exp")

  case object Floor extends InstantFunctionId("floor")

  case object HistogramQuantile extends InstantFunctionId("histogram_quantile")

  case object HistogramMaxQuantile extends InstantFunctionId("histogram_max_quantile")

  case object HistogramBucket extends InstantFunctionId("histogram_bucket")

  case object Ln extends InstantFunctionId("ln")

  case object Log10 extends InstantFunctionId("log10")

  case object Log2 extends InstantFunctionId("log2")

  case object Round extends InstantFunctionId("round")

  case object Sqrt extends InstantFunctionId("sqrt")

  case object DaysInMonth extends InstantFunctionId("days_in_month")

  case object DayOfMonth extends InstantFunctionId("day_of_month")

  case object DayOfWeek extends InstantFunctionId("day_of_week")

  case object Hour extends InstantFunctionId("hour")

  case object Minute extends InstantFunctionId("minute")

  case object Month extends InstantFunctionId("month")

  case object Year extends InstantFunctionId("year")

  // TODO time, vector, scalar
}

sealed abstract class RangeFunctionId(override val entryName: String) extends EnumEntry

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

sealed abstract class FiloFunctionId(override val entryName: String) extends EnumEntry

object FiloFunctionId extends Enum[FiloFunctionId] {
  val values = findValues

  case object ChunkMetaAll extends FiloFunctionId("_filodb_chunkmeta_all")
}

sealed abstract class AggregationOperator(override val entryName: String) extends EnumEntry

object AggregationOperator extends Enum[AggregationOperator] {
  val values = findValues

  case object Avg extends AggregationOperator("avg")

  case object Count extends AggregationOperator("count")

  case object Sum extends AggregationOperator("sum")

  case object Min extends AggregationOperator("min")

  case object Max extends AggregationOperator("max")

  case object Stddev extends AggregationOperator("stddev")

  case object Stdvar extends AggregationOperator("stdvar")

  case object TopK extends AggregationOperator("topk")

  case object BottomK extends AggregationOperator("bottomk")

  case object CountValues extends AggregationOperator("count_values")

  case object Quantile extends AggregationOperator("quantile")

}

sealed abstract class BinaryOperator extends EnumEntry {
  def precedence: Int
  def isRightAssociative : Boolean
}

sealed class MathOperator (val precedence: Int = 0, val isRightAssociative: Boolean = false) extends BinaryOperator

sealed class SetOperator(val precedence: Int = 0, val isRightAssociative: Boolean = false) extends BinaryOperator

sealed class ComparisonOperator(val precedence: Int = 0, val isRightAssociative: Boolean = false) extends BinaryOperator

object BinaryOperator extends Enum[BinaryOperator] {
  val values = findValues
  case object SUB extends MathOperator(4)

  case object ADD extends MathOperator(4)

  case object MUL extends MathOperator(5)

  case object MOD extends MathOperator(5)

  case object DIV extends MathOperator(5)

  case object POW extends MathOperator(6, true)

  case object LAND extends SetOperator(2)

  case object LOR extends SetOperator(1)

  case object LUnless extends SetOperator(2)

  case object EQL extends ComparisonOperator(3)

  case object NEQ extends ComparisonOperator(3)

  case object LTE extends ComparisonOperator(3)

  case object LSS extends ComparisonOperator(3)

  case object GTE extends ComparisonOperator(3)

  case object GTR extends ComparisonOperator(3)

  case object EQL_BOOL extends ComparisonOperator(3)

  case object NEQ_BOOL extends ComparisonOperator(3)

  case object LTE_BOOL extends ComparisonOperator(3)

  case object LSS_BOOL extends ComparisonOperator(3)

  case object GTE_BOOL extends ComparisonOperator(3)

  case object GTR_BOOL extends ComparisonOperator(3)

  case object EQLRegex extends BinaryOperator { // FIXME when implemented
    override def precedence: Int = 0
    override def isRightAssociative: Boolean = false
  }

  case object NEQRegex extends BinaryOperator { // FIXME when implemented
    override def precedence: Int = 0
    override def isRightAssociative: Boolean = false
  }
}

sealed trait Cardinality extends EnumEntry

object Cardinality extends Enum[Cardinality] {
  val values = findValues

  case object OneToOne extends Cardinality

  case object OneToMany extends Cardinality

  case object ManyToOne extends Cardinality

  case object ManyToMany extends Cardinality

}

sealed abstract class MiscellaneousFunctionId(override val entryName: String) extends EnumEntry

object MiscellaneousFunctionId extends Enum[MiscellaneousFunctionId] {
  val values = findValues

  case object LabelReplace extends MiscellaneousFunctionId("label_replace")

  case object LabelJoin extends MiscellaneousFunctionId("label_join")

  case object Timestamp extends MiscellaneousFunctionId("timestamp")
}

sealed abstract class SortFunctionId(override val entryName: String) extends EnumEntry

object SortFunctionId extends Enum[SortFunctionId] {
  val values = findValues
  case object Sort extends SortFunctionId("sort")

  case object SortDesc extends SortFunctionId("sort_desc")
}

sealed abstract class ScalarFunctionId(override val entryName: String) extends EnumEntry

object ScalarFunctionId extends Enum[ScalarFunctionId] {
  val values = findValues

  case object Scalar extends ScalarFunctionId("scalar")

  case object Time extends ScalarFunctionId("time")

  case object DaysInMonth extends ScalarFunctionId("days_in_month")

  case object DayOfMonth extends ScalarFunctionId("day_of_month")

  case object DayOfWeek extends ScalarFunctionId("day_of_week")

  case object Hour extends ScalarFunctionId("hour")

  case object Minute extends ScalarFunctionId("minute")

  case object Month extends ScalarFunctionId("month")

  case object Year extends ScalarFunctionId("year")

}

sealed abstract class VectorFunctionId(override val entryName: String) extends EnumEntry

object VectorFunctionId extends Enum[VectorFunctionId] {
  val values = findValues
  case object Vector extends VectorFunctionId("vector")
}


//scalastyle:on


