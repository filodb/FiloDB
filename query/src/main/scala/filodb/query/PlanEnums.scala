package filodb.query

import enumeratum.{Enum, EnumEntry}

//scalastyle:off
sealed abstract class InstantFunctionId(override val entryName: String) extends EnumEntry

object InstantFunctionId extends Enum[InstantFunctionId] {
  val values = findValues

  case object Abs extends InstantFunctionId("abs")
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
}


/**
 * Below trait is for defining the type of arguments expected by all promQL functions defined below.
 * All the classes extending the ParamSpec trait will check if the input arg is of certain type or not. If not,
 * the throw default error message if not overriden in the object.
 */

sealed trait ParamSpec
final case class RangeVectorParam(errorMsg: String = "Expected type range vector in call to function") extends ParamSpec
final case class InstantVectorParam(errorMsg: String = "Expected type instant vector in call to function") extends ParamSpec
final case class ScalarParam(errorMsg: String = "Expected type scalar in call to function") extends ParamSpec
final case class ScalarRangeParam(min: Double, max: Double, errorMsg: String) extends ParamSpec
sealed abstract class RangeFunctionId(override val entryName: String, val paramSpec: Seq[ParamSpec]) extends EnumEntry

object RangeFunctionId extends Enum[RangeFunctionId] {
  val values = findValues

  case object Last extends RangeFunctionId("last",Seq(RangeVectorParam()))
  case object AvgOverTime extends RangeFunctionId("avg_over_time", Seq(RangeVectorParam()))
  case object Changes extends RangeFunctionId("changes", Seq(RangeVectorParam()))
  case object CountOverTime extends RangeFunctionId("count_over_time", Seq(RangeVectorParam()))
  case object Delta extends RangeFunctionId("delta", Seq(RangeVectorParam()))
  case object Deriv extends RangeFunctionId("deriv", Seq(RangeVectorParam()))
  case object HoltWinters extends RangeFunctionId("holt_winters", Seq(RangeVectorParam(),
    ScalarRangeParam(0, 1, "Invalid Smoothing factor. Expected: 0 < sf < 1, got:"), ScalarRangeParam(0, 1, "Invalid Trend factor. Expected: 0 < tf < 1, got:")))
  case object ZScore extends RangeFunctionId("z_score", Seq(RangeVectorParam()))
  case object Idelta extends RangeFunctionId("idelta", Seq(RangeVectorParam()))
  case object Increase extends RangeFunctionId("increase", Seq(RangeVectorParam()))
  case object Irate extends RangeFunctionId("irate", Seq(RangeVectorParam()))
  case object MaxOverTime extends RangeFunctionId("max_over_time", Seq(RangeVectorParam()))
  case object MinOverTime extends RangeFunctionId("min_over_time", Seq(RangeVectorParam()))
  case object PredictLinear extends RangeFunctionId("predict_linear", Seq(RangeVectorParam(), ScalarParam()))
  case object QuantileOverTime extends RangeFunctionId("quantile_over_time", Seq(ScalarParam(), RangeVectorParam()))
  case object Rate extends RangeFunctionId("rate", Seq(RangeVectorParam()))
  case object Resets extends RangeFunctionId("resets", Seq(RangeVectorParam()))
  case object StdDevOverTime extends RangeFunctionId("stddev_over_time", Seq(RangeVectorParam()))
  case object StdVarOverTime extends RangeFunctionId("stdvar_over_time", Seq(RangeVectorParam()))
  case object SumOverTime extends RangeFunctionId("sum_over_time", Seq(RangeVectorParam()))
  case object Timestamp extends RangeFunctionId("timestamp", Seq(InstantVectorParam()))
  case object AbsentOverTime extends RangeFunctionId("absent_over_time", Seq(RangeVectorParam()))
}

sealed abstract class FiloFunctionId(override val entryName: String) extends EnumEntry

object FiloFunctionId extends Enum[FiloFunctionId] {
  val values = findValues

  case object ChunkMetaAll extends FiloFunctionId("_filodb_chunkmeta_all")
}

sealed abstract class AggregationOperator(override val entryName: String) extends EnumEntry

object QueryFunctionConstants {
  val stdVal = "stdval"
}

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
  def operatorString: String
}

sealed class MathOperator (val operatorString: String, val precedence: Int = 0, val isRightAssociative: Boolean = false) extends BinaryOperator
sealed class SetOperator(val operatorString: String, val precedence: Int = 0, val isRightAssociative: Boolean = false) extends BinaryOperator
sealed class ComparisonOperator(val operatorString: String, val precedence: Int = 0, val isRightAssociative: Boolean = false) extends BinaryOperator

object BinaryOperator extends Enum[BinaryOperator] {
  val values = findValues

  case object SUB extends MathOperator("-", 4)
  case object ADD extends MathOperator("+", 4)
  case object MUL extends MathOperator("*", 5)
  case object MOD extends MathOperator("+", 5)
  case object DIV extends MathOperator("/", 5)
  case object POW extends MathOperator("^", 6, true)
  case object LAND extends SetOperator("and", 2)
  case object LOR extends SetOperator("or", 1)
  case object LUnless extends SetOperator("unless", 2)
  case object EQL extends ComparisonOperator("==", 3)
  case object NEQ extends ComparisonOperator("!=", 3)
  case object LTE extends ComparisonOperator("<=", 3)
  case object LSS extends ComparisonOperator("<", 3)
  case object GTE extends ComparisonOperator(">=", 3)
  case object GTR extends ComparisonOperator(">", 3)
  case object EQL_BOOL extends ComparisonOperator("== bool", 3)
  case object NEQ_BOOL extends ComparisonOperator("!= bool",3)
  case object LTE_BOOL extends ComparisonOperator("<= bool", 3)
  case object LSS_BOOL extends ComparisonOperator("< bool", 3)
  case object GTE_BOOL extends ComparisonOperator(">= bool", 3)
  case object GTR_BOOL extends ComparisonOperator("> bool", 3)

  case object EQLRegex extends BinaryOperator { // FIXME when implemented
    override def precedence: Int = 0
    override def isRightAssociative: Boolean = false
    override def operatorString: String = ???
  }

  case object NEQRegex extends BinaryOperator { // FIXME when implemented
    override def precedence: Int = 0
    override def isRightAssociative: Boolean = false
    override def operatorString: String = ???
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
  case object HistToPromVectors extends MiscellaneousFunctionId("hist_to_prom_vectors")
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

sealed abstract class AbsentFunctionId(override val entryName: String) extends EnumEntry

object AbsentFunctionId extends Enum[AbsentFunctionId] {
  val values = findValues

  case object Absent extends AbsentFunctionId("absent")
}
