package filodb.query.exec.rangefn

import filodb.core.query.ResultSchema

import java.time.{Instant, LocalDateTime, YearMonth, ZoneId, ZoneOffset}
import spire.syntax.cfor._
import filodb.memory.format.vectors.{Histogram, MaxMinHistogram, MutableHistogram}
import filodb.query.InstantFunctionId
import filodb.query.InstantFunctionId._

/**
  * Applies a function transforming a single value into another value, both of type Double.
  */
trait DoubleInstantFunction {
  /**
    * Apply the required instant function against the given value.
    *
    * @param value Sample against which the function will be applied
    * @return Calculated value
    */
  def apply(value: Double, scalarParam: Seq[Double] = Nil): Double
}

trait EmptyParamsInstantFunction extends DoubleInstantFunction
sealed trait HistogramInstantFunction {
  def isHToDoubleFunc: Boolean = this.isInstanceOf[HistToDoubleIFunction]
  def isHMaxMinToDoubleFunc: Boolean = this.isInstanceOf[HMaxMinToDoubleIFunction]
  def asHToDouble: HistToDoubleIFunction = this.asInstanceOf[HistToDoubleIFunction]
  def asHMinMaxToDouble: HMaxMinToDoubleIFunction = this.asInstanceOf[HMaxMinToDoubleIFunction]
  def asHToH: HistToHistIFunction = this.asInstanceOf[HistToHistIFunction]
}

/**
 * An instant function taking a histogram and returning a Double value
 */
trait HistToDoubleIFunction extends HistogramInstantFunction {
  /**
    * Apply the required instant function against the given value.
    *
    * @param value Sample against which the function will be applied
    * @return Calculated value
    */
  def apply(value: Histogram, scalarParam: Seq[Double] = Nil): Double
}

/**
 * An instant function taking a histogram and double and returning a Double value
 */
trait HMaxMinToDoubleIFunction extends HistogramInstantFunction {
  /**
    * Apply the required instant function against the given value.
    *
    * @param value Sample against which the function will be applied
    * @return Calculated value
    */
  def apply(h: Histogram, max: Double, min: Double, scalarParams: Seq[Double] = Nil): Double
}

/**
 * An instant function taking a histogram and returning another histogram
 */
trait HistToHistIFunction extends HistogramInstantFunction {
  /**
    * Apply the required instant function against the given value.
    *
    * @param value Sample against which the function will be applied
    * @return Calculated value
    */
  def apply(value: Histogram): Histogram
}

object InstantFunction {
  /**
    * Returns the DoubleInstantFunction given the function ID and parameters.
    *
    * @param function to be invoked
    * @return the function
    */
  //scalastyle:off cyclomatic.complexity
  def double(function: InstantFunctionId): DoubleInstantFunction = {
    function match {
      case Abs                => AbsImpl()
      case Ceil               => CeilImpl()
      case ClampMax           => ClampMaxImpl()
      case ClampMin           => ClampMinImpl()
      case Exp                => ExpImpl()
      case Floor              => FloorImpl()
      case Ln                 => LnImpl()
      case Log10              => Log10Impl()
      case Log2               => Log2Impl()
      case Round              => RoundImpl()
      case Sgn                => SgnImpl()
      case Sqrt               => SqrtImpl()
      case Month              => MonthImpl()
      case Year               => YearImpl()
      case Hour               => HourImpl()
      case Minute             => MinuteImpl()
      case DayOfWeek          => DayOfWeekImpl()
      case DaysInMonth        => DaysInMonthImpl()
      case DayOfMonth         => DayOfMonthImpl()
      case OrVectorDouble           => OrVectorImpl()
      case _                  => throw new UnsupportedOperationException(s"$function not supported.")
    }
  }
  //scalastyle:on cyclomatic.complexity

  /**
   * Returns the HistogramInstantFunction given the function ID and parameters
   */
  def histogram(function: InstantFunctionId, sourceSchema: ResultSchema): HistogramInstantFunction = function match {
    case HistogramQuantile =>
      if (sourceSchema.isHistMaxMin) HistogramQuantileWithMaxMinImpl() else HistogramQuantileImpl()
    case HistogramMaxQuantile => HistogramMaxQuantileImpl()
    case HistogramBucket => HistogramBucketImpl()
    case _ => throw new UnsupportedOperationException(s"$function not supported.")
}

/**
  * abs(v instant-vector) returns the input vector with all
  * sample values converted to their absolute value.
  */
case class AbsImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double] = Nil): Double = scala.math.abs(value)
}

/**
  * ceil(v instant-vector) rounds the sample values of
  * all elements in v up to the nearest integer.
  */
case class CeilImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double] = Nil): Double = scala.math.ceil(value)
}

/**
  * clamp_max(v instant-vector, max scalar) clamps the sample values
  * of all elements in v to have an upper limit of max.
  */
final case class ClampMaxImpl() extends DoubleInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double]): Double = {
    require(scalarParams.size == 1,
      "Cannot use ClampMax without providing a upper limit of max.")
    scala.math.min(value, scalarParams.head)
  }
}

final case class OrVectorImpl() extends DoubleInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double]): Double = {
    require(scalarParams.size == 1, "OrVector needs a single double param")
    if (value.isNaN) scalarParams.head else value
  }
}

/**
  * clamp_min(v instant-vector, min scalar) clamps the sample values
  * of all elements in v to have a lower limit of min.
  */
final case class ClampMinImpl() extends DoubleInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double]): Double = {
    require(scalarParams.size == 1,
      "Cannot use ClampMin without providing a lower limit of min.")
    scala.math.max(value, scalarParams.head)
  }
}

/**
  * exp(v instant-vector) calculates the exponential
  * function for all elements in v
  */
final case class ExpImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double] = Nil): Double = scala.math.exp(value)
}

/**
  * floor(v instant-vector) rounds the sample values of all
  * elements in v down to the nearest integer.
  */
final case class FloorImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double] = Nil): Double = {
    require(scalarParams.isEmpty, "No additional parameters required for the instant function.")
    scala.math.floor(value)
  }
}

/**
  * ln(v instant-vector) calculates the natural
  * logarithm for all elements in v
  */
final case class LnImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double] = Nil): Double = {
    require(scalarParams.isEmpty, "No additional parameters required for the instant function.")
    scala.math.log(value)
  }
}

/**
  * log10(v instant-vector) calculates the decimal
  * logarithm for all elements in v.
  */
final case class Log10Impl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double] = Nil): Double = {
    require(scalarParams.isEmpty, "No additional parameters required for Log10.")
    scala.math.log10(value)
  }
}

/**
  * log2(v instant-vector) calculates the binary
  * logarithm for all elements in v.
  */
final case class Log2Impl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double] = Nil): Double = {
    require(scalarParams.isEmpty, "No additional parameters required for the instant function.")
    scala.math.log10(value) / scala.math.log10(2.0)
  }
}

/**
  * round(v instant-vector, to_nearest=1 scalar) rounds the sample
  * values of all elements in v to the nearest integer.
  * Ties are resolved by rounding up. The optional to_nearest argument
  * allows specifying the nearest multiple to which the sample values
  * should be rounded. This multiple may also be a fraction.
  */
final case class RoundImpl() extends DoubleInstantFunction {

  override def apply(value: Double, scalarParams: Seq[Double]): Double = {
    require(scalarParams.size <= 1, "Only one optional parameters allowed for Round.")

      def toNearestInverse =
      if (scalarParams.size == 1) {
        // Invert as it seems to cause fewer floating point accuracy issues.
        1.0 / scalarParams.head
      } else {
        1.0
      }

    if (value.isNaN || value.isInfinite)
      value
    else
      scala.math.floor(value * toNearestInverse + 0.5) / toNearestInverse
  }
}

/**
 * sgn(v instant-vector) for all elements v[i] in v, returns:
 *   -1 iff v[i] <  0;
 *    0 iff v[i] == 0;
 *    1 iff v[i] >  0
 */
final case class SgnImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double] = Nil): Double = {
    require(scalarParams.isEmpty, "No additional parameters required for the instant function.")
    scala.math.signum(value)
  }
}

/**
  * sqrt(v instant-vector) calculates the square root of all elements in v.
  */
final case class SqrtImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double] = Nil): Double = {
    require(scalarParams.isEmpty, "No additional parameters required for the instant function.")
    scala.math.sqrt(value)
  }
}

final case class MonthImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double] = Nil): Double = {
    require(scalarParams.isEmpty, "No additional parameters required for the instant function.")
    if (value.isNaN || value.isInfinite) {
      value
    } else {
      val instant = Instant.ofEpochSecond(value.toLong)
      val ldt = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"))
      ldt.getMonthValue
    }
  }
}

final case class YearImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double] = Nil): Double = {
    require(scalarParams.isEmpty, "No additional parameters required for the instant function.")
    if (value.isNaN || value.isInfinite) {
      value
    } else {
      LocalDateTime.ofEpochSecond(value.toLong, 0, ZoneOffset.UTC).getYear()
    }
  }
}

final case class HourImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double] = Nil): Double = {
    require(scalarParams.isEmpty, "No additional parameters required for the instant function.")
    if (value.isNaN || value.isInfinite) {
      value
    } else {
      LocalDateTime.ofEpochSecond(value.toLong, 0, ZoneOffset.UTC).getHour()
    }
  }
}

final case class MinuteImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double] = Nil): Double = {
    require(scalarParams.isEmpty, "No additional parameters required for the instant function.")
    if (value.isNaN || value.isInfinite) {
      value
    } else {
      LocalDateTime.ofEpochSecond(value.toLong, 0, ZoneOffset.UTC).getMinute()
    }
  }
}

final case class DayOfWeekImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double] = Nil): Double = {
    require(scalarParams.isEmpty, "No additional parameters required for the instant function.")
    if (value.isNaN || value.isInfinite) {
      value
    } else {
      val dayOfWeek = LocalDateTime.ofEpochSecond(value.toLong, 0, ZoneOffset.UTC).getDayOfWeek.getValue
      // Prometheus range is 0 to 6 where 0 is Sunday
      if (dayOfWeek == 7) {
        0
      }
      else {
        dayOfWeek
      }
    }
  }
}

final case class DayOfMonthImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double] = Nil): Double = {
    require(scalarParams.isEmpty, "No additional parameters required for the instant function.")
    if (value.isNaN || value.isInfinite) {
      value
    } else {
      LocalDateTime.ofEpochSecond(value.toLong, 0, ZoneOffset.UTC).getDayOfMonth
    }
  }
}

final case class DaysInMonthImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParams: Seq[Double] = Nil): Double = {
    require(scalarParams.isEmpty, "No additional parameters required for the instant function.")
    if (value.isNaN || value.isInfinite) {
      value
    } else {
      val ldt = LocalDateTime.ofEpochSecond(value.toLong, 0, ZoneOffset.UTC)
      YearMonth.from(ldt).lengthOfMonth()
    }
  }
}

/**
 * Histogram quantile function for Histogram columns, where all buckets are together.
 */
final case class HistogramQuantileImpl() extends HistToDoubleIFunction {
  final def apply(value: Histogram, scalarParams: Seq[Double]): Double = {
    require(scalarParams.length == 1, "Quantile (between 0 and 1) required for histogram quantile")
    val q = scalarParams(0)
    value.quantile(q)
  }
}

/**
 * Histogram quantile function for Histogram columns, where all buckets are together. This will take in consideration
 * of min and max columns
 */
final case class HistogramQuantileWithMaxMinImpl() extends HMaxMinToDoubleIFunction {
  final def apply(value: Histogram, max: Double, min: Double, scalarParams: Seq[Double]): Double = {
    require(scalarParams.length == 1, "Quantile (between 0 and 1) required for histogram quantile")
    val maxMinHist = value match {
      case h: MutableHistogram => MaxMinHistogram(h, max, min)
      case other: Histogram => MaxMinHistogram(MutableHistogram(other), max, min)
    }
    maxMinHist.quantile(scalarParams(0))
  }
}

/**
 * Histogram max quantile function for Histogram column + extra max (Double) column.
 */
final case class HistogramMaxQuantileImpl() extends HMaxMinToDoubleIFunction {
  /**
    * @param scalarParams - a single value between 0 and 1, the quantile to calculate.
    */
  final def apply(hist: Histogram, max: Double, scalarParams: Seq[Double]): Double = {
    require(scalarParams.length == 1, "Quantile (between 0 and 1) required for histogram quantile")
    val maxHist = hist match {
      case h: MutableHistogram => MaxMinHistogram(h, max)
      case other: Histogram    => MaxMinHistogram(MutableHistogram(other), max)
    }
    maxHist.quantile(scalarParams(0))
  }
}

/**
 * Function to extract one bucket from any histogram (could be computed, not just raw).
 */
final case class HistogramBucketImpl() extends HistToDoubleIFunction {

  /**
    * @param scalarParams - a single value which is the Double bucket or "le" to extract.  If it does not correspond
    *                     to any existing bucket then NaN is returned.
    */
  final def apply(value: Histogram, scalarParams: Seq[Double]): Double = {
    require(scalarParams.length == 1, "Bucket/le required for histogram bucket")
    val bucket = scalarParams(0)
    if (bucket == Double.PositiveInfinity) {
      // Just get the top bucket if bucket scheme has +Inf at top, or return NaN
      if (value.bucketTop(value.numBuckets - 1) == Double.PositiveInfinity) value.topBucketValue
      else throw new IllegalArgumentException(s"+Inf bucket not in the last position!")
    } else {
      cforRange { 0 until value.numBuckets } { b =>
        // This comparison does not work for +Inf
        if (Math.abs(value.bucketTop(b) - bucket) <= 1E-10) return value.bucketValue(b)
      }
      Double.NaN

    }
  }
}
