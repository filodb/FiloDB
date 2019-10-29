package filodb.query.exec.rangefn

//import filodb.core.query.DoubleScalar
import java.time.{Instant, LocalDateTime, YearMonth, ZoneId, ZoneOffset}

//import filodb.core.query.ScalarVector
import scalaxy.loops._
import filodb.memory.format.vectors.{Histogram, MaxHistogram, MutableHistogram}
import filodb.query.InstantFunctionId
import filodb.query.InstantFunctionId.{Log2, Sqrt, _}

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

trait EmptyParamsInstantFunction extends DoubleInstantFunction {
  /**
    * Validate the function before invoking the function.
    */
  //require(scalarParam.isEmpty, "No additional parameters required for the instant function.")
}

sealed trait HistogramInstantFunction {
  def isHToDoubleFunc: Boolean = this.isInstanceOf[HistToDoubleIFunction]
  def isHistDoubleToDoubleFunc: Boolean = this.isInstanceOf[HDToDoubleIFunction]
  def asHToDouble: HistToDoubleIFunction = this.asInstanceOf[HistToDoubleIFunction]
  def asHDToDouble: HDToDoubleIFunction = this.asInstanceOf[HDToDoubleIFunction]
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
trait HDToDoubleIFunction extends HistogramInstantFunction {
  /**
    * Apply the required instant function against the given value.
    *
    * @param value Sample against which the function will be applied
    * @return Calculated value
    */
  def apply(h: Histogram, d: Double, scalarParam: Seq[Double] = Nil): Double
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
    * @param funcParams - Additional required function parameters
    * @return the function
    */
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
      case Sqrt               => SqrtImpl()
      case Month              => MonthImpl()
      case Year               => YearImpl()
      case Hour               => HourImpl()
      case Minute             => MinuteImpl()
      case DayOfWeek          => DayOfWeekImpl()
      case DaysInMonth        => DaysInMonthImpl()
      case DayOfMonth         => DayOfMonthImpl()
      case _                  => throw new UnsupportedOperationException(s"$function not supported.")
    }
  }

  /**
   * Returns the HistogramInstantFunction given the function ID and parameters
   */
  def histogram(function: InstantFunctionId): HistogramInstantFunction = function match {
    case HistogramQuantile    => HistogramQuantileImpl()
    case HistogramMaxQuantile => HistogramMaxQuantileImpl()
    case HistogramBucket      => HistogramBucketImpl()
    case _                    => throw new UnsupportedOperationException(s"$function not supported.")
  }
}

/**
  * abs(v instant-vector) returns the input vector with all
  * sample values converted to their absolute value.
  */
case class AbsImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParam: Seq[Double] = Nil): Double = scala.math.abs(value)
}

/**
  * ceil(v instant-vector) rounds the sample values of
  * all elements in v up to the nearest integer.
  */
case class CeilImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParam: Seq[Double] = Nil): Double = scala.math.ceil(value)
}

/**
  * clamp_max(v instant-vector, max scalar) clamps the sample values
  * of all elements in v to have an upper limit of max.
  */
case class ClampMaxImpl() extends DoubleInstantFunction {
  override def apply(value: Double, scalarParam: Seq[Double]): Double = {
    require(scalarParam.size == 1,
      "Cannot use ClampMax without providing a upper limit of max.")
    scala.math.min(value, scalarParam.head)
  }
}

/**
  * clamp_min(v instant-vector, min scalar) clamps the sample values
  * of all elements in v to have a lower limit of min.
  */
case class ClampMinImpl() extends DoubleInstantFunction {
  override def apply(value: Double, scalarParam: Seq[Double]): Double = {
    require(scalarParam.size == 1,
      "Cannot use ClampMin without providing a lower limit of min.")
    scala.math.max(value, scalarParam.head)
  }
}

/**
  * exp(v instant-vector) calculates the exponential
  * function for all elements in v
  */
case class ExpImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParam: Seq[Double] = Nil): Double = scala.math.exp(value)
}

/**
  * floor(v instant-vector) rounds the sample values of all
  * elements in v down to the nearest integer.
  */
case class FloorImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParam: Seq[Double] = Nil): Double = scala.math.floor(value)
}

/**
  * ln(v instant-vector) calculates the natural
  * logarithm for all elements in v
  */
case class LnImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParam: Seq[Double] = Nil): Double = scala.math.log(value)
}

/**
  * log10(v instant-vector) calculates the decimal
  * logarithm for all elements in v.
  */
case class Log10Impl() extends EmptyParamsInstantFunction {

  override def apply(value: Double, scalarParam: Seq[Double] = Nil): Double = {
    /**
      * Validate the function before invoking the function.
      */
    require(scalarParam.isEmpty, "No additional parameters required for Log10.")
    scala.math.log10(value)
  }
}

/**
  * log2(v instant-vector) calculates the binary
  * logarithm for all elements in v.
  */
case class Log2Impl() extends EmptyParamsInstantFunction {
  override def apply(value: Double,  scalarParam: Seq[Double] = Nil): Double =
    scala.math.log10(value)/scala.math.log10(2.0)
}

/**
  * round(v instant-vector, to_nearest=1 scalar) rounds the sample
  * values of all elements in v to the nearest integer.
  * Ties are resolved by rounding up. The optional to_nearest argument
  * allows specifying the nearest multiple to which the sample values
  * should be rounded. This multiple may also be a fraction.
  */
case class RoundImpl() extends DoubleInstantFunction {

  override def apply(value: Double, scalarParam: Seq[Double]): Double = {
    /**
      * Validate the function before invoking the function.
      */
    require(scalarParam.size <= 1, "Only one optional parameters allowed for Round.")

      def toNearestInverse =
      if (scalarParam.size == 1) {
        // Invert as it seems to cause fewer floating point accuracy issues.
        1.0 / scalarParam.head
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
  * sqrt(v instant-vector) calculates the square root of all elements in v.
  */
case class SqrtImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParam: Seq[Double] = Nil): Double = scala.math.sqrt(value)
}

case class MonthImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParam: Seq[Double] = Nil): Double = {
    if (value.isNaN || value.isInfinite) {
      value
    } else {
      val instant = Instant.ofEpochSecond(value.toLong)
      val ldt = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"))
      ldt.getMonthValue
    }
  }
}

case class YearImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParam: Seq[Double] = Nil): Double = {
    if (value.isNaN || value.isInfinite) {
      value
    } else {
      LocalDateTime.ofEpochSecond(value.toLong, 0, ZoneOffset.UTC).getYear()
    }
  }
}

case class HourImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParam: Seq[Double] = Nil): Double = {
    if (value.isNaN || value.isInfinite) {
      value
    } else {
      LocalDateTime.ofEpochSecond(value.toLong, 0, ZoneOffset.UTC).getHour()
    }
  }
}

case class MinuteImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParam: Seq[Double] = Nil): Double = {
    if (value.isNaN || value.isInfinite) {
      value
    } else {
      LocalDateTime.ofEpochSecond(value.toLong, 0, ZoneOffset.UTC).getMinute()
    }
  }
}

case class DayOfWeekImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParam: Seq[Double] = Nil): Double = {
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

case class DayOfMonthImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParam: Seq[Double] = Nil): Double = {
    if (value.isNaN || value.isInfinite) {
      value
    } else {
      LocalDateTime.ofEpochSecond(value.toLong, 0, ZoneOffset.UTC).getDayOfMonth
    }
  }
}

case class DaysInMonthImpl() extends EmptyParamsInstantFunction {
  override def apply(value: Double, scalarParam: Seq[Double] = Nil): Double = {
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
 * @param funcParams - a single value between 0 and 1, the quantile to calculate.
 */
case class HistogramQuantileImpl() extends HistToDoubleIFunction {
  final def apply(value: Histogram, scalarParam: Seq[Double]): Double = {
    require(scalarParam.length == 1, "Quantile (between 0 and 1) required for histogram quantile")
    val q = scalarParam(0)
    value.quantile(q)
  }
}

/**
 * Histogram max quantile function for Histogram column + extra max (Double) column.
 * //@param scalarParam - a single value between 0 and 1, the quantile to calculate.
 */
case class HistogramMaxQuantileImpl() extends HDToDoubleIFunction {
  final def apply(hist: Histogram, max: Double, scalarParam: Seq[Double]): Double = {
    require(scalarParam.length == 1, "Quantile (between 0 and 1) required for histogram quantile")
    val maxHist = hist match {
      case h: MutableHistogram => MaxHistogram(h, max)
      case other: Histogram    => MaxHistogram(MutableHistogram(other), max)
    }
    maxHist.quantile(scalarParam(0))
  }
}

/**
 * Function to extract one bucket from any histogram (could be computed, not just raw).
 * @param funcParams - a single value which is the Double bucket or "le" to extract.  If it does not correspond
 *                     to any existing bucket then NaN is returned.
 */
case class HistogramBucketImpl() extends HistToDoubleIFunction {
  final def apply(value: Histogram, scalarParam: Seq[Double]): Double = {
    require(scalarParam.length == 1, "Bucket/le required for histogram bucket")
    val bucket = scalarParam(0)
    for { b <- 0 until value.numBuckets optimized } {
      if (Math.abs(value.bucketTop(b) - bucket) <= 1E-10) return value.bucketValue(b)
    }
    Double.NaN
  }
}