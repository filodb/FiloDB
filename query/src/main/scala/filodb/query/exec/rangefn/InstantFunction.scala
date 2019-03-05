package filodb.query.exec.rangefn

import filodb.memory.format.vectors.Histogram
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
  def apply(value: Double): Double
}

trait EmptyParamsInstantFunction extends DoubleInstantFunction {
  def funcParams: Seq[Any]

  /**
    * Validate the function before invoking the function.
    */
  require(funcParams.isEmpty, "No additional parameters required for the instant function.")
}

sealed trait HistogramInstantFunction {
  def isHToDoubleFunc: Boolean = this.isInstanceOf[HistToDoubleIFunction]
  def asHToDouble: HistToDoubleIFunction = this.asInstanceOf[HistToDoubleIFunction]
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
  def apply(value: Histogram): Double
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
  def double(function: InstantFunctionId, funcParams: Seq[Any]): DoubleInstantFunction = {
    function match {
      case Abs                => AbsImpl(funcParams)
      case Ceil               => CeilImpl(funcParams)
      case ClampMax           => ClampMaxImpl(funcParams)
      case ClampMin           => ClampMinImpl(funcParams)
      case Exp                => ExpImpl(funcParams)
      case Floor              => FloorImpl(funcParams)
      case Ln                 => LnImpl(funcParams)
      case Log10              => Log10Impl(funcParams)
      case Log2               => Log2Impl(funcParams)
      case Round              => RoundImpl(funcParams)
      case Sqrt               => SqrtImpl(funcParams)
      case _                  => throw new UnsupportedOperationException(s"$function not supported.")
    }
  }

  /**
   * Returns the HistogramInstantFunction given the function ID and parameters
   */
  def histogram(function: InstantFunctionId, funcParams: Seq[Any]): HistogramInstantFunction = function match {
    case HistogramQuantile    => HistogramQuantileImpl(funcParams)
    case _                    => throw new UnsupportedOperationException(s"$function not supported.")
  }
}

/**
  * abs(v instant-vector) returns the input vector with all
  * sample values converted to their absolute value.
  * @param funcParams - Additional function parameters
  */
case class AbsImpl(funcParams: Seq[Any]) extends EmptyParamsInstantFunction {
  override def apply(value: Double): Double = scala.math.abs(value)
}

/**
  * ceil(v instant-vector) rounds the sample values of
  * all elements in v up to the nearest integer.
  * @param funcParams - Additional function parameters
  */
case class CeilImpl(funcParams: Seq[Any]) extends EmptyParamsInstantFunction {
  override def apply(value: Double): Double = scala.math.ceil(value)
}

/**
  * clamp_max(v instant-vector, max scalar) clamps the sample values
  * of all elements in v to have an upper limit of max.
  * @param funcParams - Additional function parameters
  */
case class ClampMaxImpl(funcParams: Seq[Any]) extends DoubleInstantFunction {
  /**
    * Validate the function before invoking the function.
    */
  require(funcParams.size == 1,
    "Cannot use ClampMax without providing a upper limit of max.")
  require(funcParams.head.isInstanceOf[Number],
    "Cannot use ClampMax without providing a upper limit of max as a Number.")

  override def apply(value: Double): Double =
    scala.math.min(value, funcParams.head.asInstanceOf[Number].doubleValue())
}

/**
  * clamp_min(v instant-vector, min scalar) clamps the sample values
  * of all elements in v to have a lower limit of min.
  * @param funcParams - Additional function parameters
  */
case class ClampMinImpl(funcParams: Seq[Any]) extends DoubleInstantFunction {
  /**
    * Validate the function before invoking the function.
    */
  require(funcParams.size == 1,
    "Cannot use ClampMin without providing a lower limit of min.")
  require(funcParams.head.isInstanceOf[Number],
    "Cannot use ClampMin without providing a lower limit of min as a Number.")

  override def apply(value: Double): Double =
    scala.math.max(value, funcParams.head.asInstanceOf[Number].doubleValue())
}

/**
  * exp(v instant-vector) calculates the exponential
  * function for all elements in v
  * @param funcParams - Additional function parameters
  */
case class ExpImpl(funcParams: Seq[Any]) extends EmptyParamsInstantFunction {
  override def apply(value: Double): Double = scala.math.exp(value)
}

/**
  * floor(v instant-vector) rounds the sample values of all
  * elements in v down to the nearest integer.
  * @param funcParams - Additional function parameters
  */
case class FloorImpl(funcParams: Seq[Any]) extends EmptyParamsInstantFunction {
  override def apply(value: Double): Double = scala.math.floor(value)
}

/**
  * ln(v instant-vector) calculates the natural
  * logarithm for all elements in v
  * @param funcParams - Additional function parameters
  */
case class LnImpl(funcParams: Seq[Any]) extends EmptyParamsInstantFunction {
  override def apply(value: Double): Double = scala.math.log(value)
}

/**
  * log10(v instant-vector) calculates the decimal
  * logarithm for all elements in v.
  * @param funcParams - Additional function parameters
  */
case class Log10Impl(funcParams: Seq[Any]) extends EmptyParamsInstantFunction {
  /**
    * Validate the function before invoking the function.
    */
  require(funcParams.isEmpty, "No additional parameters required for Log10.")

  override def apply(value: Double): Double = scala.math.log10(value)
}

/**
  * log2(v instant-vector) calculates the binary
  * logarithm for all elements in v.
  * @param funcParams - Additional function parameters
  */
case class Log2Impl(funcParams: Seq[Any]) extends EmptyParamsInstantFunction {
  override def apply(value: Double): Double =
    scala.math.log10(value)/scala.math.log10(2.0)
}

/**
  * round(v instant-vector, to_nearest=1 scalar) rounds the sample
  * values of all elements in v to the nearest integer.
  * Ties are resolved by rounding up. The optional to_nearest argument
  * allows specifying the nearest multiple to which the sample values
  * should be rounded. This multiple may also be a fraction.
  * @param funcParams - Additional function parameters
  */
case class RoundImpl(funcParams: Seq[Any]) extends DoubleInstantFunction {
  /**
    * Validate the function before invoking the function.
    */
  require(funcParams.size <= 1, "Only one optional parameters allowed for Round.")

  private val toNearestInverse = {
    if (funcParams.size == 1) {
      require(funcParams.head.isInstanceOf[Number],
        "to_nearest optional parameter should be a Number.")
      // Invert as it seems to cause fewer floating point accuracy issues.
      1.0 / funcParams.head.asInstanceOf[Number].doubleValue()
    } else {
      1.0
    }
  }

  override def apply(value: Double): Double = {
    if (value.isNaN || value.isInfinite)
      value
    else
      scala.math.floor(value * toNearestInverse + 0.5) / toNearestInverse
  }
}

/**
  * sqrt(v instant-vector) calculates the square root of all elements in v.
  * @param funcParams - Additional function parameters
  */
case class SqrtImpl(funcParams: Seq[Any]) extends EmptyParamsInstantFunction {
  override def apply(value: Double): Double = scala.math.sqrt(value)
}

/**
 * Histogram quantile function for Histogram columns, where all buckets are together.
 * @param funcParams - a single value between 0 and 1, the quantile to calculate.
 */
case class HistogramQuantileImpl(funcParams: Seq[Any]) extends HistToDoubleIFunction {
  require(funcParams.length == 1, "Quantile (between 0 and 1) required for histogram quantile")
  require(funcParams(0).isInstanceOf[Number], "histogram_quantile parameter must be a number")
  val q = funcParams(0).asInstanceOf[Number].doubleValue()

  final def apply(value: Histogram): Double = value.quantile(q)
}