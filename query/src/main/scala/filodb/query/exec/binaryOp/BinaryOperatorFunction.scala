package filodb.query.exec.binaryOp

import filodb.query.BinaryOperator
import filodb.query.BinaryOperator._

trait ScalarFunction {
  def calculate (lhs: Double, rhs: Double): Double
}

object BinaryOperatorFunction {

  /**
    * This function returns a function that can be applied to generate the result.
    *
    * @param function to be invoked
    * @return the function
    */

  // scalastyle:off
  def factoryMethod(function: BinaryOperator): ScalarFunction = {
    function match {

      case SUB                => new ScalarFunction { override def calculate(lhs: Double, rhs: Double): Double = lhs - rhs }
      case ADD                => new ScalarFunction { override def calculate(lhs: Double, rhs: Double): Double = lhs + rhs }
      case MUL                => new ScalarFunction { override def calculate(lhs: Double, rhs: Double): Double = lhs * rhs }
      case MOD                => new ScalarFunction { override def calculate(lhs: Double, rhs: Double): Double = lhs % rhs }
      case DIV                => new ScalarFunction { override def calculate(lhs: Double, rhs: Double): Double = lhs / rhs }
      case POW                => new ScalarFunction { override def calculate(lhs: Double, rhs: Double): Double = math.pow(lhs, rhs) }
      case LSS                => new ScalarFunction { override def calculate(lhs: Double, rhs: Double): Double = if (lhs < rhs)  lhs else Double.NaN }
      case LTE                => new ScalarFunction { override def calculate(lhs: Double, rhs: Double): Double = if (lhs <= rhs) lhs else Double.NaN }
      case GTR                => new ScalarFunction { override def calculate(lhs: Double, rhs: Double): Double = if (lhs > rhs)  lhs else Double.NaN }
      case GTE                => new ScalarFunction { override def calculate(lhs: Double, rhs: Double): Double = if (lhs >= rhs) lhs else Double.NaN }
      case EQL                => new ScalarFunction { override def calculate(lhs: Double, rhs: Double): Double = if (lhs == rhs) lhs else Double.NaN }
      case NEQ                => new ScalarFunction { override def calculate(lhs: Double, rhs: Double): Double = if (lhs != rhs) lhs else Double.NaN }
      case LSS_BOOL           => new ScalarFunction {
                                  override def calculate(lhs: Double, rhs: Double): Double = {
                                    if (lhs.isNaN || rhs.isNaN) Double.NaN
                                    else if (lhs < rhs) 1.0 else 0.0
                                  }
                                 }
      case LTE_BOOL           => new ScalarFunction {
                                  override def calculate(lhs: Double, rhs: Double): Double = {
                                    if (lhs.isNaN || rhs.isNaN) Double.NaN
                                    else if (lhs <= rhs) 1.0 else 0.0
                                  }
                                 }
      case GTR_BOOL           => new ScalarFunction {
                                  override def calculate(lhs: Double, rhs: Double): Double = {
                                    if (lhs.isNaN || rhs.isNaN) Double.NaN
                                    else if (lhs > rhs) 1.0 else 0.0
                                  }
                                 }
      case GTE_BOOL           => new ScalarFunction {
                                  override def calculate(lhs: Double, rhs: Double): Double = {
                                    if (lhs.isNaN || rhs.isNaN) Double.NaN
                                    else if (lhs >= rhs) 1.0 else 0.0
                                  }
                                 }
      case EQL_BOOL           => new ScalarFunction {
                                  override def calculate(lhs: Double, rhs: Double): Double = {
                                    if (lhs.isNaN || rhs.isNaN) Double.NaN
                                    else if (lhs == rhs) 1.0 else 0.0
                                  }
                                 }
      case NEQ_BOOL           => new ScalarFunction {
                                  override def calculate(lhs: Double, rhs: Double): Double = {
                                    if (lhs.isNaN || rhs.isNaN) Double.NaN
                                    else if (lhs != rhs) 1.0 else 0.0
                                  }
                                 }
      case _                  => throw new UnsupportedOperationException(s"$function not supported.")
    }
  }
}

