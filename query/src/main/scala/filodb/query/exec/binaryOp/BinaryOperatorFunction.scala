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
  def factoryMethod(function: BinaryOperator): ScalarFunction = {
    function match {

      case SUB                => new ScalarFunction {
        override def calculate(lhs: Double, rhs: Double): Double = lhs - rhs
      }
      case ADD                => new ScalarFunction {
        override def calculate(lhs: Double, rhs: Double): Double = lhs + rhs
      }
      case MUL                => new ScalarFunction {
        override def calculate(lhs: Double, rhs: Double): Double = lhs * rhs
      }
      case MOD                => new ScalarFunction {
        override def calculate(lhs: Double, rhs: Double): Double = lhs % rhs
      }
      case DIV                => new ScalarFunction {
        override def calculate(lhs: Double, rhs: Double): Double = lhs / rhs
      }
      case POW                => new ScalarFunction {
        override def calculate(lhs: Double, rhs: Double): Double = math.pow(lhs, rhs)
      }
      case _                  => throw new UnsupportedOperationException(s"$function not supported.")
    }
  }
}

