package filodb.prometheus.ast


//import filodb.query.ComparisonOperator


trait Scalars extends Operators with Base {

  sealed trait ScalarExpression extends Expression {
    def toScalar: Double
  }

  case class Scalar(toScalar: Double) extends ScalarExpression

  case class ArithmeticExpression(lhs: Scalar, op: ArithmeticOp, rhs: Scalar)
    extends ScalarExpression {
    override def toScalar: Double = {
      op match {
        case Pow => Math.pow(lhs.toScalar, rhs.toScalar)
        case Mul => lhs.toScalar * rhs.toScalar
        case Div => lhs.toScalar / rhs.toScalar
        case Mod => lhs.toScalar % rhs.toScalar
        case Add => lhs.toScalar + rhs.toScalar
        case Sub => lhs.toScalar - rhs.toScalar
      }
    }
  }

  case class BooleanExpression(lhs: Scalar, op: Comparision, rhs: Scalar)
    extends ScalarExpression {
    override def toScalar: Double = {
      op match {
        case Eq(true) => if (lhs == rhs) 1.0 else 0.0
        case _  => throw new IllegalArgumentException("to do")
      }
    }
  }

}