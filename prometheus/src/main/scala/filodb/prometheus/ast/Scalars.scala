package filodb.prometheus.ast

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
    require(op.isBool, "comparisons between scalars must use BOOL modifier")
    override def toScalar: Double = {
      op match {
        case Eq(true)       => if (lhs.toScalar == rhs.toScalar) 1.0 else 0.0
        case NotEqual(true) => if (lhs.toScalar != rhs.toScalar) 1.0 else 0.0
        case Gt(true)       => if (lhs.toScalar > rhs.toScalar)  1.0 else 0.0
        case Gte(true)      => if (lhs.toScalar >= rhs.toScalar) 1.0 else 0.0
        case Lt(true)       => if (lhs.toScalar < rhs.toScalar)  1.0 else 0.0
        case Lte(true)      => if (lhs.toScalar <= rhs.toScalar) 1.0 else 0.0
        case _              => throw new IllegalArgumentException(s"$op not supported")
      }
    }
  }

}