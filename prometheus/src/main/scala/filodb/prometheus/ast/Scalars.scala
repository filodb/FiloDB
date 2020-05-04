package filodb.prometheus.ast

trait Scalars extends Operators with Base {

  sealed trait ScalarExpression extends Expression {
    def toScalar: Double
  }

  case class Scalar(toScalar: Double) extends ScalarExpression
}