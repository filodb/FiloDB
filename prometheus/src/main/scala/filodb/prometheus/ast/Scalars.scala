package filodb.prometheus.ast

sealed trait ScalarExpression extends Expression {
  def toScalar: Double
}

case class Scalar(toScalar: Double) extends ScalarExpression

case class Limit(limit: Double)
