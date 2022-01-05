package filodb.prometheus.ast

sealed trait ScalarExpression extends Expression {
  def toScalar: Double
}

case class Scalar(toScalar: Double) extends ScalarExpression {
  override def acceptVisitor(vis: FilodbExpressionValidatorVisitor): Expression = {
    vis.visit(this)
  }
}

case class Limit(limit: Double)
