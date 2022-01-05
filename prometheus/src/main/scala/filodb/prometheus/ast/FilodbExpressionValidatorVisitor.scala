package filodb.prometheus.ast

class FilodbExpressionValidatorVisitor {
  def visit(ex: Expression) : Expression = {
    ex
  }
  def visit(ex: Vector): Expression = {
    val nameLabel = ex.labelSelection.find(_.label == Vectors.PromMetricLabel)
    if (nameLabel.isEmpty && ex.metricName.isEmpty)
      throw new IllegalArgumentException("Metric name is not present")
    ex
  }
  def visit(ex: Function): Expression = {
    ex.allParams.foreach(_.acceptVisitor(this))
    ex
  }
  def visit(ex: UnaryExpression) : Expression = {
    ex.operand.acceptVisitor(this)
    ex
  }
  def visit(ex: PrecedenceExpression) : Expression = {
    ex.expression.acceptVisitor(this)
    ex
  }
  def visit(ex: BinaryExpression) : Expression = {
    ex.lhs.acceptVisitor(this)
    ex.rhs.acceptVisitor(this)
    ex
  }
  def visit(ex: AggregateExpression): Expression = {
    ex.allParams.foreach(_.acceptVisitor(this))
    ex
  }
}
