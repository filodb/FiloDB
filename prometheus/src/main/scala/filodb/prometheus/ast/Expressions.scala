package filodb.prometheus.ast

import filodb.query.{BinaryJoin, Cardinality, PeriodicSeriesPlan, ScalarVectorBinaryOperation}


trait Expressions extends Aggregates with Functions {

  case class UnaryExpression(operator: Operator, operand: Expression) extends Expression {
    //TODO Need to pass an operator to a series
  }

  case class BinaryExpression(lhs: Expression,
                              operator: Operator,
                              vectorMatch: Option[VectorMatch],
                              rhs: Expression) extends Expression with PeriodicSeries {

    operator match {
      case setOp: SetOp =>
        if (lhs.isInstanceOf[ScalarExpression] || rhs.isInstanceOf[ScalarExpression])
          throw new IllegalArgumentException("set operators not allowed in binary scalar expression")

      case comparison: Comparision if !comparison.isBool =>
        if (lhs.isInstanceOf[ScalarExpression] && rhs.isInstanceOf[ScalarExpression])
          throw new IllegalArgumentException("comparisons between scalars must use BOOL modifier")
      case _ =>
    }
    if (vectorMatch.isDefined) {
      vectorMatch.get.validate(operator, lhs, rhs)
    }


    override def toPeriodicSeriesPlan(timeParams: TimeRangeParams): PeriodicSeriesPlan = {
      if (lhs.isInstanceOf[ScalarExpression] && rhs.isInstanceOf[ScalarExpression]) {
        throw new UnsupportedOperationException("Binary operations on scalars is not supported yet")
      }

      lhs match {
        case expression: ScalarExpression if rhs.isInstanceOf[PeriodicSeries] =>
          val scalar = expression.toScalar
          val seriesPlan = rhs.asInstanceOf[PeriodicSeries].toPeriodicSeriesPlan(timeParams)
          ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlan, scalarIsLhs = true)
        case series: PeriodicSeries if rhs.isInstanceOf[ScalarExpression] =>
          val scalar = rhs.asInstanceOf[ScalarExpression].toScalar
          val seriesPlan = series.toPeriodicSeriesPlan(timeParams)
          ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlan, scalarIsLhs = false)
        case series: PeriodicSeries if rhs.isInstanceOf[PeriodicSeries] =>
          val seriesPlanLhs = series.toPeriodicSeriesPlan(timeParams)
          val seriesPlanRhs = rhs.asInstanceOf[PeriodicSeries].toPeriodicSeriesPlan(timeParams)
          val cardinality = vectorMatch.map(_.cardinality.cardinality).getOrElse(Cardinality.OneToOne)
          BinaryJoin(seriesPlanLhs, operator.getPlanOperator, cardinality, seriesPlanRhs)
      }
    }
  }


}
