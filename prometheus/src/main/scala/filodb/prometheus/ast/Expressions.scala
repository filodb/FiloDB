package filodb.prometheus.ast

import filodb.core.query.RangeParams
import filodb.query._


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
    // scalastyle:off method.length
    override def toSeriesPlan(timeParams: TimeRangeParams): PeriodicSeriesPlan = {
      if (lhs.isInstanceOf[ScalarExpression] && rhs.isInstanceOf[ScalarExpression]) {
        ScalarBinaryOperation(operator.getPlanOperator, lhs.asInstanceOf[ScalarExpression].toScalar,
          rhs.asInstanceOf[ScalarExpression].toScalar, RangeParams(timeParams.start, timeParams.step, timeParams.end))
      } else {

        (lhs, rhs) match {
          case (lh: Function, rh: Function) if lh.isScalarFunction() && rh.isScalarFunction() =>
            val scalar = lh.toSeriesPlan(timeParams).asInstanceOf[ScalarPlan]
            val seriesPlanRhs = rh.toSeriesPlan(timeParams)
            ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlanRhs, true)

          case (lh: ScalarExpression, rh: PeriodicSeries) =>
            val scalar = ScalarFixedDoublePlan(lh.toScalar,
              RangeParams(timeParams.start, timeParams.step, timeParams.end))
            val seriesPlan = rh.toSeriesPlan(timeParams)
            ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlan, scalarIsLhs = true)

          case (lh: PeriodicSeries, rh: ScalarExpression) =>
            val scalar = ScalarFixedDoublePlan(rh.toScalar, RangeParams(timeParams.start, timeParams.step,
              timeParams.end))
            val seriesPlan = lh.toSeriesPlan(timeParams)
            ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlan, scalarIsLhs = false)

          case (lh: Function, rh: PeriodicSeries) if lh.isScalarFunction() =>
            val scalar = lh.toSeriesPlan(timeParams).asInstanceOf[ScalarPlan]
            val seriesPlanRhs = rh.toSeriesPlan(timeParams)
            ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlanRhs, scalarIsLhs = true)

          case (lh: PeriodicSeries, rh: Function) if rh.isScalarFunction =>
            val scalar = rh.toSeriesPlan(timeParams).asInstanceOf[ScalarPlan]
            val seriesPlanlhs = lh.toSeriesPlan(timeParams)
            ScalarVectorBinaryOperation(operator.getPlanOperator, scalar, seriesPlanlhs, scalarIsLhs = false)

          case (lh: PeriodicSeries, rh: PeriodicSeries) =>
            val seriesPlanLhs = lh.toSeriesPlan(timeParams)
            val seriesPlanRhs = rh.toSeriesPlan(timeParams)
            val cardinality = if (operator.getPlanOperator.isInstanceOf[SetOperator])
              Cardinality.ManyToMany
            else
              vectorMatch.map(_.cardinality.cardinality).getOrElse(Cardinality.OneToOne)

            val matcher = vectorMatch.flatMap(_.matching)
            val onLabels = matcher.filter(_.isInstanceOf[On]).map(_.labels)
            val ignoringLabels = matcher.filter(_.isInstanceOf[Ignoring]).map(_.labels)

            BinaryJoin(seriesPlanLhs, operator.getPlanOperator, cardinality, seriesPlanRhs,
              onLabels.getOrElse(Nil), ignoringLabels.getOrElse(Nil),
              vectorMatch.flatMap(_.grouping).map(_.labels).getOrElse(Nil))

          case _ => throw new UnsupportedOperationException("Invalid operands")
        }
      }
    }
    // scalastyle:on method.length
  }

}
