package filodb.prometheus.ast

import filodb.query.{Aggregate, AggregationOperator, PeriodicSeriesPlan}


trait Aggregates extends Vectors with TimeUnits with Base {

  sealed trait AggregateGrouping {
    def labels: Seq[String]
  }

  case class Without(labels: Seq[String]) extends AggregateGrouping

  case class By(labels: Seq[String]) extends AggregateGrouping

  case class AggregateExpression(name: String, params: Seq[Expression],
                                 aggregateGrouping: Option[AggregateGrouping],
                                 altFunctionParams: Seq[Expression]) extends Expression with PeriodicSeries {

    val aggOpOption: Option[AggregationOperator] = AggregationOperator.withNameInsensitiveOption(name)

    if (aggOpOption.isEmpty) {
      throw new IllegalArgumentException(s"Unsupported aggregation operator [$name]")
    }

    if (params.nonEmpty && altFunctionParams.nonEmpty) {
      throw new IllegalArgumentException("Can define function params only once")
    }

    val allParams: Seq[Expression] = if (params.isEmpty) altFunctionParams else params

    if (allParams.size < 1 || allParams.size > 2) {
      throw new IllegalArgumentException("Aggregate functions have at least 1 parameter and utmost 2 parameters")
    }

    var parameter: Seq[Any] = Nil
    private val aggOp = aggOpOption.get
    private val secondParamNeeded = aggOp.equals(AggregationOperator.BottomK) ||
      aggOp.equals(AggregationOperator.TopK) || aggOp.equals(AggregationOperator.CountValues) ||
      aggOp.equals(AggregationOperator.Quantile)

    if (secondParamNeeded && allParams.size < 2)
      throw new IllegalArgumentException("2 parameters required for count_values, quantile, topk and bottomk")

    if (allParams.size == 2) {
      if (!secondParamNeeded) {
        throw new IllegalArgumentException("parameter is only required for count_values, quantile, topk and bottomk")
      }
      allParams.head match {
        case num: ScalarExpression =>
          parameter = Seq(num.toScalar)
        case s: InstantExpression =>
          parameter = Seq(s.metricName)
        case _ =>
          throw new IllegalArgumentException("First parameter to aggregate operator can be a string or number")
      }
    }

    val last: Expression = if (allParams.size == 1) allParams.head else allParams(1)
    val series: PeriodicSeries = last match {
      case s: PeriodicSeries => s
      case _ =>
        throw new IllegalArgumentException(
          s"Second parameter to aggregate operator $name should be a vector, is instead $last"
        )
    }


    def toPeriodicSeriesPlan(timeParams: TimeRangeParams): PeriodicSeriesPlan = {
      val periodicSeriesPlan = series.toPeriodicSeriesPlan(timeParams)

      aggregateGrouping match {
        case Some(b: By) =>
          Aggregate(aggOpOption.get,
            periodicSeriesPlan,
            parameter,
            b.labels,
            Nil
          )
        case Some(w: Without) =>
          Aggregate(aggOpOption.get,
            periodicSeriesPlan,
            parameter,
            Nil,
            w.labels
          )
        case None =>
          Aggregate(aggOpOption.get,
            periodicSeriesPlan,
            parameter,
            Nil,
            Nil
          )
      }
    }

  }

}