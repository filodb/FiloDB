package filodb.prometheus.ast

import filodb.core.query
import filodb.core.query.ColumnFilter
import filodb.query._


trait Vectors extends Scalars with TimeUnits with Base {

  sealed trait JoinMatching {
    def labels: Seq[String]
  }

  case class Ignoring(labels: Seq[String]) extends JoinMatching

  case class On(labels: Seq[String]) extends JoinMatching

  sealed trait JoinGrouping {
    def labels: Seq[String]
  }

  case class GroupLeft(labels: Seq[String]) extends JoinGrouping

  case class GroupRight(labels: Seq[String]) extends JoinGrouping

  sealed trait Cardinal {
    def cardinality: Cardinality
  }

  case object OneToOne extends Cardinal {
    def cardinality: Cardinality = Cardinality.OneToOne
  }

  case object OneToMany extends Cardinal {
    def cardinality: Cardinality = Cardinality.OneToMany
  }

  case object ManyToOne extends Cardinal {
    def cardinality: Cardinality = Cardinality.ManyToOne
  }

  case object ManyToMany extends Cardinal {
    def cardinality: Cardinality = Cardinality.ManyToMany
  }

  case class VectorMatch(matching: Option[JoinMatching],
                         grouping: Option[JoinGrouping]) {
    lazy val cardinality: Cardinal = grouping match {
      case Some(GroupLeft(_)) => OneToMany
      case Some(GroupRight(_)) => ManyToOne
      case None => OneToOne
    }

    def notEmpty: Boolean = matching.isDefined || grouping.isDefined

    def validate(operator: Operator, lhs: Expression, rhs: Expression): Unit = {
      if (notEmpty && (lhs.isInstanceOf[Scalar] || rhs.isInstanceOf[Scalar])) {
        throw new IllegalArgumentException("vector matching only allowed between instant vectors")
      }
      if (grouping.isDefined && operator.isInstanceOf[SetOp]) {
        throw new IllegalArgumentException("no grouping allowed for and, or, unless operations")
      }
      validateGroupAndMatch()
    }

    private def validateGroupAndMatch(): Unit = if (grouping.isDefined && matching.isDefined) {
      val group = grouping.get
      val matcher = matching.get
      val matchLabels = matcher.labels
      val groupLabels = group.labels
      groupLabels.foreach { label =>
        if (matchLabels.contains(label) && matcher.isInstanceOf[On]) {
          throw new IllegalArgumentException("Labels must not occur in ON and GROUP clause at once")
        }
      }
    }
  }


  sealed trait Vector extends Expression

  /**
    * Instant vector selectors allow the selection of a set of time series
    * and a single sample value for each at a given timestamp (instant):
    * in the simplest form, only a metric name is specified.
    * This results in an instant vector containing elements
    * for all time series that have this metric name.
    * It is possible to filter these time series further by
    * appending a set of labels to match in curly braces ({}).
    */

  case class InstantExpression(metricName: String,
                               labelSelection: Seq[LabelMatch],
                               offset: Option[Duration]) extends Vector with PeriodicSeries {

    private val nameLabels = labelSelection.filter(_.label == "__name__")

    if (nameLabels.nonEmpty && !nameLabels.head.label.equals(metricName)) {
      throw new IllegalArgumentException("Metric name should not be set twice")
    }

    private val columnFilters = labelSelection.map { labelMatch =>
      //TODO All operators need to be taken care
      ColumnFilter(labelMatch.label, query.Filter.Equals(labelMatch.value))
    }

    private val nameFilter = ColumnFilter("__name__", query.Filter.Equals(metricName))

    def toPeriodicSeriesPlan(queryParams: QueryParams): PeriodicSeriesPlan = {
      PeriodicSeries(
        RawSeries(IntervalSelector(Seq(queryParams.start * 1000),
                                   Seq(queryParams.end * 1000)), columnFilters :+ nameFilter, Nil),
        queryParams.start * 1000, queryParams.step * 1000, queryParams.end * 1000
      )
    }
  }

  /**
    * Range vector literals work like instant vector literals,
    * except that they select a range of samples back from the current instant.
    * Syntactically, a range duration is appended in square brackets ([])
    * at the end of a vector selector to specify how far back in time values
    * should be fetched for each resulting range vector element.
    */
  case class RangeExpression(metricName: String,
                             labelSelection: Seq[LabelMatch],
                             window: Duration,
                             offset: Option[Duration]) extends Vector with SimpleSeries{
    private val nameLabels = labelSelection.filter(_.label == "__name__")

    if (nameLabels.nonEmpty && !nameLabels.head.label.equals(metricName)) {
      throw new IllegalArgumentException("Metric name should not be set twice")
    }

    private val columnFilters = labelSelection.map { labelMatch =>
      //TODO All operators need to be taken care
      ColumnFilter(labelMatch.label, query.Filter.Equals(labelMatch.value))
    }

    private val nameFilter = ColumnFilter("__name__", query.Filter.Equals(metricName))

    val allFilters: Seq[ColumnFilter] = columnFilters :+ nameFilter

    def toRawSeriesPlan(queryParams: QueryParams, isRoot: Boolean): RawSeriesPlan = {
      if (isRoot && queryParams.start != queryParams.end) {
        throw new UnsupportedOperationException("Range expression is not allowed in query_range")
      }
      // multiply by 1000 to convert unix timestamp in seconds to millis
      RawSeries(IntervalSelector(Seq(queryParams.start * 1000 - window.millis),
                                 Seq(queryParams.end * 1000)), allFilters, Nil)
    }

  }

}
