package filodb.prometheus.ast

import filodb.core.query
import filodb.core.query.ColumnFilter
import filodb.query._

object Vectors {
  // The "tag" or key used to indicate the column to query to FiloDB.  A non-standard Prom extension.
  val ColumnSelectorLabel = "__col__"

  val PromMetricLabel = "__name__"
}

trait Vectors extends Scalars with TimeUnits with Base {
  import Vectors._

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
      case Some(GroupLeft(_)) => ManyToOne
      case Some(GroupRight(_)) => OneToMany
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

  sealed trait Vector extends Expression {

    def metricName: Option[String]
    def labelSelection: Seq[LabelMatch]

    def realMetricName: String = {
      val nameLabelValue = labelSelection.find(_.label == PromMetricLabel).map(_.value)
      if (nameLabelValue.nonEmpty && metricName.nonEmpty) {
        throw new IllegalArgumentException("Metric name should not be set twice")
      }
      metricName.orElse(nameLabelValue)
        .getOrElse(throw new IllegalArgumentException("Metric name is not present"))
    }

    protected def labelMatchesToFilters(labels: Seq[LabelMatch]) =
      labels.map { labelMatch =>
        val labelValue = labelMatch.value.replace("\\\\", "\\")
                                         .replace("\\\"", "\"")
                                         .replace("\\n", "\n")
                                         .replace("\\t", "\t")
          labelMatch.labelMatchOp match {
          case EqualMatch      => ColumnFilter(labelMatch.label, query.Filter.Equals(labelValue))
          case NotRegexMatch   => ColumnFilter(labelMatch.label, query.Filter.NotEqualsRegex(labelValue))
          case RegexMatch      => ColumnFilter(labelMatch.label, query.Filter.EqualsRegex(labelValue))
          case NotEqual(false) => ColumnFilter(labelMatch.label, query.Filter.NotEquals(labelValue))
          case other: Any      => throw new IllegalArgumentException(s"Unknown match operator $other")
        }
      // Remove the column selector as that is not a real time series filter
      }.filterNot(_.column == ColumnSelectorLabel)

    protected def labelMatchesToColumnName(labels: Seq[LabelMatch]): Seq[String] =
      labels.collect {
        // TODO: We may support multiple columns with the regex filter or some other
        //       custom "in" operator that we can specify in PromQL
        case LabelMatch(ColumnSelectorLabel, EqualMatch, col) => col
      }
  }

  /**
    * Instant vector selectors allow the selection of a set of time series
    * and a single sample value for each at a given timestamp (instant):
    * in the simplest form, only a metric name is specified.
    * This results in an instant vector containing elements
    * for all time series that have this metric name.
    * It is possible to filter these time series further by
    * appending a set of labels to match in curly braces ({}).
    */

  case class InstantExpression(override val metricName: Option[String],
                               override val labelSelection: Seq[LabelMatch],
                               offset: Option[Duration]) extends Vector with PeriodicSeries {

    val staleDataLookbackSeconds = 5 * 60 // 5 minutes

    private[prometheus] val columnFilters = labelMatchesToFilters(labelSelection)
    private[prometheus] val columns = labelMatchesToColumnName(labelSelection)
    private[prometheus] val nameFilter = ColumnFilter(PromMetricLabel, query.Filter.Equals(realMetricName))
    def getColFilters: Seq[ColumnFilter] = if (metricName.isDefined) columnFilters :+ nameFilter else columnFilters

    def toPeriodicSeriesPlan(timeParams: TimeRangeParams): PeriodicSeriesPlan = {
    val offsetMillis : Long = offset.map(_.millis).getOrElse(0)

      // we start from 5 minutes earlier that provided start time in order to include last sample for the
      // start timestamp. Prometheus goes back unto 5 minutes to get sample before declaring as stale
      PeriodicSeries(
        RawSeries(timeParamToSelector(timeParams, staleDataLookbackSeconds * 1000, offsetMillis),
          getColFilters, columns),
        timeParams.start * 1000 - offsetMillis, timeParams.step * 1000, timeParams.end * 1000 - offsetMillis,
        offset.map(_.millis)
      )
    }

    def toMetadataPlan(timeParams: TimeRangeParams): SeriesKeysByFilters = {
      SeriesKeysByFilters(getColFilters, timeParams.start * 1000, timeParams.end * 1000)
    }
  }

  /**
    * Range vector literals work like instant vector literals,
    * except that they select a range of samples back from the current instant.
    * Syntactically, a range duration is appended in square brackets ([])
    * at the end of a vector selector to specify how far back in time values
    * should be fetched for each resulting range vector element.
    */
  case class RangeExpression(override val metricName: Option[String],
                             override val labelSelection: Seq[LabelMatch],
                             window: Duration,
                             offset: Option[Duration]) extends Vector with SimpleSeries {

    private[prometheus] val columnFilters = labelMatchesToFilters(labelSelection)
    private[prometheus] val columns = labelMatchesToColumnName(labelSelection)
    private[prometheus] val nameFilter = ColumnFilter(PromMetricLabel, query.Filter.Equals(realMetricName))

    val allFilters: Seq[ColumnFilter] = if (metricName.isDefined) columnFilters :+ nameFilter else columnFilters

    def toRawSeriesPlan(timeParams: TimeRangeParams, isRoot: Boolean): RawSeriesPlan = {
      if (isRoot && timeParams.start != timeParams.end) {
        throw new UnsupportedOperationException("Range expression is not allowed in query_range")
      }
      // multiply by 1000 to convert unix timestamp in seconds to millis
      RawSeries(timeParamToSelector(timeParams, window.millis, offset.map(_.millis).getOrElse(0)), allFilters, columns)
    }

  }

}
