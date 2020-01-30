package filodb.prometheus.ast

import scala.util.Try

import filodb.core.query
import filodb.core.query.{ColumnFilter, RangeParams}
import filodb.query._

object Vectors {
  val PromMetricLabel = "__name__"
  val TypeLabel       = "_type_"
  val BucketFilterLabel = "_bucket_"
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

    // Convert metricName{labels} -> {labels, __name__="metricName"} so it's uniform
    lazy val mergeNameToLabels: Seq[LabelMatch] = {
      val nameLabel = labelSelection.find(_.label == PromMetricLabel)
      if (nameLabel.isEmpty && metricName.isEmpty)
        throw new IllegalArgumentException("Metric name is not present")
      if (metricName.nonEmpty) {
        if (nameLabel.nonEmpty) throw new IllegalArgumentException("Metric name should not be set twice")
        // metric name specified but no __name__ label.  Add it
        labelSelection :+ LabelMatch(PromMetricLabel, EqualMatch, metricName.get)
      } else {
        labelSelection
      }
    }

    def realMetricName: String = mergeNameToLabels.find(_.label == PromMetricLabel).get.value

    // Returns (trimmedMetricName, column) after stripping ::columnName
    private def extractStripColumn(metricName: String): (String, Option[String]) = {
      val parts = metricName.split("::", 2)
      if (parts.size > 1) {
        require(parts(1).nonEmpty, "cannot use empty column name")
        (parts(0), Some(parts(1)))
      } else (metricName, None)
    }

    private def parseBucketValue(value: String): Option[Double] =
      if (value == "+Inf") Some(Double.PositiveInfinity) else Try(value.toDouble).toOption

    /**
     * Converts LabelMatches to ColumnFilters.  Along the way:
     * - extracts ::col column name expressions in metric names to columns
     * - removes ::col in __name__ label matches as needed
     * Also extracts special _bucket_ histogram bucket filter
     */
    protected def labelMatchesToFilters(labels: Seq[LabelMatch]) = {
      var column: Option[String] = None
      var bucketOpt: Option[Double] = None
      val filters = labels.filter { labelMatch =>
        if (labelMatch.label == BucketFilterLabel) {
          bucketOpt = parseBucketValue(labelMatch.value)
          false
        } else true
      }.map { labelMatch =>
        val labelVal = labelMatch.value.replace("\\\\", "\\")
                                         .replace("\\\"", "\"")
                                         .replace("\\n", "\n")
                                         .replace("\\t", "\t")
        val labelValue = if (labelMatch.label == PromMetricLabel) {
          val (newValue, colNameOpt) = extractStripColumn(labelVal)
          colNameOpt.foreach { col => column = Some(col) }
          newValue
        } else { labelVal }
        labelMatch.labelMatchOp match {
          case EqualMatch      => ColumnFilter(labelMatch.label, query.Filter.Equals(labelValue))
          case NotRegexMatch   => ColumnFilter(labelMatch.label, query.Filter.NotEqualsRegex(labelValue))
          case RegexMatch      => ColumnFilter(labelMatch.label, query.Filter.EqualsRegex(labelValue))
          case NotEqual(false) => ColumnFilter(labelMatch.label, query.Filter.NotEquals(labelValue))
          case other: Any      => throw new IllegalArgumentException(s"Unknown match operator $other")
        }
      }
      (filters, column, bucketOpt)
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
  case class InstantExpression(val metricName: Option[String],
                               val labelSelection: Seq[LabelMatch],
                               offset: Option[Duration]) extends Vector with PeriodicSeries {

    val staleDataLookbackSeconds = 5 * 60 // 5 minutes
    val offsetMillis : Long = offset.map(_.millis).getOrElse(0)

    private[prometheus] val (columnFilters, column, bucketOpt) = labelMatchesToFilters(mergeNameToLabels)

    def toSeriesPlan(timeParams: TimeRangeParams): PeriodicSeriesPlan = {
      val offsetMillis : Long = offset.map(_.millis).getOrElse(0)

      // we start from 5 minutes earlier that provided start time in order to include last sample for the
      // start timestamp. Prometheus goes back unto 5 minutes to get sample before declaring as stale
      val ps = PeriodicSeries(
        RawSeries(timeParamToSelector(timeParams, staleDataLookbackSeconds * 1000, offsetMillis),
          columnFilters, column.toSeq),
        timeParams.start * 1000 - offsetMillis, timeParams.step * 1000, timeParams.end * 1000 - offsetMillis,
        offset.map(_.millis)
      )
      bucketOpt.map { bOpt =>
        // It's a fixed value, the range params don't matter at all
        val param = ScalarFixedDoublePlan(bOpt, RangeParams(0, Long.MaxValue, 60000L))
        ApplyInstantFunction(ps, InstantFunctionId.HistogramBucket, Seq(param))
      }.getOrElse(ps)
    }

    def toMetadataPlan(timeParams: TimeRangeParams): SeriesKeysByFilters = {
      SeriesKeysByFilters(columnFilters, timeParams.start * 1000, timeParams.end * 1000)
    }

    def toRawSeriesPlan(timeParams: TimeRangeParams): RawSeries = {
      RawSeries(timeParamToSelector(timeParams, staleDataLookbackSeconds * 1000, offsetMillis),
        columnFilters, column.toSeq)
    }
  }

  /**
    * Range vector literals work like instant vector literals,
    * except that they select a range of samples back from the current instant.
    * Syntactically, a range duration is appended in square brackets ([])
    * at the end of a vector selector to specify how far back in time values
    * should be fetched for each resulting range vector element.
    */
  case class RangeExpression(val metricName: Option[String],
                             val labelSelection: Seq[LabelMatch],
                             window: Duration,
                             offset: Option[Duration]) extends Vector with SimpleSeries {

    private[prometheus] val (columnFilters, column, bucketOpt) = labelMatchesToFilters(mergeNameToLabels)

    def toSeriesPlan(timeParams: TimeRangeParams, isRoot: Boolean): RawSeriesLikePlan = {
      if (isRoot && timeParams.start != timeParams.end) {
        throw new UnsupportedOperationException("Range expression is not allowed in query_range")
      }
      // multiply by 1000 to convert unix timestamp in seconds to millis
      val rs = RawSeries(timeParamToSelector(timeParams, window.millis, offset.map(_.millis).getOrElse(0)),
                         columnFilters, column.toSeq)
      bucketOpt.map { bOpt =>
        // It's a fixed value, the range params don't matter at all
        val param = ScalarFixedDoublePlan(bOpt, RangeParams(0, Long.MaxValue, 60000L))
        ApplyInstantFunctionRaw(rs, InstantFunctionId.HistogramBucket, Seq(param))
      }.getOrElse(rs)
    }

  }

}
