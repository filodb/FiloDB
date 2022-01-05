package filodb.prometheus.ast

import scala.util.Try

import filodb.core.{query, GlobalConfig}
import filodb.core.query.{ColumnFilter, RangeParams}
import filodb.prometheus.parse.Parser
import filodb.query._

object Vectors {
  val PromMetricLabel = "__name__"
  val TypeLabel       = "_type_"
  val BucketFilterLabel = "_bucket_"
}


object WindowConstants {
  val conf = GlobalConfig.systemConfig
  val staleDataLookbackMillis = conf.getConfig("filodb.query").
                                  getDuration("stale-sample-after").toMillis
}

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

case class SubqueryExpression(
    subquery: PeriodicSeries, sqcl: SubqueryClause, offset: Option[Duration], limit: Option[Int]
) extends Expression with PeriodicSeries {

  override def acceptVisitor(vis: FilodbExpressionValidatorVisitor): Expression = {
    vis.visit(this)
  }

  def toSeriesPlan(timeParams: TimeRangeParams): PeriodicSeriesPlan = {
    // There are only two places for the subquery to be defined in the abstract syntax tree:
    // a) top level expression (equivalent of query_range API)
    // b) argument of a range function
    // In case b), the toSeriesPlan is called on the inner subquery expression, not
    // on this class itself, hence this method is only for case a).
    // Top level expression of a range query, should return an
    // instant vector but subqueries by definition return range vectors.
    // It's illegal to have a top level subquery expression to be called from query_range API
    // when start and end parameters are not the same.
    require(timeParams.start == timeParams.end, "Subquery is not allowed as a top level expression for query_range")
    val offsetSec : Long = offset match {
      case None => 0
      case Some(duration) => duration.millis(1L) / 1000
    }
    val stepToUseMs = SubqueryUtils.getSubqueryStepMs(sqcl.step);
    var startS = timeParams.start - (sqcl.window.millis(1L) / 1000) - offsetSec
    var endS = timeParams.start - offsetSec
    startS = SubqueryUtils.getStartForFastSubquery(startS, stepToUseMs/1000 )
    endS = SubqueryUtils.getEndForFastSubquery(endS, stepToUseMs/1000 )
    val timeParamsToUse = TimeStepParams(
      startS,
      stepToUseMs/1000,
      endS
    )
    TopLevelSubquery(
      subquery.toSeriesPlan(timeParamsToUse),
      startS * 1000,
      stepToUseMs,
      endS * 1000,
      sqcl.window.millis(1L),
      offset.map(duration => duration.millis(1L))
    )

  }

}

sealed trait Vector extends Expression {
  import Vectors._

  def metricName: Option[String]
  def labelSelection: Seq[LabelMatch]
  val regexColumnName: String = "::(?=[^::]+$)" //regex pattern to extract ::columnName at the end

  // Convert metricName{labels} -> {labels, __name__="metricName"} so it's uniform
  val mergeNameToLabels: Seq[LabelMatch] = {
    val nameLabel = labelSelection.find(_.label == PromMetricLabel)
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
    val parts = metricName.split(regexColumnName)
    if (parts.size > 1) {
      require(parts(1).nonEmpty, "cannot use empty column name")
      (parts(0), Some(parts(1)))
    } else (metricName, None)
  }

  private def parseBucketValue(value: String): Option[Double] =
    if (value.toLowerCase == "+inf") Some(Double.PositiveInfinity) else Try(value.toDouble).toOption

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
      val labelVal = labelMatch.value
      val labelValue = if (labelMatch.label == PromMetricLabel) {
        val (newValue, colNameOpt) = extractStripColumn(labelVal)
        colNameOpt.foreach { col => column = Some(col) }
        newValue
      } else { labelVal }
      labelMatch.labelMatchOp match {
        case EqualMatch      => ColumnFilter(labelMatch.label, query.Filter.Equals(labelValue))
        case NotRegexMatch   => require(labelValue.length <= Parser.REGEX_MAX_LEN,
                                         s"Regular expression filters should be <= ${Parser.REGEX_MAX_LEN} characters")
                                ColumnFilter(labelMatch.label, query.Filter.NotEqualsRegex(labelValue))
        case RegexMatch      => require(labelValue.length <= Parser.REGEX_MAX_LEN,
                                         s"Regular expression filters should be <= ${Parser.REGEX_MAX_LEN} characters")
                                ColumnFilter(labelMatch.label, query.Filter.EqualsRegex(labelValue))
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
case class InstantExpression(metricName: Option[String],
                             labelSelection: Seq[LabelMatch],
                             offset: Option[Duration]) extends Vector with PeriodicSeries {

  import WindowConstants._

  val (columnFilters, column, bucketOpt) = labelMatchesToFilters(mergeNameToLabels)

  override def acceptVisitor(vis: FilodbExpressionValidatorVisitor): Expression = {
    vis.visit(this)
  }

  def toSeriesPlan(timeParams: TimeRangeParams): PeriodicSeriesPlan = {
    // we start from 5 minutes earlier that provided start time in order to include last sample for the
    // start timestamp. Prometheus goes back up to 5 minutes to get sample before declaring as stale
    val ps = PeriodicSeries(
      RawSeries(Base.timeParamToSelector(timeParams), columnFilters, column.toSeq, Some(staleDataLookbackMillis),
        offset.map(_.millis(timeParams.step * 1000))),
      timeParams.start * 1000, timeParams.step * 1000, timeParams.end * 1000,
      offset.map(_.millis(timeParams.step * 1000))
    )
    bucketOpt.map { bOpt =>
      // It's a fixed value, the range params don't matter at all
      val param = ScalarFixedDoublePlan(bOpt, RangeParams(0, Long.MaxValue, 60000L))
      ApplyInstantFunction(ps, InstantFunctionId.HistogramBucket, Seq(param))
    }.getOrElse(ps)
  }

  def toMetadataPlan(timeParams: TimeRangeParams, fetchFirstLastSampleTimes: Boolean): SeriesKeysByFilters = {
    SeriesKeysByFilters(columnFilters, fetchFirstLastSampleTimes, timeParams.start * 1000, timeParams.end * 1000)
  }

  def toLabelNamesPlan(timeParams: TimeRangeParams): LabelNames = {
    LabelNames(columnFilters, timeParams.start * 1000, timeParams.end * 1000)
  }

  def toLabelCardinalityPlan(timeParams: TimeRangeParams): LabelCardinality =
    LabelCardinality(columnFilters, timeParams.start * 1000, timeParams.end * 1000)

  def toRawSeriesPlan(timeParams: TimeRangeParams, offsetMs: Option[Long] = None): RawSeries = {
    RawSeries(Base.timeParamToSelector(timeParams), columnFilters, column.toSeq, Some(staleDataLookbackMillis),
      offsetMs)
  }

  /**
   * Returns a mapping from selector labels (including an explicit/implicit __name__) to values.
   * Throws an IllegalArgumentException if any of the values are not matched by equality without a regex.
   *
   * Note: this InstantExpression does not require a metric name in order
   *   for this method to run to completion.
   */
  def toEqualLabelMap() : Map[String, String] = {
    mergeNameToLabels.map{ labelMatch =>
      if (labelMatch.labelMatchOp != EqualMatch) {
        throw new IllegalArgumentException(
          "cannot convert to label map if any values aren't matched by equality")
      }
      labelMatch.label -> labelMatch.value
    }.toMap
  }
}

/**
  * Range vector literals work like instant vector literals,
  * except that they select a range of samples back from the current instant.
  * Syntactically, a range duration is appended in square brackets ([])
  * at the end of a vector selector to specify how far back in time values
  * should be fetched for each resulting range vector element.
  */
case class RangeExpression(metricName: Option[String],
                           labelSelection: Seq[LabelMatch],
                           window: Duration,
                           offset: Option[Duration]) extends Vector with SimpleSeries {

  private[prometheus] val (columnFilters, column, bucketOpt) = labelMatchesToFilters(mergeNameToLabels)

  override def acceptVisitor(vis: FilodbExpressionValidatorVisitor): Expression = {
    vis.visit(this)
  }

  def toSeriesPlan(timeParams: TimeRangeParams, isRoot: Boolean): RawSeriesLikePlan = {
    if (isRoot && timeParams.start != timeParams.end) {
      throw new UnsupportedOperationException("Range expression is not allowed in query_range")
    }
    // multiply by 1000 to convert unix timestamp in seconds to millis
    val rs = RawSeries(Base.timeParamToSelector(timeParams), columnFilters, column.toSeq,
      Some(window.millis(timeParams.step * 1000)),
      offset.map(_.millis(timeParams.step * 1000))
    )
    bucketOpt.map { bOpt =>
      // It's a fixed value, the range params don't matter at all
      val param = ScalarFixedDoublePlan(bOpt, RangeParams(0, Long.MaxValue, 60000L))
      ApplyInstantFunctionRaw(rs, InstantFunctionId.HistogramBucket, Seq(param))
    }.getOrElse(rs)
  }
}



