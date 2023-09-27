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

  // UPDATE April 04, 2023: Updating the regex to split only on "::" and not on ":::". The metric names might have
  // ":::" for certain cases, like the aggregated metrics from streaming aggregation. For ex -
  // "requests:::rule_1::sum" should give -> ("requests:::rule_1", "sum") on split
  val regexColumnName: String = "(?=.)+(?<!:)::(?=[^:]+$)" //regex pattern to extract ::columnName at the end

  // Convert metricName{labels} -> {labels, __name__="metricName"} so it's uniform
  // Note: see makeMergeNameToLabels before changing the laziness.
  lazy val mergeNameToLabels: Seq[LabelMatch] = makeMergeNameToLabels()

  // These constraints are also applied within makeMergeNameToLabels, but they are
  //   *immediately* applied here (i.e. not lazily as with mergeNameToLabels).
  applyPromqlConstraints()

  /**
   * Apply PromQL-scope constraints to the vector selector:
   *   (1) only one __name__ specifier (implicit or otherwise) exists.
   *   (2) at least one label matcher exists.
   */
  def applyPromqlConstraints(): Unit = {
    val nameLabel = labelSelection.find(_.label == PromMetricLabel)
    if (metricName.nonEmpty) {
      if (nameLabel.nonEmpty) {
        throw new IllegalArgumentException(
          "Metric name should not be set twice")
      }
    } else if (labelSelection.size == 0) {
      throw new IllegalArgumentException(
        "At least one label matcher is required")
    }
  }

  /**
   * Constructs the mergeNameToLabels member.
   *
   * Note:
   * At present, FiloDB requires the existence of a metric name for all vector selectors, and
   *   that requirement is enforced at the initialization of mergeNameToLabels. However,
   *   this makes it impossible to initialize a Vector type with PromQL-valid label selectors
   *   that lack an implicit/explicit __name__ label.
   *
   * If you need to make use of a name-optional Vector (i.e. as a temporary vessel for parsed PromQL),
   *   the hacky workaround is as follows:
   *     (1) lazily initialize mergeNameToLabels so a metric name is not explicitly required
   *         at the Vector's initialization, and
   *     (2) when needed, locally initialize a "mergeNameToLabels-like" value without
   *         a name requirement.
   *
   * This method exists only to consolidate code for the initialization of these values.
   */
  def makeMergeNameToLabels(requireName: Boolean = true): Seq[LabelMatch] = {
    applyPromqlConstraints()
    val nameLabel = labelSelection.find(_.label == PromMetricLabel)
    if (requireName && nameLabel.isEmpty && metricName.isEmpty) {
      // not included in applyPromqlConstraints
      throw new IllegalArgumentException("Metric name is not present")
    }
    if (metricName.nonEmpty) {
      // metric name specified but no __name__ label.  Add it
      labelSelection :+ LabelMatch(PromMetricLabel, EqualMatch, metricName.get)
    } else {
      labelSelection
    }
  }

  def realMetricName: String = mergeNameToLabels.find(_.label == PromMetricLabel).get.value

  // Returns (trimmedMetricName, column) after stripping ::columnName
  protected def extractStripColumn(metricName: String): (String, Option[String]) = {
    val parts = metricName.split(regexColumnName)
    if (parts.size > 1) {
      require(parts(1).nonEmpty, "cannot use empty column name")
      require(!parts(1).isBlank, "cannot use blank/whitespace column name")
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
 * Adding a class to be able to unit test few methods
 * of the sealed trait Vector like "extractStripColumn"
 */
case class VectorSpec() extends Vector{
  override def labelSelection: Seq[LabelMatch] = Seq.empty
  override def metricName: Option[String] = None

  // helper function created to unit test "extractStripColumn" function in sealted trait Vector
  def getMetricAndColumnName(metricName: String): (String, Option[String]) = {
    extractStripColumn(metricName)
  }
  override def applyPromqlConstraints(): Unit = {}
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

  // TODO: mergeNameToLabels requires a metric name at its initialization, but a valid PromQL
  //   instant selector does not necessarily contain a metric name. This name requirement should
  //   be moved elsewhere. For now, this (and mergeNameToLabels) must remain lazy in order
  //   to allow nameless instant selectors (see toLabelMap() as an example).
  lazy val (columnFilters, column, bucketOpt) = labelMatchesToFilters(mergeNameToLabels)

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
   * Returns pre-validation ColumnFilters (i.e. that may not include a __name__ filter).
   */
  def getUnvalidatedColumnFilters(): Seq[ColumnFilter] = {
    val labels = makeMergeNameToLabels(requireName = false)
    labelMatchesToFilters(labels)._1
  }

  /**
   * Returns a mapping from selector labels (including an explicit/implicit __name__) to values.
   * Throws an IllegalArgumentException if any of the values are not matched by equality without a regex.
   *
   * Note: this InstantExpression does not require a metric name in order
   *   for this method to run to completion.
   */
  def toEqualLabelMap() : Map[String, String] = {
    // See Vector::mergeNameToLabels for details about why we're not using the member variable.
    val mergeNameToLabelsLocal = getUnvalidatedColumnFilters()
    mergeNameToLabelsLocal.map{ columnFilter =>
      columnFilter.filter match {
        case eqFilt: query.Filter.Equals =>
          columnFilter.column -> eqFilt.value.toString
        case _ =>
          throw new IllegalArgumentException(
            "cannot convert to label map if any values aren't matched by equality")
      }
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



