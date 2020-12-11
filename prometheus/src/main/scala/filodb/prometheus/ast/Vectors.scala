package filodb.prometheus.ast

import scala.util.Try

import filodb.core.{query, GlobalConfig}
import filodb.core.query.{ColumnFilter, RangeParams}
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
    val regexColumnName: String = "::(?=[^::]+$)" //regex pattern to extract ::columnName at the end

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
  case class InstantExpression(metricName: Option[String],
                               labelSelection: Seq[LabelMatch],
                               subquery: Option[Subquery],
                               offset: Option[Duration]) extends Vector with PeriodicSeries {

    import WindowConstants._

    private[prometheus] val (columnFilters, column, bucketOpt) = labelMatchesToFilters(mergeNameToLabels)

    def toSeriesPlan(timeParams: TimeRangeParams): PeriodicSeriesPlan = {
      // we start from 5 minutes earlier that provided start time in order to include last sample for the
      // start timestamp. Prometheus goes back up to 5 minutes to get sample before declaring as stale

      // If we have subquery defined and timeParams (1) step defined or (2) end
      // different from the start. This is certainly a situation when we should throw an exception.
      // For top level InstantExpression this means if we have a range_query API called with
      // a subquery in top level, this is obviously invalid as top level of a range query should be returning an
      // instant vector but subqueries by definition return range vectors.
      // In the existing codebase, however, it could be that timeParams are globabl top level params and they indeed
      // could differ from the subquery parameter, suppose we have
      //    sum_over_time(metric{}[10:1m])[1d:1h]
      // the meaning of the above is we produce a range vector with 25 values which would be sum_over_time of 10 samples
      // of last 10 minutes of last 25 hours.This means that metric{}[10:1m] InstantExpression needs to be executed with
      // 25 different start timestamps and no step and end defined.
      // I have not finished reviewing the code to understand whether we indeed have capability of passing
      // proper timeParams from the top level to the lower level. If it is not done
      // now, we MUST implement it, ie timeParam are not immutable params coming all the way from the HTTP API call
      // parameters. The top level expression should be filled out with HTTP API params, however, if the tope level
      // expression happens to be a subquery it should pass appropriate timeparams (not the top level original time
      // params).
      // For now, I only test this code for subquery in top level InstantExpression. The work to verify that it works
      // in the lower levels will come later. TODO
      // b) we should throw an exception if timeParams that we pass in have anything but start timestamp defined.
      var timeParamsToUse = timeParams;
      if (subquery.isDefined) {
        if (timeParams.step != 0 || timeParams.start != timeParams.end) {
          throw new UnsupportedOperationException("Subquery is not allowed as a top level expression for query_range")
        }
        //How do I know what's the default step? TODO
        //for now, let's put step 10 seconds
        var stepToUse = 10L
        if (subquery.get.step.isDefined) {
          stepToUse = subquery.get.step.get.millis(1L) / 1000
        }
        timeParamsToUse = TimeStepParams(
          timeParams.start,
          stepToUse,
          timeParams.start + (subquery.get.interval.millis(1L) / 1000) //don't understand why I need to pass 1
        );
      }
      val ps = PeriodicSeries(
        RawSeries(
          timeParamToSelector(timeParamsToUse),
          columnFilters,
          column.toSeq,
          Some(staleDataLookbackMillis),
          offset.map(_.millis(timeParamsToUse.step * 1000))
        ),
        timeParamsToUse.start * 1000, timeParamsToUse.step * 1000, timeParamsToUse.end * 1000,
        offset.map(_.millis(timeParamsToUse.step * 1000))
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

    def toRawSeriesPlan(timeParams: TimeRangeParams, offsetMs: Option[Long] = None): RawSeries = {
      RawSeries(timeParamToSelector(timeParams), columnFilters, column.toSeq, Some(staleDataLookbackMillis),
        offsetMs)
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
                             timeInterval: TimeInterval,
                             offset: Option[Duration]) extends Vector with SimpleSeries {

    private[prometheus] val (columnFilters, column, bucketOpt) = labelMatchesToFilters(mergeNameToLabels)

    def toSeriesPlan(timeParams: TimeRangeParams, isRoot: Boolean): RawSeriesLikePlan = {
      if (isRoot && timeParams.start != timeParams.end) {
        throw new UnsupportedOperationException("Range expression is not allowed in query_range")
      }
        // multiply by 1000 to convert unix timestamp in seconds to millis
        val rs = RawSeries(
          timeParamToSelector(timeParams),
          columnFilters,
          column.toSeq,
          Some(timeInterval.duration.millis(timeParams.step * 1000)),
          offset.map(_.millis(timeParams.step * 1000))
        )
        bucketOpt.map { bOpt =>
          // It's a fixed value, the range params don't matter at all
          val param = ScalarFixedDoublePlan(bOpt, RangeParams(0, Long.MaxValue, 60000L))
          ApplyInstantFunctionRaw(rs, InstantFunctionId.HistogramBucket, Seq(param))
        }.getOrElse(rs)

    }

  }

}
