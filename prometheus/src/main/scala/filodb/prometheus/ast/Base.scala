package filodb.prometheus.ast

import filodb.query.{MetadataQueryPlan, PeriodicSeriesPlan, _}

sealed trait TimeRangeParams {
  def start: Long   // in seconds sine Epoch
  def end: Long
  def step: Long
}


/**
 * NOTE: start and end are in SECONDS since Epoch
 */
final case class TimeStepParams(start: Long, step: Long, end: Long) extends TimeRangeParams
final case class InMemoryParam(step: Long) extends TimeRangeParams {
  val start = 0L
  val end = System.currentTimeMillis / 1000
}
final case class WriteBuffersParam(step: Long) extends TimeRangeParams {
  val start = 0L
  val end = System.currentTimeMillis / 1000
}

trait Base {
  trait Expression

  trait Series

  trait PeriodicSeries extends Series {
    def toSeriesPlan(timeParams: TimeRangeParams): PeriodicSeriesPlan
  }

  trait SimpleSeries extends Series {
    def toSeriesPlan(timeParams: TimeRangeParams, isRoot: Boolean): RawSeriesLikePlan
  }

  trait Metadata extends Expression {
    def toMetadataQueryPlan(timeParam: TimeRangeParams): MetadataQueryPlan
  }

  /**
   * Converts a TimeRangeParams into a RangeSelector at timeParam.start - startOffset
   * timeParam.start is in seconds, startOffset is in millis
   * @param startOffset lookback time
   * @param offset offset function time
   */
  def timeParamToSelector(timeParam: TimeRangeParams, startOffset: Long, offset: Long = 0): RangeSelector =
    timeParam match {
      case TimeStepParams(start, step, end) => IntervalSelector(start * 1000 - startOffset - offset,
                                               end * 1000 - offset)
      case InMemoryParam(_)                 => InMemoryChunksSelector
      case WriteBuffersParam(_)             => WriteBufferSelector
    }

  /**
    * An identifier is an unquoted string
    */
  case class Identifier(str: String) extends Expression

}
