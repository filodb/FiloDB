package filodb.prometheus.ast

import filodb.query.{MetadataQueryPlan, PeriodicSeriesPlan, RawSeriesPlan, _}

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
    def toPeriodicSeriesPlan(timeParams: TimeRangeParams): PeriodicSeriesPlan
  }

  trait SimpleSeries extends Series {
    def toRawSeriesPlan(timeParams: TimeRangeParams, isRoot: Boolean): RawSeriesPlan
  }

  trait Metadata extends Expression {
    def toMetadataQueryPlan(timeParam: TimeRangeParams): MetadataQueryPlan
  }

  /**
   * Converts a TimeRangeParams into a RangeSelector at timeParam.start - startOffset
   * timeParam.start is in seconds, startOffset is in millis
   */
  def timeParamToSelector(timeParam: TimeRangeParams, startOffset: Long): RangeSelector = timeParam match {
    case TimeStepParams(start, step, end) => IntervalSelector(Seq(start * 1000 - startOffset), Seq(end * 1000))
    case InMemoryParam(_)                 => InMemoryChunksSelector
    case WriteBuffersParam(_)             => WriteBufferSelector
  }

  /**
    * An identifier is an unquoted string
    */
  case class Identifier(str: String) extends Expression

}
