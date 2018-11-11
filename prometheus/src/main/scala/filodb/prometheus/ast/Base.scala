package filodb.prometheus.ast

import filodb.query._

sealed trait TimeParams {
  def start: Long   // in seconds sine Epoch
  def end: Long
  def step: Long
}

/**
 * NOTE: start and end are in SECONDS since Epoch
 */
final case class QueryParams(start: Long, step: Long, end: Long) extends TimeParams
final case class InMemoryParam(step: Long) extends TimeParams {
  val start = 0L
  val end = System.currentTimeMillis / 1000
}
final case class WriteBuffersParam(step: Long) extends TimeParams {
  val start = 0L
  val end = System.currentTimeMillis / 1000
}

trait Base {
  trait Expression

  trait Series

  trait PeriodicSeries extends Series {
    def toPeriodicSeriesPlan(timeParams: TimeParams): PeriodicSeriesPlan
  }

  trait SimpleSeries extends Series {
    def toRawSeriesPlan(timeParams: TimeParams, isRoot: Boolean): RawSeriesPlan
  }

  /**
   * Converts a TimeParams into a RangeSelector at timeParam.start - startOffset
   * timeParam.start is in seconds, startOffset is in millis
   */
  def timeParamToSelector(timeParam: TimeParams, startOffset: Long): RangeSelector = timeParam match {
    case QueryParams(start, step, end) => IntervalSelector(Seq(start * 1000 - startOffset), Seq(end * 1000))
    case InMemoryParam(_)              => InMemoryChunksSelector
    case WriteBuffersParam(_)          => WriteBufferSelector
  }

  /**
    * An identifier is an unquoted string
    */
  case class Identifier(str: String) extends Expression


}
