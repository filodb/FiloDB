package filodb.query.exec

import scala.collection.mutable

import monix.reactive.Observable

import filodb.core.query.{IteratorBackedRangeVector, RangeVector, ResultSchema}
import filodb.memory.format.RowReader
import filodb.query.RangeFunctionId

/**
  * Transforms raw reported samples to samples with
  * regular interval from start to end, with step as
  * interval. At the same time, it can optionally apply a range vector function
  * to time windows of indicated length.
  */
final case class PeriodicSamplesMapper(start: Long,
                                       step: Int,
                                       end: Long,
                                       window: Option[Long],
                                       functionId: Option[RangeFunctionId],
                                       funcParams: Seq[Any] = Nil) extends RangeVectorTransformer {

  protected[exec] def args: String =
    s"start=$start, step=$step, end=$end, window=$window, functionId=$functionId, funcParams=$funcParams"

  def apply(source: Observable[RangeVector], sourceSchema: ResultSchema): Observable[RangeVector] = {
    RangeVectorTransformer.requireTimeSeries(sourceSchema)
    source.map { rv =>
      IteratorBackedRangeVector(rv.key,
        new SlidingWindowIterator(rv.rows, start, step, end, window.getOrElse(0),
          RangeFunction(functionId, funcParams)))
    }
  }
}

/**
  * TODO If needed, replace this with a more "correct" or optimal implementation
  */
class MetricRowReader(t: Long, v: Double) extends RowReader {
  def timestamp: Long = getLong(0)
  def value: Double = getDouble(1)
  def getDouble(columnNo: Int): Double =
    if (columnNo == 1) v else throw new IllegalArgumentException()
  override def getLong(columnNo: Int): Long =
    if (columnNo == 0) t else throw new IllegalArgumentException()
  override def notNull(columnNo: Int): Boolean = throw new IllegalArgumentException()
  override def getFloat(columnNo: Int): Float = throw new IllegalArgumentException()
  override def getInt(columnNo: Int): Int = throw new IllegalArgumentException()
  override def getBoolean(columnNo: Int): Boolean = throw new IllegalArgumentException()
  override def getAny(columnNo: Int): Any = throw new IllegalArgumentException()
  override def getString(columnNo: Int): String = throw new IllegalArgumentException()
}


/**
  * Decorates a raw series iterator to apply a range vector function
  * on periodic time windows
  */
class SlidingWindowIterator(raw: Iterator[RowReader],
                            start: Long,
                            step: Int,
                            end: Long,
                            window: Long,
                            rangeFunction: RangeFunction) extends Iterator[MetricRowReader] {
  var curPeriod = start
  val windowSamples = mutable.Queue[RowReader]()
  val rows = if (rangeFunction.needsCounterNormalization) {
    new NormalizingCounterIterator(raw).buffered
  } else {
    raw.buffered
  }
  // now we have a buffered iterator that we can use to peek at next element

  override def hasNext: Boolean = curPeriod <= end
  override def next(): MetricRowReader = {
    // add elements to window
    while (rows.hasNext && rows.head.getLong(0) <= curPeriod) {
      val toAdd = rows.next()
      windowSamples.enqueue(toAdd)
      rangeFunction.addToWindow(toAdd)
    }
    // remove elements from window
    val curWindowStart = curPeriod - window
    while (windowSamples.size > 1 && windowSamples(1).getLong(0) < curWindowStart) {
      val removed = windowSamples.dequeue()
      rangeFunction.removeFromWindow(removed)
    }
    // apply function on window
    val sample = rangeFunction.apply(curPeriod, windowSamples)
    curPeriod = curPeriod + step
    sample
  }
}

/**
  * Used to create a monotonically increasing counter from raw reported counter values.
  */
class NormalizingCounterIterator(iter: Iterator[RowReader]) extends Iterator[MetricRowReader] {
  var last: Double = 0

  override def hasNext: Boolean = iter.hasNext

  override def next(): MetricRowReader = {
    val curRow = iter.next()
    val cur = curRow.getDouble(1)
    if (cur < last) {
      last = cur + last
    } else {
      last = cur
    }
    new MetricRowReader(curRow.getLong(0), curRow.getDouble(1))
  }
}
