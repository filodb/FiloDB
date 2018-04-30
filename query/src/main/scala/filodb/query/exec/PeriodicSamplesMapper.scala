package filodb.query.exec

import scala.collection.mutable

import monix.reactive.Observable

import filodb.core.query.{IteratorBackedRangeVector, RangeVector, ResultSchema}
import filodb.memory.format.RowReader
import filodb.query.{QueryConfig, RangeFunctionId}

/**
  * Transforms raw reported samples to samples with
  * regular interval from start to end, with step as
  * interval. At the same time, it can optionally apply a range vector function
  * to time windows of indicated length.
  */
final case class PeriodicSamplesMapper(start: Long,
                                       step: Long,
                                       end: Long,
                                       window: Option[Int],
                                       functionId: Option[RangeFunctionId],
                                       funcParams: Seq[Any] = Nil) extends RangeVectorTransformer {
  require(start <= end, "start should be <= end")
  require(step > 0, "step should be > 0")
  val numSamples = (end-start)/step
  if (functionId.nonEmpty) require(window.nonEmpty && window.get > 0,
                                  "Need positive window lengths to apply range function")

  protected[exec] def args: String =
    s"start=$start, step=$step, end=$end, window=$window, functionId=$functionId, funcParams=$funcParams"

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            sourceSchema: ResultSchema): Observable[RangeVector] = {
    require(step > 0 &&  numSamples < queryConfig.maxSamplesPerRangeVector,
      s"Increase step or reduce range - not more than $queryConfig.maxSamplesPerRangeVector samples allowed in result")
    RangeVectorTransformer.requireTimeSeries(sourceSchema)
    source.map { rv =>
      IteratorBackedRangeVector(rv.key,
        new SlidingWindowIterator(rv.rows, start, step, end, window.getOrElse(0),
          RangeFunction(functionId, funcParams), queryConfig))
    }
  }
}

/**
  * Represents intermediate sample which will be part of a transformed RangeVector.
  * It is mutable. Consumers from iterators should be aware of the semantics
  * of ability to save the next() value.
  */
class MutableSample extends RowReader {
  var timestamp: Long = 0
  var value: Double = 0
  def set(t: Long, v: Double): Unit = { timestamp = t; value = v; }
  def copyFrom(r: RowReader): MutableSample = { timestamp = r.getLong(0); value = r.getDouble(1); this }
  def getDouble(columnNo: Int): Double = if (columnNo == 1) value else throw new IllegalArgumentException()
  def getLong(columnNo: Int): Long = if (columnNo == 0) timestamp else throw new IllegalArgumentException()
  def notNull(columnNo: Int): Boolean = columnNo == 0 || columnNo == 1
  def getFloat(columnNo: Int): Float = throw new IllegalArgumentException()
  def getInt(columnNo: Int): Int = throw new IllegalArgumentException()
  def getBoolean(columnNo: Int): Boolean = throw new IllegalArgumentException()
  def getAny(columnNo: Int): Any = throw new IllegalArgumentException()
  def getString(columnNo: Int): String = throw new IllegalArgumentException()
  override def toString: String = f"$timestamp->$value%.2f"
}

/**
  * Decorates a raw series iterator to apply a range vector function
  * on periodic time windows
  */
class SlidingWindowIterator(raw: Iterator[RowReader],
                            start: Long,
                            step: Long,
                            end: Long,
                            window: Int,
                            rangeFunction: RangeFunction,
                            queryConfig: QueryConfig) extends Iterator[MutableSample] {
  var sampleToEmit = new MutableSample()
  var curWindowEnd = start
  val windowSamples = mutable.Queue[MutableSample]()
  val rows = if (rangeFunction.needsCounterCorrection) {
    new BufferableCounterCorrectionIterator(raw).buffered
  } else {
    new BufferableIterator(raw).buffered
  }
  // now we have a buffered iterator that we can use to peek at next element

  val windowSamplesPool = new MutableSamplePool()

  override def hasNext: Boolean = curWindowEnd <= end
  override def next(): MutableSample = {
    val curWindowStart = curWindowEnd - window
    // add elements to window until end of current window has reached
    while (rows.hasNext && rows.head.timestamp <= curWindowEnd) {
      val next = rows.next()
      // skip elements that are outside of the current window, except for last sample.
      if (next.timestamp >= curWindowStart ||    // inside current window
         (rows.hasNext && rows.head.timestamp > curWindowStart) ||   // last sample outside current window
         !rows.hasNext) {       // no more rows
        val toAdd = windowSamplesPool.get.copyFrom(next)
        windowSamples.enqueue(toAdd)
        rangeFunction.addToWindow(toAdd)
      }
    }
    // remove elements outside current window that were part of previous window
    // but ensure at least one sample present
    while (windowSamples.size > 1 && windowSamples.head.timestamp < curWindowStart) {
      val removed = windowSamples.dequeue()
      rangeFunction.removeFromWindow(removed)
      windowSamplesPool.putBack(removed)
    }
    // apply function on window samples
    rangeFunction.apply(curWindowEnd, window, windowSamples, sampleToEmit, queryConfig)
    curWindowEnd = curWindowEnd + step
    sampleToEmit
  }
}

/**
  * Exists so that we can reuse Sample objects and reduce object creation per
  * raw sample. Beware: This is mutable, and not thread-safe.
  */
class MutableSamplePool {
  val pool = mutable.Queue[MutableSample]()
  def get: MutableSample = if (pool.isEmpty) new MutableSample() else pool.dequeue()
  def putBack(r: MutableSample): Unit = pool.enqueue(r)
}

/**
  * Iterator of mutable objects that allows look-ahead for ONE value.
  * Caller can do iterator.buffered safely
  */
class BufferableIterator(iter: Iterator[RowReader]) extends Iterator[MutableSample] {
  var prev = new MutableSample()
  var cur = new MutableSample()
  override def hasNext: Boolean = iter.hasNext
  override def next(): MutableSample = {
    // swap prev an cur
    val temp = prev
    prev = cur
    cur = temp
    // place value in cur and return
    cur.copyFrom(iter.next())
    cur
  }
}

/**
  * Used to create a monotonically increasing counter from raw reported counter values.
  * Is bufferable - caller can do iterator.buffered safely since it buffer ONE value
  */
class BufferableCounterCorrectionIterator(iter: Iterator[RowReader]) extends Iterator[MutableSample] {
  var lastVal: Double = 0
  var prev = new MutableSample()
  var cur = new MutableSample()
  override def hasNext: Boolean = iter.hasNext
  override def next(): MutableSample = {
    val next = iter.next()
    val nextVal = next.getDouble(1)
    if (nextVal < lastVal) {
      lastVal = nextVal + lastVal
    } else {
      lastVal = nextVal
    }
    // swap prev an cur
    val temp = prev
    prev = cur
    cur = temp
    // place value in cur and return
    cur.set(next.getLong(0), lastVal)
    cur
  }
}
