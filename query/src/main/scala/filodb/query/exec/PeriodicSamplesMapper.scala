package filodb.query.exec

import scala.collection.mutable

import monix.reactive.Observable

import filodb.core.query.{IteratorBackedRangeVector, RangeVector, ResultSchema}
import filodb.memory.format.RowReader
import filodb.query.{QueryConfig, RangeFunctionId}
import filodb.query.exec.rangefn.{RangeFunction, Window}
import filodb.query.util.IndexedArrayQueue

/**
  * Transforms raw reported samples to samples with
  * regular interval from start to end, with step as
  * interval. At the same time, it can optionally apply a range vector function
  * to time windows of indicated length.
  */
final case class PeriodicSamplesMapper(start: Long,
                                       step: Long,
                                       end: Long,
                                       window: Option[Long],
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
            limit: Int,
            sourceSchema: ResultSchema): Observable[RangeVector] = {
    RangeVectorTransformer.requireTimeSeries(sourceSchema)
    source.map { rv =>
      IteratorBackedRangeVector(rv.key,
        new SlidingWindowIterator(rv.rows, start, step, end, window.getOrElse(0L),
          RangeFunction(functionId, funcParams), queryConfig))
    }
  }
}

class QueueBasedWindow(q: IndexedArrayQueue[TransientRow]) extends Window {
  def size: Int = q.size
  def apply(i: Int): TransientRow = q(i)
  def head: TransientRow = q.head
  def last: TransientRow = q.last
}

/**
  * Decorates a raw series iterator to apply a range vector function
  * on periodic time windows
  */
class SlidingWindowIterator(raw: Iterator[RowReader],
                            start: Long,
                            step: Long,
                            end: Long,
                            window: Long,
                            rangeFunction: RangeFunction,
                            queryConfig: QueryConfig) extends Iterator[TransientRow] {
  private var sampleToEmit = new TransientRow()
  private var curWindowEnd = start

  // sliding window queue
  private val windowQueue = new IndexedArrayQueue[TransientRow]()

  // this is the object that will be exposed to the RangeFunction
  private val windowSamples = new QueueBasedWindow(windowQueue)

  // we need buffered iterator so we can use to peek at next element.
  // At same time, do counter correction if necessary
  private val rows = if (rangeFunction.needsCounterCorrection) {
    new BufferableCounterCorrectionIterator(raw).buffered
  } else {
    new BufferableIterator(raw).buffered
  }

  // to avoid creation of object per sample, we use a pool
  val windowSamplesPool = new TransientRowPool()

  override def hasNext: Boolean = curWindowEnd <= end
  override def next(): TransientRow = {
    val curWindowStart = curWindowEnd - window
    // add elements to window until end of current window has reached
    while (rows.hasNext && rows.head.timestamp <= curWindowEnd) {
      val next = rows.next()
      // skip elements that are outside of the current window, except for last sample.
      if (next.timestamp >= curWindowStart ||    // inside current window
         (rangeFunction.needsLastSample && rows.hasNext && rows.head.timestamp > curWindowStart) ||
         (rangeFunction.needsLastSample && !rows.hasNext)) { // no more rows
        val toAdd = windowSamplesPool.get
        toAdd.copyFrom(next)
        windowQueue.add(toAdd)
        rangeFunction.addToWindow(toAdd)
      }
    }
    // remove elements outside current window that were part of previous window
    // but ensure at least one sample present
    while (windowQueue.size > 1 && windowQueue.head.timestamp < curWindowStart) {
      val removed = windowQueue.remove()
      rangeFunction.removeFromWindow(removed)
      windowSamplesPool.putBack(removed)
    }
    // apply function on window samples
    rangeFunction.apply(curWindowStart, curWindowEnd, windowSamples, sampleToEmit, queryConfig)
    curWindowEnd = curWindowEnd + step
    sampleToEmit
  }
}

/**
  * Exists so that we can reuse TransientRow objects and reduce object creation per
  * raw sample. Beware: This is mutable, and not thread-safe.
  */
class TransientRowPool {
  val pool = mutable.Queue[TransientRow]()
  def get: TransientRow = if (pool.isEmpty) new TransientRow() else pool.dequeue()
  def putBack(r: TransientRow): Unit = pool.enqueue(r)
}

/**
  * Iterator of mutable objects that allows look-ahead for ONE value.
  * Caller can do iterator.buffered safely
  */
class BufferableIterator(iter: Iterator[RowReader]) extends Iterator[TransientRow] {
  var prev = new TransientRow()
  var cur = new TransientRow()
  override def hasNext: Boolean = iter.hasNext
  override def next(): TransientRow = {
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
class BufferableCounterCorrectionIterator(iter: Iterator[RowReader]) extends Iterator[TransientRow] {
  var lastVal: Double = 0
  var prev = new TransientRow()
  var cur = new TransientRow()
  override def hasNext: Boolean = iter.hasNext
  override def next(): TransientRow = {
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
    cur.setLong(0, next.getLong(0))
    cur.setDouble(1, lastVal)
    cur
  }
}
