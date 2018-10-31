package filodb.query.exec

import monix.reactive.Observable
import org.jctools.queues.SpscUnboundedArrayQueue

import filodb.core.query.{IteratorBackedRangeVector, RangeVector, ResultSchema}
import filodb.memory.format.RowReader
import filodb.query.{Query, QueryConfig, RangeFunctionId}
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

  if (functionId.nonEmpty) require(window.nonEmpty && window.get > 0,
                                  "Need positive window lengths to apply range function")
  else require(window.isEmpty, "Should not specify window length when not applying windowing function")

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
  override def toString: String = q.toString
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
  private val sampleToEmit = new TransientRow()
  private var curWindowEnd = start

  // sliding window queue
  private val windowQueue = new IndexedArrayQueue[TransientRow]()

  // this is the object that will be exposed to the RangeFunction
  private val windowSamples = new QueueBasedWindow(windowQueue)

  // TODO This can be removed once we fix order during ingestion. Or is it required to validate anyway?
  private val rawInOrder = new DropOutOfOrderSamplesIterator(raw)

  // we need buffered iterator so we can use to peek at next element.
  // At same time, do counter correction if necessary
  private val rows = if (rangeFunction.needsCounterCorrection) {
    new BufferableCounterCorrectionIterator(rawInOrder).buffered
  } else {
    new BufferableIterator(rawInOrder).buffered
  }

  // to avoid creation of object per sample, we use a pool
  val windowSamplesPool = new TransientRowPool()

  override def hasNext: Boolean = curWindowEnd <= end
  override def next(): TransientRow = {
    val curWindowStart = curWindowEnd - window
    // current window is: (curWindowStart, curWindowEnd]. Excludes start, includes end.

    // Add elements to window until end of current window has reached
    while (rows.hasNext && rows.head.timestamp <= curWindowEnd) {
      val cur = rows.next()
      if (shouldAddCurToWindow(curWindowStart, cur)) {
        val toAdd = windowSamplesPool.get
        toAdd.copyFrom(cur)
        windowQueue.add(toAdd)
        rangeFunction.addedToWindow(toAdd, windowSamples)
      }
    }

    // remove elements from head of window that were part of previous window
    while (shouldRemoveWindowHead(curWindowStart)) {
      val removed = windowQueue.remove()
      rangeFunction.removedFromWindow(removed, windowSamples)
      windowSamplesPool.putBack(removed)
    }
    // apply function on window samples
    rangeFunction.apply(curWindowStart, curWindowEnd, windowSamples, sampleToEmit, queryConfig)
    curWindowEnd = curWindowEnd + step
    sampleToEmit
  }

  /**
    * Decides if a sample should be added to current window.
    * We skip elements that are outside of the current window, except for last sample.
    *
    * @param cur sample to check for eligibility
    * @param curWindowStart start time of the current window
    */
  private def shouldAddCurToWindow(curWindowStart: Long, cur: TransientRow): Boolean = {
    // One of the following three conditions need to hold true:

    // 1. cur is inside current window
    (cur.timestamp > curWindowStart) ||
      // 2. needLastSample and cur is the last sample because next sample is inside window
      (rangeFunction.needsLastSample && rows.hasNext && rows.head.timestamp > curWindowStart) ||
      // 3. needLastSample and no more rows after cur
      (rangeFunction.needsLastSample && !rows.hasNext)
  }

  /**
    * Decides if first item in sliding window should be removed.
    *
    * Remove elements outside current window that were part of
    * previous window but ensure at least one sample present if needsLastSample is true.
    *
    * @param curWindowStart start time of the current window
    */
  private def shouldRemoveWindowHead(curWindowStart: Long): Boolean = {

    (!windowQueue.isEmpty) && (
      // One of the following two conditions need to hold true:

      // 1. if no need for last sample, and head is outside the window
      (!rangeFunction.needsLastSample && windowQueue.head.timestamp <= curWindowStart) ||
        // 2. if needs last sample, then ok to remove window's head only if there is more than one item in window
        (rangeFunction.needsLastSample && windowQueue.size > 1 && windowQueue.head.timestamp <= curWindowStart)
    )
  }
}

/**
  * Exists so that we can reuse TransientRow objects and reduce object creation per
  * raw sample. Beware: This is mutable, and not thread-safe.
  */
class TransientRowPool {
  val pool = new SpscUnboundedArrayQueue[TransientRow](16)
  @inline final def get: TransientRow = if (pool.isEmpty) new TransientRow() else pool.remove()
  @inline final def putBack(r: TransientRow): Unit = pool.add(r)
}

/**
  * Iterator of mutable objects that allows look-ahead for ONE value.
  * Caller can do iterator.buffered safely
  */
class BufferableIterator(iter: Iterator[RowReader]) extends Iterator[TransientRow] {
  private var prev = new TransientRow()
  private var cur = new TransientRow()
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
  * Is bufferable - caller can do iterator.buffered safely since it buffers ONE value
  */
class BufferableCounterCorrectionIterator(iter: Iterator[RowReader]) extends Iterator[TransientRow] {
  private var lastVal: Double = 0
  private var prev = new TransientRow()
  private var cur = new TransientRow()
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

class DropOutOfOrderSamplesIterator(iter: Iterator[RowReader]) extends Iterator[TransientRow] {
  // Initial -1 time since it will be less than any valid timestamp and will allow first sample to go through
  private val cur = new TransientRow(-1, -1)
  private val nextVal = new TransientRow(-1, -1)
  private var hasNextVal = false
  setNext()

  override def hasNext: Boolean = hasNextVal
  override def next(): TransientRow = {
    cur.copyFrom(nextVal)
    setNext()
    cur
  }

  def setNext(): Unit = {
    hasNextVal = false
    while (!hasNextVal && iter.hasNext) {
      val nxt = iter.next()
      val t = nxt.getLong(0)
      val v = nxt.getDouble(1)
      if (t > cur.timestamp) { // if next sample is later than current sample
        nextVal.setValues(t, v)
        hasNextVal = true
      } else {
        Query.droppedSamples.increment()
      }
    }
  }
}
