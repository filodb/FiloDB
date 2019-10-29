package filodb.query.exec

import com.typesafe.scalalogging.StrictLogging
import monix.reactive.Observable
import org.jctools.queues.SpscUnboundedArrayQueue

import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.core.store.{ChunkSetInfo, WindowedChunkIterator}
import filodb.memory.format.{vectors => bv, _}
import filodb.query._
import filodb.query.Query.qLogger
import filodb.query.exec.rangefn._
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
                                       functionId: Option[InternalRangeFunction],
                                       funcParams: Seq[FuncArgs] = Nil) extends RangeVectorTransformer {
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
            sourceSchema: ResultSchema,
            paramResponse: Observable[ScalarVector] = Observable.empty): Observable[RangeVector] = {
    // enforcement of minimum step is good since we have a high limit on number of samples
    if (step < queryConfig.minStepMs)
      throw new BadQueryException(s"step should be at least ${queryConfig.minStepMs/1000}s")
    val valColType = RangeVectorTransformer.valueColumnType(sourceSchema)
    val maxCol = if (valColType == ColumnType.HistogramColumn && sourceSchema.colIDs.length > 2)
                   sourceSchema.columns.zip(sourceSchema.colIDs).find(_._1.name == "max").map(_._2) else None
    val rangeFuncGen = RangeFunction.generatorFor(sourceSchema, functionId, valColType, queryConfig, funcParams, maxCol)

    // Generate one range function to check if it is chunked
    val sampleRangeFunc = rangeFuncGen()
    // Really, use the stale lookback window size, not 0 which doesn't make sense
    // Default value for window  should be queryConfig.staleSampleAfterMs + 1 for empty functionId,
    // so that it returns value present at time - staleSampleAfterMs
    val windowLength = window.getOrElse(if (functionId.isEmpty) queryConfig.staleSampleAfterMs + 1 else 0L)

    sampleRangeFunc match {
      case c: ChunkedRangeFunction[_] if valColType == ColumnType.HistogramColumn =>
        source.map { rv =>
          val histRow = if (maxCol.isDefined) new TransientHistMaxRow() else new TransientHistRow()
          IteratorBackedRangeVector(rv.key,
            new ChunkedWindowIteratorH(rv.asInstanceOf[RawDataRangeVector], start, step, end,
                                       windowLength, rangeFuncGen().asChunkedH, queryConfig, histRow))
        }
      case c: ChunkedRangeFunction[_] =>
        source.map { rv =>
          qLogger.trace(s"Creating ChunkedWindowIterator for rv=${rv.key}, step=$step windowLength=$windowLength")
          IteratorBackedRangeVector(rv.key,
            new ChunkedWindowIteratorD(rv.asInstanceOf[RawDataRangeVector], start, step, end,
                                       windowLength, rangeFuncGen().asChunkedD, queryConfig))
        }
      // Iterator-based: Wrap long columns to yield a double value
      case f: RangeFunction if valColType == ColumnType.LongColumn =>
        source.map { rv =>
          IteratorBackedRangeVector(rv.key,
            new SlidingWindowIterator(new LongToDoubleIterator(rv.rows), start, step, end, window.getOrElse(0L),
              rangeFuncGen().asSliding, queryConfig))
        }
      // Otherwise just feed in the double column
      case f: RangeFunction =>
        source.map { rv =>
          IteratorBackedRangeVector(rv.key,
            new SlidingWindowIterator(rv.rows, start, step, end, window.getOrElse(0L),
              rangeFuncGen().asSliding, queryConfig))
        }
    }
  }

  // Transform source double or long to double schema
  override def schema(source: ResultSchema): ResultSchema = {
    // Special treatment for downsampled gauge schema - return regular timestamp/value
    if (source.columns == Schemas.dsGauge.dataInfos) {
      source.copy(columns = Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
        ColumnInfo("value", ColumnType.DoubleColumn)))
    } else {
      source.copy(columns = source.columns.zipWithIndex.map {
        // Transform if its not a row key column
        case (ColumnInfo(name, ColumnType.LongColumn), i) if i >= source.numRowKeyColumns =>
          ColumnInfo(name, ColumnType.DoubleColumn)
        case (ColumnInfo(name, ColumnType.IntColumn), i) if i >= source.numRowKeyColumns =>
          ColumnInfo(name, ColumnType.DoubleColumn)
        case (c: ColumnInfo, _) => c
      }, fixedVectorLen = Some(((end - start) / step).toInt + 1))
    }
  }
}

/**
 * A low-overhead iterator which works on one window at a time, optimally applying columnar techniques
 * to compute each window as fast as possible on multiple rows at a time.
 */
abstract class ChunkedWindowIterator[R <: filodb.core.query.MutableRowReader](
    rv: RawDataRangeVector,
    start: Long,
    step: Long,
    end: Long,
    window: Long,
    rangeFunction: ChunkedRangeFunction[R],
    queryConfig: QueryConfig)
extends Iterator[R] with StrictLogging {
  // Lazily open the iterator and obtain the lock. This allows one thread to create the
  // iterator, but the lock is owned by the thread actually performing the iteration.
  private lazy val windowIt = {
    val it = new WindowedChunkIterator(rv, start, step, end, window)
    // Need to hold the shared lock explicitly, because the window iterator needs to
    // pre-fetch chunks to determine the window. This pre-fetching can force the internal
    // iterator to close, which would release the lock too soon.
    it.lock()
    it
  }

  def sampleToEmit: R

  override def hasNext: Boolean = windowIt.hasMoreWindows
  override def next: R = {
    rangeFunction.reset()

    // Lazy variables have an extra lookup cost, due to a volatile bitmap field generated by
    // the compiler. Copy to a local variable to reduce some overhead.
    val wit = windowIt

    wit.nextWindow()
    while (wit.hasNext) {
      val queryInfo = wit.next
      val nextInfo = ChunkSetInfo(queryInfo.infoPtr)
      try {
        rangeFunction.addChunks(queryInfo.tsVector, queryInfo.tsReader, queryInfo.valueVector, queryInfo.valueReader,
                                wit.curWindowStart, wit.curWindowEnd, nextInfo, queryConfig)
      } catch {
        case e: Exception =>
          val timestampVector = nextInfo.vectorPtr(rv.timestampColID)
          val tsReader = bv.LongBinaryVector(timestampVector)
          qLogger.error(s"addChunks Exception: info.numRows=${nextInfo.numRows} " +
                       s"info.endTime=${nextInfo.endTime} curWindowEnd=${wit.curWindowEnd} " +
                       s"tsReader=$tsReader timestampVectorLength=${tsReader.length(timestampVector)}")
          throw e
      }
    }
    rangeFunction.apply(wit.curWindowStart, wit.curWindowEnd, sampleToEmit)

    if (!wit.hasMoreWindows) {
      // Release the shared lock and close the iterator, in case it also holds a lock.
      wit.unlock()
      wit.close()
    }

    sampleToEmit
  }
}

class ChunkedWindowIteratorD(rv: RawDataRangeVector,
    start: Long, step: Long, end: Long, window: Long,
    rangeFunction: ChunkedRangeFunction[TransientRow],
    queryConfig: QueryConfig,
    // put emitter here in constructor for faster access
    var sampleToEmit: TransientRow = new TransientRow()) extends
ChunkedWindowIterator[TransientRow](rv, start, step, end, window, rangeFunction, queryConfig)

class ChunkedWindowIteratorH(rv: RawDataRangeVector,
    start: Long, step: Long, end: Long, window: Long,
    rangeFunction: ChunkedRangeFunction[TransientHistRow],
    queryConfig: QueryConfig,
    // put emitter here in constructor for faster access
    var sampleToEmit: TransientHistRow = new TransientHistRow()) extends
ChunkedWindowIterator[TransientHistRow](rv, start, step, end, window, rangeFunction, queryConfig)

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

  // NOTE: Ingestion now has a facility to drop out of order samples.  HOWEVER, there is one edge case that may happen
  // which is that the first sample ingested after recovery may not be in order w.r.t. previous persisted timestamp.
  // So this is retained for now while we consider a more permanent out of order solution.
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
  * Converts the long value column to double.
  */
class LongToDoubleIterator(iter: Iterator[RowReader]) extends Iterator[TransientRow] {
  val sampleToEmit = new TransientRow()
  override final def hasNext: Boolean = iter.hasNext
  override final def next(): TransientRow = {
    val next = iter.next()
    sampleToEmit.setLong(0, next.getLong(0))
    sampleToEmit.setDouble(1, next.getLong(1).toDouble)
    sampleToEmit
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
  private var correction = 0d
  private var prevVal = 0d
  private var prev = new TransientRow()
  private var cur = new TransientRow()
  override def hasNext: Boolean = iter.hasNext
  override def next(): TransientRow = {
    val next = iter.next()
    val nextVal = next.getDouble(1)
    if (nextVal < prevVal) {
      correction += prevVal
    }
    // swap prev an cur
    val temp = prev
    prev = cur
    cur = temp
    // place value in cur and return
    cur.setLong(0, next.getLong(0))
    cur.setDouble(1, nextVal + correction)
    prevVal = nextVal
    cur
  }
}

class DropOutOfOrderSamplesIterator(iter: Iterator[RowReader]) extends Iterator[TransientRow] {
  // Initial -1 time since it will be less than any valid timestamp and will allow first sample to go through
  private val cur = new TransientRow(-1, -1)
  private val nextVal = new TransientRow(-1, -1)
  private var hasNextVal = false
  private var hasNextDefined = false

  override def hasNext: Boolean = {
    if (!hasNextDefined) {
      setNext()
      hasNextDefined = true
    }
    hasNextVal
  }

  override def next(): TransientRow = {
    if (!hasNextDefined) {
      setNext()
      hasNextDefined = true
    }
    cur.copyFrom(nextVal)
    setNext()
    cur
  }

  private def setNext(): Unit = {
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
