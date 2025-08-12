package filodb.query.exec

import com.typesafe.scalalogging.StrictLogging
import monix.reactive.Observable
import org.jctools.queues.SpscUnboundedArrayQueue

import filodb.core.GlobalConfig.systemConfig
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.core.store.WindowedChunkIterator
import filodb.memory.format._
import filodb.memory.format.vectors.{CustomBuckets, HistogramBuckets, LongBinaryVector, MutableHistogram}
import filodb.query._
import filodb.query.Query.qLogger
import filodb.query.exec.InternalRangeFunction.AvgWithSumAndCountOverTime
import filodb.query.exec.rangefn._
import filodb.query.util.IndexedArrayQueue

/**
  * Transforms raw reported samples to samples with
  * regular interval from start to end, with step as
  * interval. At the same time, it can optionally apply a range vector function
  * to time windows of indicated length.
  *
  * @param stepMultipleNotationUsed if counter based range function is used, and this flag is
  *                                 enabled, then publish interval is padded to lookback window length
  */
final case class PeriodicSamplesMapper(startMs: Long,
                                       stepMs: Long,
                                       endMs: Long,
                                       window: Option[Long],
                                       functionId: Option[InternalRangeFunction],
                                       stepMultipleNotationUsed: Boolean = false,
                                       funcParams: Seq[FuncArgs] = Nil,
                                       offsetMs: Option[Long] = None,
                                       rawSource: Boolean = true,
                                       leftInclusiveWindow: Boolean = false
) extends RangeVectorTransformer {
  val windowToUse =
    window.map(windowLengthMs => if (leftInclusiveWindow) (windowLengthMs + 1) else windowLengthMs)
  require(startMs <= endMs, s"start $startMs should be <= end $endMs")
  require(startMs == endMs || stepMs > 0, s"step $stepMs should be > 0 for range query")
  val adjustedStep = if (stepMs > 0) stepMs else stepMs + 1 // needed for iterators to terminate when start == end

  val startWithOffset = startMs - offsetMs.getOrElse(0L)
  val endWithOffset = endMs - offsetMs.getOrElse(0L)
  val outputRvRange = Some(RvRange(startMs, stepMs, endMs))

  val isLastFn = functionId.isEmpty || functionId.contains(InternalRangeFunction.LastSampleHistMaxMin) ||
    functionId.contains(InternalRangeFunction.Timestamp)

  if (!isLastFn) require(windowToUse.nonEmpty && windowToUse.get > 0,
                                  "Need positive window lengths to apply range function")
  else require(windowToUse.isEmpty, "Should not specify window length when not applying windowing function")

  protected[exec] def args: String = s"start=$startMs, step=$stepMs, end=$endMs," +
    s" window=$window, functionId=$functionId, rawSource=$rawSource, offsetMs=$offsetMs"

 //scalastyle:off
  def apply(source: Observable[RangeVector],
            querySession: QuerySession,
            limit: Int,
            sourceSchema: ResultSchema,
            paramResponse: Seq[Observable[ScalarRangeVector]]): Observable[RangeVector] = {
    // enforcement of minimum step is good since we have a high limit on number of samples
    if (startMs < endMs && stepMs < querySession.queryConfig.minStepMs) // range query with small step
      throw new BadQueryException(s"step should be at least ${querySession.queryConfig.minStepMs/1000}s")
    val valColType = ResultSchema.valueColumnType(sourceSchema)
    // If a max and min column is present, the ExecPlan's job is to put it into column 3
    val hasMaxMinCol = valColType == ColumnType.HistogramColumn && sourceSchema.colIDs.length > 3 &&
                      sourceSchema.columns(2).name == "max" && sourceSchema.columns(3).name == "min"
    val rangeFuncGen = RangeFunction.generatorFor(sourceSchema, functionId, valColType, querySession.queryConfig,
                                                  funcParams, rawSource)

    // Generate one range function to check if it is chunked
    val sampleRangeFunc = rangeFuncGen()
    // Really, use the stale lookback window size, not 0 which doesn't make sense
    // Default value for window  should be queryConfig.staleSampleAfterMs + 1 for empty functionId,
    // so that it returns value present at time - staleSampleAfterMs
    val windowLength = windowToUse.getOrElse(if (isLastFn) querySession.queryConfig.staleSampleAfterMs + 1 else 0L)

    val rvs = sampleRangeFunc match {
      case _: ChunkedRangeFunction[_] if valColType == ColumnType.HistogramColumn =>
        source.map { rv =>
          val histRow = if (hasMaxMinCol) new TransientHistMaxMinRow() else new TransientHistRow()
          val rdrv = rv.asInstanceOf[RawDataRangeVector]
          val chunkedHRangeFunc = rangeFuncGen().asChunkedH
          val minResolutionMs = rdrv.minResolutionMs
          val extendedWindow = extendLookback(rdrv, windowLength)
          if (chunkedHRangeFunc.isInstanceOf[CounterChunkedRangeFunction[_]] && extendedWindow < minResolutionMs * 2)
            throw new IllegalArgumentException(s"Minimum resolution of data for this time range is " +
              s"${minResolutionMs}ms. However, a lookback of ${windowLength}ms was chosen. This will not " +
              s"yield intended results for rate/increase functions since each lookback window can contain " +
              s"lesser than 2 samples. Increase lookback to more than ${minResolutionMs * 2}ms")
          IteratorBackedRangeVector(rv.key,
            new ChunkedWindowIteratorH(rdrv, startWithOffset, adjustedStep, endWithOffset,
                    extendedWindow, chunkedHRangeFunc, querySession, histRow), outputRvRange)
        }
      case _: ChunkedRangeFunction[_] =>
        source.map { rv =>
          qLogger.trace(s"Creating ChunkedWindowIterator for rv=${rv.key}, adjustedStep=$adjustedStep " +
            s"windowLength=$windowLength")
          val rdrv = rv.asInstanceOf[RawDataRangeVector]
          val minResolutionMs = rdrv.minResolutionMs
          val chunkedDRangeFunc = rangeFuncGen().asChunkedD
          val extendedWindow = extendLookback(rdrv, windowLength)
          if (chunkedDRangeFunc.isInstanceOf[CounterChunkedRangeFunction[_]] &&
              extendedWindow < rdrv.minResolutionMs * 2)
            throw new IllegalArgumentException(s"Minimum resolution of data for this time range is " +
              s"${minResolutionMs}ms. However, a lookback of ${windowLength}ms was chosen. This will not " +
              s"yield intended results for rate/increase functions since each lookback window can contain " +
              s"lesser than 2 samples. Increase lookback to more than ${minResolutionMs * 2}ms")
          IteratorBackedRangeVector(rv.key,
            new ChunkedWindowIteratorD(rdrv, startWithOffset, adjustedStep, endWithOffset,
                    extendedWindow, chunkedDRangeFunc, querySession), outputRvRange)
        }
      // Iterator-based: Wrap long columns to yield a double value
      case _: RangeFunction[_] if valColType == ColumnType.LongColumn =>
        source.map { rv =>
          val windowPlusPubInt = extendLookback(rv, windowLength)
          IteratorBackedRangeVector(rv.key,
            new SlidingWindowIterator(new LongToDoubleIterator(rv.rows()), startWithOffset, adjustedStep, endWithOffset,
              windowPlusPubInt,
              rangeFuncGen().asSlidingD, querySession.queryConfig, leftInclusiveWindow,
              new TransientRow()), outputRvRange)
        }
      case _: RangeFunction[_]   if valColType == ColumnType.HistogramColumn =>
        source.map { rv =>
          val windowPlusPubInt = extendLookback(rv, windowLength)
          IteratorBackedRangeVector(rv.key,
            new SlidingWindowIterator(rv.rows(), startWithOffset, adjustedStep, endWithOffset, windowPlusPubInt,
              rangeFuncGen().asSlidingH,
              querySession.queryConfig, leftInclusiveWindow,
              if (hasMaxMinCol) new TransientHistMaxMinRow() else new TransientHistRow()), outputRvRange)
        }
      // Otherwise just feed in the double column
      case _: RangeFunction[_] =>
        source.map { rv =>
          val windowPlusPubInt = extendLookback(rv, windowLength)
          IteratorBackedRangeVector(rv.key,
            new SlidingWindowIterator(rv.rows(), startWithOffset, adjustedStep, endWithOffset, windowPlusPubInt,
              rangeFuncGen().asSlidingD,
              querySession.queryConfig, leftInclusiveWindow, new TransientRow()), outputRvRange)
        }
    }

    // Adds offset to timestamp to generate output of offset function, since the time should be according to query
    // time parameters
    offsetMs.map(o => rvs.map { rv =>
      new RangeVector {
        override def key: RangeVectorKey = rv.key
        override def rows(): RangeVectorCursor = rv.rows.mapRow { r =>
          val updatedRow = r match {
            case tr: TransientRow =>
              val ur = new TransientRow()
              ur.setLong(0, tr.getLong(0) + o)
              ur.setDouble(1, tr.getDouble(1))
              ur
            case thmr: TransientHistMaxMinRow =>
              val ur = new TransientHistMaxMinRow()
              ur.setValues(thmr.getLong(0) + o, thmr.value)
              ur.setDouble(2, thmr.getDouble(2))
              ur.setDouble(3, thmr.getDouble(3))
              ur
            case thr: TransientHistRow =>
              val ur = new TransientHistRow()
              ur.setValues(thr.getLong(0) + o, thr.value)
              ur
            case _ => throw new IllegalArgumentException("Unsupported row type for offset handling")
          }
          updatedRow
        }
        override def outputRange: Option[RvRange] = outputRvRange
      }
    }).getOrElse(rvs)
  }
  //scalastyle:on

  /**
   * If a rate function is used in the downsample dataset where publish interval is known,
   * and the requested window length is less than 2*publishInterval then extend lookback automatically
   * so we don't return empty result for rate.
   * Typically, you don't modify the query parameters, but this is a special case only for the downsample
   * dataset. We do this only for rate since it is already a per-second normalized value. Doing this for increase
   * will lead to incorrect results.
   */
  private[exec] def extendLookback(rv: RangeVector, window: Long): Long = {
    window
// following code is commented, but uncomment when we want to extend window for rate function

//    if (functionId.contains(InternalRangeFunction.Rate)) { // only extend for rate function
//      val pubInt = rv match {
//        case rvrd: RawDataRangeVector =>
//          if (rvrd.partition.schema.hasCumulativeTemporalityColumn) // only extend for cumulative schemas
//            rvrd.partition.publishInterval.getOrElse(0L)
//          else 0L
//        case _ => 0L
//      }
//      if (window < 2 * pubInt) 2 * pubInt // 2 * publish interval since we want 2 samples for rate
//      else window
//    } else {
//      window
//    }
  }

  // Transform source double or long to double schema
  override def schema(source: ResultSchema): ResultSchema = {
    // Special treatment for downsampled gauge schema.
    // Source schema will be the selected columns. Result of this mapper should be regular timestamp/value
    // since the avg will be calculated using sum and count
    // FIXME dont like that this is hardcoded; but the check is needed.
    if (functionId.contains(AvgWithSumAndCountOverTime) &&
                        source.columns.map(_.name) == Seq("timestamp", "sum", "count")) {
      source.copy(columns = Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
                                ColumnInfo("value", ColumnType.DoubleColumn)))
    } else {
      source.copy(columns = source.columns.zipWithIndex.map {
        // Transform if its not a row key column
        case (ColumnInfo(name, ColumnType.LongColumn, _), i) if i >= source.numRowKeyColumns =>
          ColumnInfo("value", ColumnType.DoubleColumn)
        case (ColumnInfo(name, ColumnType.IntColumn, _), i) if i >= source.numRowKeyColumns =>
          ColumnInfo(name, ColumnType.DoubleColumn)
        // For double columns, just rename the output so that it's the same no matter source schema.
        // After all after applying a range function, source column doesn't matter anymore
        // NOTE: we only rename if i is 1 or second column.  If its 2 it might be max which cannot be renamed
        case (ColumnInfo(name, ColumnType.DoubleColumn, _), i) if i == 1 =>
          ColumnInfo("value", ColumnType.DoubleColumn)
        case (c: ColumnInfo, _) => c
      }, fixedVectorLen = if (endMs == startMs) Some(1) else Some(((endMs - startMs) / adjustedStep).toInt + 1))
    }
  }
}

object FiloQueryConfig {
  val isInclusiveRange = systemConfig.getBoolean("filodb.query.inclusive-range")
}

/**
 * A low-overhead iterator which works on one window at a time, optimally applying columnar techniques
 * to compute each window as fast as possible on multiple rows at a time.
 */
abstract class ChunkedWindowIterator[R <: MutableRowReader](
    rv: RawDataRangeVector,
    start: Long,
    step: Long,
    end: Long,
    window: Long,
    rangeFunction: ChunkedRangeFunction[R],
    querySession: QuerySession)
extends WrappedCursor(rv.rows()) with StrictLogging {
  require(step > 0, s"Adjusted step $step not > 0")
  // Lazily open the iterator and obtain the lock. This allows one thread to create the
  // iterator, but the lock is owned by the thread actually performing the iteration.
  private lazy val windowIt = {
    val it = new WindowedChunkIterator(rv, start, step, end, window, querySession.qContext,
      isInclusiveRange = FiloQueryConfig.isInclusiveRange)
    // Need to hold the shared lock explicitly, because the window iterator needs to
    // pre-fetch chunks to determine the window. This pre-fetching can force the internal
    // iterator to close, which would release the lock too soon.
    it.lock()
    it
  }

  private var isClosed: Boolean = false

  override def close(): Unit = {
    if (!isClosed) {
      isClosed = true
      val wit = windowIt
      wit.unlock()
      wit.close()
      super.close()
    }
  }

  def sampleToEmit: R

  override def hasNext: Boolean = windowIt.hasMoreWindows
  override def doNext: R = {
    rangeFunction.reset()

    // Lazy variables have an extra lookup cost, due to a volatile bitmap field generated by
    // the compiler. Copy to a local variable to reduce some overhead.
    val wit = windowIt

    wit.nextWindow()
    while (wit.hasNext) {
      val nextInfo = wit.next
      try {
        rangeFunction.addChunks(nextInfo.getTsVectorAccessor, nextInfo.getTsVectorAddr, nextInfo.getTsReader,
                                nextInfo.getValueVectorAccessor, nextInfo.getValueVectorAddr, nextInfo.getValueReader,
                                wit.curWindowStart, wit.curWindowEnd, nextInfo, querySession.queryConfig)
      } catch {
        case e: Exception =>
          val tsReader = LongBinaryVector(nextInfo.getTsVectorAccessor, nextInfo.getTsVectorAddr)
          val valReader = rv.partition.schema.data.reader(rv.valueColID,
                                                          nextInfo.getValueVectorAccessor,
                                                          nextInfo.getValueVectorAddr)
          qLogger.error(s"addChunks Exception: ChunkInfo=[${nextInfo.debugString}] " +
            s"curWinStart=${wit.curWindowStart} curWindowEnd=${wit.curWindowEnd} tsReader=$tsReader " +
            s"timestampVectorLength=${tsReader.length(nextInfo.getTsVectorAccessor, nextInfo.getTsVectorAddr)} " +
            s"valueVectorLength=${valReader.length(nextInfo.getValueVectorAccessor, nextInfo.getValueVectorAddr)} " +
            s"partition ${rv.partition.stringPartition} " +
            s"start=$start end=$end step=$step", e)
          throw e
      }
    }
    rangeFunction.apply(wit.curWindowStart, wit.curWindowEnd, sampleToEmit)

    if (!wit.hasMoreWindows) {
      // Release the shared lock and close the iterator, in case it also holds a lock.
      close()
    }

    sampleToEmit
  }
}

class ChunkedWindowIteratorD(rv: RawDataRangeVector,
    start: Long, step: Long, end: Long, window: Long,
    rangeFunction: ChunkedRangeFunction[TransientRow],
    querySession: QuerySession,
    // put emitter here in constructor for faster access
    var sampleToEmit: TransientRow = new TransientRow()) extends
ChunkedWindowIterator[TransientRow](rv, start, step, end, window, rangeFunction, querySession)

class ChunkedWindowIteratorH(rv: RawDataRangeVector,
    start: Long, step: Long, end: Long, window: Long,
    rangeFunction: ChunkedRangeFunction[TransientHistRow],
    querySession: QuerySession,
    // put emitter here in constructor for faster access
    var sampleToEmit: TransientHistRow = new TransientHistRow()) extends
ChunkedWindowIterator[TransientHistRow](rv, start, step, end, window, rangeFunction, querySession)

class QueueBasedWindow[T <: MutableRowReader](q: IndexedArrayQueue[T]) extends Window[T] {
  def size: Int = q.size

  def apply(i: Int): T = q(i)
  def head: T = q.head
  def last: T = q.last
  override def toString: String = q.toString()
}

/**
  * Decorates a raw series iterator to apply a range vector function
  * on periodic time windows
  */
class SlidingWindowIterator[T <: MutableRowReader](raw: RangeVectorCursor,
                            start: Long,
                            step: Long,
                            end: Long,
                            window: Long,
                            rangeFunction: RangeFunction[T],
                            queryConfig: QueryConfig,
                            leftWindowInclusive : Boolean = false, rowReaderFactory: => T)
  extends RangeVectorCursor {
  require(step > 0, s"Adjusted step $step not > 0")
  private var curWindowEnd = start

  private val sampleToEmit: T = rowReaderFactory

  // sliding window queue
  private val windowQueue = new IndexedArrayQueue[T]()

  // this is the object that will be exposed to the RangeFunction
  private val windowSamples = new QueueBasedWindow[T](windowQueue)

  // NOTE: Ingestion now has a facility to drop out of order samples.  HOWEVER, there is one edge case that may happen
  // which is that the first sample ingested after recovery may not be in order w.r.t. previous persisted timestamp.
  // So this is retained for now while we consider a more permanent out of order solution.
  private val rawInOrder = new DropOutOfOrderSamplesIterator(raw, rowReaderFactory)

  // we need buffered iterator so we can use to peek at next element.
  // At same time, do counter correction if necessary
  private val rows = if (rangeFunction.needsCounterCorrection) {
    sampleToEmit match {
      case _: TransientRow      => new BufferableCounterCorrectionIterator(rawInOrder).buffered
      case _: TransientHistRow  => new BufferableCounterCorrectionIteratorH(rawInOrder).buffered
    }
  } else {
    new BufferableIterator(rawInOrder, rowReaderFactory).buffered
  }

  // to avoid creation of object per sample, we use a pool
  private val windowSamplesPool = new TransientRowPool[T](rowReaderFactory)

  override def close(): Unit = raw.close()

  override def hasNext: Boolean = curWindowEnd <= end
  override def next(): T = {
    val curWindowStart = curWindowEnd - window
    // current window is: [curWindowStart, curWindowEnd]. Includes start, end.

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
  private def shouldAddCurToWindow(curWindowStart: Long, cur: MutableRowReader): Boolean = {
    // cur is inside current window
    val windowStart = if (FiloQueryConfig.isInclusiveRange) curWindowStart else curWindowStart + 1
    cur.timestamp >= windowStart
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
    val windowStart = if (FiloQueryConfig.isInclusiveRange) curWindowStart else curWindowStart - 1
    (!windowQueue.isEmpty) && windowQueue.head.timestamp < windowStart
  }
}

/**
  * Converts the long value column to double.
  */
class LongToDoubleIterator(iter: RangeVectorCursor) extends RangeVectorCursor {
  val sampleToEmit = new TransientRow()
  override final def hasNext: Boolean = iter.hasNext
  override final def next(): TransientRow = {
    val next = iter.next()
    sampleToEmit.setLong(0, next.getLong(0))
    sampleToEmit.setDouble(1, next.getLong(1).toDouble)
    sampleToEmit
  }

  override def close(): Unit = iter.close()
}

/**
  * Exists so that we can reuse TransientRow objects and reduce object creation per
  * raw sample. Beware: This is mutable, and not thread-safe.
  */
class TransientRowPool[T <: MutableRowReader](inst: => T) {
  val pool = new SpscUnboundedArrayQueue[T](16)
  @inline final def get: T = if (pool.isEmpty) inst else pool.remove()
  @inline final def putBack(r: T): Unit = pool.add(r)
}

/**
  * Iterator of mutable objects that allows look-ahead for ONE value.
  * Caller can do iterator.buffered safely
  */
class BufferableIterator[R <: MutableRowReader](iter: Iterator[RowReader],
                                                rowReaderFactory: => R) extends Iterator[R] {
  private var prev = rowReaderFactory
  private var cur = rowReaderFactory
  override def hasNext: Boolean = iter.hasNext
  override def next(): R = {
    // swap prev an cur
    val temp = prev
    prev = cur
    cur = temp
    // place value in cur and return
    cur.copyFrom(iter.next())
    cur
  }
}

class BufferableCounterCorrectionIteratorH(iter: Iterator[RowReader]) extends Iterator[TransientHistRow] {
  private var corrections: Array[Double] = Array.empty
  private var prevBuckets: Array[Double] = Array.empty
  private var buckets: HistogramBuckets = CustomBuckets(Array.empty)
  private var prev = new TransientHistRow()
  private var cur = new TransientHistRow()

  override def hasNext: Boolean = iter.hasNext

  override def next(): TransientHistRow = {
    val next = iter.next()
    val nextHist = next.getHistogram(1)

    if (corrections.isEmpty) {
      corrections = Array.fill(nextHist.numBuckets)(0.0)
      prevBuckets = Array.fill(nextHist.numBuckets)(0.0)
      buckets = CustomBuckets((0 until nextHist.numBuckets).map(nextHist.bucketTop).toArray)
    }

    // Apply counter correction to each bucket
    val correctedBuckets = Array.ofDim[Double](nextHist.numBuckets)
    for (i <- 0 until nextHist.numBuckets) {
      var bucketVal = nextHist.bucketValue(i)
      if (bucketVal.isNaN) bucketVal = 0 // explicit counter reset
      if (bucketVal < prevBuckets(i)) {
        corrections(i) += prevBuckets(i)
      }
      prevBuckets(i) = bucketVal
      correctedBuckets(i) = bucketVal + corrections(i)
    }

    // swap prev and cur
    val temp = prev
    prev = cur
    cur = temp

    // place values in cur and return
    cur.setLong(0, next.getLong(0))
    cur.setValues(1, MutableHistogram(buckets, correctedBuckets))
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
    var nextVal = next.getDouble(1)
    if (nextVal.isNaN) nextVal = 0 // explicit counter reset due to end of time series marker
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

class DropOutOfOrderSamplesIterator[R <: MutableRowReader](
                                    iter: Iterator[RowReader],
                                    rowReaderFactory: => R) extends Iterator[R] {
  // Initial -1 time since it will be less than any valid timestamp and will allow first sample to go through
  private val cur = rowReaderFactory
  cur.setLong(0, -1)
  private val nextVal = rowReaderFactory
  nextVal.setLong(0, -1)
  private var hasNextVal = false
  private var hasNextDefined = false

  override def hasNext: Boolean = {
    if (!hasNextDefined) {
      setNext()
      hasNextDefined = true
    }
    hasNextVal
  }

  override def next(): R = {
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
      if (t > cur.timestamp) { // if next sample is later than current sample
        nextVal.copyFrom(nxt)
        hasNextVal = true
      } else {
        Query.droppedSamples.increment()
      }
    }
  }
}
