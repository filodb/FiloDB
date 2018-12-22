package filodb.query.exec.rangefn

import filodb.core.store.ChunkSetInfo
import filodb.memory.format.{vectors => bv, BinaryVector}
import filodb.query.{QueryConfig, RangeFunctionId}
import filodb.query.exec._
import filodb.query.RangeFunctionId._

/**
  * Container for samples within a window of samples
  * over which a range function can be applied
  */
trait Window {
  def apply(i: Int): TransientRow
  def size: Int
  def head: TransientRow
  def last: TransientRow
}

/**
  * All Range Vector Functions are implementation of this trait.
  * There are multiple choices for function implementation:
  * 1. Use the `addToWindow` and `removeFromWindow` events to evaluate the next value to emit.
  *    This may result in O(n) complexity for emiting the entire range vector.
  * 2. Use the entire window content in `apply` to emit the next value. Depending on whether the
  *    entire window is examined, this may result in O(n) or O(n-squared) for the entire range vector.
  * 3. Use the addChunks() API of the ChunkedRangeFunction subtrait for more efficient proessing of windows
  *    in chunks of rows (rather than one at a time)
  */
trait RangeFunction {
  /**
    * Needs last sample prior to window start
    */
  def needsLastSample: Boolean = false

  /**
    * Values added to window will be converted to monotonically increasing. Mark
    * as true only if the function will always operate on counters.
    */
  def needsCounterCorrection: Boolean = false

  /**
    * Called when a sample is added to the sliding window
    */
  def addedToWindow(row: TransientRow, window: Window): Unit

  /**
    * Called when a sample is removed from sliding window
    */
  def removedFromWindow(row: TransientRow, window: Window): Unit

  /**
    * Called when wrapping iterator needs to emit a sample using the window.
    *
    * Samples in the window are samples reported in the requested window length.
    * Window also includes the last sample outside the window if it was reported
    * for the time within stale sample period
    *
    * Timestamp of samples in window is always <= the timestamp param of this function.
    *
    * @param startTimestamp start timestamp of the time window
    * @param endTimestamp timestamp to use in emitted sample. It is also the endTimestamp for the window
    * @param window samples contained in the window
    * @param sampleToEmit To keep control on reader creation the method must set
    *                  the value to emit in this param object that comes from a reader pool
    */
  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit
}

/**
 * Improved RangeFunction API with direct chunk access for faster columnar/bulk operations
 */
trait ChunkedRangeFunction extends RangeFunction {
  def addedToWindow(row: TransientRow, window: Window): Unit = {}

  def removedFromWindow(row: TransientRow, window: Window): Unit = {}

  def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {}

  /**
   * Resets the state
   */
  def reset(): Unit = {}

  /**
   * Called by the ChunkedWindowIterator to add multiple rows at a time to the range function for efficiency.
   * The idea is to call chunk-based methods such as sum and binarySearch.
   * @param tsCol ColumnID for timestamp column
   * @param valueCol ColumnID for value column
   * @param info ChunkSetInfo with information for specific chunks
   * @param startTime starting timestamp in millis since Epoch for time window
   */
  def addChunks(tsCol: Int, valueCol: Int, info: ChunkSetInfo,
                startTime: Long, endTime: Long, queryConfig: QueryConfig): Unit

  /**
   * Return the computed result in the sampleToEmit
   */
  def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit
}

/**
 * Standard ChunkedRangeFunction implementation extracting the start and ending row numbers from the timestamp
 * and returning the double value vector and reader with the row numbers
 */
trait ChunkedDoubleRangeFunction extends ChunkedRangeFunction {
  def addChunks(tsCol: Int, valueCol: Int, info: ChunkSetInfo,
                startTime: Long, endTime: Long, queryConfig: QueryConfig): Unit = {
    val timestampVector = info.vectorPtr(tsCol)
    val tsReader = bv.LongBinaryVector(timestampVector)
    val doubleVector = info.vectorPtr(valueCol)
    val dblReader = bv.DoubleVector(doubleVector)

    // First row >= startTime, so we can just drop bit 31 (dont care if it matches exactly)
    val startRowNum = tsReader.binarySearch(timestampVector, startTime) & 0x7fffffff
    val endRowNum = tsReader.ceilingIndex(timestampVector, endTime)

    addTimeDoubleChunks(doubleVector, dblReader, startRowNum, endRowNum)
  }

  /**
   * Add a Double BinaryVector in the range (startRowNum, endRowNum) to the range computation
   * @param startRowNum the row number for timestamp greater than or equal to startTime
   * @param endRowNum the row number with the timestamp <= endTime
   */
  def addTimeDoubleChunks(doubleVect: BinaryVector.BinaryVectorPtr,
                          doubleReader: bv.DoubleVectorDataReader,
                          startRowNum: Int,
                          endRowNum: Int): Unit
}

object RangeFunction {
  def apply(func: Option[RangeFunctionId],
            funcParams: Seq[Any] = Nil,
            useChunked: Boolean = true): RangeFunction = if (useChunked) func match {
    case None                 => new LastSampleChunkedFunction()
    case Some(CountOverTime)  => new CountOverTimeChunkedFunction()
    case Some(SumOverTime)    => new SumOverTimeChunkedFunction()
    case Some(AvgOverTime)    => new AvgOverTimeChunkedFunctionD()
    case Some(StdDevOverTime) => new StdDevOverTimeChunkedFunctionD()
    case Some(StdVarOverTime) => new StdVarOverTimeChunkedFunctionD()
    case _                    => iterator(func, funcParams)
  } else iterator(func, funcParams)

  def iterator(func: Option[RangeFunctionId],
               funcParams: Seq[Any] = Nil): RangeFunction = func match {
    case None                   => LastSampleFunction // when no window function is asked, use last sample for instant
    case Some(Rate)             => RateFunction
    case Some(Increase)         => IncreaseFunction
    case Some(Delta)            => DeltaFunction
    case Some(Resets)           => ResetsFunction
    case Some(Irate)            => IRateFunction
    case Some(Idelta)           => IDeltaFunction
    case Some(Deriv)            => DerivFunction
    case Some(MaxOverTime)      => new MinMaxOverTimeFunction(Ordering[Double])
    case Some(MinOverTime)      => new MinMaxOverTimeFunction(Ordering[Double].reverse)
    case Some(CountOverTime)    => new CountOverTimeFunction()
    case Some(SumOverTime)      => new SumOverTimeFunction()
    case Some(AvgOverTime)      => new AvgOverTimeFunction()
    case Some(StdDevOverTime)   => new StdDevOverTimeFunction()
    case Some(StdVarOverTime)   => new StdVarOverTimeFunction()
    case _                      => ???
  }
}

object LastSampleFunction extends RangeFunction {
  override def needsLastSample: Boolean = true
  def addedToWindow(row: TransientRow, window: Window): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window): Unit = {}
  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    if (window.size > 1)
      throw new IllegalStateException(s"Window had more than 1 sample. Possible out of order samples. Window: $window")
    if (window.size == 0 || (endTimestamp - window.head.getLong(0)) > queryConfig.staleSampleAfterMs) {
      sampleToEmit.setValues(endTimestamp, Double.NaN)
    } else {
      sampleToEmit.setValues(endTimestamp, window.head.getDouble(1))
    }
  }
}

/**
 * Directly obtain the last sample from chunks for much much faster performance compared to above
 */
class LastSampleChunkedFunction(var timestamp: Long = -1L,
                                var value: Double = Double.NaN) extends ChunkedRangeFunction {
  override final def reset(): Unit = { timestamp = -1L; value = Double.NaN }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, value)
  }
  // Add each chunk and update timestamp and value such that latest sample wins
  final def addChunks(tsCol: Int, valueCol: Int, info: ChunkSetInfo,
                startTime: Long, endTime: Long, queryConfig: QueryConfig): Unit = {
    val timestampVector = info.vectorPtr(tsCol)
    val tsReader = bv.LongBinaryVector(timestampVector)
    val endRowNum = tsReader.ceilingIndex(timestampVector, endTime)

    // update timestamp only if
    //   1) endRowNum >= 0 (timestamp within chunk)
    //   2) timestamp is within stale window; AND
    //   3) timestamp is greater than current timestamp (for multiple chunk scenarios)
    if (endRowNum >= 0) {
      val ts = tsReader(timestampVector, endRowNum)
      if ((endTime - ts) <= queryConfig.staleSampleAfterMs && ts > timestamp) {
        timestamp = ts
        val dblVector = info.vectorPtr(valueCol)
        val dblReader = bv.DoubleVector(dblVector)
        value = dblReader(dblVector, endRowNum)
      }
    }
  }
}
