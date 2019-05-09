package filodb.query.exec.rangefn

import filodb.core.metadata.Column.ColumnType
import filodb.core.store.ChunkSetInfo
import filodb.memory.format.{vectors => bv, BinaryVector, VectorDataReader}
import filodb.memory.format.BinaryVector.BinaryVectorPtr
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

// Just a marker trait for all RangeFunction implementations, sliding and chunked
sealed trait BaseRangeFunction {
  def asSliding: RangeFunction = this.asInstanceOf[RangeFunction]
  def asChunkedD: ChunkedRangeFunction[TransientRow] = this.asInstanceOf[ChunkedRangeFunction[TransientRow]]
  def asChunkedH: ChunkedRangeFunction[TransientHistRow] = this.asInstanceOf[ChunkedRangeFunction[TransientHistRow]]
}

/**
  * Double-based Range Vector Functions that work with the SlidingWindowIterator.
  * There are multiple choices for function implementation:
  * 1. Use the `addToWindow` and `removeFromWindow` events to evaluate the next value to emit.
  *    This may result in O(n) complexity for emitting the entire range vector.
  * 2. Use the entire window content in `apply` to emit the next value. Depending on whether the
  *    entire window is examined, this may result in O(n) or O(n-squared) for the entire range vector.
  */
trait RangeFunction extends BaseRangeFunction {
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
 * Improved RangeFunction API with direct chunk access for faster columnar/bulk operations.
 * It also allows for different MutableRows to be plugged in for different types.
 */
trait ChunkedRangeFunction[R <: MutableRowReader] extends BaseRangeFunction {
  /**
   * Resets the state
   */
  def reset(): Unit = {}

  /**
   * Called by the ChunkedWindowIterator to add multiple rows at a time to the range function for efficiency.
   * The idea is to call chunk-based methods such as sum and binarySearch.
   * @param tsVector raw pointer to the timestamp BinaryVector
   * @param tsReader a LongVectorDataReader for parsing the tsVector
   * @param valueVector a raw pointer to the value BinaryVector
   * @param valueReader a VectorDataReader for the value BinaryVector. Method must cast to appropriate type.
   * @param startTime starting timestamp in millis since Epoch for time window, inclusive
   * @param endTime ending timestamp in millis since Epoch for time window, inclusive
   */
  def addChunks(tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfo, queryConfig: QueryConfig): Unit

  /**
   * Return the computed result in the sampleToEmit
   */
  def apply(endTimestamp: Long, sampleToEmit: R): Unit
}

/**
 * A trait for RangeFunctions that operate on both a start time and an end time.  Will find the start and end
 * row numbers for the chunk.
 */
trait TimeRangeFunction[R <: MutableRowReader] extends ChunkedRangeFunction[R] {
  final def addChunks(tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                      valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                      startTime: Long, endTime: Long, info: ChunkSetInfo, queryConfig: QueryConfig): Unit = {
    // TODO: abstract this pattern of start/end row # out. Probably when cursors are implemented
    // First row >= startTime, so we can just drop bit 31 (dont care if it matches exactly)
    val startRowNum = tsReader.binarySearch(tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVector, endTime), info.numRows - 1)

    // At least one sample is present
    if (startRowNum <= endRowNum)
      addTimeChunks(valueVector, valueReader, startRowNum, endRowNum)
  }

  def addTimeChunks(vectPtr: BinaryVector.BinaryVectorPtr,
                    reader: VectorDataReader,
                    startRowNum: Int,
                    endRowNum: Int): Unit
}

/**
 * Standard ChunkedRangeFunction implementation extracting the start and ending row numbers from the timestamp
 * and returning the double value vector and reader with the row numbers
 */
trait ChunkedDoubleRangeFunction extends TimeRangeFunction[TransientRow] {
  final def addTimeChunks(vectPtr: BinaryVector.BinaryVectorPtr,
                          reader: VectorDataReader,
                          startRowNum: Int,
                          endRowNum: Int): Unit =
    addTimeDoubleChunks(vectPtr, reader.asDoubleReader, startRowNum, endRowNum)

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

trait ChunkedLongRangeFunction extends TimeRangeFunction[TransientRow] {
  final def addTimeChunks(vectPtr: BinaryVector.BinaryVectorPtr,
                          reader: VectorDataReader,
                          startRowNum: Int,
                          endRowNum: Int): Unit =
    addTimeLongChunks(vectPtr, reader.asLongReader, startRowNum, endRowNum)

  /**
   * Add a Long BinaryVector in the range (startRowNum, endRowNum) to the range computation
   * @param startRowNum the row number for timestamp greater than or equal to startTime
   * @param endRowNum the row number with the timestamp <= endTime
   */
  def addTimeLongChunks(longVect: BinaryVector.BinaryVectorPtr,
                        longReader: bv.LongVectorDataReader,
                        startRowNum: Int,
                        endRowNum: Int): Unit
}

object RangeFunction {
  type RangeFunctionGenerator = () => BaseRangeFunction

  /**
   * Returns a (probably new) instance of RangeFunction given the func ID and column type
   */
  def apply(func: Option[RangeFunctionId],
            columnType: ColumnType,
            funcParams: Seq[Any] = Nil,
            maxCol: Option[Int] = None,
            useChunked: Boolean): BaseRangeFunction =
    generatorFor(func, columnType, funcParams, maxCol, useChunked)()

  /**
   * Given a function type and column type, returns a RangeFunctionGenerator
   */
  def generatorFor(func: Option[RangeFunctionId],
                   columnType: ColumnType,
                   funcParams: Seq[Any] = Nil,
                   maxCol: Option[Int] = None,
                   useChunked: Boolean = true): RangeFunctionGenerator =
    if (useChunked) columnType match {
      case ColumnType.DoubleColumn => doubleChunkedFunction(func, funcParams)
      case ColumnType.LongColumn   => longChunkedFunction(func, funcParams)
      case ColumnType.TimestampColumn => longChunkedFunction(func, funcParams)
      case ColumnType.HistogramColumn => histChunkedFunction(func, funcParams, maxCol)
      case other: ColumnType       => throw new IllegalArgumentException(s"Column type $other not supported")
    } else {
      iteratingFunction(func, funcParams)
    }

  /**
   * Returns a function to generate a ChunkedRangeFunction for Long columns
   */
  def longChunkedFunction(func: Option[RangeFunctionId],
                          funcParams: Seq[Any] = Nil): RangeFunctionGenerator = func match {
    case None                 => () => new LastSampleChunkedFunctionL
    case Some(CountOverTime)  => () => new CountOverTimeChunkedFunction()
    case Some(SumOverTime)    => () => new SumOverTimeChunkedFunctionL
    case Some(AvgOverTime)    => () => new AvgOverTimeChunkedFunctionL
    case Some(MinOverTime)    => () => new MinOverTimeChunkedFunctionL
    case Some(MaxOverTime)    => () => new MaxOverTimeChunkedFunctionL
    case Some(StdDevOverTime) => () => new StdDevOverTimeChunkedFunctionL
    case Some(StdVarOverTime) => () => new StdVarOverTimeChunkedFunctionL
    case _                    => iteratingFunction(func, funcParams)
  }

  /**
   * Returns a function to generate a ChunkedRangeFunction for Double columns
   */
  def doubleChunkedFunction(func: Option[RangeFunctionId],
                            funcParams: Seq[Any] = Nil): RangeFunctionGenerator = func match {
    case None                 => () => new LastSampleChunkedFunctionD
    case Some(CountOverTime)  => () => new CountOverTimeChunkedFunctionD()
    case Some(SumOverTime)    => () => new SumOverTimeChunkedFunctionD
    case Some(AvgOverTime)    => () => new AvgOverTimeChunkedFunctionD
    case Some(MinOverTime)    => () => new MinOverTimeChunkedFunctionD
    case Some(MaxOverTime)    => () => new MaxOverTimeChunkedFunctionD
    case Some(StdDevOverTime) => () => new StdDevOverTimeChunkedFunctionD
    case Some(StdVarOverTime) => () => new StdVarOverTimeChunkedFunctionD
    case _                    => iteratingFunction(func, funcParams)
  }

  def histChunkedFunction(func: Option[RangeFunctionId],
                          funcParams: Seq[Any] = Nil,
                          maxCol: Option[Int] = None): RangeFunctionGenerator = func match {
    case None                 => () => new LastSampleChunkedFunctionH
    case Some(SumOverTime) if maxCol.isDefined => () => new SumAndMaxOverTimeFuncHD(maxCol.get)
    case Some(SumOverTime)    => () => new SumOverTimeChunkedFunctionH
    case _                    => ???
  }

  /**
   * Returns a function to generate the RangeFunction for SlidingWindowIterator.
   * Note that these functions are Double-based, so a converting iterator eg LongToDoubleIterator may be needed.
   */
  def iteratingFunction(func: Option[RangeFunctionId],
                        funcParams: Seq[Any] = Nil): RangeFunctionGenerator = func match {
    // when no window function is asked, use last sample for instant
    case None                   => () => LastSampleFunction
    case Some(Rate)             => () => RateFunction
    case Some(Increase)         => () => IncreaseFunction
    case Some(Delta)            => () => DeltaFunction
    case Some(Resets)           => () => new ResetsFunction()
    case Some(Irate)            => () => IRateFunction
    case Some(Idelta)           => () => IDeltaFunction
    case Some(Deriv)            => () => DerivFunction
    case Some(MaxOverTime)      => () => new MinMaxOverTimeFunction(Ordering[Double])
    case Some(MinOverTime)      => () => new MinMaxOverTimeFunction(Ordering[Double].reverse)
    case Some(CountOverTime)    => () => new CountOverTimeFunction()
    case Some(SumOverTime)      => () => new SumOverTimeFunction()
    case Some(AvgOverTime)      => () => new AvgOverTimeFunction()
    case Some(StdDevOverTime)   => () => new StdDevOverTimeFunction()
    case Some(StdVarOverTime)   => () => new StdVarOverTimeFunction()
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
 * TODO: one day we should expose higher level means of returning the actual Long value instead of converting to Double
 */
abstract class LastSampleChunkedFunction[R <: MutableRowReader](var timestamp: Long = -1L)
extends ChunkedRangeFunction[R] {
  // Add each chunk and update timestamp and value such that latest sample wins
  final def addChunks(tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                      valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                      startTime: Long, endTime: Long, info: ChunkSetInfo, queryConfig: QueryConfig): Unit = {
    // Just in case timestamp vectors are a bit longer than others.
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVector, endTime), info.numRows - 1)

    // update timestamp only if
    //   1) endRowNum >= 0 (timestamp within chunk)
    //   2) timestamp is within stale window; AND
    //   3) timestamp is greater than current timestamp (for multiple chunk scenarios)
    if (endRowNum >= 0) {
      val ts = tsReader(tsVector, endRowNum)
      if ((endTime - ts) <= queryConfig.staleSampleAfterMs && ts > timestamp)
        updateValue(ts, valueVector, valueReader, endRowNum)
    }
  }

  def updateValue(ts: Long, valVector: BinaryVectorPtr, valReader: VectorDataReader, endRowNum: Int): Unit
}

// LastSample functions with double value, based on TransientRow
abstract class LastSampleChunkedFuncDblVal(var value: Double = Double.NaN)
extends LastSampleChunkedFunction[TransientRow] {
  override final def reset(): Unit = { timestamp = -1L; value = Double.NaN }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, value)
  }
}

// LastSample function for Histogram columns
class LastSampleChunkedFunctionH(var value: bv.HistogramWithBuckets = bv.Histogram.empty)
extends LastSampleChunkedFunction[TransientHistRow] {
  override final def reset(): Unit = { timestamp = -1L; value = bv.Histogram.empty }
  final def apply(endTimestamp: Long, sampleToEmit: TransientHistRow): Unit = {
    sampleToEmit.setValues(endTimestamp, value)
  }
  final def updateValue(ts: Long, valVector: BinaryVectorPtr, valReader: VectorDataReader, endRowNum: Int): Unit = {
    timestamp = ts
    value = valReader.asHistReader(endRowNum)
  }
}

class LastSampleChunkedFunctionD extends LastSampleChunkedFuncDblVal() {
  final def updateValue(ts: Long, valVector: BinaryVectorPtr, valReader: VectorDataReader, endRowNum: Int): Unit = {
    val dblReader = valReader.asDoubleReader
    val doubleVal = dblReader(valVector, endRowNum)
    // If the last value is NaN, that may be Prometheus end of time series marker.
    // In that case try to get the sample before last.
    // If endRowNum==0, we are at beginning of chunk, and if the window included the last chunk, then
    // the call to addChunks to the last chunk would have gotten the last sample value anyways.
    if (java.lang.Double.isNaN(doubleVal)) {
      if (endRowNum > 0) {
        timestamp = ts
        value = dblReader(valVector, endRowNum - 1)
      }
    } else {
      timestamp = ts
      value = doubleVal
    }
  }
}

class LastSampleChunkedFunctionL extends LastSampleChunkedFuncDblVal() {
  final def updateValue(ts: Long, valVector: BinaryVectorPtr, valReader: VectorDataReader, endRowNum: Int): Unit = {
    val longReader = valReader.asLongReader
    timestamp = ts
    value = longReader(valVector, endRowNum).toDouble
  }
}
