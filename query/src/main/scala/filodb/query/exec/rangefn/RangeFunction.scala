package filodb.query.exec.rangefn

import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.Schema
import filodb.core.query._
import filodb.core.store.ChunkSetInfoReader
import filodb.memory.format.{vectors => bv, _}
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.query.Query
//import filodb.query.RangeFunctionId.MedianAbsoluteDeviationOverTime
import filodb.query.exec._

/**
  * Container for samples within a window of samples
  * over which a range function can be applied
  */
trait Window[T <: MutableRowReader] {
  def apply(i: Int): T
  def size: Int
  def head: T
  def last: T
}

// Just a marker trait for all RangeFunction implementations, sliding and chunked
sealed trait BaseRangeFunction {
  def asSlidingD: RangeFunction[TransientRow] = this.asInstanceOf[RangeFunction[TransientRow]]
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
trait RangeFunction[R <: MutableRowReader] extends BaseRangeFunction {
  /**
    * Values added to window will be converted to monotonically increasing. Mark
    * as true only if the function will always operate on counters.
    */
  def needsCounterCorrection: Boolean = false

  /**
    * Called when a sample is added to the sliding window
    */
  def addedToWindow(row: R, window: Window[R]): Unit

  /**
    * Called when a sample is removed from sliding window
    */
  def removedFromWindow(row: R, window: Window[R]): Unit

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
            window: Window[R],
            sampleToEmit: R,
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
  // scalastyle:off parameter.number
  def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVectorAcc: MemoryReader, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit

  /**
   * Return the computed result in sampleToEmit for the given window.
   */
  def apply(windowStart: Long, windowEnd: Long, sampleToEmit: R): Unit =
    apply(windowEnd, sampleToEmit)

  /**
   * Return the computed result in the sampleToEmit
   * @param endTimestamp the ending timestamp of the current window
   */
  def apply(endTimestamp: Long, sampleToEmit: R): Unit
}

/**
 * A ChunkedRangeFunction for Prom-style counters dealing with resets/corrections.
 * The algorithm relies on logic in the chunks to detect corrections, and carries over correction
 * values from chunk to chunk for correctness.  Data is assumed to be ordered.
 * For more details see [doc/query_engine.md]
 */
trait CounterChunkedRangeFunction[R <: MutableRowReader] extends ChunkedRangeFunction[R] {
  var correctionMeta: CorrectionMeta = NoCorrection

  // reset is called before first chunk.  Reset correction metadata
  override def reset(): Unit = { correctionMeta = NoCorrection }

  // scalastyle:off parameter.number
  def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVectorAcc: MemoryReader, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
    val ccReader = valueReader.asInstanceOf[CounterVectorReader]
    val startRowNum = tsReader.binarySearch(tsVectorAcc, tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)

    // For each chunk:
    // Check if any dropoff from end of last chunk to beg of this chunk (unless it's the first chunk)
    // Compute the carryover (ie adjusted correction amount)
    correctionMeta = ccReader.detectDropAndCorrection(valueVectorAcc, valueVector, correctionMeta)

    // At least one sample is present
    if (startRowNum <= endRowNum) {
      try {
        addTimeChunks(valueVectorAcc, valueVector, ccReader, startRowNum, endRowNum,
          tsReader(tsVectorAcc, tsVector, startRowNum), tsReader(tsVectorAcc, tsVector, endRowNum))
      } catch { case e: ArrayIndexOutOfBoundsException =>
        Query.qLogger.error(s"ArrayIndexOutOfBoundsException startRowNum=$startRowNum endRowNum=$endRowNum")
        throw e
      }
    }

    // Add any corrections from this chunk, pass on lastValue also to next chunk computation
    correctionMeta = ccReader.updateCorrection(valueVectorAcc, valueVector, correctionMeta)
  }

  /**
   * Implements the logic for processing chunked data given row numbers and times for the
   * start and end.
   */
  def addTimeChunks(acc: MemoryReader, vector: BinaryVectorPtr, reader: CounterVectorReader,
                    startRowNum: Int, endRowNum: Int,
                    startTime: Long, endTime: Long): Unit
}

/**
 * A trait for RangeFunctions that operate on both a start time and an end time.  Will find the start and end
 * row numbers for the chunk.
 */
trait TimeRangeFunction[R <: MutableRowReader] extends ChunkedRangeFunction[R] {
  // scalastyle:off parameter.number
  def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVectorAcc: MemoryReader, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
    // TODO: abstract this pattern of start/end row # out. Probably when cursors are implemented
    // First row >= startTime, so we can just drop bit 31 (dont care if it matches exactly)
    val startRowNum = tsReader.binarySearch(tsVectorAcc, tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)

    // At least one sample is present
    if (startRowNum <= endRowNum)
      addTimeChunks(valueVectorAcc, valueVector, valueReader, startRowNum, endRowNum)
  }

  def addTimeChunks(vectAcc: MemoryReader,
                    vectPtr: BinaryVector.BinaryVectorPtr,
                    reader: VectorDataReader,
                    startRowNum: Int,
                    endRowNum: Int): Unit
}

/**
 * Standard ChunkedRangeFunction implementation extracting the start and ending row numbers from the timestamp
 * and returning the double value vector and reader with the row numbers
 */
trait ChunkedDoubleRangeFunction extends TimeRangeFunction[TransientRow] {
  final def addTimeChunks(vectAcc: MemoryReader,
                          vectPtr: BinaryVector.BinaryVectorPtr,
                          reader: VectorDataReader,
                          startRowNum: Int,
                          endRowNum: Int): Unit =
    addTimeDoubleChunks(vectAcc, vectPtr, reader.asDoubleReader, startRowNum, endRowNum)

  /**
   * Add a Double BinaryVector in the range (startRowNum, endRowNum) to the range computation
   * @param startRowNum the row number for timestamp greater than or equal to startTime
   * @param endRowNum the row number with the timestamp <= endTime
   */
  def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
                          doubleVect: BinaryVector.BinaryVectorPtr,
                          doubleReader: bv.DoubleVectorDataReader,
                          startRowNum: Int,
                          endRowNum: Int): Unit
}

trait ChunkedLongRangeFunction extends TimeRangeFunction[TransientRow] {
  final def addTimeChunks(vectAcc: MemoryReader,
                          vectPtr: BinaryVector.BinaryVectorPtr,
                          reader: VectorDataReader,
                          startRowNum: Int,
                          endRowNum: Int): Unit =
    addTimeLongChunks(vectAcc, vectPtr, reader.asLongReader, startRowNum, endRowNum)

  /**
   * Add a Long BinaryVector in the range (startRowNum, endRowNum) to the range computation
   * @param startRowNum the row number for timestamp greater than or equal to startTime
   * @param endRowNum the row number with the timestamp <= endTime
   */
  def addTimeLongChunks(longVectAcc: MemoryReader,
                        longVect: BinaryVector.BinaryVectorPtr,
                        longReader: bv.LongVectorDataReader,
                        startRowNum: Int,
                        endRowNum: Int): Unit
}

object RangeFunction {
  type RangeFunctionGenerator = () => BaseRangeFunction

  import InternalRangeFunction._

  def downsampleColsFromRangeFunction(schema: Schema, f: Option[InternalRangeFunction]): Seq[String] = {
    f match {
      case None                   => Seq("avg")
      case Some(CountOverTime)    => Seq("count")
      case Some(Changes)          => Seq("avg")
      case Some(Delta)            => Seq("avg")
      case Some(Idelta)           => Seq("avg")
      case Some(Deriv)            => Seq("avg")
      case Some(HoltWinters)      => Seq("avg")
      case Some(PredictLinear)    => Seq("avg")
      case Some(SumOverTime)      => Seq("sum")
      case Some(AvgOverTime)      => Seq("sum", "count")
      case Some(AvgWithSumAndCountOverTime) => Seq("sum", "count")
      case Some(StdDevOverTime)   => Seq("avg")
      case Some(StdVarOverTime)   => Seq("avg")
      case Some(QuantileOverTime) => Seq("avg")
      case Some(MinOverTime)      => Seq("min")
      case Some(MaxOverTime)      => Seq("max")
      case Some(Last)             => Seq("avg")
      case other                  => Seq(schema.data.valueColName)
    }
  }

  // Convert range function for downsample schema
  def downsampleRangeFunction(f: Option[InternalRangeFunction]): Option[InternalRangeFunction] =
    f match {
      case Some(CountOverTime)    => Some(SumOverTime)
      case Some(AvgOverTime)      => Some(AvgWithSumAndCountOverTime)
      case other                  => other
    }

  /**
   * Returns a (probably new) instance of RangeFunction given the func ID and column type
   */
  def apply(schema: ResultSchema,
            func: Option[InternalRangeFunction],
            columnType: ColumnType,
            config: QueryConfig,
            funcParams: Seq[FuncArgs] = Nil,
            useChunked: Boolean): BaseRangeFunction =
    generatorFor(schema, func, columnType, config, funcParams, useChunked)()

  /**
   * Given a function type and column type, returns a RangeFunctionGenerator
   */
  def generatorFor(schema: ResultSchema,
                   func: Option[InternalRangeFunction],
                   columnType: ColumnType,
                   config: QueryConfig,
                   funcParams: Seq[FuncArgs] = Nil,
                   useChunked: Boolean = true): RangeFunctionGenerator = {
    if (useChunked) columnType match {
      case ColumnType.DoubleColumn => doubleChunkedFunction(schema, func, config, funcParams)
      case ColumnType.LongColumn => longChunkedFunction(schema, func, funcParams)
      case ColumnType.TimestampColumn => longChunkedFunction(schema, func, funcParams)
      case ColumnType.HistogramColumn => histChunkedFunction(schema, func, funcParams)
      case other: ColumnType => throw new IllegalArgumentException(s"Column type $other not supported")
    } else {
      iteratingFunction(func, schema, funcParams)
    }
  }

  /**
   * Returns a function to generate a ChunkedRangeFunction for Long columns
   */
  def longChunkedFunction(schema: ResultSchema,
                          func: Option[InternalRangeFunction],
                          funcParams: Seq[FuncArgs] = Nil): RangeFunctionGenerator = {
    func match {
      case None                                   => () => new LastSampleChunkedFunctionL
      case Some(CountOverTime)                    => () => new CountOverTimeChunkedFunction()
      case Some(SumOverTime)                      => () => new SumOverTimeChunkedFunctionL
      case Some(AvgWithSumAndCountOverTime)       => require(schema.columns(2).name == "count")
                                                     () => new AvgWithSumAndCountOverTimeFuncL(schema.colIDs(2))
      case Some(AvgOverTime)                      => () => new AvgOverTimeChunkedFunctionL
      case Some(MinOverTime)                      => () => new MinOverTimeChunkedFunctionL
      case Some(MaxOverTime)                      => () => new MaxOverTimeChunkedFunctionL
      case Some(StdDevOverTime)                   => () => new StdDevOverTimeChunkedFunctionL
      case Some(StdVarOverTime)                   => () => new StdVarOverTimeChunkedFunctionL
      case Some(Changes)                          => () => new ChangesChunkedFunctionL
      case Some(QuantileOverTime)                 => () => new QuantileOverTimeChunkedFunctionL(funcParams)
      case Some(PredictLinear)                    => () => new PredictLinearChunkedFunctionL(funcParams)
      case Some(Last)                             => () => new LastSampleChunkedFunctionL
      case Some(MedianAbsoluteDeviationOverTime)  => () => new MedianAbsoluteDeviationOverTimeChunkedFunctionL()
      case _                                      => iteratingFunction(func, schema, funcParams)
    }
  }

  /**
   * Returns a function to generate a ChunkedRangeFunction for Double columns
   */
  // scalastyle:off cyclomatic.complexity
  def doubleChunkedFunction(schema: ResultSchema,
                            func: Option[InternalRangeFunction],
                            config: QueryConfig,
                            funcParams: Seq[FuncArgs] = Nil): RangeFunctionGenerator = {
    func match {
      case None                                   => () => new LastSampleChunkedFunctionD
      case Some(Last)                             => () => new LastSampleChunkedFunctionD
      case Some(Increase) if config.fasterRateEnabled && schema.columns(1).isCumulative
                                                  => () => new ChunkedIncreaseFunction
      case Some(Rate) if config.fasterRateEnabled && schema.columns(1).isCumulative
                                                  => () => new ChunkedRateFunction
      case Some(Increase) if !schema.columns(1).isCumulative
                                                  => () => new SumOverTimeChunkedFunctionD
      case Some(Rate)     if !schema.columns(1).isCumulative
                                                  => () => new RateOverDeltaChunkedFunctionD

      case Some(CountOverTime)                    => () => new CountOverTimeChunkedFunctionD()
      case Some(SumOverTime)                      => () => new SumOverTimeChunkedFunctionD
      case Some(AvgWithSumAndCountOverTime)
                                                  => require(schema.columns(2).name == "count")
                                     () => new AvgWithSumAndCountOverTimeFuncD(schema.colIDs(2))
      case Some(AvgOverTime)                      => () => new AvgOverTimeChunkedFunctionD
      case Some(MinOverTime)                      => () => new MinOverTimeChunkedFunctionD
      case Some(MaxOverTime)                      => () => new MaxOverTimeChunkedFunctionD
      case Some(StdDevOverTime)                   => () => new StdDevOverTimeChunkedFunctionD
      case Some(StdVarOverTime)                   => () => new StdVarOverTimeChunkedFunctionD
      case Some(Changes)                          => () => new ChangesChunkedFunctionD()
      case Some(QuantileOverTime)                 => () => new QuantileOverTimeChunkedFunctionD(funcParams)
      case Some(HoltWinters)                      => () => new HoltWintersChunkedFunctionD(funcParams)
      case Some(Timestamp)                        => () => new TimestampChunkedFunction()
      case Some(ZScore)                           => () => new ZScoreChunkedFunctionD()
      case Some(PredictLinear)                    => () => new PredictLinearChunkedFunctionD(funcParams)
      case Some(PresentOverTime)                  => () => new PresentOverTimeChunkedFunctionD()
      case Some(MedianAbsoluteDeviationOverTime)  => () => new MedianAbsoluteDeviationOverTimeChunkedFunctionD()
      case _                                      => iteratingFunction(func, schema, funcParams)
    }
  }
  // scalastyle:on cyclomatic.complexity

  def histMaxMinRangeFunction(f: Option[InternalRangeFunction]): Option[InternalRangeFunction] =
    f match {
      case None                   => Some(LastSampleHistMaxMin)
      case Some(SumOverTime)      => Some(SumAndMaxOverTime)
      case Some(Rate)             => Some(RateAndMinMaxOverTime)
      case other                  => other
    }

  def histChunkedFunction(schema: ResultSchema,
                          func: Option[InternalRangeFunction],
                          funcParams: Seq[FuncArgs] = Nil): RangeFunctionGenerator = func match {
    case None                 => () => new LastSampleChunkedFunctionH
    case Some(LastSampleHistMaxMin) => require(schema.columns(2).name == "max" && schema.columns(3).name == "min")
                                 () => new LastSampleChunkedFunctionHMax(schema.colIDs(2), schema.colIDs(3))
    case Some(SumAndMaxOverTime) => require(schema.columns(2).name == "max")
                                 () => new SumAndMaxOverTimeFuncHD(schema.colIDs(2))
    case Some(RateAndMinMaxOverTime) => require(schema.columns(2).name == "max" && schema.columns(3).name == "min")
                                 () => new RateAndMinMaxOverTimeFuncHD(schema.colIDs(2), schema.colIDs(3))
    case Some(Last)           => () => new LastSampleChunkedFunctionH
    case Some(SumOverTime)    => () => new SumOverTimeChunkedFunctionH
    case Some(Rate) if schema.columns(1).isCumulative
                              => () => new HistRateFunction
    case Some(Increase) if schema.columns(1).isCumulative
                              => () => new HistIncreaseFunction
    case Some(Rate)           => () => new RateOverDeltaChunkedFunctionH
    case Some(Increase)       => () => new SumOverTimeChunkedFunctionH
    case _                    => ??? //TODO enumerate all possible cases
  }

  /**
   * Returns a function to generate the RangeFunction for SlidingWindowIterator.
   * Note that these functions are Double-based, so a converting iterator eg LongToDoubleIterator may be needed.
   */
  // scalastyle:off cyclomatic.complexity
  def iteratingFunction(func: Option[InternalRangeFunction],
                        schema: ResultSchema,
                        funcParams: Seq[Any] = Nil): RangeFunctionGenerator = func match {
    // when no window function is asked, use last sample for instant
    case None                                   => () => LastSampleFunction
    case Some(Last)                             => () => LastSampleFunction
    case Some(Rate) if schema.columns(1).isCumulative
                                                => () => RateFunction
    case Some(Increase) if schema.columns(1).isCumulative
                                                => () => IncreaseFunction
    case Some(Rate)                             => () => new RateOverDeltaFunction()
    case Some(Increase)                         => () => new SumOverTimeFunction() // Sum of deltas over time
    case Some(Delta)                            => () => DeltaFunction
    case Some(Resets)                           => () => new ResetsFunction()
    case Some(Irate) if schema.columns(1).isCumulative
                                                => () => IRateFunction
    case Some(Idelta)                           => () => IDeltaFunction
    case Some(Irate)                            => () => IRatePeriodicFunction
    case Some(Deriv)                            => () => DerivFunction
    case Some(MaxOverTime)                      => () => new MinMaxOverTimeFunction(Ordering[Double])
    case Some(MinOverTime)                      => () => new MinMaxOverTimeFunction(Ordering[Double].reverse)
    case Some(CountOverTime)                    => () => new CountOverTimeFunction()
    case Some(SumOverTime)                      => () => new SumOverTimeFunction()
    case Some(AvgOverTime)                      => () => new AvgOverTimeFunction()
    case Some(StdDevOverTime)                   => () => new StdDevOverTimeFunction()
    case Some(StdVarOverTime)                   => () => new StdVarOverTimeFunction()
    case Some(Changes)                          => () => ChangesOverTimeFunction
    case Some(QuantileOverTime)                 => () => new QuantileOverTimeFunction(funcParams)
    case Some(MedianAbsoluteDeviationOverTime)  => () => new MedianAbsoluteDeviationOverTimeFunction(funcParams)
    case Some(LastOverTimeIsMadOutlier)         => () => new LastOverTimeIsMadOutlierFunction(funcParams)
    case _                                      => ??? //TODO enumerate all possible cases
  }
}

object LastSampleFunction extends RangeFunction[TransientRow] {
  def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}
  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window[TransientRow],
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    for (i <- (window.size - 1) to 0 by -1) {
      val row = window.apply(i)
      val rowValue = row.getDouble(1)
      if (!rowValue.isNaN ) {
        sampleToEmit.setValues(endTimestamp, rowValue)
        return
      }
    }
    sampleToEmit.setValues(endTimestamp, Double.NaN)
  }
}

/**
 * Directly obtain the last sample from chunks for much much faster performance compared to above
 * TODO: one day we should expose higher level means of returning the actual Long value instead of converting to Double
 */
abstract class LastSampleChunkedFunction[R <: MutableRowReader](var timestamp: Long = -1L)
extends ChunkedRangeFunction[R] {
  // Add each chunk and update timestamp and value such that latest sample wins
  // scalastyle:off parameter.number
  def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVectorAcc: MemoryReader, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
    // Just in case timestamp vectors are a bit longer than others.
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)

    // update timestamp only if
    //   1) endRowNum >= 0 (timestamp within chunk)
    //   2) timestamp is within window; AND
    //   3) timestamp is greater than current timestamp (for multiple chunk scenarios)
    if (endRowNum >= 0) {
      val ts = tsReader(tsVectorAcc, tsVector, endRowNum)
      if (ts >= startTime && ts > timestamp)
        updateValue(ts, valueVectorAcc, valueVector, valueReader, endRowNum)
    }
  }

  def updateValue(ts: Long, valAcc: MemoryReader, valVector: BinaryVectorPtr,
                  valReader: VectorDataReader, endRowNum: Int): Unit
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
  def updateValue(ts: Long, valAcc: MemoryReader, valVector: BinaryVectorPtr,
                  valReader: VectorDataReader, endRowNum: Int): Unit = {
    timestamp = ts
    value = valReader.asHistReader(endRowNum)
  }
}

class LastSampleChunkedFunctionHMax(maxColID: Int,
                                    minColId: Int,
                                    var timestamp: Long = -1L,
                                    var value: bv.HistogramWithBuckets = bv.Histogram.empty,
                                    var max: Double = Double.NaN,
                                    var min: Double = Double.NaN) extends ChunkedRangeFunction[TransientHistMaxMinRow] {
  override final def reset(): Unit = { timestamp = -1L; value = bv.Histogram.empty; max = Double.NaN; min = Double.NaN }
  final def apply(endTimestamp: Long, sampleToEmit: TransientHistMaxMinRow): Unit = {
    sampleToEmit.setValues(endTimestamp, value)
    sampleToEmit.setDouble(2, max)
    sampleToEmit.setDouble(3, min)
  }

  // Add each chunk and update timestamp and value such that latest sample wins
  // scalastyle:off parameter.number
  def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVectorAcc: MemoryReader, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
    // Just in case timestamp vectors are a bit longer than others.
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)

    // update timestamp only if
    //   1) endRowNum >= 0 (timestamp within chunk)
    //   2) timestamp is within window; AND
    //   3) timestamp is greater than current timestamp (for multiple chunk scenarios)
    if (endRowNum >= 0) {
      val ts = tsReader(tsVectorAcc, tsVector, endRowNum)
      if (ts >= startTime && ts > timestamp) {
        val maxvectAcc = info.vectorAccessor(maxColID)
        val maxVectOff = info.vectorAddress(maxColID)
        val minvectAcc = info.vectorAccessor(minColId)
        val minVectOff = info.vectorAddress(minColId)
        timestamp = ts
        value = valueReader.asHistReader(endRowNum)
        max = bv.DoubleVector(maxvectAcc, maxVectOff)(maxvectAcc, maxVectOff, endRowNum)
        min = bv.DoubleVector(minvectAcc, minVectOff)(minvectAcc, minVectOff, endRowNum)
      }
    }
  }
}

class LastSampleChunkedFunctionD extends LastSampleChunkedFuncDblVal() {
  def updateValue(ts: Long, valAcc: MemoryReader, valVector: BinaryVectorPtr,
                  valReader: VectorDataReader, endRowNum: Int): Unit = {
    val dblReader = valReader.asDoubleReader
    val doubleVal = dblReader(valAcc, valVector, endRowNum)
    // If the last value is NaN, that may be Prometheus end of time series marker.
    // In that case try to get the sample before last.
    // If endRowNum==0, we are at beginning of chunk, and if the window included the last chunk, then
    // the call to addChunks to the last chunk would have gotten the last sample value anyways.
    if (java.lang.Double.isNaN(doubleVal)) {
      if (endRowNum > 0) {
        timestamp = ts
        value = dblReader(valAcc, valVector, endRowNum - 1)
      }
    } else {
      timestamp = ts
      value = doubleVal
    }
  }
}

class LastSampleChunkedFunctionL extends LastSampleChunkedFuncDblVal() {
  def updateValue(ts: Long, valAcc: MemoryReader, valVector: BinaryVectorPtr,
                  valReader: VectorDataReader, endRowNum: Int): Unit = {
    val longReader = valReader.asLongReader
    timestamp = ts
    value = longReader(valAcc, valVector, endRowNum).toDouble
  }
}

class TimestampChunkedFunction (var value: Double = Double.NaN) extends ChunkedRangeFunction[TransientRow] {
  def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVectorAcc: MemoryReader, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)

    if (endRowNum >= 0) {
      val ts = tsReader(tsVectorAcc, tsVector, endRowNum)
      // Timestamp value should be in seconds
      value = (ts.toDouble / 1000f)
    }
  }

  override final def reset(): Unit = { value = Double.NaN }

  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, value)
  }
}

class PresentOverTimeChunkedFunctionD extends LastSampleChunkedFuncDblVal() {
  def updateValue(ts: Long, valAcc: MemoryReader, valVector: BinaryVectorPtr,
                  valReader: VectorDataReader, endRowNum: Int): Unit = {
    val dblReader = valReader.asDoubleReader
    val doubleVal = dblReader(valAcc, valVector, endRowNum)
    // If the last value is NaN, that may be Prometheus end of time series marker.
    // In that case try to get the sample before last.
    // If endRowNum==0, we are at beginning of chunk, and if the window included the last chunk, then
    // the call to addChunks to the last chunk would have gotten the last sample value anyways.
    if (java.lang.Double.isNaN(doubleVal)) {
      if (endRowNum > 0) {
        timestamp = ts
        val lastVal = dblReader(valAcc, valVector, endRowNum - 1)
        value = if (lastVal.isNaN) Double.NaN else 1
      }
    } else {
      timestamp = ts
      value = 1
    }
  }
}