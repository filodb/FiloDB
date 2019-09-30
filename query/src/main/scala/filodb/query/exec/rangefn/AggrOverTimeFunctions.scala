package filodb.query.exec.rangefn

import java.lang.{Double => JLDouble}
import java.util

import filodb.core.store.ChunkSetInfo
import filodb.memory.format.{vectors => bv, BinaryVector, VectorDataReader}
import filodb.query.QueryConfig
import filodb.query.exec.{TransientHistMaxRow, TransientHistRow, TransientRow}

class MinMaxOverTimeFunction(ord: Ordering[Double]) extends RangeFunction {
  val minMaxDeque = new util.ArrayDeque[TransientRow]()

  override def addedToWindow(row: TransientRow, window: Window): Unit = {
    while (!minMaxDeque.isEmpty && ord.compare(minMaxDeque.peekLast().value, row.value) < 0) minMaxDeque.removeLast()
    minMaxDeque.addLast(row)
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
    while (!minMaxDeque.isEmpty && minMaxDeque.peekFirst().timestamp <= row.timestamp) minMaxDeque.removeFirst()
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    if (minMaxDeque.isEmpty) sampleToEmit.setValues(endTimestamp, Double.NaN)
    else sampleToEmit.setValues(endTimestamp, minMaxDeque.peekFirst().value)
  }
}

class MinOverTimeChunkedFunctionD(var min: Double = Double.NaN) extends ChunkedDoubleRangeFunction {
  override final def reset(): Unit = { min = Double.NaN }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, min)
  }
  final def addTimeDoubleChunks(doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    var rowNum = startRowNum
    val it = doubleReader.iterate(doubleVect, startRowNum)
    while (rowNum <= endRowNum) {
      val nextVal = it.next
      min = if (min.isNaN) nextVal else Math.min(min, nextVal)
      rowNum += 1
    }
  }
}

class MinOverTimeChunkedFunctionL(var min: Long = Long.MaxValue) extends ChunkedLongRangeFunction {
  override final def reset(): Unit = { min = Long.MaxValue }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, min.toDouble)
  }
  final def addTimeLongChunks(longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    var rowNum = startRowNum
    val it = longReader.iterate(longVect, startRowNum)
    while (rowNum <= endRowNum) {
      min = Math.min(min, it.next)
      rowNum += 1
    }
  }
}

class MaxOverTimeChunkedFunctionD(var max: Double = Double.NaN) extends ChunkedDoubleRangeFunction {
  override final def reset(): Unit = { max = Double.NaN }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, max)
  }
  final def addTimeDoubleChunks(doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    var rowNum = startRowNum
    val it = doubleReader.iterate(doubleVect, startRowNum)
    while (rowNum <= endRowNum) {
      val nextVal = it.next
      max = if (max.isNaN) nextVal else Math.max(max, nextVal) // cannot compare NaN, always < anything else
      rowNum += 1
    }
  }
}

class MaxOverTimeChunkedFunctionL(var max: Long = Long.MinValue) extends ChunkedLongRangeFunction {
  override final def reset(): Unit = { max = Long.MinValue }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, max.toDouble)
  }
  final def addTimeLongChunks(longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    var rowNum = startRowNum
    val it = longReader.iterate(longVect, startRowNum)
    while (rowNum <= endRowNum) {
      max = Math.max(max, it.next)
      rowNum += 1
    }
  }
}

class SumOverTimeFunction(var sum: Double = Double.NaN, var count: Int = 0) extends RangeFunction {
  override def addedToWindow(row: TransientRow, window: Window): Unit = {
    if (!JLDouble.isNaN(row.value)) {
      if (sum.isNaN) {
        sum = 0d
      }
      sum += row.value
      count += 1
    }
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
    if (!JLDouble.isNaN(row.value)) {
      if (sum.isNaN) {
        sum = 0d
      }
      sum -= row.value
      count -= 1
      if (count == 0) { // There is no value in window
        sum = Double.NaN
      }
    }
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    sampleToEmit.setValues(endTimestamp, sum)
  }
}

abstract class SumOverTimeChunkedFunction(var sum: Double = Double.NaN) extends ChunkedRangeFunction[TransientRow] {
  override final def reset(): Unit = { sum = Double.NaN }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, sum)
  }
}

class SumOverTimeChunkedFunctionD extends SumOverTimeChunkedFunction() with ChunkedDoubleRangeFunction {
  final def addTimeDoubleChunks(doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    // NaN values are ignored by default in the sum method
    if (sum.isNaN) {
      sum = 0d
    }
    sum += doubleReader.sum(doubleVect, startRowNum, endRowNum)
  }
}

class SumOverTimeChunkedFunctionL extends SumOverTimeChunkedFunction() with ChunkedLongRangeFunction {
  final def addTimeLongChunks(longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    if (sum.isNaN) {
      sum = 0d
    }
    sum += longReader.sum(longVect, startRowNum, endRowNum)
  }
}

class SumOverTimeChunkedFunctionH(var h: bv.MutableHistogram = bv.Histogram.empty)
extends TimeRangeFunction[TransientHistRow] {
  override final def reset(): Unit = { h = bv.Histogram.empty }
  final def apply(endTimestamp: Long, sampleToEmit: TransientHistRow): Unit = {
    sampleToEmit.setValues(endTimestamp, h)
  }

  final def addTimeChunks(vectPtr: BinaryVector.BinaryVectorPtr,
                          reader: VectorDataReader,
                          startRowNum: Int,
                          endRowNum: Int): Unit = {
    val sum = reader.asHistReader.sum(startRowNum, endRowNum)
    h match {
      // sum is mutable histogram, copy to be sure it's our own copy
      case hist if hist.numBuckets == 0 => h = sum.copy
      case hist: bv.MutableHistogram    => hist.add(sum)
    }
  }
}

/**
 * Sums Histograms over time and also computes Max over time of a Max field.
 * @param maxColID the data column ID containing the max column
 */
class SumAndMaxOverTimeFuncHD(maxColID: Int) extends ChunkedRangeFunction[TransientHistMaxRow] {
  private val hFunc = new SumOverTimeChunkedFunctionH
  private val maxFunc = new MaxOverTimeChunkedFunctionD

  override final def reset(): Unit = {
    hFunc.reset()
    maxFunc.reset()
  }
  final def apply(endTimestamp: Long, sampleToEmit: TransientHistMaxRow): Unit = {
    sampleToEmit.setValues(endTimestamp, hFunc.h)
    sampleToEmit.setDouble(2, maxFunc.max)
  }

  import BinaryVector.BinaryVectorPtr

  final def addChunks(tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                      valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                      startTime: Long, endTime: Long, info: ChunkSetInfo, queryConfig: QueryConfig): Unit = {
    // Do BinarySearch for start/end pos only once for both columns == WIN!
    val startRowNum = tsReader.binarySearch(tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVector, endTime), info.numRows - 1)

    // At least one sample is present
    if (startRowNum <= endRowNum) {
      hFunc.addTimeChunks(valueVector, valueReader, startRowNum, endRowNum)

      // Get valueVector/reader for max column
      val maxVectPtr = info.vectorPtr(maxColID)
      maxFunc.addTimeChunks(maxVectPtr, bv.DoubleVector(maxVectPtr), startRowNum, endRowNum)
    }
  }
}

/**
  * Computes Average Over Time using sum and count columns.
  * Used in when calculating avg_over_time using downsampled data
  */
class AvgWithSumAndCountOverTimeFuncD(countColId: Int) extends ChunkedRangeFunction[TransientRow] {
  private val sumFunc = new SumOverTimeChunkedFunctionD
  private val countFunc = new SumOverTimeChunkedFunctionD

  override final def reset(): Unit = {
    sumFunc.reset()
    countFunc.reset()
  }

  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, sumFunc.sum / countFunc.sum)
  }

  import BinaryVector.BinaryVectorPtr

  final def addChunks(tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                      valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                      startTime: Long, endTime: Long, info: ChunkSetInfo, queryConfig: QueryConfig): Unit = {
    // Do BinarySearch for start/end pos only once for both columns == WIN!
    val startRowNum = tsReader.binarySearch(tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVector, endTime), info.numRows - 1)

    // At least one sample is present
    if (startRowNum <= endRowNum) {
      sumFunc.addTimeChunks(valueVector, valueReader, startRowNum, endRowNum)

      // Get valueVector/reader for count column
      val countVectPtr = info.vectorPtr(countColId)
      countFunc.addTimeChunks(countVectPtr, bv.DoubleVector(countVectPtr), startRowNum, endRowNum)
    }
  }
}

/**
  * Computes Average Over Time using sum and count columns.
  * Used in when calculating avg_over_time using downsampled data
  */
class AvgWithSumAndCountOverTimeFuncL(countColId: Int) extends ChunkedRangeFunction[TransientRow] {
  private val sumFunc = new SumOverTimeChunkedFunctionL
  private val countFunc = new CountOverTimeChunkedFunction

  override final def reset(): Unit = {
    sumFunc.reset()
    countFunc.reset()
  }

  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, sumFunc.sum / countFunc.count)
  }

  import BinaryVector.BinaryVectorPtr

  final def addChunks(tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                      valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                      startTime: Long, endTime: Long, info: ChunkSetInfo, queryConfig: QueryConfig): Unit = {
    // Do BinarySearch for start/end pos only once for both columns == WIN!
    val startRowNum = tsReader.binarySearch(tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVector, endTime), info.numRows - 1)

    // At least one sample is present
    if (startRowNum <= endRowNum) {
      sumFunc.addTimeChunks(valueVector, valueReader, startRowNum, endRowNum)

      // Get valueVector/reader for count column
      val cntVectPtr = info.vectorPtr(countColId)
      countFunc.addTimeChunks(cntVectPtr, bv.DoubleVector(cntVectPtr), startRowNum, endRowNum)
    }
  }
}

class CountOverTimeFunction(var count: Double = Double.NaN) extends RangeFunction {
  override def addedToWindow(row: TransientRow, window: Window): Unit = {
    if (!JLDouble.isNaN(row.value)) {
      if (count.isNaN) {
        count = 0d
      }
      count += 1
    }
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
    if (!JLDouble.isNaN(row.value)) {
      if (count.isNaN) {
        count = 0d
      }
      count -= 1
      if (count==0) { //Reset count as no sample is present
        count = Double.NaN
      }
    }
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    sampleToEmit.setValues(endTimestamp, count)
  }
}

class CountOverTimeChunkedFunction(var count: Int = 0) extends TimeRangeFunction[TransientRow] {
  override final def reset(): Unit = { count = 0 }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, count.toDouble)
  }

  def addTimeChunks(vectPtr: BinaryVector.BinaryVectorPtr,
                    reader: VectorDataReader,
                    startRowNum: Int,
                    endRowNum: Int): Unit = {
    val numRows = endRowNum - startRowNum + 1
    count += numRows
  }
}

// Special count_over_time chunked function for doubles needed to not count NaNs whih are used by
// Prometheus to mark end of a time series.
// TODO: handle end of time series a different, better way.  This function shouldn't be needed.
class CountOverTimeChunkedFunctionD(var count: Double = Double.NaN) extends ChunkedDoubleRangeFunction {
  override final def reset(): Unit = { count = Double.NaN }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, count)
  }
  final def addTimeDoubleChunks(doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    if (count.isNaN) {
      count = 0d
    }
    count += doubleReader.count(doubleVect, startRowNum, endRowNum)
  }
}

class AvgOverTimeFunction(var sum: Double = Double.NaN, var count: Int = 0) extends RangeFunction {
  override def addedToWindow(row: TransientRow, window: Window): Unit = {
    if (!JLDouble.isNaN(row.value)) {
      if (sum.isNaN) {
        sum = 0d;
      }
      sum += row.value
      count += 1
    }
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
    if (!JLDouble.isNaN(row.value)) {
      if (sum.isNaN) {
        sum = 0d;
      }
      sum -= row.value
      count -= 1
    }
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    sampleToEmit.setValues(endTimestamp, sum/count)
  }
}

abstract class AvgOverTimeChunkedFunction(var sum: Double = Double.NaN, var count: Double = 0)
  extends ChunkedRangeFunction[TransientRow] {
  override final def reset(): Unit = {
    sum = Double.NaN;
    count = 0d
  }

  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, if (count > 0) sum/count else if (sum.isNaN()) sum else 0d)
  }
}

class AvgOverTimeChunkedFunctionD extends AvgOverTimeChunkedFunction() with ChunkedDoubleRangeFunction {
  final def addTimeDoubleChunks(doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    if (sum.isNaN) {
      sum = 0d
    }
    sum += doubleReader.sum(doubleVect, startRowNum, endRowNum)
    count += doubleReader.count(doubleVect, startRowNum, endRowNum)
  }
}

class AvgOverTimeChunkedFunctionL extends AvgOverTimeChunkedFunction() with ChunkedLongRangeFunction {
  final def addTimeLongChunks(longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    sum += longReader.sum(longVect, startRowNum, endRowNum)
    count += (endRowNum - startRowNum + 1)
  }
}

class StdDevOverTimeFunction(var sum: Double = 0d,
                             var count: Int = 0,
                             var squaredSum: Double = 0d) extends RangeFunction {
  override def addedToWindow(row: TransientRow, window: Window): Unit = {
    sum += row.value
    squaredSum += row.value * row.value
    count += 1
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
    sum -= row.value
    squaredSum -= row.value * row.value
    count -= 1
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    val avg = sum/count
    val stdDev = Math.sqrt(squaredSum/count - avg*avg)
    sampleToEmit.setValues(endTimestamp, stdDev)
  }
}

class StdVarOverTimeFunction(var sum: Double = 0d,
                             var count: Int = 0,
                             var squaredSum: Double = 0d) extends RangeFunction {
  override def addedToWindow(row: TransientRow, window: Window): Unit = {
    sum += row.value
    squaredSum += row.value * row.value
    count += 1
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
    sum -= row.value
    squaredSum -= row.value * row.value
    count -= 1
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    val avg = sum/count
    val stdVar = squaredSum/count - avg*avg
    sampleToEmit.setValues(endTimestamp, stdVar)
  }
}

abstract class VarOverTimeChunkedFunctionD(var sum: Double = 0d,
                                           var count: Int = 0,
                                           var squaredSum: Double = 0d) extends ChunkedDoubleRangeFunction {
  override final def reset(): Unit = { sum = 0d; count = 0; squaredSum = 0d }
  final def addTimeDoubleChunks(doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    val it = doubleReader.iterate(doubleVect, startRowNum)
    var _sum = 0d
    var _sqSum = 0d
    var elemNo = startRowNum
    while (elemNo <= endRowNum) {
      val nextValue = it.next
      if (!JLDouble.isNaN(nextValue)) {
        _sum += nextValue
        _sqSum += nextValue * nextValue
        elemNo += 1
      }
    }
    count += (endRowNum - startRowNum + 1)
    sum += _sum
    squaredSum += _sqSum
  }
}

class StdDevOverTimeChunkedFunctionD extends VarOverTimeChunkedFunctionD() {
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    val avg = if (count > 0) sum/count else 0d
    val stdDev = Math.sqrt(squaredSum/count - avg*avg)
    sampleToEmit.setValues(endTimestamp, stdDev)
  }
}

class StdVarOverTimeChunkedFunctionD extends VarOverTimeChunkedFunctionD() {
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    val avg = if (count > 0) sum/count else 0d
    val stdVar = squaredSum/count - avg*avg
    sampleToEmit.setValues(endTimestamp, stdVar)
  }
}

abstract class VarOverTimeChunkedFunctionL(var sum: Double = 0d,
                                           var count: Int = 0,
                                           var squaredSum: Double = 0d) extends ChunkedLongRangeFunction {
  override final def reset(): Unit = { sum = 0d; count = 0; squaredSum = 0d }
  final def addTimeLongChunks(longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    val it = longReader.iterate(longVect, startRowNum)
    var _sum = 0d
    var _sqSum = 0d
    var elemNo = startRowNum
    while (elemNo <= endRowNum) {
      val nextValue = it.next.toDouble
      _sum += nextValue
      _sqSum += nextValue * nextValue
      elemNo += 1
    }
    count += (endRowNum - startRowNum + 1)
    sum += _sum
    squaredSum += _sqSum
  }
}

class StdDevOverTimeChunkedFunctionL extends VarOverTimeChunkedFunctionL() {
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    val avg = if (count > 0) sum/count else 0d
    val stdDev = Math.sqrt(squaredSum/count - avg*avg)
    sampleToEmit.setValues(endTimestamp, stdDev)
  }
}

class StdVarOverTimeChunkedFunctionL extends VarOverTimeChunkedFunctionL() {
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    val avg = if (count > 0) sum/count else 0d
    val stdVar = squaredSum/count - avg*avg
    sampleToEmit.setValues(endTimestamp, stdVar)
  }
}

abstract class ChangesChunkedFunction(var changes: Double = Double.NaN) extends ChunkedRangeFunction[TransientRow] {
  override final def reset(): Unit = { changes = Double.NaN }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, changes)
  }
}

class ChangesChunkedFunctionD() extends ChangesChunkedFunction() with
  ChunkedDoubleRangeFunction {
  final def addTimeDoubleChunks(doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    if (changes.isNaN) {
      changes = 0d
    }
    changes += doubleReader.changes(doubleVect, startRowNum, endRowNum)
  }
}

// scalastyle:off
class ChangesChunkedFunctionL extends ChangesChunkedFunction with
  ChunkedLongRangeFunction{
  final def addTimeLongChunks(longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    if (changes.isNaN) {
      changes = 0d
    }
    changes += longReader.changes(longVect, startRowNum, endRowNum)
  }
}