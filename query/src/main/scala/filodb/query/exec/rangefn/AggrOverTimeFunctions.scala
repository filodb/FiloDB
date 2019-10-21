package filodb.query.exec.rangefn

import java.lang.{Double => JLDouble}

import debox.Buffer
import java.util

import filodb.core.store.ChunkSetInfoT
import filodb.memory.format.{BinaryVector, MemoryAccessor, VectorDataReader}
import filodb.memory.format.{vectors => bv}
import filodb.memory.format.vectors.DoubleIterator
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
  final def addTimeDoubleChunks(doubleVectAcc: MemoryAccessor,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    var rowNum = startRowNum
    val it = doubleReader.iterate(doubleVectAcc, doubleVect, startRowNum)
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
  final def addTimeLongChunks(longVectAcc: MemoryAccessor,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    var rowNum = startRowNum
    val it = longReader.iterate(longVectAcc, longVect, startRowNum)
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
  final def addTimeDoubleChunks(doubleVectAcc: MemoryAccessor,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    var rowNum = startRowNum
    val it = doubleReader.iterate(doubleVectAcc, doubleVect, startRowNum)
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
  final def addTimeLongChunks(longVectAcc: MemoryAccessor,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    var rowNum = startRowNum
    val it = longReader.iterate(longVectAcc, longVect, startRowNum)
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
  final def addTimeDoubleChunks(doubleVectAcc: MemoryAccessor,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    // NaN values are ignored by default in the sum method
    if (sum.isNaN) {
      sum = 0d
    }
    sum += doubleReader.sum(doubleVectAcc, doubleVect, startRowNum, endRowNum)
  }
}

class SumOverTimeChunkedFunctionL extends SumOverTimeChunkedFunction() with ChunkedLongRangeFunction {
  final def addTimeLongChunks(longVectAcc: MemoryAccessor,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    if (sum.isNaN) {
      sum = 0d
    }
    sum += longReader.sum(longVectAcc, longVect, startRowNum, endRowNum)
  }
}

class SumOverTimeChunkedFunctionH(var h: bv.MutableHistogram = bv.Histogram.empty)
extends TimeRangeFunction[TransientHistRow] {
  override final def reset(): Unit = { h = bv.Histogram.empty }
  final def apply(endTimestamp: Long, sampleToEmit: TransientHistRow): Unit = {
    sampleToEmit.setValues(endTimestamp, h)
  }

  final def addTimeChunks(vectAcc: MemoryAccessor,
                          vectPtr: BinaryVector.BinaryVectorPtr,
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

  // scalastyle:off parameter.number
  def addChunks(tsVectorAcc: MemoryAccessor, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVectorAcc: MemoryAccessor, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfoT, queryConfig: QueryConfig): Unit = {
    // Do BinarySearch for start/end pos only once for both columns == WIN!
    val startRowNum = tsReader.binarySearch(tsVectorAcc, tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)

    // At least one sample is present
    if (startRowNum <= endRowNum) {
      hFunc.addTimeChunks(valueVectorAcc, valueVector, valueReader, startRowNum, endRowNum)

      // Get valueVector/reader for max column
      val maxVectAcc = info.vectorAccessor(maxColID)
      val maxVectPtr = info.vectorAddress(maxColID)
      maxFunc.addTimeChunks(maxVectAcc, maxVectPtr, bv.DoubleVector(maxVectAcc, maxVectPtr), startRowNum, endRowNum)
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

  // scalastyle:off parameter.number
  def addChunks(tsVectorAcc: MemoryAccessor, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVectorAcc: MemoryAccessor, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfoT, queryConfig: QueryConfig): Unit = {
    // Do BinarySearch for start/end pos only once for both columns == WIN!
    val startRowNum = tsReader.binarySearch(tsVectorAcc, tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)

    // At least one sample is present
    if (startRowNum <= endRowNum) {
      sumFunc.addTimeChunks(valueVectorAcc, valueVector, valueReader, startRowNum, endRowNum)

      // Get valueVector/reader for count column
      val countVectAcc = info.vectorAccessor(countColId)
      val countVectPtr = info.vectorAddress(countColId)
      countFunc.addTimeChunks(countVectAcc, countVectPtr,
        bv.DoubleVector(countVectAcc, countVectPtr), startRowNum, endRowNum)
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

  // scalastyle:off parameter.number
  def addChunks(tsVectorAcc: MemoryAccessor, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVectorAcc: MemoryAccessor, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfoT, queryConfig: QueryConfig): Unit = {
    // Do BinarySearch for start/end pos only once for both columns == WIN!
    val startRowNum = tsReader.binarySearch(tsVectorAcc, tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)

    // At least one sample is present
    if (startRowNum <= endRowNum) {
      sumFunc.addTimeChunks(valueVectorAcc, valueVector, valueReader, startRowNum, endRowNum)

      // Get valueVector/reader for count column
      val cntVectAcc = info.vectorAccessor(countColId)
      val cntVectPtr = info.vectorAddress(countColId)
      countFunc.addTimeChunks(cntVectAcc, cntVectPtr, bv.DoubleVector(cntVectAcc, cntVectPtr), startRowNum, endRowNum)
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

  def addTimeChunks(vectAcc: MemoryAccessor,
                    vectPtr: BinaryVector.BinaryVectorPtr,
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
  final def addTimeDoubleChunks(doubleVectAcc: MemoryAccessor,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    if (count.isNaN) {
      count = 0d
    }
    count += doubleReader.count(doubleVectAcc, doubleVect, startRowNum, endRowNum)
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
  final def addTimeDoubleChunks(doubleVectAcc: MemoryAccessor,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    if (sum.isNaN) {
      sum = 0d
    }
    sum += doubleReader.sum(doubleVectAcc, doubleVect, startRowNum, endRowNum)
    count += doubleReader.count(doubleVectAcc, doubleVect, startRowNum, endRowNum)
  }
}

class AvgOverTimeChunkedFunctionL extends AvgOverTimeChunkedFunction() with ChunkedLongRangeFunction {
  final def addTimeLongChunks(longVectAcc: MemoryAccessor,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    sum += longReader.sum(longVectAcc, longVect, startRowNum, endRowNum)
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
  final def addTimeDoubleChunks(doubleVectAcc: MemoryAccessor,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    val it = doubleReader.iterate(doubleVectAcc, doubleVect, startRowNum)
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
  final def addTimeLongChunks(longVectAcc: MemoryAccessor,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    val it = longReader.iterate(longVectAcc, longVect, startRowNum)
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

abstract class ChangesChunkedFunction(var changes: Double = Double.NaN, var prev: Double = Double.NaN)
  extends ChunkedRangeFunction[TransientRow] {
  override final def reset(): Unit = { changes = Double.NaN; prev = Double.NaN }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, changes)
  }
}

class ChangesChunkedFunctionD() extends ChangesChunkedFunction() with
  ChunkedDoubleRangeFunction {
  final def addTimeDoubleChunks(doubleVectAcc: MemoryAccessor,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    if (changes.isNaN) {
      changes = 0d
    }

    val changesResult = doubleReader.changes(doubleVectAcc, doubleVect, startRowNum, endRowNum, prev)
    changes += changesResult._1
    prev = changesResult._2
  }
}

// scalastyle:off
class ChangesChunkedFunctionL extends ChangesChunkedFunction with
  ChunkedLongRangeFunction{
  final def addTimeLongChunks(longVectAcc: MemoryAccessor,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    if (changes.isNaN) {
      changes = 0d
    }
    val changesResult = longReader.changes(longVectAcc, longVect, startRowNum, endRowNum, prev.toLong)
    changes += changesResult._1
    prev = changesResult._2
  }
}

abstract class QuantileOverTimeChunkedFunction(funcParams: Seq[Any],
                                               var quantileResult: Double = Double.NaN,
                                               var values: Buffer[Double] = Buffer.empty[Double])
  extends ChunkedRangeFunction[TransientRow] {
  override final def reset(): Unit = { quantileResult = Double.NaN; values = Buffer.empty[Double] }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    val q = funcParams.head.asInstanceOf[Number].doubleValue()
    if (!quantileResult.equals(Double.NegativeInfinity) || !quantileResult.equals(Double.PositiveInfinity)) {
      val counter = values.length
      values.sort(spire.algebra.Order.fromOrdering[Double])
      val (weight, upperIndex, lowerIndex) = calculateRank(q, counter)
      if (counter > 0) {
        quantileResult = values(lowerIndex)*(1-weight) + values(upperIndex)*weight
      }
    }
    sampleToEmit.setValues(endTimestamp, quantileResult)
  }
  def calculateRank(q: Double, counter: Int): (Double, Int, Int) = {
    val rank = q*(counter - 1)
    val lowerIndex = Math.max(0, Math.floor(rank))
    val upperIndex = Math.min(counter - 1, lowerIndex + 1)
    val weight = rank - math.floor(rank)
    (weight, upperIndex.toInt, lowerIndex.toInt)
  }
}

class QuantileOverTimeChunkedFunctionD(funcParams: Seq[Any]) extends QuantileOverTimeChunkedFunction(funcParams)
  with ChunkedDoubleRangeFunction {
  require(funcParams.size == 1, "quantile_over_time function needs a single quantile argument")
  require(funcParams.head.isInstanceOf[Number], "quantile parameter must be a number")
  final def addTimeDoubleChunks(doubleVectAcc: MemoryAccessor,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    val q = funcParams.head.asInstanceOf[Number].doubleValue()
    if (q < 0) quantileResult = Double.NegativeInfinity
    else if (q > 1) quantileResult = Double.PositiveInfinity
    else {
      var rowNum = startRowNum
      val it = doubleReader.iterate(doubleVectAcc, doubleVect, startRowNum)
      while (rowNum <= endRowNum) {
        var nextvalue = it.next
        // There are many possible values of NaN.  Use a function to ignore them reliably.
        if (!JLDouble.isNaN(nextvalue)) {
          values += nextvalue
        }
        rowNum += 1
      }
    }
  }
}

class QuantileOverTimeChunkedFunctionL(funcParams: Seq[Any])
  extends QuantileOverTimeChunkedFunction(funcParams) with ChunkedLongRangeFunction {
  require(funcParams.size == 1, "quantile_over_time function needs a single quantile argument")
  require(funcParams.head.isInstanceOf[Number], "quantile parameter must be a number")
  final def addTimeLongChunks(longVectAcc: MemoryAccessor,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    val q = funcParams.head.asInstanceOf[Number].doubleValue()
    if (q < 0) quantileResult = Double.NegativeInfinity
    else if (q > 1) quantileResult = Double.PositiveInfinity
    else {
      var rowNum = startRowNum
      val it = longReader.iterate(longVectAcc, longVect, startRowNum)
      while (rowNum <= endRowNum) {
        var nextvalue = it.next
        values += nextvalue
        rowNum += 1
      }
    }
  }
}

abstract class HoltWintersChunkedFunction(funcParams: Seq[Any],
                                          var b0: Double = Double.NaN,
                                          var s0: Double = Double.NaN,
                                          var nextvalue: Double = Double.NaN,
                                          var smoothedResult: Double = Double.NaN)
  extends ChunkedRangeFunction[TransientRow] {

  override final def reset(): Unit = { s0 = Double.NaN
                                       b0 = Double.NaN
                                       nextvalue = Double.NaN
                                       smoothedResult = Double.NaN }

  def parseParameters(funcParams: Seq[Any]): (Double, Double) = {
    require(funcParams.size == 2, "Holt winters needs 2 parameters")
    require(funcParams.head.isInstanceOf[Number], "sf parameter must be a number")
    require(funcParams(1).isInstanceOf[Number], "tf parameter must be a number")
    val sf = funcParams.head.asInstanceOf[Number].doubleValue()
    val tf = funcParams(1).asInstanceOf[Number].doubleValue()
    require(sf >= 0 & sf <= 1, "Sf should be in between 0 and 1")
    require(tf >= 0 & tf <= 1, "tf should be in between 0 and 1")
    (sf, tf)
  }

  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, smoothedResult)
  }
}

/**
  * @param funcParams - Additional required function parameters
  * Refer https://en.wikipedia.org/wiki/Exponential_smoothing#Double_exponential_smoothing
  */
class HoltWintersChunkedFunctionD(funcParams: Seq[Any]) extends HoltWintersChunkedFunction(funcParams)
  with ChunkedDoubleRangeFunction {

  val (sf, tf) = parseParameters(funcParams)

  // Returns the first non-Nan value encountered
  def getNextValue(startRowNum: Int, endRowNum: Int, it: DoubleIterator): (Double, Int) = {
    var res = Double.NaN
    var currRowNum = startRowNum
    while (currRowNum <= endRowNum && JLDouble.isNaN(res)) {
      val nextvalue = it.next
      // There are many possible values of NaN.  Use a function to ignore them reliably.
      if (!JLDouble.isNaN(nextvalue)) {
        res = nextvalue
      }
      currRowNum += 1
    }
    (res, currRowNum)
  }

  final def addTimeDoubleChunks(doubleVectAcc: MemoryAccessor,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    val it = doubleReader.iterate(doubleVectAcc, doubleVect, startRowNum)
    var rowNum = startRowNum
    if (JLDouble.isNaN(s0) && JLDouble.isNaN(b0)) {
      // check if it is a new chunk
      val (_s0, firstrow) = getNextValue(startRowNum, endRowNum, it)
      val (_b0, currRow) = getNextValue(firstrow, endRowNum, it)
      nextvalue = _b0
      b0 = _b0 - _s0
      rowNum = currRow - 1
      s0 = _s0
    } else if (JLDouble.isNaN(b0)) {
      // check if the previous chunk had only one element
      val (_b0, currRow) = getNextValue(startRowNum, endRowNum, it)
      nextvalue = _b0
      b0 = _b0 - s0
      rowNum = currRow - 1
    }
    else {
      // continuation of a previous chunk
      it.next
    }
    if (!JLDouble.isNaN(b0)) {
      while (rowNum <= endRowNum) {
        // There are many possible values of NaN.  Use a function to ignore them reliably.
        if (!JLDouble.isNaN(nextvalue)) {
          val _s0  = sf*nextvalue + (1-sf)*(s0 + b0)
          b0 = tf*(_s0 - s0) + (1-tf)*b0
          s0 = _s0
        }
        nextvalue = it.next
        rowNum += 1
      }
      smoothedResult = s0
    }
  }
}

class HoltWintersChunkedFunctionL(funcParams: Seq[Any]) extends HoltWintersChunkedFunction(funcParams)
  with ChunkedLongRangeFunction {

  val (sf, tf) = parseParameters(funcParams)

  final def addTimeLongChunks(longVectAcc: MemoryAccessor,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    val it = longReader.iterate(longVectAcc, longVect, startRowNum)
    var rowNum = startRowNum
    if (JLDouble.isNaN(b0)) {
      if (endRowNum - startRowNum >= 2) {
        s0 = it.next.toDouble
        b0 = it.next.toDouble
        nextvalue = b0
        b0 = b0 - s0
        rowNum = startRowNum + 1
      }
    } else {
      it.next
    }
    if (!JLDouble.isNaN(b0)) {
      while (rowNum <= endRowNum) {
        // There are many possible values of NaN.  Use a function to ignore them reliably.
        var nextvalue = it.next
        val smoothedResult  = sf*nextvalue + (1-sf)*(s0 + b0)
        b0 = tf*(smoothedResult - s0) + (1-tf)*b0
        s0 = smoothedResult
        nextvalue = it.next
        rowNum += 1
      }
    }
  }
}
