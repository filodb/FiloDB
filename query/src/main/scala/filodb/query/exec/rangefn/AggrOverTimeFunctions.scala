package filodb.query.exec.rangefn

import java.util

import filodb.core.store.ChunkSetInfo
import filodb.memory.format.{vectors => bv, BinaryVector}
import filodb.query.QueryConfig
import filodb.query.exec.TransientRow

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

class SumOverTimeFunction(var sum: Double = 0d) extends RangeFunction {
  override def addedToWindow(row: TransientRow, window: Window): Unit = {
    sum += row.value
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
    sum -= row.value
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    sampleToEmit.setValues(endTimestamp, sum)
  }
}

class SumOverTimeChunkedFunction(var sum: Double = 0d) extends ChunkedDoubleRangeFunction {
  override final def reset(): Unit = { sum = 0d }

  final def addTimeDoubleChunks(doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    sum += doubleReader.sum(doubleVect, startRowNum, endRowNum)
  }

  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, sum)
  }
}

class CountOverTimeFunction(var count: Int = 0) extends RangeFunction {
  override def addedToWindow(row: TransientRow, window: Window): Unit = {
    count += 1
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
    count -= 1
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    sampleToEmit.setValues(endTimestamp, count.toDouble)
  }
}

class CountOverTimeChunkedFunction(var count: Int = 0) extends ChunkedRangeFunction {
  override final def reset(): Unit = { count = 0 }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, count.toDouble)
  }
  final def addChunks(tsCol: Int, valueCol: Int, info: ChunkSetInfo,
                startTime: Long, endTime: Long, queryConfig: QueryConfig): Unit = {
    val timestampVector = info.vectorPtr(tsCol)
    val tsReader = bv.LongBinaryVector(timestampVector)

    // First row >= startTime, so we can just drop bit 31 (dont care if it matches exactly)
    val startRowNum = tsReader.binarySearch(timestampVector, startTime) & 0x7fffffff
    val endRowNum = tsReader.ceilingIndex(timestampVector, endTime)
    val numRows = endRowNum - startRowNum + 1
    count += numRows
  }
}

class AvgOverTimeFunction(var sum: Double = 0d, var count: Int = 0) extends RangeFunction {
  override def addedToWindow(row: TransientRow, window: Window): Unit = {
    sum += row.value
    count += 1
  }

  override def removedFromWindow(row: TransientRow, window: Window): Unit = {
    sum -= row.value
    count -= 1
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    sampleToEmit.setValues(endTimestamp, sum/count)
  }
}

class AvgOverTimeChunkedFunctionD(var sum: Double = 0d, var count: Int = 0) extends ChunkedDoubleRangeFunction {
  override final def reset(): Unit = { sum = 0d; count = 0 }

  final def addTimeDoubleChunks(doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    sum += doubleReader.sum(doubleVect, startRowNum, endRowNum)
    count += (endRowNum - startRowNum + 1)
  }

  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, if (count > 0) sum/count else 0d)
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
      _sum += nextValue
      _sqSum += nextValue * nextValue
      elemNo += 1
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