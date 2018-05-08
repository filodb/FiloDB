package filodb.query.exec.rangefn

import java.util

import filodb.query.QueryConfig
import filodb.query.exec.TransientRow

class MinMaxOverTimeFunction(ord: Ordering[Double]) extends RangeFunction {

  val minMaxDeque = new util.ArrayDeque[TransientRow]()

  override def addToWindow(row: TransientRow): Unit = {
    while (!minMaxDeque.isEmpty && ord.compare(minMaxDeque.peekLast().value, row.value) < 0) minMaxDeque.removeLast()
    minMaxDeque.addLast(row)
  }

  override def removeFromWindow(row: TransientRow): Unit = {
    while (!minMaxDeque.isEmpty && minMaxDeque.peekFirst().timestamp <= row.timestamp) minMaxDeque.removeFirst()
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    if (minMaxDeque.isEmpty) sampleToEmit.set(endTimestamp, Double.NaN)
    else sampleToEmit.set(endTimestamp, minMaxDeque.peekFirst().value)
  }
}

class SumOverTimeFunction extends RangeFunction {

  var sum = 0d

  override def addToWindow(row: TransientRow): Unit = {
    sum += row.value
  }

  override def removeFromWindow(row: TransientRow): Unit = {
    sum -= row.value
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    sampleToEmit.set(endTimestamp, sum)
  }
}

class CountOverTimeFunction extends RangeFunction {

  var count = 0d
  override def addToWindow(row: TransientRow): Unit = {
    count += 1
  }

  override def removeFromWindow(row: TransientRow): Unit = {
    count -= 1
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    sampleToEmit.set(endTimestamp, count)
  }
}

class AvgOverTimeFunction extends RangeFunction {

  var sum = 0d
  var count = 0

  override def addToWindow(row: TransientRow): Unit = {
    sum += row.value
    count += 1
  }

  override def removeFromWindow(row: TransientRow): Unit = {
    sum -= row.value
    count -= 1
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    sampleToEmit.set(endTimestamp, sum/count)
  }
}

class StdDevOverTimeFunction extends RangeFunction {

  var sum = 0d
  var count = 0
  var squaredSum = 0d

  override def addToWindow(row: TransientRow): Unit = {
    sum += row.value
    squaredSum += row.value * row.value
    count += 1
  }

  override def removeFromWindow(row: TransientRow): Unit = {
    sum -= row.value
    squaredSum -= row.value * row.value
    count -= 1
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    val avg = sum/count
    val stdDev = Math.sqrt(squaredSum/count - avg*avg)
    sampleToEmit.set(endTimestamp, stdDev)
  }
}

class StdVarOverTimeFunction extends RangeFunction {

  var sum = 0d
  var count = 0
  var squaredSum = 0d

  override def addToWindow(row: TransientRow): Unit = {
    sum += row.value
    squaredSum += row.value * row.value
    count += 1
  }

  override def removeFromWindow(row: TransientRow): Unit = {
    sum -= row.value
    squaredSum -= row.value * row.value
    count -= 1
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window,
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    val avg = sum/count
    val stdVar = squaredSum/count - avg*avg
    sampleToEmit.set(endTimestamp, stdVar)
  }
}