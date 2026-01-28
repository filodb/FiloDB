package filodb.query.exec.rangefn

import java.lang.{Double => JLDouble}
import java.util

import debox.Buffer

import filodb.core.query.{QueryConfig, TransientHistMaxMinRow, TransientHistRow, TransientRow}
import filodb.core.store.ChunkSetInfoReader
import filodb.memory.format.{BinaryVector, CounterVectorReader, MemoryReader, VectorDataReader}
import filodb.memory.format.{vectors => bv}
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.vectors.DoubleIterator
import filodb.query.exec.{FuncArgs, StaticFuncArgs}

class MinMaxOverTimeFunction(ord: Ordering[Double]) extends RangeFunction[TransientRow] {
  val minMaxDeque = new util.ArrayDeque[TransientRow]()

  override def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
    if (!row.value.isNaN) {
      while (!minMaxDeque.isEmpty && ord.compare(minMaxDeque.peekLast().value, row.value) < 0) minMaxDeque.removeLast()
      minMaxDeque.addLast(row)
    }
  }

  override def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
    while (!minMaxDeque.isEmpty && minMaxDeque.peekFirst().timestamp <= row.timestamp) minMaxDeque.removeFirst()
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window[TransientRow],
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
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
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
  final def addTimeLongChunks(longVectAcc: MemoryReader,
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
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
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
  final def addTimeLongChunks(longVectAcc: MemoryReader,
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

/**
 * Sliding window tracker for finding maximum or minimum values efficiently over time windows.
 * Uses a monotonic deque approach to maintain O(1) amortized insertion/removal operations.
 *
 * The tracker maintains a deque where values are stored in monotonic order according to the
 * provided ordering. When a new value is added, all values that are "worse" (smaller for max,
 * larger for min) are removed from the tail, ensuring the head always contains the optimal value.
 *
 * Examples:
 * - For max tracking with values [10, 5, 15, 3, 12] over time:
 *   * Add (t1, 10): deque = [(t1, 10)]
 *   * Add (t2, 5): deque = [(t1, 10), (t2, 5)] (5 < 10, so keep both)
 *   * Add (t3, 15): deque = [(t3, 15)] (15 > 10 and 5, remove both)
 *   * Add (t4, 3): deque = [(t3, 15), (t4, 3)] (3 < 15, so keep both)
 *   * Add (t5, 12): deque = [(t3, 15), (t5, 12)] (12 > 3, remove 3; 12 < 15, keep 15)
 *   * Head value is always maximum: 15
 *
 * - For min tracking with same values:
 *   * The deque would maintain smallest values, with head containing minimum
 *
 * Time complexity: O(1) amortized for offer(), O(log n) worst case for individual operations
 * Space complexity: O(k) where k is the effective window size (typically much smaller than total values)
 *
 * @param ordering Determines whether to track max (Ordering.Double) or min (Ordering.Double.reverse)
 */
private case class MaxMinTracker(ordering: Ordering[Double]) {
  private val values: util.ArrayDeque[(Long, Double)] = new util.ArrayDeque[(Long, Double)]()

  def offer(ts: Long, value: Double): Boolean = {
    if (!value.isNaN) {
      while(!values.isEmpty && ordering.compare(values.peekLast()._2, value) < 0) {
        values.removeLast()
      }
      values.offerLast((ts, value))
    } else {
      false
    }
  }

  /**
   * Removes all values from the deque that have timestamps older than or equal to the given timestamp.
   * This maintains the sliding window property by evicting expired values from the head of the deque.
   *
   * The method removes values from the front (head) of the deque since the deque maintains values
   * in chronological order, with older values at the head and newer values at the tail.
   *
   * IMPORTANT: This method works identically for both max and min tracking orderings because it only
   * examines timestamps (._1), not the actual values (._2). The ordering parameter affects only
   * the offer() method's value-based filtering, not the time-based cleanup performed here.
   *
   * Examples (same behavior regardless of max/min tracking):
   * - Max tracking deque: [(t1=100, 15), (t3=300, 10), (t5=500, 20)]
   * - Min tracking deque: [(t1=100, 10), (t3=300, 15), (t5=500, 20)]
   * - removeValuesOlderThan(250): removes (t1=100, value) in both cases
   * - removeValuesOlderThan(300): removes (t3=300, value) in both cases
   * - removeValuesOlderThan(600): removes all values in both cases
   *
   * This is typically called when a sliding time window moves forward and old values
   * are no longer within the window bounds.
   *
   * Time complexity: O(k) where k is the number of expired values (amortized O(1) per value)
   *
   * @param ts The cutoff timestamp - values with timestamp <= ts will be removed
   */
  def removeValuesOlderThan(ts: Long): Unit = {
    while(!values.isEmpty && values.peekFirst()._1 <= ts) {
      values.removeFirst()
    }
  }

  def headValue(): Option[(Long, Double)] = if (values.isEmpty) None else Some(values.peekFirst())
}

class SumAndMaxOverTimeFunctionHD(var sum: bv.MutableHistogram = bv.Histogram.empty)
  extends RangeFunction[TransientHistRow] {
    private val sumOverTimeFunction: SumOverTimeFunctionH = new SumOverTimeFunctionH(sum, 0)
    private val maxTracker: MaxMinTracker = MaxMinTracker(Ordering.Double)
  /**
   * Called when a sample is added to the sliding window
   */
  override def addedToWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {
    sumOverTimeFunction.addedToWindow(row, window)
    maxTracker.offer(row.getLong(0), row.getDouble(2))
  }

  /**
   * Called when a sample is removed from sliding window
   */
  override def removedFromWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {
    sumOverTimeFunction.removedFromWindow(row, window)
    val tsToRemove = row.getLong(0);
    maxTracker removeValuesOlderThan tsToRemove
  }

  override def apply(startTimestamp: Long, endTimestamp: Long,
                     window: Window[TransientHistRow],
                     sampleToEmit: TransientHistRow,
                     queryConfig: QueryConfig): Unit = {
    sampleToEmit.setValues(endTimestamp, sumOverTimeFunction.sum)
    sampleToEmit.setDouble(
        2, maxTracker.headValue().map { case (_, value) => value }.getOrElse(Double.NaN)
    )
  }
}


class RateAndMaxMinOverTimeFunctionHD(var sum: bv.MutableHistogram = bv.Histogram.empty)
  extends RangeFunction[TransientHistRow] {
  private val rateOverDeltaFunction: RateOverDeltaFunctionH = new RateOverDeltaFunctionH
  private val maxTracker: MaxMinTracker = MaxMinTracker(Ordering.Double)
  private val minTracker: MaxMinTracker = MaxMinTracker(Ordering.Double.reverse)
  /**
   * Called when a sample is added to the sliding window
   */
  override def addedToWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {
    rateOverDeltaFunction.addedToWindow(row, window)
    val ts = row.getLong(0);
    minTracker.offer(ts, row.getDouble(3))
    maxTracker.offer(ts, row.getDouble(2))
  }

  /**
   * Called when a sample is removed from sliding window
   */
  override def removedFromWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {
    rateOverDeltaFunction.removedFromWindow(row, window)
    val tsToRemove = row.getLong(0);
    minTracker removeValuesOlderThan tsToRemove
    maxTracker removeValuesOlderThan tsToRemove
  }

  override def apply(startTimestamp: Long, endTimestamp: Long,
                     window: Window[TransientHistRow],
                     sampleToEmit: TransientHistRow,
                     queryConfig: QueryConfig): Unit = {
    val rateHist = rateOverDeltaFunction.computeRateHistogram(startTimestamp, endTimestamp)
    sampleToEmit.setValues(endTimestamp, rateHist)
    sampleToEmit.setDouble(
      3, minTracker.headValue().map { case (_, value) => value }.getOrElse(Double.NaN)
    )
    sampleToEmit.setDouble(
      2, maxTracker.headValue().map { case (_, value) => value }.getOrElse(Double.NaN)
    )
  }
}

class SumOverTimeFunctionH(var sum: bv.MutableHistogram = bv.Histogram.empty, var count: Int = 0)
  extends RangeFunction[TransientHistRow] {

  override def addedToWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {
    val histValue = row.value

    if (histValue.numBuckets > 0) {
      sum match {
        // First histogram - copy it to ensure we have our own mutable copy
        case hist if hist.numBuckets == 0 => sum = bv.MutableHistogram(histValue)
        // Add to existing sum
        case hist: bv.MutableHistogram    => hist.add(histValue)
      }
      count += 1
    }
  }

  override def removedFromWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {
    // TODO: Very expensive, is there a better way? Can we subtract assuming the counter correction is already done?
    val histValue = row.value
    if (histValue.numBuckets > 0) {
      // Since histogram subtraction is complex and not typically supported,
      // we recalculate the sum from scratch using all remaining items in the window
      sum = bv.Histogram.empty
      count = 0

      // Recalculate sum from all remaining items in window
      for (i <- 0 until window.size) {
        val windowHist = window(i).value
        if (windowHist.numBuckets > 0) {
          sum match {
            case hist if hist.numBuckets == 0 => sum = bv.MutableHistogram(windowHist)
            case hist: bv.MutableHistogram    => hist.add(windowHist)
          }
          count += 1
        }
      }
    }
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window[TransientHistRow],
                     sampleToEmit: TransientHistRow, queryConfig: QueryConfig): Unit = {
    sampleToEmit.setValues(endTimestamp, sum)
  }
}

class AvgOverDeltaFunctionH extends RangeFunction[TransientHistRow] {
  private val sumOverTime = new SumOverTimeFunctionH
  override def addedToWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit =
    sumOverTime.addedToWindow(row, window)

  override def removedFromWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit =
    sumOverTime.removedFromWindow(row, window)

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window[TransientHistRow],
                     sampleToEmit: TransientHistRow,
                     queryConfig: QueryConfig): Unit = {
    val mh = sumOverTime.sum
    sampleToEmit.setValues(endTimestamp, bv.MutableHistogram(mh.buckets, mh.values.map(_ / sumOverTime.count)))
  }
}


class SumOverTimeFunction(var sum: Double = Double.NaN, var count: Int = 0) extends RangeFunction[TransientRow] {
  override def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
    if (!JLDouble.isNaN(row.value)) {
      if (sum.isNaN) {
        sum = 0d
      }
      sum += row.value
      count += 1
    }
  }

  override def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
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

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window[TransientRow],
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    sampleToEmit.setValues(endTimestamp, sum)
  }
}

object ChangesOverTimeFunction extends RangeFunction[TransientRow] {
  override def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
  }

  override def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
  }

  override def apply(
    startTimestamp: Long, endTimestamp: Long, window: Window[TransientRow],
    sampleToEmit: TransientRow,
    queryConfig: QueryConfig
  ): Unit = {
    var lastValue = Double.NaN
    var changes = Double.NaN
    if (window.size > 0) lastValue = window.head.getDouble(1)
    if (!lastValue.isNaN) {
      changes = 0
    }
    var i = 1;
    while (i < window.size) {
      val curValue = window.apply(i).getDouble(1)
      if (!curValue.isNaN && !lastValue.isNaN) {
        if (curValue != lastValue) {
            changes = changes + 1
        }
      }
      if (!curValue.isNaN) {
        lastValue = curValue
        if (changes.isNaN) {
          changes = 0
        }
      }
      i = i + 1
    }
    sampleToEmit.setValues(endTimestamp, changes)
  }
}


object QuantileOverTimeFunction {
  def calculateRank(q: Double, counter: Int): (Double, Int, Int) = {
    val rank = q*(counter - 1)
    val lowerIndex = Math.max(0, Math.floor(rank))
    val upperIndex = Math.min(counter - 1, lowerIndex + 1)
    val weight = rank - math.floor(rank)
    (weight, upperIndex.toInt, lowerIndex.toInt)
  }
}

class QuantileOverTimeFunction(funcParams: Seq[Any]) extends RangeFunction[TransientRow] {
  override def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
  }

  override def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
  }

  override def apply(
    startTimestamp: Long, endTimestamp: Long, window: Window[TransientRow],
    sampleToEmit: TransientRow,
    queryConfig: QueryConfig
  ): Unit = {
    require(funcParams.head.isInstanceOf[StaticFuncArgs], "quantile parameter must be a number")
    val q = funcParams.head.asInstanceOf[StaticFuncArgs].scalar

    val values: Buffer[Double] = Buffer.ofSize(window.size)
    var i = 0;
    while (i < window.size) {
      val curValue = window.apply(i).getDouble(1)
      if (!curValue.isNaN) {
        values.append(curValue)
      }
      i = i + 1
    }
    val counter = values.length
    values.sort(spire.algebra.Order.fromOrdering[Double])
    val (weight, upperIndex, lowerIndex) = QuantileOverTimeFunction.calculateRank(q, counter)
    var quantileResult : Double = Double.NaN
    if (counter > 0) {
      quantileResult = values(lowerIndex)*(1-weight) + values(upperIndex)*weight
    }
    sampleToEmit.setValues(endTimestamp, quantileResult)

  }
}

class MedianAbsoluteDeviationOverTimeFunction(funcParams: Seq[Any]) extends RangeFunction[TransientRow] {
  override def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
  }

  override def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
  }

  override def apply(
                      startTimestamp: Long, endTimestamp: Long, window: Window[TransientRow],
                      sampleToEmit: TransientRow,
                      queryConfig: QueryConfig
                    ): Unit = {
    val q = 0.5
    val values: Buffer[Double] = Buffer.ofSize(window.size)
    for (i <- 0 until window.size) {
      val curValue = window.apply(i).getDouble(1)
      if (!curValue.isNaN) {
        values.append(curValue)
      }
    }
    val size = values.length
    values.sort(spire.algebra.Order.fromOrdering[Double])
    val (weight, upperIndex, lowerIndex) = QuantileOverTimeFunction.calculateRank(q, size)
    var median : Double = Double.NaN
    if (size > 0) {
      median = values(lowerIndex)*(1-weight) + values(upperIndex)*weight
    }

    var medianAbsoluteDeviationResult : Double = Double.NaN
    val diffFromMedians: Buffer[Double] = Buffer.ofSize(window.size)

    for (i <- 0 until window.size) {
      val curValue = window.apply(i).getDouble(1)
      diffFromMedians.append(Math.abs(median - curValue))
    }
    diffFromMedians.sort(spire.algebra.Order.fromOrdering[Double])
    if (size > 0) {
      medianAbsoluteDeviationResult = diffFromMedians(lowerIndex)*(1-weight) + diffFromMedians(upperIndex)*weight
    }
    sampleToEmit.setValues(endTimestamp, medianAbsoluteDeviationResult)
  }
}

class LastOverTimeIsMadOutlierFunction(funcParams: Seq[Any]) extends RangeFunction[TransientRow] {
  require(funcParams.size == 2, "last_over_time_is_mad_outlier function needs a two scalar arguments" +
                  " (tolerance, bounds)")
  require(funcParams(0).isInstanceOf[StaticFuncArgs], "first tolerance parameter must be a number; " +
    "higher value means more tolerance to anomalies")
  require(funcParams(1).isInstanceOf[StaticFuncArgs], "second bounds parameter must be a number(0, 1, 2)")

  private val boundsCheck = funcParams(1).asInstanceOf[StaticFuncArgs].scalar.toInt
  require(boundsCheck == 0 || boundsCheck == 1 || boundsCheck == 2,
    "boundsCheck parameter should be 0 (lower only), 1 (both lower and upper) or 2 (upper only)")

  private val tolerance = funcParams.head.asInstanceOf[StaticFuncArgs].scalar
  require(tolerance > 0, "tolerance must be a positive number")

  override def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
  }

  override def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window[TransientRow],
                      sampleToEmit: TransientRow,
                      queryConfig: QueryConfig
                    ): Unit = {
    val size = window.size
    if (size > 0) {
      // find median
      val q = 0.5
      val values: Buffer[Double] = Buffer.ofSize(size)
      for (i <- 0 until size) {
        val curValue = window.apply(i).getDouble(1)
        if (!curValue.isNaN) {
          values.append(curValue)
        }
      }
      values.sort(spire.algebra.Order.fromOrdering[Double])
      val (weight, upperIndex, lowerIndex) = QuantileOverTimeFunction.calculateRank(q, size)
      val median = values(lowerIndex)*(1-weight) + values(upperIndex)*weight

      // distance from median
      val distFromMedian: Buffer[Double] = Buffer.ofSize(size)
      for (i <- 0 until size) {
        val curValue = window.apply(i).getDouble(1)
        distFromMedian.append(Math.abs(median - curValue))
      }

      // mad = median of absolute distances from median
      distFromMedian.sort(spire.algebra.Order.fromOrdering[Double])
      val mad = distFromMedian(lowerIndex)*(1-weight) + distFromMedian(upperIndex)*weight

      // classify last point as anomaly if it's more than `tolerance * mad` away from median
      val lowerBound = median - tolerance * mad
      val upperBound = median + tolerance * mad
      val lastValue = window.last.getDouble(1)
      if ((lastValue < lowerBound && boundsCheck <= 1) || (lastValue > upperBound && boundsCheck >= 1)) {
        sampleToEmit.setValues(endTimestamp, lastValue)
      } else {
        sampleToEmit.setValues(endTimestamp, Double.NaN)
      }
    } else {
      sampleToEmit.setValues(endTimestamp, Double.NaN)
    }
  }
}

abstract class SumOverTimeChunkedFunction(var sum: Double = Double.NaN) extends ChunkedRangeFunction[TransientRow] {
  override final def reset(): Unit = { sum = Double.NaN }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, sum)
  }
}

class SumOverTimeChunkedFunctionD extends SumOverTimeChunkedFunction() with ChunkedDoubleRangeFunction {
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {

    //Takes care of computing sum in all seq types : combination of NaN & not NaN & all numbers.
    val chunkSum = doubleReader.sum(doubleVectAcc, doubleVect, startRowNum, endRowNum)
    if(!JLDouble.isNaN(chunkSum) && JLDouble.isNaN(sum)) sum = 0d
    sum += chunkSum
  }
}

class SumOverTimeChunkedFunctionL extends SumOverTimeChunkedFunction() with ChunkedLongRangeFunction {
  final def addTimeLongChunks(longVectAcc: MemoryReader,
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

  final def addTimeChunks(vectAcc: MemoryReader,
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
class SumAndMaxOverTimeFuncHD(maxColID: Int) extends ChunkedRangeFunction[TransientHistMaxMinRow] {
  private val hFunc = new SumOverTimeChunkedFunctionH
  private val maxFunc = new MaxOverTimeChunkedFunctionD

  override final def reset(): Unit = {
    hFunc.reset()
    maxFunc.reset()
  }
  final def apply(endTimestamp: Long, sampleToEmit: TransientHistMaxMinRow): Unit = {
    sampleToEmit.setValues(endTimestamp, hFunc.h)
    sampleToEmit.setDouble(2, maxFunc.max)
  }

  import BinaryVector.BinaryVectorPtr

  // scalastyle:off parameter.number
  def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVectorAcc: MemoryReader, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
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
 * Cumulative histogram rate function with min/max column support.
 * Similar to DeltaRateAndMinMaxOverTimeFuncHD but for cumulative histograms with counter correction.
 * Composes existing functions for clean implementation.
 *
 * @param maxColId Column ID for max values
 * @param minColId Column ID for min values
 */
class CumulativeHistRateAndMinMaxFunction(maxColId: Int, minColId: Int)
  extends CounterChunkedRangeFunction[TransientHistMaxMinRow] {

  // Reuse existing functions through composition
  private val histFunc = new HistRateFunction
  private val maxFunc = new MaxOverTimeChunkedFunctionD
  private val minFunc = new MinOverTimeChunkedFunctionD

  override def reset(): Unit = {
    histFunc.reset()
    maxFunc.reset()
    minFunc.reset()
    super.reset()
  }

  /**
   * Delegate histogram processing to HistRateFunction.
   * This is called by parent CounterChunkedRangeFunction.addChunks() after counter correction detection.
   */
  def addTimeChunks(acc: MemoryReader, vector: BinaryVectorPtr, reader: CounterVectorReader,
                    startRowNum: Int, endRowNum: Int,
                    startTime: Long, endTime: Long): Unit = {
    // Sync correction metadata to histogram function
    histFunc.correctionMeta = correctionMeta
    // Delegate to histogram rate function
    histFunc.addTimeChunks(acc, vector, reader, startRowNum, endRowNum, startTime, endTime)
  }

  // scalastyle:off parameter.number
  /**
   * Override addChunks to also process max and min columns in addition to histogram.
   * Parent handles histogram with counter correction, we add max/min processing.
   */
  override def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                         valueVectorAcc: MemoryReader, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                         startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
    // Call parent to handle histogram with counter correction
    super.addChunks(tsVectorAcc, tsVector, tsReader, valueVectorAcc, valueVector, valueReader,
      startTime, endTime, info, queryConfig)

    // Binary search for start/end row numbers once for max/min columns (same as DeltaRateAndMinMaxOverTimeFuncHD)
    val startRowNum = tsReader.binarySearch(tsVectorAcc, tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)

    // At least one sample is present
    if (startRowNum <= endRowNum) {
      // Process max column
      val maxVectAcc = info.vectorAccessor(maxColId)
      val maxVectPtr = info.vectorAddress(maxColId)
      maxFunc.addTimeDoubleChunks(maxVectAcc, maxVectPtr, bv.DoubleVector(maxVectAcc, maxVectPtr),
        startRowNum, endRowNum)

      // Process min column
      val minVectAcc = info.vectorAccessor(minColId)
      val minVectPtr = info.vectorAddress(minColId)
      minFunc.addTimeDoubleChunks(minVectAcc, minVectPtr, bv.DoubleVector(minVectAcc, minVectPtr),
        startRowNum, endRowNum)
    }
  }
  // scalastyle:on parameter.number

  override def apply(windowStart: Long, windowEnd: Long, sampleToEmit: TransientHistMaxMinRow): Unit = {
    // Delegate histogram rate calculation to HistRateFunction
    // TransientHistMaxMinRow extends TransientHistRow, so this works
    histFunc.apply(windowStart, windowEnd, sampleToEmit)

    // Set max and min values (same pattern as DeltaRateAndMinMaxOverTimeFuncHD)
    sampleToEmit.setDouble(2, maxFunc.max)
    sampleToEmit.setDouble(3, minFunc.min)
  }

  def apply(endTimestamp: Long, sampleToEmit: TransientHistMaxMinRow): Unit = ??? // should not be invoked
}

class DeltaRateAndMinMaxOverTimeFuncHD(maxColId: Int, minColId: Int)
                  extends ChunkedRangeFunction[TransientHistMaxMinRow] {
  private val hFunc = new RateOverDeltaChunkedFunctionH
  private val maxFunc = new MaxOverTimeChunkedFunctionD
  private val minFunc = new MinOverTimeChunkedFunctionD

  override final def reset(): Unit = {
    hFunc.reset()
    maxFunc.reset()
    minFunc.reset()
  }

  override def apply(windowStart: Long, windowEnd: Long, sampleToEmit: TransientHistMaxMinRow): Unit = {
    hFunc.apply(windowStart, windowEnd, sampleToEmit)
    sampleToEmit.setDouble(2, maxFunc.max)
    sampleToEmit.setDouble(3, minFunc.min)
  }
  final def apply(endTimestamp: Long, sampleToEmit: TransientHistMaxMinRow): Unit = ??? // should not be invoked

  import BinaryVector.BinaryVectorPtr

  // scalastyle:off parameter.number
  def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVectorAcc: MemoryReader, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
    // Do BinarySearch for start/end pos only once for all columns == WIN!
    val startRowNum = tsReader.binarySearch(tsVectorAcc, tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)

    // At least one sample is present
    if (startRowNum <= endRowNum) {
      hFunc.addTimeChunks(valueVectorAcc, valueVector, valueReader, startRowNum, endRowNum)

      // Get valueVector/reader for max column
      val maxVectAcc = info.vectorAccessor(maxColId)
      val maxVectPtr = info.vectorAddress(maxColId)
      maxFunc.addTimeChunks(maxVectAcc, maxVectPtr, bv.DoubleVector(maxVectAcc, maxVectPtr), startRowNum, endRowNum)

      // Get valueVector/reader for min column
      val minVectAcc = info.vectorAccessor(minColId)
      val minVectPtr = info.vectorAddress(minColId)
      minFunc.addTimeChunks(minVectAcc, minVectPtr, bv.DoubleVector(minVectAcc, minVectPtr), startRowNum, endRowNum)
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
  def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVectorAcc: MemoryReader, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
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
  def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                valueVectorAcc: MemoryReader, valueVector: BinaryVectorPtr, valueReader: VectorDataReader,
                startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
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

class CountOverTimeFunction(var count: Double = Double.NaN) extends RangeFunction[TransientRow] {
  override def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
    if (!JLDouble.isNaN(row.value)) {
      if (count.isNaN) {
        count = 0d
      }
      count += 1
    }
  }

  override def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
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

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window[TransientRow],
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

  def addTimeChunks(vectAcc: MemoryReader,
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
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
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

class AvgOverTimeFunction(var sum: Double = Double.NaN, var count: Int = 0) extends RangeFunction[TransientRow] {
  override def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
    if (!JLDouble.isNaN(row.value)) {
      if (sum.isNaN) {
        sum = 0d;
      }
      sum += row.value
      count += 1
    }
  }

  override def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
    if (!JLDouble.isNaN(row.value)) {
      if (sum.isNaN) {
        sum = 0d;
      }
      sum -= row.value
      count -= 1
    }
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window[TransientRow],
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    if (count == 0) {
      sampleToEmit.setValues(endTimestamp, Double.NaN)
    } else {
      sampleToEmit.setValues(endTimestamp, sum/count)
    }
  }
}

abstract class AvgOverTimeChunkedFunction(var sum: Double = Double.NaN, var count: Int = 0)
  extends ChunkedRangeFunction[TransientRow] {
  override final def reset(): Unit = {
    sum = Double.NaN
    count = 0
  }

  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    sampleToEmit.setValues(endTimestamp, if (count > 0) sum / count else if (sum.isNaN()) sum else 0d)
  }
}

class AvgOverTimeChunkedFunctionD extends AvgOverTimeChunkedFunction() with ChunkedDoubleRangeFunction {
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {

    // Takes care of computing sum in all seq types : combination of NaN & not NaN & all numbers.
    val chunkSum = doubleReader.sum(doubleVectAcc, doubleVect, startRowNum, endRowNum)
    if(!JLDouble.isNaN(chunkSum) && JLDouble.isNaN(sum)) sum = 0d
    sum += chunkSum
    count += doubleReader.count(doubleVectAcc, doubleVect, startRowNum, endRowNum)
  }
}

class AvgOverTimeChunkedFunctionL extends AvgOverTimeChunkedFunction() with ChunkedLongRangeFunction {
  final def addTimeLongChunks(longVectAcc: MemoryReader,
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
                             var squaredSum: Double = 0d) extends RangeFunction[TransientRow] {
  override def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
    if (!row.value.isNaN) {
      sum += row.value
      squaredSum += row.value * row.value
      count += 1
    }
  }

  override def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
    if (!row.value.isNaN) {
      sum -= row.value
      squaredSum -= row.value * row.value
      count -= 1
    }
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window[TransientRow],
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    val avg = sum/count
    val stdDev = Math.sqrt(squaredSum/count - avg*avg)
    sampleToEmit.setValues(endTimestamp, stdDev)
  }
}

class StdVarOverTimeFunction(var sum: Double = 0d,
                             var count: Int = 0,
                             var squaredSum: Double = 0d) extends RangeFunction[TransientRow] {
  override def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
    sum += row.value
    squaredSum += row.value * row.value
    count += 1
  }

  override def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
    sum -= row.value
    squaredSum -= row.value * row.value
    count -= 1
  }

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window[TransientRow],
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    val avg = sum/count
    val stdVar = squaredSum/count - avg*avg
    sampleToEmit.setValues(endTimestamp, stdVar)
  }
}

abstract class VarOverTimeChunkedFunctionD(var sum: Double = Double.NaN,
                                           var count: Int = 0,
                                           var squaredSum: Double = Double.NaN,
                                           var lastSample: Double = Double.NaN) extends ChunkedDoubleRangeFunction {
  override final def reset(): Unit = { sum = Double.NaN; count = 0; squaredSum = Double.NaN }
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    // Takes care of computing sum in all seq types : combination of NaN & not NaN & all numbers.
    val it = doubleReader.iterate(doubleVectAcc, doubleVect, startRowNum)
    var elemNo = startRowNum
    var chunkSum = Double.NaN
    var chunkSquaredSum = Double.NaN
    var chunkCount = 0
    while (elemNo <= endRowNum) {
      val nextValue = it.next
      if (!JLDouble.isNaN(nextValue)) {
        if (chunkSum.isNaN()) chunkSum = 0d
        if (chunkSquaredSum.isNaN()) chunkSquaredSum = 0d
        if (elemNo == endRowNum) lastSample = nextValue
        chunkSum += nextValue
        chunkSquaredSum += nextValue * nextValue
        chunkCount +=1
      }
      elemNo += 1
    }
    if(!JLDouble.isNaN(chunkSum) && JLDouble.isNaN(sum)) sum = 0d
    sum += chunkSum
    if(!JLDouble.isNaN(chunkSquaredSum) && JLDouble.isNaN(squaredSum)) squaredSum = 0d
    squaredSum += chunkSquaredSum
    count += chunkCount
  }
}

class StdDevOverTimeChunkedFunctionD extends VarOverTimeChunkedFunctionD() {
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    var stdDev = Double.NaN
    if (count > 0) {
      val avg = sum / count
      stdDev = Math.sqrt((squaredSum / count) - (avg * avg))
    }
    else if (sum.isNaN()) stdDev = sum
    else stdDev = 0d
    sampleToEmit.setValues(endTimestamp, stdDev)
  }
}

class StdVarOverTimeChunkedFunctionD extends VarOverTimeChunkedFunctionD() {
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    var stdVar = Double.NaN
    if (count > 0) {
      val avg = sum / count
      stdVar = (squaredSum / count) - (avg * avg)
    }
    else if (sum.isNaN()) stdVar = sum
    else stdVar = 0d
    sampleToEmit.setValues(endTimestamp, stdVar)
  }
}

abstract class VarOverTimeChunkedFunctionL(var sum: Double = 0d,
                                           var count: Int = 0,
                                           var squaredSum: Double = 0d) extends ChunkedLongRangeFunction {
  override final def reset(): Unit = { sum = 0d; count = 0; squaredSum = 0d }
  final def addTimeLongChunks(longVectAcc: MemoryReader,
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
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
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
  final def addTimeLongChunks(longVectAcc: MemoryReader,
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

abstract class QuantileOverTimeChunkedFunction(funcParams: Seq[FuncArgs],
                                               var quantileResult: Double = Double.NaN,
                                               var values: Buffer[Double] = Buffer.empty[Double])
  extends ChunkedRangeFunction[TransientRow] {
  override final def reset(): Unit = { quantileResult = Double.NaN; values = Buffer.empty[Double] }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    require(funcParams.head.isInstanceOf[StaticFuncArgs], "quantile parameter must be a number")
    val q = funcParams.head.asInstanceOf[StaticFuncArgs].scalar
    if (!quantileResult.equals(Double.NegativeInfinity) || !quantileResult.equals(Double.PositiveInfinity)) {
      val counter = values.length
      values.sort(spire.algebra.Order.fromOrdering[Double])
      val (weight, upperIndex, lowerIndex) = QuantileOverTimeFunction.calculateRank(q, counter)
      if (counter > 0) {
        quantileResult = values(lowerIndex)*(1-weight) + values(upperIndex)*weight
      }
    }
    sampleToEmit.setValues(endTimestamp, quantileResult)
  }

}

abstract class MedianAbsoluteDeviationOverTimeChunkedFunction(var medianAbsoluteDeviationResult: Double = Double.NaN,
                                                               var values: Buffer[Double] = Buffer.empty[Double])
  extends ChunkedRangeFunction[TransientRow] {
  override final def reset(): Unit = { medianAbsoluteDeviationResult = Double.NaN; values = Buffer.empty[Double] }
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    val size = values.length
    val (weight, upperIndex, lowerIndex) = QuantileOverTimeFunction.calculateRank(0.5, size)
    values.sort(spire.algebra.Order.fromOrdering[Double])
    var median: Double = Double.NaN
    if (size > 0) {
      median = values(lowerIndex) * (1 - weight) + values(upperIndex) * weight
      val diffFromMedians: Buffer[Double] = Buffer.ofSize(values.length)
      val iter = values.iterator
      for (value <- values) {
        diffFromMedians.append(Math.abs(median - value))
      }
      diffFromMedians.sort(spire.algebra.Order.fromOrdering[Double])
      medianAbsoluteDeviationResult = diffFromMedians(lowerIndex) * (1 - weight) + diffFromMedians(upperIndex) * weight
    }
    sampleToEmit.setValues(endTimestamp, medianAbsoluteDeviationResult)
  }

}

class QuantileOverTimeChunkedFunctionD(funcParams: Seq[FuncArgs]) extends QuantileOverTimeChunkedFunction(funcParams)
  with ChunkedDoubleRangeFunction {
  require(funcParams.size == 1, "quantile_over_time function needs a single quantile argument")
  require(funcParams.head.isInstanceOf[StaticFuncArgs], "quantile parameter must be a number")
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    //Only support StaticFuncArgs for now as we don't have time to get value from scalar vector
    val q = funcParams.head.asInstanceOf[StaticFuncArgs].scalar
    var counter = 0

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

class MedianAbsoluteDeviationOverTimeChunkedFunctionD
  extends MedianAbsoluteDeviationOverTimeChunkedFunction
  with ChunkedDoubleRangeFunction {
  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit = {
    val it = doubleReader.iterate(doubleVectAcc, doubleVect, startRowNum)

    for (_ <- startRowNum to endRowNum) {
      val nextvalue = it.next
      // There are many possible values of NaN.  Use a function to ignore them reliably.
      if (!JLDouble.isNaN(nextvalue)) {
        values += nextvalue
      }
    }
  }
}

class QuantileOverTimeChunkedFunctionL(funcParams: Seq[FuncArgs])
  extends QuantileOverTimeChunkedFunction(funcParams) with ChunkedLongRangeFunction {
  require(funcParams.size == 1, "quantile_over_time function needs a single quantile argument")
  require(funcParams.head.isInstanceOf[StaticFuncArgs], "quantile parameter must be a number")
  final def addTimeLongChunks(longVectAcc: MemoryReader,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
    val q = funcParams.head.asInstanceOf[StaticFuncArgs].scalar
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

class MedianAbsoluteDeviationOverTimeChunkedFunctionL
  extends MedianAbsoluteDeviationOverTimeChunkedFunction with ChunkedLongRangeFunction {
  final def addTimeLongChunks(longVectAcc: MemoryReader,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit = {
      val it = longReader.iterate(longVectAcc, longVect, startRowNum)
      for (_ <- startRowNum to endRowNum) {
        val nextvalue = it.next
        values += nextvalue
      }
  }
}

abstract class HoltWintersChunkedFunction(funcParams: Seq[FuncArgs],
                                          var b0: Double = Double.NaN,
                                          var s0: Double = Double.NaN,
                                          var nextvalue: Double = Double.NaN,
                                          var smoothedResult: Double = Double.NaN)
  extends ChunkedRangeFunction[TransientRow] {

  override final def reset(): Unit = { s0 = Double.NaN
                                       b0 = Double.NaN
                                       nextvalue = Double.NaN
                                       smoothedResult = Double.NaN }

  def parseParameters(funcParams: Seq[FuncArgs]): (Double, Double) = {
    require(funcParams.size == 2, "Holt winters needs 2 parameters")
    require(funcParams.head.isInstanceOf[StaticFuncArgs], "sf parameter must be a number")
    require(funcParams(1).isInstanceOf[StaticFuncArgs], "tf parameter must be a number")
    val sf = funcParams.head.asInstanceOf[StaticFuncArgs].scalar
    val tf = funcParams(1).asInstanceOf[StaticFuncArgs].scalar
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
class HoltWintersChunkedFunctionD(funcParams: Seq[FuncArgs]) extends HoltWintersChunkedFunction(funcParams)
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

  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
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
        // There are many possible values of NaN. Use a function to ignore them reliably.
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

class HoltWintersChunkedFunctionL(funcParams: Seq[FuncArgs]) extends HoltWintersChunkedFunction(funcParams)
  with ChunkedLongRangeFunction {

  val (sf, tf) = parseParameters(funcParams)

  final def addTimeLongChunks(longVectAcc: MemoryReader,
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

/**
  * Predicts the value of time series t seconds from now based on range vector
  * Refer https://en.wikipedia.org/wiki/Simple_linear_regression
**/
abstract class PredictLinearChunkedFunction(funcParams: Seq[Any],
                                            var sumX: Double = Double.NaN,
                                            var sumY: Double = Double.NaN,
                                            var sumXY: Double = Double.NaN,
                                            var sumX2: Double = Double.NaN,
                                            var counter: Int = 0)
  extends ChunkedRangeFunction[TransientRow] {
  override final def reset(): Unit = { sumX = Double.NaN; sumY = Double.NaN;
    sumXY = Double.NaN; sumX2 = Double.NaN;
    counter = 0}
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    val duration = funcParams.head.asInstanceOf[StaticFuncArgs].scalar
    val covXY = sumXY - sumX*sumY/counter
    val varX = sumX2 - sumX*sumX/counter
    val slope = covXY / varX
    val intercept = sumY/counter - slope*sumX/counter // keeping it, needed for predict_linear function = slope*duration + intercept
    if (counter >= 2) {
      sampleToEmit.setValues(endTimestamp, slope*duration + intercept)
    } else {
      sampleToEmit.setValues(endTimestamp, Double.NaN)
    }
  }
}

class PredictLinearChunkedFunctionD(funcParams: Seq[Any]) extends PredictLinearChunkedFunction(funcParams)
  with ChunkedRangeFunction[TransientRow] {
  require(funcParams.size == 1, "predict_linear function needs a single time argument")
  require(funcParams.head.isInstanceOf[StaticFuncArgs], "duration parameter must be a number")
  final def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVector.BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                      valueVectorAcc: MemoryReader, valueVector: BinaryVector.BinaryVectorPtr, valueReader: VectorDataReader,
                      startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
    var startRowNum = tsReader.binarySearch(tsVectorAcc, tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)
    val itTimestamp = tsReader.asLongReader.iterate(tsVectorAcc, tsVector, startRowNum)
    val it = valueReader.asDoubleReader.iterate(valueVectorAcc, valueVector, startRowNum)
    while (startRowNum <= endRowNum) {
      val nextvalue = it.next
      val nexttime = itTimestamp.next
      // There are many possible values of NaN.  Use a function to ignore them reliably.
      if (!JLDouble.isNaN(nextvalue)) {
        val x = (nexttime-endTime)/1000.0
        if (sumY.isNaN) {
          sumY = nextvalue
          sumX = x
          sumXY = x * nextvalue
          sumX2 = x * x
        } else {
          sumY += nextvalue
          sumX += x
          sumXY += x * nextvalue
          sumX2 += x * x
        }
        counter += 1
      }
      startRowNum += 1
    }
  }
}


class PredictLinearChunkedFunctionL(funcParams: Seq[Any]) extends PredictLinearChunkedFunction(funcParams)
  with ChunkedRangeFunction[TransientRow] {
  require(funcParams.size == 1, "predict_linear function needs a single duration argument")
  require(funcParams.head.isInstanceOf[StaticFuncArgs], "duration parameter must be a number")
  final def addChunks(tsVectorAcc: MemoryReader, tsVector: BinaryVector.BinaryVectorPtr, tsReader: bv.LongVectorDataReader,
                      valueVectorAcc: MemoryReader, valueVector: BinaryVector.BinaryVectorPtr, valueReader: VectorDataReader,
                      startTime: Long, endTime: Long, info: ChunkSetInfoReader, queryConfig: QueryConfig): Unit = {
    var startRowNum = tsReader.binarySearch(tsVectorAcc, tsVector, startTime) & 0x7fffffff
    val endRowNum = Math.min(tsReader.ceilingIndex(tsVectorAcc, tsVector, endTime), info.numRows - 1)
    val itTimestamp = tsReader.asLongReader.iterate(tsVectorAcc, tsVector, startRowNum)
    val it = valueReader.asLongReader.iterate(valueVectorAcc, valueVector, startRowNum)
    while (startRowNum <= endRowNum) {
      val nextvalue = it.next
      val nexttime = itTimestamp.next
      val x = (nexttime-endTime)/1000.0
      if (sumY.isNaN) {
        sumY = nextvalue
        sumX = x
        sumXY = x * nextvalue
        sumX2 = x * x
      } else {
        sumY += nextvalue
        sumX += x
        sumXY += x * nextvalue
        sumX2 += x * x
      }
      counter += 1
      startRowNum += 1
    }
  }
}

/**
  * It represents the distance between the raw score and the population mean in units of the standard deviation.
  * Refer https://en.wikipedia.org/wiki/Standard_score#Calculation to understand how to calculate
  **/
class ZScoreChunkedFunctionD extends VarOverTimeChunkedFunctionD() {
  final def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = {
    var zscore = Double.NaN
    if (count > 0) {
      val avg = sum / count
      val stdDev = Math.sqrt(squaredSum / count - avg*avg)
      zscore = (lastSample - avg) / stdDev
    }
    else if (sum.isNaN()) zscore = sum
    else zscore = 0d
    sampleToEmit.setValues(endTimestamp, zscore)
  }
}