package filodb.query.exec.rangefn

import spire.syntax.cfor._

import filodb.core.query.{QueryConfig, TransientHistRow, TransientRow}
import filodb.memory.format.{ vectors => bv, BinaryVector, CounterVectorReader, MemoryReader, VectorDataReader}
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.vectors.{Base2ExpHistogramBuckets, MutableHistogram}
import filodb.query.exec.FiloQueryConfig

object RateFunctions {

  /**
   * Logic is kept consistent with Prometheus' extrapolatedRate function in order to get consistent results.
   * We can look at optimizations (if any) later.
   */
  def extrapolatedRate(startTimestamp: Long,
                       endTimestamp: Long,
                       window: Window[TransientRow],
                       isCounter: Boolean,
                       isRate: Boolean): Double =
    if (window.size < 2) {
      Double.NaN  // cannot calculate result without 2 samples
    } else {
      require(window.head.timestamp >= startTimestamp, "Possible internal error, found samples < startTimestamp")
      require(window.last.timestamp <= endTimestamp, "Possible internal error, found samples > endTimestamp")
      extrapolatedRate(startTimestamp, endTimestamp, window.size,
                       window.head.timestamp, window.head.value,
                       window.last.timestamp, window.last.value,
                       isCounter, isRate)
    }

  /**
   * Histogram version of extrapolatedRate for TransientHistRow - returns HistogramWithBuckets
   */
  def extrapolatedRateH(startTimestamp: Long,
                        endTimestamp: Long,
                        window: Window[TransientHistRow],
                        isCounter: Boolean,
                        isRate: Boolean): bv.HistogramWithBuckets =
    if (window.size < 2) {
      bv.HistogramWithBuckets.empty  // cannot calculate result without 2 samples
    } else {
      require(window.head.timestamp >= startTimestamp, "Possible internal error, found samples < startTimestamp")
      require(window.last.timestamp <= endTimestamp, "Possible internal error, found samples > endTimestamp")

      val firstHist = window.head.value
      val lastHist = window.last.value

      // Check if histograms have compatible bucket schemes
      if (firstHist.buckets == lastHist.buckets && firstHist.numBuckets > 0) {
        val rateArray = new Array[Double](firstHist.numBuckets)
        cforRange { 0 until rateArray.size } { b =>
          rateArray(b) = extrapolatedRate(startTimestamp, endTimestamp, window.size,
                                        window.head.timestamp, firstHist.bucketValue(b),
                                        window.last.timestamp, lastHist.bucketValue(b),
                                        isCounter, isRate)
        }
        bv.MutableHistogram(firstHist.buckets, rateArray)
      } else {
        // Return empty histogram for incompatible bucket schemes
        bv.HistogramWithBuckets.empty
      }
    }

  /**
   * Calculates rate/delta/increase based on window information and between sample1 and sample2
   * @param numSamples the number of samples inclusive of start and end
   */
  //scalastyle:off parameter.number
  def extrapolatedRate(windowStart: Long,
                       windowEnd: Long,
                       numSamples: Int,
                       sample1Time: Long, sample1Value: Double,
                       sample2Time: Long, sample2Value: Double,
                       isCounter: Boolean,
                       isRate: Boolean): Double = {
    var durationToStart = (sample1Time - windowStart).toDouble / 1000
    val durationToEnd = (windowEnd - sample2Time).toDouble / 1000
    val sampledInterval = (sample2Time - sample1Time).toDouble / 1000
    val averageDurationBetweenSamples = sampledInterval / (numSamples.toDouble - 1)
    val delta = sample2Value - sample1Value
    //scalastyle:off
    // println(s"XXX: windowStart=$windowStart windowEnd=$windowEnd numSamples=$numSamples")
    // println(s"\tsample1: t=$sample1Time v=$sample1Value\n\tsample2: t=$sample2Time v=$sample2Value")
    //scalastyle:on

    if (isCounter && delta > 0 && sample1Value >= 0) {
      // Counters cannot be negative. If we have any slope at all (i.e. resultValue went up), we can extrapolate
      // the zero point of the counter. If the duration to the  zero point is shorter than the durationToStart, we
      // take the zero point as the start of the series, thereby avoiding extrapolation to negative counter values.
      val durationToZero = sampledInterval * (sample1Value / delta)
      if (durationToZero < durationToStart) {
        durationToStart = durationToZero
      }
    }
    // If the first/last samples are close to the boundaries of the range, extrapolate the result. This is as we
    // expect that another sample will exist given the spacing between samples we've seen thus far, with an
    // allowance for noise.
    val extrapolationThreshold = averageDurationBetweenSamples * 1.1
    var extrapolateToInterval: Double = sampledInterval
    extrapolateToInterval +=
      (if (durationToStart < extrapolationThreshold) durationToStart else averageDurationBetweenSamples / 2)
    extrapolateToInterval +=
      (if (durationToEnd < extrapolationThreshold) durationToEnd else averageDurationBetweenSamples / 2)
    val scaledDelta = delta * (extrapolateToInterval / sampledInterval)
    // for rate, we need rate as a per-second value
    val result = if (isRate) (scaledDelta / (windowEnd - windowStart) * 1000) else scaledDelta
    result
  }
}
//scalastyle:on parameter.number

object IncreaseFunction extends RangeFunction[TransientRow] {

  override def needsCounterCorrection: Boolean = true
  def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window[TransientRow],
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RateFunctions.extrapolatedRate(startTimestamp,
      endTimestamp, window, true, false)
    sampleToEmit.setValues(endTimestamp, result) // TODO need to use a NA instead of NaN
  }
}

object RateFunction extends RangeFunction[TransientRow] {

  override def needsCounterCorrection: Boolean = true
  def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window[TransientRow],
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RateFunctions.extrapolatedRate(startTimestamp,
      endTimestamp, window, true, true)
    sampleToEmit.setValues(endTimestamp, result) // TODO need to use a NA instead of NaN
  }
}

object RateFunctionH extends RangeFunction[TransientHistRow] {

  override def needsCounterCorrection: Boolean = true
  def addedToWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {}
  def removedFromWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window[TransientHistRow],
            sampleToEmit: TransientHistRow,
            queryConfig: QueryConfig): Unit = {
    val result = RateFunctions.extrapolatedRateH(startTimestamp,
      endTimestamp, window, true, true)
    sampleToEmit.setValues(endTimestamp, result) // TODO need to use a NA instead of NaN
  }
}

object DeltaFunction extends RangeFunction[TransientRow] {

  def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window[TransientRow],
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RateFunctions.extrapolatedRate(startTimestamp,
      endTimestamp, window, false, false)
    sampleToEmit.setValues(endTimestamp, result)
  }
}

class RateOverDeltaFunction extends RangeFunction[TransientRow] {
  private val sumOverTime = new SumOverTimeFunction
  override def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit =
    sumOverTime.addedToWindow(row, window)

  override def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit =
    sumOverTime.removedFromWindow(row, window)

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window[TransientRow],
                     sampleToEmit: TransientRow,
                     queryConfig: QueryConfig): Unit = {
    sampleToEmit.setValues(endTimestamp, sumOverTime.sum / (endTimestamp - startTimestamp) * 1000 )
  }
}

class RateOverDeltaFunctionH extends RangeFunction[TransientHistRow] {
  private val sumOverTime = new SumOverTimeFunctionH
  override def addedToWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit =
    sumOverTime.addedToWindow(row, window)

  override def removedFromWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit =
    sumOverTime.removedFromWindow(row, window)

  override def apply(startTimestamp: Long, endTimestamp: Long, window: Window[TransientHistRow],
                     sampleToEmit: TransientHistRow,
                     queryConfig: QueryConfig): Unit = {
    val mh = sumOverTime.sum
    val timeDelta = (endTimestamp - startTimestamp) / 1000.0
    // divide each value of the histogram bucket by time
    sampleToEmit.setValues(endTimestamp, MutableHistogram(mh.buckets, mh.values.map(_/timeDelta)))
  }
}

/**
 * A base class for chunked calculation of rate and other things that depend on
 * counter correction.
 * The algorithm is pretty simple: for each time window, for each chunk, we compare
 * the timestamps and update the lowest and highest so that we end up with the earliest
 * and latest first and last samples.  Basically we continually expand the window until
 * we have the biggest one.  Counter correction is done by passing the correction
 * to methods to extract the corrected value that include passed in correction factor.
 * It is O(nWindows * nChunks) which is usually << O(nSamples).
 */
abstract class ChunkedRateFunctionBase extends CounterChunkedRangeFunction[TransientRow] {
  var numSamples = 0
  var lowestTime = Long.MaxValue
  var lowestValue = Double.NaN
  var highestTime = 0L
  var highestValue = Double.NaN

  def isCounter: Boolean
  def isRate: Boolean

  override def reset(): Unit = {
    numSamples = 0
    lowestTime = Long.MaxValue
    lowestValue = Double.NaN
    highestTime = 0L
    highestValue = Double.NaN
    super.reset()
  }

  def addTimeChunks(acc: MemoryReader, vector: BinaryVectorPtr, reader: CounterVectorReader,
                    startRowNum: Int, endRowNum: Int,
                    startTime: Long, endTime: Long): Unit = {
    val dblReader = reader.asDoubleReader
    // if the chunk is a single row NaN value, then return. Prometheus end of time series marker.
    // This is to make sure we don't set highestValue to zero. avoids negative rate/increase values.
    if (startRowNum == 0 && endRowNum == 0 && dblReader.apply(acc, vector, startRowNum).isNaN)
      return
    if (startTime < lowestTime || endTime > highestTime) {
      numSamples += endRowNum - startRowNum + 1
      if (startTime < lowestTime) {
        lowestTime = startTime
        lowestValue = dblReader.correctedValue(acc, vector, startRowNum, correctionMeta)
      }
      if (endTime > highestTime) {
        highestTime = endTime
        highestValue = dblReader.correctedValue(acc, vector, endRowNum, correctionMeta)
      }
    }
  }

  override def apply(windowStart: Long, windowEnd: Long, sampleToEmit: TransientRow): Unit = {
    if (highestTime > lowestTime) {
      // NOTE: It seems in order to match previous code, we have to adjust the windowStart by -1 so it's "inclusive"
      val curWindowStart = if (FiloQueryConfig.isInclusiveRange)
                             windowStart
                           else
                             windowStart - 1
      val result = RateFunctions.extrapolatedRate(
                    curWindowStart, windowEnd, numSamples,
                     lowestTime, lowestValue,
                     highestTime, highestValue,
                     isCounter, isRate)
      sampleToEmit.setValues(windowEnd, result)
    } else {
      sampleToEmit.setValues(windowEnd, Double.NaN)
    }
  }

  def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = ???
}

class ChunkedRateFunction extends ChunkedRateFunctionBase {
  def isCounter: Boolean = true
  def isRate: Boolean    = true
}

class ChunkedIncreaseFunction extends ChunkedRateFunctionBase {
  def isCounter: Boolean = true
  def isRate: Boolean    = false
}

class ChunkedDeltaFunction extends ChunkedRateFunctionBase {
  def isCounter: Boolean = false
  def isRate: Boolean    = false

  // We have to override addTimeChunks as delta function does not care about corrections
  override def addTimeChunks(acc: MemoryReader, vector: BinaryVectorPtr, reader: CounterVectorReader,
                             startRowNum: Int, endRowNum: Int,
                             startTime: Long, endTime: Long): Unit = {
    val dblReader = reader.asDoubleReader
    if (startTime < lowestTime || endTime > highestTime) {
      numSamples += endRowNum - startRowNum + 1
      if (startTime < lowestTime) {
        lowestTime = startTime
        lowestValue = dblReader(acc, vector, startRowNum)
      }
      if (endTime > highestTime) {
        highestTime = endTime
        highestValue = dblReader(acc, vector, endRowNum)
      }
    }
  }
}

/**
 * A base class for chunked calculation of rate etc for increasing/counter-like histograms.
 * Note that the rate of two histograms is itself a histogram.
 * Similar algorithm to ChunkedRateFunctionBase.
 * It is O(nWindows * nChunks) which is usually << O(nSamples).
 */
abstract class HistogramRateFunctionBase extends CounterChunkedRangeFunction[TransientHistRow] {
  var numSamples = 0
  var lowestTime = Long.MaxValue
  var lowestValue: bv.HistogramWithBuckets = bv.HistogramWithBuckets.empty
  var highestTime = 0L
  var highestValue: bv.HistogramWithBuckets = bv.HistogramWithBuckets.empty

  def isCounter: Boolean
  def isRate: Boolean

  override def reset(): Unit = {
    numSamples = 0
    lowestTime = Long.MaxValue
    lowestValue = bv.HistogramWithBuckets.empty
    highestTime = 0L
    highestValue = bv.HistogramWithBuckets.empty
    super.reset()
  }

  def addTimeChunks(acc: MemoryReader, vector: BinaryVectorPtr, reader: CounterVectorReader,
                    startRowNum: Int, endRowNum: Int,
                    startTime: Long, endTime: Long): Unit = reader match {
    case histReader: bv.CounterHistogramReader =>
      if (startTime < lowestTime || endTime > highestTime) {
        numSamples += endRowNum - startRowNum + 1
        if (startTime < lowestTime) {
          lowestTime = startTime
          lowestValue = histReader.correctedValue(startRowNum, correctionMeta)
        }
        if (endTime > highestTime) {
          highestTime = endTime
          highestValue = histReader.correctedValue(endRowNum, correctionMeta)
        }
      }
    case other: CounterVectorReader =>
  }

  override def apply(windowStart: Long, windowEnd: Long, sampleToEmit: TransientHistRow): Unit = {
    if (highestTime > lowestTime) {
      // NOTE: It seems in order to match previous code, we have to adjust the windowStart by -1 so it's "inclusive"
      // TODO: handle case where schemas are different and we need to interpolate schemas
      if (highestValue.buckets == lowestValue.buckets) {
        val rateArray = new Array[Double](lowestValue.numBuckets)

        val curWindowStart = if (FiloQueryConfig.isInclusiveRange)
                                windowStart
                             else
                                windowStart - 1

        cforRange { 0 until rateArray.size } { b =>
          rateArray(b) = RateFunctions.extrapolatedRate(
                          curWindowStart, windowEnd, numSamples,
                           lowestTime, lowestValue.bucketValue(b),
                           highestTime, highestValue.bucketValue(b),
                           isCounter, isRate)
        }
        sampleToEmit.setValues(windowEnd, bv.MutableHistogram(lowestValue.buckets, rateArray))
      } else if (highestValue.buckets.isInstanceOf[Base2ExpHistogramBuckets] &&
                 lowestValue.buckets.isInstanceOf[Base2ExpHistogramBuckets]) {
        // Assume highestValue.buckets.scale <= lowestValue.buckets.scale since client always
        // reduces scale over time. This is confirmed in java otel sdk.
        val hvb = highestValue.buckets.asInstanceOf[Base2ExpHistogramBuckets]
        val lvb = lowestValue.buckets.asInstanceOf[Base2ExpHistogramBuckets]
        if (hvb.canAccommodate(lvb)) {
          // TODO then handle rate calculation for different bucket scheme (due to difference in scale or buckets)
          ???
        } else {
          sampleToEmit.setValues(windowEnd, bv.HistogramWithBuckets.empty)
        }
      } else {
        sampleToEmit.setValues(windowEnd, bv.HistogramWithBuckets.empty)
      }
    } else {
      sampleToEmit.setValues(windowEnd, bv.HistogramWithBuckets.empty)
    }
  }

  def apply(endTimestamp: Long, sampleToEmit: TransientHistRow): Unit = ???
}

class HistRateFunction extends HistogramRateFunctionBase {
  def isCounter: Boolean = true
  def isRate: Boolean    = true
}

class HistIncreaseFunction extends HistogramRateFunctionBase {
  def isCounter: Boolean = true
  def isRate: Boolean    = false
}

/**
 * Rate over Delta aggregation temporality metrics.
 * It essentially translates to = sum_over_time / window_length_in_seconds
 */
class RateOverDeltaChunkedFunctionD extends ChunkedDoubleRangeFunction {

  private val sumFunc = new SumOverTimeChunkedFunctionD
  override final def reset(): Unit = {sumFunc.reset() }

  final def addTimeDoubleChunks(doubleVectAcc: MemoryReader,
                                doubleVect: BinaryVector.BinaryVectorPtr,
                                doubleReader: bv.DoubleVectorDataReader,
                                startRowNum: Int,
                                endRowNum: Int): Unit =
    sumFunc.addTimeDoubleChunks(doubleVectAcc, doubleVect, doubleReader, startRowNum, endRowNum)

  override def apply(windowStart: Long, windowEnd: Long, sampleToEmit: TransientRow): Unit = {
    val curWindowStart = if (FiloQueryConfig.isInclusiveRange)
                            windowStart
                         else
                            windowStart - 1
    sampleToEmit.setValues(windowEnd, sumFunc.sum / (windowEnd - (curWindowStart)) * 1000)
  }

  override def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = ???
}

class RateOverDeltaChunkedFunctionL extends ChunkedLongRangeFunction {
  private val sumFunc = new SumOverTimeChunkedFunctionL

  override final def reset(): Unit = sumFunc.reset()

  final def addTimeLongChunks(longVectAcc: MemoryReader,
                              longVect: BinaryVector.BinaryVectorPtr,
                              longReader: bv.LongVectorDataReader,
                              startRowNum: Int,
                              endRowNum: Int): Unit =
    sumFunc.addTimeChunks(longVectAcc, longVect, longReader, startRowNum, endRowNum)

  override def apply(windowStart: Long, windowEnd: Long, sampleToEmit: TransientRow): Unit = {
    val curWindowStart = if (FiloQueryConfig.isInclusiveRange)
                            windowStart
                         else
                            windowStart - 1
    sampleToEmit.setValues(windowEnd, sumFunc.sum / (windowEnd - (curWindowStart)) * 1000)
  }

  override def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = ???
}

class RateOverDeltaChunkedFunctionH(var h: bv.MutableHistogram = bv.Histogram.empty)
  extends TimeRangeFunction[TransientHistRow] {

  private val hFunc = new SumOverTimeChunkedFunctionH
  override final def reset(): Unit = hFunc.reset()

  override def apply(windowStart: Long, windowEnd: Long, sampleToEmit: TransientHistRow): Unit = {
    val rateArray = new Array[Double](hFunc.h.numBuckets)
    cforRange {
      0 until rateArray.size
    } { b =>
      rateArray(b) = hFunc.h.bucketValue(b) / (windowEnd - windowStart) * 1000
    }
    sampleToEmit.setValues(windowEnd, bv.MutableHistogram(hFunc.h.buckets, rateArray))
  }

  final def addTimeChunks(vectAcc: MemoryReader,
                          vectPtr: BinaryVector.BinaryVectorPtr,
                          reader: VectorDataReader,
                          startRowNum: Int,
                          endRowNum: Int): Unit =
    hFunc.addTimeChunks(vectAcc, vectPtr, reader, startRowNum, endRowNum)

  def apply(endTimestamp: Long, sampleToEmit: TransientHistRow): Unit = ???
}

// Histogram Range Functions using extrapolatedRate

object IncreaseFunctionH extends RangeFunction[TransientHistRow] {
  override def needsCounterCorrection: Boolean = true
  def addedToWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {}
  def removedFromWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window[TransientHistRow],
            sampleToEmit: TransientHistRow,
            queryConfig: QueryConfig): Unit = {
    val result = RateFunctions.extrapolatedRateH(startTimestamp,
      endTimestamp, window, true, false)
    sampleToEmit.setValues(endTimestamp, result)
  }
}


object DeltaFunctionH extends RangeFunction[TransientHistRow] {
  def addedToWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {}
  def removedFromWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window[TransientHistRow],
            sampleToEmit: TransientHistRow,
            queryConfig: QueryConfig): Unit = {
    val result = RateFunctions.extrapolatedRateH(startTimestamp,
      endTimestamp, window, false, false)
    sampleToEmit.setValues(endTimestamp, result)
  }
}