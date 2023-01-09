package filodb.query.exec.rangefn

import spire.syntax.cfor._

import filodb.core.query.{QueryConfig, TransientHistRow, TransientRow}
import filodb.memory.format.{vectors => bv, CounterVectorReader, MemoryReader, VectorDataReader}
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.vectors.HistogramBuckets

object RateFunctions {

  /**
    * Logic is kept consistent with Prometheus' extrapolatedRate function in order to get consistent results.
    * We can look at optimizations (if any) later.
    */
  def extrapolatedRate(startTimestamp: Long,
                       endTimestamp: Long,
                       window: Window,
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
   * extrapolate rate for period-counter.
   * Logic is kept consistent with Prometheus' extrapolatedRate function to extrapolate
   */
  def extrapolatedPeriodicRate(startTimestamp: Long,
                       endTimestamp: Long,
                       window: Window,
                       isRate: Boolean): Double =
    if (window.size < 2) {
      Double.NaN // cannot calculate result without 2 samples
    } else {
      require(window.head.timestamp >= startTimestamp, "Possible internal error, found samples < startTimestamp")
      require(window.last.timestamp <= endTimestamp, "Possible internal error, found samples > endTimestamp")
      extrapolatedPeriodicRate(startTimestamp, endTimestamp, window.size,
        window.head.timestamp,
        window.last.timestamp,
        (0 until window.size).map(window(_).value).sum.toLong,
        isRate)
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

  /**
   * Calculates rate/delta/increase (period-counter) based on window information and between sample1 and sample2
   *
   * @param numSamples the number of samples inclusive of start and end
   */
  //scalastyle:off parameter.number
  def extrapolatedPeriodicRate(windowStart: Long,
                               windowEnd: Long,
                               numSamples: Int,
                               sample1Time: Long,
                               sample2Time: Long,
                               sum: Double,
                               isRate: Boolean): Double = {
    val durationToStart = (sample1Time - windowStart).toDouble / 1000
    val durationToEnd = (windowEnd - sample2Time).toDouble / 1000
    val sampledInterval = (sample2Time - sample1Time).toDouble / 1000
    val averageDurationBetweenSamples = sampledInterval / (numSamples.toDouble - 1)

    // If the first/last samples are close to the boundaries of the range, extrapolate the result. This is as we
    // expect that another sample will exist given the spacing between samples we've seen thus far, with an
    // allowance for noise.
    val extrapolationThreshold = averageDurationBetweenSamples * 1.1
    var extrapolateToInterval: Double = sampledInterval
    extrapolateToInterval +=
      (if (durationToStart < extrapolationThreshold) durationToStart else averageDurationBetweenSamples / 2)
    extrapolateToInterval +=
      (if (durationToEnd < extrapolationThreshold) durationToEnd else averageDurationBetweenSamples / 2)
    val scaledSum = sum * (extrapolateToInterval / sampledInterval)
    // for rate, we need rate as a per-second value
    val result = if (isRate) (scaledSum / (windowEnd - windowStart) * 1000) else scaledSum
    result
  }
}
//scalastyle:on parameter.number

object IncreaseFunction extends RangeFunction {

  override def needsCounterCorrection: Boolean = true
  def addedToWindow(row: TransientRow, window: Window): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RateFunctions.extrapolatedRate(startTimestamp,
      endTimestamp, window, true, false)
    sampleToEmit.setValues(endTimestamp, result) // TODO need to use a NA instead of NaN
  }
}

object RateFunction extends RangeFunction {

  override def needsCounterCorrection: Boolean = true
  def addedToWindow(row: TransientRow, window: Window): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RateFunctions.extrapolatedRate(startTimestamp,
      endTimestamp, window, true, true)
    sampleToEmit.setValues(endTimestamp, result) // TODO need to use a NA instead of NaN
  }
}

object DeltaFunction extends RangeFunction {

  def addedToWindow(row: TransientRow, window: Window): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RateFunctions.extrapolatedRate(startTimestamp,
      endTimestamp, window, false, false)
    sampleToEmit.setValues(endTimestamp, result)
  }
}

object PeriodicIncreaseFunction extends RangeFunction {

  override def needsCounterCorrection: Boolean = false
  def addedToWindow(row: TransientRow, window: Window): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window): Unit = {}
  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RateFunctions.extrapolatedPeriodicRate(startTimestamp,
      endTimestamp, window, false)
    sampleToEmit.setValues(endTimestamp, result) // TODO need to use a NA instead of NaN
  }
}

object PeriodicRateFunction extends RangeFunction {

  override def needsCounterCorrection: Boolean = false
  def addedToWindow(row: TransientRow, window: Window): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RateFunctions.extrapolatedPeriodicRate(startTimestamp,
      endTimestamp, window, true)
    sampleToEmit.setValues(endTimestamp, result) // TODO need to use a NA instead of NaN
  }
}

object PeriodicDeltaFunction extends RangeFunction {

  def addedToWindow(row: TransientRow, window: Window): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = ???
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
      val result = RateFunctions.extrapolatedRate(
                     windowStart - 1, windowEnd, numSamples,
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
        cforRange { 0 until rateArray.size } { b =>
          rateArray(b) = RateFunctions.extrapolatedRate(
                           windowStart - 1, windowEnd, numSamples,
                           lowestTime, lowestValue.bucketValue(b),
                           highestTime, highestValue.bucketValue(b),
                           isCounter, isRate)
        }
        sampleToEmit.setValues(windowEnd, bv.MutableHistogram(lowestValue.buckets, rateArray))
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

/********************** Rate functions for Delta/Periodic counter metrics **********************/


/**
 * A base class for chunked calculation of rate/increase functions - without counter correction.
 * The algorithm is pretty simple: for each time window, for each chunk, we compare
 * the timestamps and update the lowest and highest so that we end up with the earliest
 * and latest first and last samples.  Basically we continually expand the window until
 * we have the biggest one. And also sum of the values between startRow and endRow is calculated.
 * Sum of the values gives us `increase` in values from last window. divide `increase` by `time-window` gives us `rate`
 * It is O(nWindows * nChunks) which is usually << O(nSamples).
 */
abstract class ChunkedPeriodicRateFunctionBase extends PeriodCounterChunkedRangeFunction[TransientRow] {
  var numSamples = 0
  var lowestTime = Long.MaxValue
  var lowestValue = Double.NaN
  var highestTime = 0L
  var highestValue = Double.NaN
  var sum = 0d;

  def isRate: Boolean

  override def reset(): Unit = {
    numSamples = 0
    lowestTime = Long.MaxValue
    lowestValue = Double.NaN
    highestTime = 0L
    highestValue = Double.NaN
    sum = 0d;
    super.reset()
  }

  def addTimeChunks(acc: MemoryReader, vector: BinaryVectorPtr, reader: VectorDataReader,
                    startRowNum: Int, endRowNum: Int,
                    startTime: Long, endTime: Long): Unit = {
    val dblReader = reader.asDoubleReader
    if (startTime < lowestTime || endTime > highestTime) {
      numSamples += endRowNum - startRowNum + 1
      if (startTime < lowestTime) {
        lowestTime = startTime
      }
      if (endTime > highestTime) {
        highestTime = endTime
      }
      sum += dblReader.sum(acc, vector, start = startRowNum, end = endRowNum) // Sum of the values gives us the increase
    }
  }

  override def apply(windowStart: Long, windowEnd: Long, sampleToEmit: TransientRow): Unit = {
    if (highestTime > lowestTime) {
      // NOTE: It seems in order to match previous code, we have to adjust the windowStart by -1 so it's "inclusive"
      val result = RateFunctions.extrapolatedPeriodicRate(
        windowStart - 1, windowEnd, numSamples,
        lowestTime,
        highestTime,
        sum,
        isRate)
      sampleToEmit.setValues(windowEnd, result)
    } else {
      sampleToEmit.setValues(windowEnd, Double.NaN)
    }
  }

  def apply(endTimestamp: Long, sampleToEmit: TransientRow): Unit = ???
}

class ChunkedPeriodicRateFunction extends ChunkedPeriodicRateFunctionBase {
  def isRate: Boolean    = true
}

class ChunkedPeriodicIncreaseFunction extends ChunkedPeriodicRateFunctionBase {
  def isRate: Boolean    = false
}


/**
 * A base class for chunked calculation of rate etc for period-counter-like histograms.
 * Note that the rate of two histograms is itself a histogram.
 * Similar algorithm to ChunkedPeriodicRateFunctionBase.
 * It is O(nWindows * nChunks) which is usually << O(nSamples).
 */
abstract class HistogramPeriodicRateFunctionBase extends PeriodCounterChunkedRangeFunction[TransientHistRow] {
  var numSamples = 0
  var lowestTime = Long.MaxValue
  var highestTime = 0L
  var summedHist: bv.MutableHistogram = bv.MutableHistogram.empty(HistogramBuckets.emptyBuckets)

  def isRate: Boolean

  override def reset(): Unit = {
    numSamples = 0
    lowestTime = Long.MaxValue
    highestTime = 0L
    summedHist = bv.MutableHistogram.empty(HistogramBuckets.emptyBuckets)
    super.reset()
  }

  def addTimeChunks(acc: MemoryReader, vector: BinaryVectorPtr, reader: VectorDataReader,
                    startRowNum: Int, endRowNum: Int,
                      startTime: Long, endTime: Long): Unit = reader match {
    case histReader: bv.RowHistogramReader =>
      if (startTime < lowestTime || endTime > highestTime) {
        numSamples += endRowNum - startRowNum + 1
        if (startTime < lowestTime) {
          lowestTime = startTime
        }
        if (endTime > highestTime) {
          highestTime = endTime
        }
        if (summedHist.isEmpty) {
          summedHist = histReader.sum(startRowNum, endRowNum)
        } else {
          summedHist.addNoCorrection(histReader.sum(startRowNum, endRowNum))
        }
      }
    case _ =>
  }

  override def apply(windowStart: Long, windowEnd: Long, sampleToEmit: TransientHistRow): Unit = {
    if (highestTime > lowestTime) {
      // NOTE: It seems in order to match previous code, we have to adjust the windowStart by -1 so it's "inclusive"
      // TODO: handle case where schemas are different and we need to interpolate schemas

        val rateArray = new Array[Double](summedHist.numBuckets)
        cforRange { 0 until rateArray.size } { b =>
          rateArray(b) = RateFunctions.extrapolatedPeriodicRate(
            windowStart - 1, windowEnd, numSamples,
            lowestTime,
            highestTime,
            summedHist.bucketValue(b),
            isRate)
        }
        sampleToEmit.setValues(windowEnd, bv.MutableHistogram(summedHist.buckets, rateArray))

    } else {
      sampleToEmit.setValues(windowEnd, bv.HistogramWithBuckets.empty)
    }
  }

  def apply(endTimestamp: Long, sampleToEmit: TransientHistRow): Unit = ???
}

class HistPeriodicRateFunction extends HistogramPeriodicRateFunctionBase {
  def isRate: Boolean    = true
}

class HistPeriodicIncreaseFunction extends HistogramPeriodicRateFunctionBase {
  def isRate: Boolean    = false
}


