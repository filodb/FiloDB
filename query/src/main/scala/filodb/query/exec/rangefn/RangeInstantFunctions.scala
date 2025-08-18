package filodb.query.exec.rangefn

import spire.syntax.cfor._

import filodb.core.query.{QueryConfig, TransientHistRow, TransientRow}
import filodb.memory.format.{vectors => bv}

object RangeInstantFunctions {
  def derivFunction(window: Window[TransientRow]): Double = {
    if (window.size < 2) {
      Double.NaN  // cannot calculate result without 2 samples
    } else {
      // We pass in an arbitrary timestamp that is near the values in use
      // to avoid floating point accuracy issues
      linearRegression(window, window(0).timestamp)
    }
  }

  /**
    * Logic is kept consistent with Prometheus' linearRegression function in order to get consistent results.
    * We can look at optimizations (if any) later.
    */
  private def linearRegression(window: Window[TransientRow],
                                                      interceptTime: Long, isPredict: Boolean = false): Double = {
    var n = 0.0
    var sumX = 0.0
    var sumY = 0.0
    var sumXY = 0.0
    var sumX2 = 0.0
    for (i <- 0 until window.size) {
      val sample = window(i)
      val x = (sample.timestamp-interceptTime).toDouble / 1000.0
      n += 1.0
      sumY += sample.value
      sumX += x
      sumXY += x * sample.value
      sumX2 += x * x
    }
    val covXY = sumXY - sumX*sumY/n
    val varX = sumX2 - sumX*sumX/n
    val slope = covXY / varX
    val intercept = sumY/n - slope*sumX/n // keeping it, needed for predict_linear function = slope*duration + intercept
    slope
  }

  def instantValue(startTimestamp: Long,
                   endTimestamp: Long,
                   window: Window[TransientRow],
                   isRate: Boolean): Double = {
    if (window.size < 2) {
      Double.NaN  // cannot calculate result without 2 samples
    } else {
      require(window.head.timestamp >= startTimestamp, "Possible internal error, found samples < startTimestamp")
      require(window.last.timestamp <= endTimestamp, "Possible internal error, found samples > endTimestamp")
      var lastSample = window.last.value
      var prevSampleRow = window(window.size - 2)
      var resultValue = lastSample - prevSampleRow.value

      if (isRate && lastSample < prevSampleRow.value) {
        resultValue = lastSample
      }

      if (isRate) {
        val sampledInterval = (window.last.timestamp - prevSampleRow.timestamp).toDouble
        if (sampledInterval == 0) {
          return Double.NaN // Avoid dividing by 0
        }
        // Convert to per-second.
        resultValue = resultValue/sampledInterval*1000
      }
      resultValue
    }
  }

  def instantValueH(startTimestamp: Long,
                    endTimestamp: Long,
                    window: Window[TransientHistRow],
                    isRate: Boolean): bv.HistogramWithBuckets = {
    if (window.size < 2) {
      bv.HistogramWithBuckets.empty  // cannot calculate result without 2 samples
    } else {
      require(window.head.timestamp >= startTimestamp, "Possible internal error, found samples < startTimestamp")
      require(window.last.timestamp <= endTimestamp, "Possible internal error, found samples > endTimestamp")

      val lastSample = window.last.value
      val prevSampleRow = window(window.size - 2)
      val prevSample = prevSampleRow.value

      // Check if histograms have compatible bucket schemes
      if (prevSample.buckets == lastSample.buckets && lastSample.numBuckets > 0) {
        val resultArray = new Array[Double](lastSample.numBuckets)
        cforRange { 0 until resultArray.size } { b =>
          val lastValue = lastSample.bucketValue(b)
          val prevValue = prevSample.bucketValue(b)
          var bucketResult = lastValue - prevValue

          if (isRate && lastValue < prevValue) {
            bucketResult = lastValue
          }

          if (isRate) {
            val sampledInterval = (window.last.timestamp - prevSampleRow.timestamp).toDouble
            if (sampledInterval == 0) {
              bucketResult = Double.NaN // Avoid dividing by 0
            } else {
              // Convert to per-second.
              bucketResult = bucketResult / sampledInterval * 1000
            }
          }
          resultArray(b) = bucketResult
        }
        bv.MutableHistogram(lastSample.buckets, resultArray)
      } else {
        // Return empty histogram for incompatible bucket schemes
        bv.HistogramWithBuckets.empty
      }
    }
  }

  // instant value for a period-counter is the last value in a window.
  def instantValueDeltaCounter(startTimestamp: Long,
                               endTimestamp: Long,
                               window: Window[TransientRow],
                               isRate: Boolean): Double = {
    if (window.size < 2) {
      Double.NaN // cannot calculate result without 2 samples
    } else {
      require(window.head.timestamp >= startTimestamp, "Possible internal error, found samples < startTimestamp")
      require(window.last.timestamp <= endTimestamp, "Possible internal error, found samples > endTimestamp")
      var resultValue = window.last.value // instant value for a period-counter is the last value in a window.
      val prevSampleRow = window(window.size - 2)
      if (isRate) {
        val sampledInterval = (window.last.timestamp - prevSampleRow.timestamp).toDouble
        resultValue = if (sampledInterval == 0) {
                        Double.NaN // Avoid dividing by 0
                      } else {
                        // Convert to per-second.
                        resultValue / sampledInterval * 1000
                      }
      }
      resultValue
    }
  }
}

object IRateFunction extends RangeFunction[TransientRow] {

  override def needsCounterCorrection: Boolean = true
  def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window[TransientRow],
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RangeInstantFunctions.instantValue(startTimestamp,
      endTimestamp, window, true)
    sampleToEmit.setValues(endTimestamp, result) // TODO need to use a NA instead of NaN
  }
}

object IRateFunctionH extends RangeFunction[TransientHistRow] {

  override def needsCounterCorrection: Boolean = true
  def addedToWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {}
  def removedFromWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window[TransientHistRow],
            sampleToEmit: TransientHistRow,
            queryConfig: QueryConfig): Unit = {
    val result = RangeInstantFunctions.instantValueH(startTimestamp,
      endTimestamp, window, true)
    sampleToEmit.setValues(endTimestamp, result)
  }
}


object IDeltaFunctionH extends RangeFunction[TransientHistRow] {

  override def needsCounterCorrection: Boolean = true
  def addedToWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {}
  def removedFromWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window[TransientHistRow],
            sampleToEmit: TransientHistRow,
            queryConfig: QueryConfig): Unit = {
    val result = RangeInstantFunctions.instantValueH(startTimestamp,
      endTimestamp, window, false)
    sampleToEmit.setValues(endTimestamp, result)
  }
}
object IDeltaFunction extends RangeFunction[TransientRow] {

  def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}

  def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window[TransientRow],
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RangeInstantFunctions.instantValue(startTimestamp,
      endTimestamp, window, false)
    sampleToEmit.setValues(endTimestamp, result)
  }
}

object IRatePeriodicFunction extends RangeFunction[TransientRow] {

  var lastFunc = LastSampleFunction
  def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window[TransientRow],
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    if (window.size < 1) {
      sampleToEmit.setValues(endTimestamp, Double.NaN) // cannot calculate result without at least 1 sample
      return
    }
    //If there is only one sample, default the sample interval to 60 seconds
    var sampledInterval: Double = 60
    lastFunc.apply(startTimestamp, endTimestamp, window, sampleToEmit, queryConfig)
    if (window.size >= 2) {
      val prevSampleRow = window(window.size - 2)
      sampledInterval = (window.last.timestamp - prevSampleRow.timestamp).toDouble
    }
    val result = if (sampledInterval == 0) {
      Double.NaN // Avoid dividing by 0
    } else sampleToEmit.value / sampledInterval * 1000
    sampleToEmit.setValues(endTimestamp, result)
  }
}

object IRatePeriodicFunctionH extends RangeFunction[TransientHistRow] {

  var lastFunc = LastSampleFunctionH
  def addedToWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {}
  def removedFromWindow(row: TransientHistRow, window: Window[TransientHistRow]): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window[TransientHistRow],
            sampleToEmit: TransientHistRow,
            queryConfig: QueryConfig): Unit = {
    if (window.size < 1) {
      // cannot calculate result without at least 1 sample
      sampleToEmit.setValues(endTimestamp, bv.HistogramWithBuckets.empty)
      return
    }
    //If there is only one sample, default the sample interval to 60 seconds
    var sampledInterval: Double = 60000
    lastFunc.apply(startTimestamp, endTimestamp, window, sampleToEmit, queryConfig)
    if (window.size >= 2) {
      val prevSampleRow = window(window.size - 2)
      sampledInterval = (window.last.timestamp - prevSampleRow.timestamp).toDouble
    }

    val lastHist = sampleToEmit.value
    if (lastHist.numBuckets == 0 || sampledInterval == 0) {
      // Return empty for empty histogram or zero interval
      sampleToEmit.setValues(endTimestamp, bv.HistogramWithBuckets.empty)
      return
    }

    // Calculate rate for each bucket
    val rateArray = new Array[Double](lastHist.numBuckets)
    cforRange { 0 until rateArray.size } { b =>
      rateArray(b) = lastHist.bucketValue(b) / sampledInterval * 1000
    }
    val result = bv.MutableHistogram(lastHist.buckets, rateArray)
    sampleToEmit.setValues(endTimestamp, result)
  }
}

object IDeltaPeriodicFunction extends RangeFunction[TransientRow] {

  def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window[TransientRow],
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RangeInstantFunctions.instantValueDeltaCounter(startTimestamp,
      endTimestamp, window, false)
    sampleToEmit.setValues(endTimestamp, result)
  }
}

object DerivFunction extends RangeFunction[TransientRow] {

  def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window[TransientRow],
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RangeInstantFunctions.derivFunction(window)
    sampleToEmit.setValues(endTimestamp, result) // TODO need to use a NA instead of NaN
  }
}

class ResetsFunction extends RangeFunction[TransientRow] {
  var resets = Double.NaN // NaN for windows that do not have data

  def addedToWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
    val size = window.size
    val currentValue = if (row.value.isNaN) 0 else row.value
    if (resets.isNaN && size > 0) resets = 0
    if (size > 1 ) {
      val prevValue = if (window(size - 2).value.isNaN) 0 else window(size - 2).value
      if (currentValue < prevValue) resets += 1
    }
  }

  def removedFromWindow(row: TransientRow, window: Window[TransientRow]): Unit = {
    val currentValue = if (row.value.isNaN) 0 else row.value
    if (window.size > 0) {
      val prevValue = if (window.head.value.isNaN) 0 else window.head.value
      if (currentValue > prevValue) resets -= 1
    } else resets = Double.NaN
  }

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window[TransientRow],
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    sampleToEmit.setValues(endTimestamp, resets)
  }
}
