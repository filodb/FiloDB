package filodb.query.exec.rangefn

import filodb.query.QueryConfig
import filodb.query.exec.TransientRow

object RangeInstantFunctions {
  def derivFunction(window: Window): Double = {
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
  def linearRegression(window: Window, interceptTime: Long, isPredict: Boolean = false): Double = {
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
                   window: Window,
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
          None // Avoid dividing by 0
        }
        // Convert to per-second.
        resultValue = resultValue/sampledInterval*1000
      }
      resultValue
    }
  }
}

object IRateFunction extends RangeFunction {

  override def needsCounterCorrection: Boolean = true
  def addedToWindow(row: TransientRow, window: Window): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RangeInstantFunctions.instantValue(startTimestamp,
      endTimestamp, window, true)
    sampleToEmit.setValues(endTimestamp, result) // TODO need to use a NA instead of NaN
  }
}

object IDeltaFunction extends RangeFunction {

  def addedToWindow(row: TransientRow, window: Window): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RangeInstantFunctions.instantValue(startTimestamp,
      endTimestamp, window, false)
    sampleToEmit.setValues(endTimestamp, result)
  }
}

object DerivFunction extends RangeFunction {

  def addedToWindow(row: TransientRow, window: Window): Unit = {}
  def removedFromWindow(row: TransientRow, window: Window): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RangeInstantFunctions.derivFunction(window)
    sampleToEmit.setValues(endTimestamp, result) // TODO need to use a NA instead of NaN
  }
}

object ResetsFunction extends RangeFunction {
  var resets = 0

  def addedToWindow(row: TransientRow, window: Window): Unit = {
    val size = window.size
    if (size > 1 && window(size - 2).value > row.value) {
      resets += 1
    }
  }

  def removedFromWindow(row: TransientRow, window: Window): Unit = {
    if (row.value > window.head.value) {
      resets -= 1
    }
  }

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    sampleToEmit.setValues(endTimestamp, resets)
  }
}
