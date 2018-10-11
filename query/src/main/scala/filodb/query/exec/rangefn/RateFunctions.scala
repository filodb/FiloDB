package filodb.query.exec.rangefn

import filodb.query.QueryConfig
import filodb.query.exec.TransientRow

object RateFunctions {

  /**
    * Logic is kept consistent with Prometheus' extrapolatedRate function in order to get consistent results.
    * We can look at optimizations (if any) later.
    */
  def extrapolatedRate(startTimestamp: Long,
                       endTimestamp: Long,
                       window: Window,
                       isCounter: Boolean,
                       isRate: Boolean): Option[Double] = {
    if (window.size < 2) {
      None  // cannot calculate result without 2 samples
    } else {
      require(window.head.timestamp >= startTimestamp, "Possible internal error, found samples < startTimestamp")
      require(window.last.timestamp <= endTimestamp, "Possible internal error, found samples > endTimestamp")
      var durationToStart = (window.head.timestamp - startTimestamp).toDouble / 1000
      val durationToEnd = (endTimestamp - window.last.timestamp).toDouble / 1000
      val sampledInterval = (window.last.timestamp - window.head.timestamp).toDouble / 1000
      val averageDurationBetweenSamples = sampledInterval / window.size.toDouble
      val delta = window.last.value - window.head.value

      if (isCounter && delta > 0 && window.head.value >= 0) {
        // Counters cannot be negative. If we have any slope at all (i.e. resultValue went up), we can extrapolate
        // the zero point of the counter. If the duration to the  zero point is shorter than the durationToStart, we
        // take the zero point as the start of the series, thereby avoiding extrapolation to negative counter values.
        val durationToZero = sampledInterval * (window.head.value / delta)
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
      val result = if (isRate) (scaledDelta / (endTimestamp - startTimestamp) * 1000) else scaledDelta
      Some(result)
    }
  }

  def derivFunction(window: Window): Option[Double] = {
    if (window.size < 2) {
      None  // cannot calculate result without 2 samples
    } else {
      // We pass in an arbitrary timestamp that is near the values in use
      // to avoid floating point accuracy issues
      Some(linearRegression(window, window(0).timestamp)._1)
    }
  }

  /**
    * Logic is kept consistent with Prometheus' linearRegression function in order to get consistent results.
    * We can look at optimizations (if any) later.
    */
  def linearRegression(window: Window, interceptTime: Long): Tuple2[Double, Double] = {
    var n = 0.0
    var sumX = 0.0
    var sumY = 0.0
    var sumXY = 0.0
    var sumX2 = 0.0
    for (i <- 0 until window.size) {
      var sample = window(i)
      var x = (sample.timestamp-interceptTime).toDouble / 1000.0
      n += 1.0
      sumY += sample.value
      sumX += x
      sumXY += x * sample.value
      sumX2 += x * x
    }
    val covXY = sumXY - sumX*sumY/n
    val varX = sumX2 - sumX*sumX/n
    val slope = covXY / varX
    val intercept = sumY/n - slope*sumX/n
    Tuple2 (slope, intercept)
  }

  def instantValue(startTimestamp: Long,
                       endTimestamp: Long,
                       window: Window,
                       isRate: Boolean): Option[Double] = {
    if (window.size < 2) {
      None  // cannot calculate result without 2 samples
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
      Some(resultValue)
    }
  }
}

object IncreaseFunction extends RangeFunction {

  override def needsCounterCorrection: Boolean = true
  def addToWindow(row: TransientRow): Unit = {}
  def removeFromWindow(row: TransientRow): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RateFunctions.extrapolatedRate(startTimestamp,
      endTimestamp, window, true, false)
    sampleToEmit.setValues(endTimestamp, result.getOrElse(Double.NaN)) // TODO need to use a NA instead of NaN
  }
}

object RateFunction extends RangeFunction {

  override def needsCounterCorrection: Boolean = true
  def addToWindow(row: TransientRow): Unit = {}
  def removeFromWindow(row: TransientRow): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RateFunctions.extrapolatedRate(startTimestamp,
      endTimestamp, window, true, true)
    sampleToEmit.setValues(endTimestamp, result.getOrElse(Double.NaN)) // TODO need to use a NA instead of NaN
  }
}

object DeltaFunction extends RangeFunction {

  def addToWindow(row: TransientRow): Unit = {}
  def removeFromWindow(row: TransientRow): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RateFunctions.extrapolatedRate(startTimestamp,
      endTimestamp, window, false, false)
    sampleToEmit.setValues(endTimestamp, result.getOrElse(Double.NaN)) // TODO need to use a NA instead of NaN
  }
}

object IRateFunction extends RangeFunction {

  override def needsCounterCorrection: Boolean = true
  def addToWindow(row: TransientRow): Unit = {}
  def removeFromWindow(row: TransientRow): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RateFunctions.instantValue(startTimestamp,
      endTimestamp, window, true)
    sampleToEmit.setValues(endTimestamp, result.getOrElse(Double.NaN)) // TODO need to use a NA instead of NaN
  }
}

object IDeltaFunction extends RangeFunction {

  def addToWindow(row: TransientRow): Unit = {}
  def removeFromWindow(row: TransientRow): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RateFunctions.instantValue(startTimestamp,
      endTimestamp, window, false)
    sampleToEmit.setValues(endTimestamp, result.getOrElse(Double.NaN)) // TODO need to use a NA instead of NaN
  }
}

object DerivFunction extends RangeFunction {

  def addToWindow(row: TransientRow): Unit = {}
  def removeFromWindow(row: TransientRow): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    val result = RateFunctions.derivFunction(window)
    sampleToEmit.setValues(endTimestamp, result.getOrElse(Double.NaN)) // TODO need to use a NA instead of NaN
  }
}

object ResetsFunction extends RangeFunction {

  def addToWindow(row: TransientRow): Unit = {}
  def removeFromWindow(row: TransientRow): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    var resets = 0
    var prev = window.head.value
    for (i <- 1 until window.size) {
      var current = window(i).value
      if (current < prev) {
        resets +=1
      }
      prev = current
    }
    sampleToEmit.setValues(endTimestamp, resets)
  }
}


