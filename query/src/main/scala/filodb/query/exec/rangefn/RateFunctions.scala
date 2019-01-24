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
                       isRate: Boolean): Double = {
    if (window.size < 2) {
      Double.NaN  // cannot calculate result without 2 samples
    } else {
      require(window.head.timestamp >= startTimestamp, "Possible internal error, found samples < startTimestamp")
      require(window.last.timestamp <= endTimestamp, "Possible internal error, found samples > endTimestamp")
      var durationToStart = (window.head.timestamp - startTimestamp).toDouble / 1000
      val durationToEnd = (endTimestamp - window.last.timestamp).toDouble / 1000
      val sampledInterval = (window.last.timestamp - window.head.timestamp).toDouble / 1000
      val averageDurationBetweenSamples = sampledInterval / (window.size.toDouble - 1)
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
      result
    }
  }
}

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


