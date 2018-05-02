package filodb.query.exec.rangefn

import filodb.query.QueryConfig
import filodb.query.exec.MutableSample

object DeltaBasedFunctions {

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
      None
    } else {
      require(window(1).timestamp >= startTimestamp,
        "Possible internal error, found two samples outside window that are < startTimestamp")
      val (firstSample, firstIsInside) = if (window(0).timestamp >= startTimestamp)
        (window(0), false) else (window(1), true)
      val numSamplesInWindow = if (firstIsInside) window.size else window.size-1
      if (numSamplesInWindow >= 2) {
        var durationToStart = (firstSample.timestamp - startTimestamp).toDouble / 1000
        val durationToEnd = (endTimestamp - window.last.timestamp).toDouble / 1000
        val sampledInterval = (window.last.timestamp - firstSample.timestamp) / 1000
        val averageDurationBetweenSamples = sampledInterval / numSamplesInWindow.toDouble
        var resultValue = window.last.value - firstSample.value

        if (isCounter && resultValue > 0 && firstSample.value >= 0) {
          // Counters cannot be negative. If we have any slope at all (i.e. resultValue went up), we can extrapolate
          // the zero point of the counter. If the duration to the  zero point is shorter than the durationToStart, we
          // take the zero point as the start of the series, thereby avoiding extrapolation to negative counter values.
          val durationToZero = sampledInterval * (firstSample.value / resultValue)
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
        resultValue = resultValue * (extrapolateToInterval / sampledInterval)
        if (isRate) {
          resultValue = resultValue / (endTimestamp - startTimestamp) / 1000 // we need rate as a per-second value
        }
        Some(resultValue)
      } else {
        None
      }
    }
  }
}

object IncreaseFunction extends RangeFunction {

  def needsCounterCorrection: Boolean = true
  def addToWindow(row: MutableSample): Unit = {}
  def removeFromWindow(row: MutableSample): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: MutableSample,
            queryConfig: QueryConfig): Unit = {
    val result = DeltaBasedFunctions.extrapolatedRate(startTimestamp,
      endTimestamp, window, true, false)
    sampleToEmit.set(endTimestamp, result.getOrElse(Double.NaN)) // TODO need to use a NA instead of NaN
  }
}

object RateFunction extends RangeFunction {

  def needsCounterCorrection: Boolean = true
  def addToWindow(row: MutableSample): Unit = {}
  def removeFromWindow(row: MutableSample): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: MutableSample,
            queryConfig: QueryConfig): Unit = {
    val result = DeltaBasedFunctions.extrapolatedRate(startTimestamp,
      endTimestamp, window, true, true)
    sampleToEmit.set(endTimestamp, result.getOrElse(Double.NaN)) // TODO need to use a NA instead of NaN
  }
}

object DeltaFunction extends RangeFunction {

  def needsCounterCorrection: Boolean = false
  def addToWindow(row: MutableSample): Unit = {}
  def removeFromWindow(row: MutableSample): Unit = {}

  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: MutableSample,
            queryConfig: QueryConfig): Unit = {
    val result = DeltaBasedFunctions.extrapolatedRate(startTimestamp,
      endTimestamp, window, false, false)
    sampleToEmit.set(endTimestamp, result.getOrElse(Double.NaN)) // TODO need to use a NA instead of NaN
  }
}
