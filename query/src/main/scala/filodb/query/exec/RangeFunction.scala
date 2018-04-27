package filodb.query.exec

import filodb.query.{QueryConfig, RangeFunctionId}
import filodb.query.RangeFunctionId.Rate

/**
  * All Range Vector Functions are implementation of this trait.
  * There are two choices for function implementation:
  * 1. Use the `addToWindow` and `removeFromWindow` events to evaluate the next value to emit.
  *    This may result in O(n) complexity for emiting the entire range vector.
  * 2. Use the entire window content in `apply` to emit the next value. Depending on whether the
  *    entire window is examined, this may result in O(n) or O(n-squared) for the entire range vector.
  */
trait RangeFunction {
  /**
    * Values added to window will be converted to monotonically increasing. Mark
    * as true only if the function will always operate on counters.
    */
  def needsCounterCorrection: Boolean

  /**
    * Called when a sample is added to the sliding window
    */
  def addToWindow(row: MutableSample): Unit

  /**
    * Called when a sample is removed from sliding window
    */
  def removeFromWindow(row: MutableSample): Unit

  /**
    * Called when wrapping iterator needs to emit a sample using the window.
    * Samples in the window are samples reported in the requested window length.
    * Timestamp of samples is always <= the timestamp param of this function.
    *
    * @param timestamp timestamp to use in emitted sample
    * @param windowLength length of the window in milliseconds
    * @param window samples contained in the window
    * @param sampleToEmit To keep control on reader creation the method must set
    *                  the value to emit in this param object that comes from a reader pool
    */
  def apply(timestamp: Long,
            windowLength: Int,
            window: Seq[MutableSample],
            sampleToEmit: MutableSample,
            queryConfig: QueryConfig): Unit
}

object RangeFunction {
  def apply(func: Option[RangeFunctionId],
            funcParams: Seq[Any]): RangeFunction = {
    func match {
      case None             => LastSampleFunction
      case Some(Rate)       => RateFunction
      case _                => ???
    }
  }
}

object LastSampleFunction extends RangeFunction {

  def needsCounterCorrection: Boolean = false // should not assume counter always
  def addToWindow(row: MutableSample): Unit = {}
  def removeFromWindow(row: MutableSample): Unit = {}
  def apply(timestamp: Long,
            windowLength: Int,
            window: Seq[MutableSample],
            sampleToEmit: MutableSample,
            queryConfig: QueryConfig): Unit = {
    if (window.size > 1)
      throw new IllegalStateException("Possible internal error: Last sample should have used zero length windows")
    if (window.size == 0 || (timestamp - window.head.getLong(0)) > queryConfig.staleSampleAfterMs) {
      sampleToEmit.set(timestamp, Double.NaN)
    } else {
      sampleToEmit.set(timestamp, window.head.getDouble(1))
    }
  }
}

object RateFunction extends RangeFunction {

  // TODO there is more to do here. Work in progress
  def needsCounterCorrection: Boolean = true
  def addToWindow(row: MutableSample): Unit = {}
  def removeFromWindow(row: MutableSample): Unit = {}

  def apply(timestamp: Long,
            windowLength: Int,
            window: Seq[MutableSample],
            sampleToEmit: MutableSample,
            queryConfig: QueryConfig): Unit = {
    if (window.size < 2) {
      sampleToEmit.set(timestamp, Double.NaN) // TODO is there a way to say NA here ?
    } else {
      val valueDelta = window.last.getDouble(1) - window.head.getDouble(1)
      val timeDelta = (window.last.getLong(0) - window.head.getLong(0)) / 1000 // convert to seconds
      sampleToEmit.set(timestamp, valueDelta / timeDelta)
    }
  }
}

// TODO add all other range vector functions
