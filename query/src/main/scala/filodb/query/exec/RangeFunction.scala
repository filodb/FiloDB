package filodb.query.exec

import filodb.memory.format.RowReader
import filodb.query.RangeFunctionId
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
  def needsCounterNormalization: Boolean

  /**
    * Called when a sample is added to the sliding window
    */
  def addToWindow(row: RowReader): Unit

  /**
    * Called when a sample is removed from sliding window
    */
  def removeFromWindow(row: RowReader): Unit

  /**
    * Called when wrapping iterator needs to emit a sample using the window
    * @param timestamp timestamp to use in emitted sample
    * @param window samples contained in the window
    * @return sample to be emitted
    */
  def apply(timestamp: Long, window: Seq[RowReader]): MetricRowReader
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

  def needsCounterNormalization: Boolean = false // should not assume counter always
  def addToWindow(row: RowReader): Unit = {}
  def removeFromWindow(row: RowReader): Unit = {}
  def apply(timestamp: Long, window: Seq[RowReader]): MetricRowReader = {
    if (window.size > 1)
      throw new IllegalStateException("Possible internal error: Last sample should have used zero length windows")
    if (window.size < 2) {
      new MetricRowReader(timestamp, Double.NaN)
    } else {
      val valueDelta = window.last.getDouble(1) - window.head.getDouble(1)
      val timeDelta = (window.last.getLong(0) - window.head.getLong(0)) / 1000 // convert to seconds
      new MetricRowReader(timestamp, valueDelta / timeDelta)
    }
  }
}

object RateFunction extends RangeFunction {

  def needsCounterNormalization: Boolean = true
  def addToWindow(row: RowReader): Unit = {}
  def removeFromWindow(row: RowReader): Unit = {}

  def apply(timestamp: Long, window: Seq[RowReader]): MetricRowReader = {
    if (window.size < 2) {
      new MetricRowReader(timestamp, Double.NaN)
    } else {
      val valueDelta = window.last.getDouble(1) - window.head.getDouble(1)
      val timeDelta = (window.last.getLong(0) - window.head.getLong(0)) / 1000 // convert to seconds
      new MetricRowReader(timestamp, valueDelta / timeDelta)
    }
  }
}

// TODO add all other range vector functions
