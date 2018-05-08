package filodb.query.exec.rangefn

import filodb.query.{QueryConfig, RangeFunctionId}
import filodb.query.RangeFunctionId._
import filodb.query.exec._

/**
  * Container for samples within a window of samples
  * over which a range function can be applied
  */
trait Window {
  def apply(i: Int): TransientRow
  def size: Int
  def head: TransientRow
  def last: TransientRow
}

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
    * Needs last sample prior to window start
    */
  def needsLastSample: Boolean = false

  /**
    * Values added to window will be converted to monotonically increasing. Mark
    * as true only if the function will always operate on counters.
    */
  def needsCounterCorrection: Boolean = false

  /**
    * Called when a sample is added to the sliding window
    */
  def addToWindow(row: TransientRow): Unit

  /**
    * Called when a sample is removed from sliding window
    */
  def removeFromWindow(row: TransientRow): Unit

  /**
    * Called when wrapping iterator needs to emit a sample using the window.
    *
    * Samples in the window are samples reported in the requested window length.
    * Window also includes the last sample outside the window if it was reported
    * for the time within stale sample period
    *
    * Timestamp of samples in window is always <= the timestamp param of this function.
    *
    * @param startTimestamp start timestamp of the time window
    * @param endTimestamp timestamp to use in emitted sample. It is also the endTimestamp for the window
    * @param window samples contained in the window
    * @param sampleToEmit To keep control on reader creation the method must set
    *                  the value to emit in this param object that comes from a reader pool
    */
  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit
}

object RangeFunction {
  def apply(func: Option[RangeFunctionId],
            funcParams: Seq[Any] = Nil): RangeFunction = {
    func match {
      case None                   => LastSampleFunction
      case Some(Rate)             => RateFunction
      case Some(Increase)         => IncreaseFunction
      case Some(Delta)            => DeltaFunction
      case Some(MaxOverTime)      => new MinMaxOverTimeFunction(Ordering[Double])
      case Some(MinOverTime)      => new MinMaxOverTimeFunction(Ordering[Double].reverse)
      case Some(CountOverTime)    => new CountOverTimeFunction()
      case Some(SumOverTime)      => new SumOverTimeFunction()
      case Some(AvgOverTime)      => new AvgOverTimeFunction()
      case Some(StdDevOverTime)   => new StdDevOverTimeFunction()
      case Some(StdVarOverTime)   => new StdVarOverTimeFunction()
      case _                      => ???
    }
  }
}

object LastSampleFunction extends RangeFunction {

  override def needsLastSample: Boolean = true
  def addToWindow(row: TransientRow): Unit = {}
  def removeFromWindow(row: TransientRow): Unit = {}
  def apply(startTimestamp: Long,
            endTimestamp: Long,
            window: Window,
            sampleToEmit: TransientRow,
            queryConfig: QueryConfig): Unit = {
    if (window.size > 1)
      throw new IllegalStateException("Possible internal error: Last sample should have used zero length windows")
    if (window.size == 0 || (endTimestamp - window.head.getLong(0)) > queryConfig.staleSampleAfterMs) {
      sampleToEmit.set(endTimestamp, Double.NaN)
    } else {
      sampleToEmit.set(endTimestamp, window.head.getDouble(1))
    }
  }
}
