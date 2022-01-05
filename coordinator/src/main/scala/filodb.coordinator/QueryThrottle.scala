package filodb.coordinator

import com.typesafe.scalalogging.StrictLogging
import scala.concurrent.duration.FiniteDuration

/**
 * Throttles the TenantIngestionMetering query rate according to the ratio of timeouts to query attempts.
 *
 * @param queryInterval the initial delay between each query.
 * @param intervalDiff diff added to queryInterval if the ratio of timeouts:queries in the lookback
 *                     window exceeds timeoutThreshold
 * @param timeoutThreshold ratio of timeouts:queries at which intervalDiff is added to queryInterval
 * @param lookback number of queries used to check timeoutThreshold
 */
class QueryThrottle(queryInterval: FiniteDuration,
                    intervalDiff: FiniteDuration,
                    timeoutThreshold: Double,
                    lookback: Int) extends StrictLogging {

  private var interval: FiniteDuration = queryInterval.copy()

  // these track timeouts for the past LOOKBACK queries
  private var bits = 0  // "1" indicates a timeout
  private var ibit = 0

  /**
   * Sets the next lookback bit and increments ibit.
   */
  private def setNextBit(bit: Boolean): Unit = {
    val bitVal = if (bit) 1 else 0
    bits = bits & ~(1 << ibit)      // zero the bit
    bits = bits | (bitVal << ibit)  // 'or' in the new bit
    ibit = ibit + 1
    if (ibit == lookback) {
      ibit = 0
    }
  }

  /**
   * Updates the interval according to the timeout:non-timeout ratio.
   */
  private def updateInterval(): Unit = {
    val failureRate = Integer.bitCount(bits).toDouble / lookback
    if (failureRate > timeoutThreshold) {
      interval = interval + intervalDiff
      logger.info("too many timeouts; query interval extended to " + interval.toString())
      // reset the bits
      bits = 0
    }
  }

  /**
   * Record a query timeout.
   */
  def recordTimeout(): Unit = {
    setNextBit(true)
    updateInterval()
  }

  /**
   * Record a query non-timeout.
   */
  def recordOnTime(): Unit = {
    setNextBit(false)
    updateInterval()
  }

  /**
   * Returns the current query interval.
   */
  def getInterval(): FiniteDuration = {
    interval.copy()
  }
}
