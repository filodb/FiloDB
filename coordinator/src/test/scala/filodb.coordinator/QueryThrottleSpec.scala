package filodb.coordinator

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

class QueryThrottleSpec extends AnyFunSpec with Matchers {
  it("should correctly increment query delay") {

    val INIT_INTERVAL = FiniteDuration(10, TimeUnit.SECONDS)
    val INTERVAL_DIFF = FiniteDuration(1, TimeUnit.SECONDS)

    // two timeouts are allowed before the interval is incremented
    val throttle = new QueryThrottle(
      INIT_INTERVAL,
      INTERVAL_DIFF,
      timeoutThreshold=0.67,
      lookback=3)

    // on-times should not change the interval
    throttle.getInterval() shouldEqual INIT_INTERVAL
    throttle.recordOnTime()
    throttle.getInterval() shouldEqual INIT_INTERVAL
    throttle.recordOnTime()
    throttle.getInterval() shouldEqual INIT_INTERVAL
    throttle.recordOnTime()
    throttle.getInterval() shouldEqual INIT_INTERVAL

    // use this to trach expected interval
    var interval = INIT_INTERVAL

    // fail twice
    throttle.recordTimeout()
    throttle.getInterval() shouldEqual interval
    throttle.recordTimeout()
    throttle.getInterval() shouldEqual interval

    // next failure should increment interval
    interval = interval + INTERVAL_DIFF
    throttle.recordTimeout()
    throttle.getInterval() shouldEqual interval

    // failure counter should reset-- fail twice again
    throttle.recordTimeout()
    throttle.getInterval() shouldEqual interval
    throttle.recordTimeout()
    throttle.getInterval() shouldEqual interval

    // next failure should increment the interval
    interval = interval + INTERVAL_DIFF
    throttle.recordTimeout()
    throttle.getInterval() shouldEqual interval

    // success-failure-success-etc should not change the interval
    throttle.recordTimeout()
    throttle.recordOnTime()
    throttle.recordTimeout()
    throttle.recordOnTime()
    throttle.recordTimeout()
    throttle.recordOnTime()
    throttle.getInterval() shouldEqual interval

    // successes should not reset the counter to its initialized value
    throttle.recordOnTime()
    throttle.recordOnTime()
    throttle.recordOnTime()
    throttle.recordOnTime()
    throttle.recordOnTime()
    throttle.getInterval() shouldEqual interval
  }
}
