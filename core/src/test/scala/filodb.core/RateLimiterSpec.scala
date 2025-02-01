package filodb.core

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class RateLimiterSpec extends AnyFunSpec with Matchers {
  it("should apply rate-limits accordingly") {
    val rateLimiter = new RateLimiter(Duration(2, TimeUnit.SECONDS))

    // First attempt should succeed.
    rateLimiter.attempt() shouldEqual true

    // Others before a full period has elapsed should fail.
    rateLimiter.attempt() shouldEqual false
    rateLimiter.attempt() shouldEqual false
    rateLimiter.attempt() shouldEqual false

    // Wait the period...
    Thread.sleep(2000)

    // Next attempt should succeed.
    rateLimiter.attempt() shouldEqual true

    // Again, attempts should fail until a period has elapsed.
    rateLimiter.attempt() shouldEqual false
    rateLimiter.attempt() shouldEqual false
    rateLimiter.attempt() shouldEqual false
  }
}