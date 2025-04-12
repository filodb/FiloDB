package filodb.core

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration.Duration

class RateLimiterSpec extends AnyFunSpec with Matchers {
  it("should apply rate-limits accordingly") {
    val rateLimiter = new RateLimiter(Duration(1, TimeUnit.SECONDS))

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

  it("should reasonably rate-limit concurrent threads") {
    val nThreads = 100
    val period = Duration(1, TimeUnit.SECONDS)
    val nPeriods = 5

    val rateLimiter = new RateLimiter(period)
    val pool = Executors.newFixedThreadPool(nThreads)

    // All threads will try to increment the time-appropriate counter.
    // At the end of the test, there should be at least one counted per period,
    //   and no single counter should exceed the count of threads (i.e. at least one thread
    //   was paused long enough that it updated the RateLimiter's internal timestamp
    //   to something in the previous period.
    val periodCounters = (0 until nPeriods).map(_ => new AtomicInteger(0))

    // Prep the runnable (some of these variables are updated later).
    var startMillis = -1L
    var isStarted = false
    var isShutdown = false
    val runnable: Runnable = () => {
      while (!isStarted) {
        Thread.sleep(500)
      }
      while (!isShutdown) {
        if (rateLimiter.attempt()) {
          val iPeriod = (System.currentTimeMillis() - startMillis) / period.toMillis
          periodCounters(iPeriod.toInt).incrementAndGet()
        }
      }
    }

    // Kick off the threads and start the test.
    for (i <- 0 until nThreads) {
      pool.submit(runnable)
    }
    startMillis = System.currentTimeMillis()
    isStarted = true

    // Wait for all periods to elapse.
    Thread.sleep(nPeriods * period.toMillis)

    // Shutdown and wait for everything to finish.
    isShutdown = true
    pool.shutdown()
    while (!pool.isTerminated) {
      Thread.sleep(1000)
    }

    periodCounters.forall(_.get() > 0) shouldEqual true
    periodCounters.map(_.get()).max <= nThreads

    // Typical local "println(periodCounters)" output:
    //   Vector(1, 34, 1, 1, 1)
    //   Vector(1, 20, 1, 1, 1)
    //   Vector(1, 13, 1, 2, 1)
  }
}