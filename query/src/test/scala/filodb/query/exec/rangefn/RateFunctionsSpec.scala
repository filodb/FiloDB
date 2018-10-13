package filodb.query.exec.rangefn

import scala.util.Random

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}

import filodb.query.QueryConfig
import filodb.query.exec.{QueueBasedWindow, TransientRow}
import filodb.query.util.IndexedArrayQueue

class RateFunctionsSpec extends FunSpec with Matchers {

  val rand = new Random()
  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))

  val counterSamples = Seq(  8072000L->4419.00,
                      8082100L->4511.00,
                      8092196L->4614.00,
                      8102215L->4724.00,
                      8112223L->4909.00,
                      8122388L->4948.00,
                      8132570L->5000.00,
                      8142822L->5095.00,
                      8152858L->5102.00,
                      8163000L->5201.00)

  val q = new IndexedArrayQueue[TransientRow]()
  counterSamples.foreach { case (t, v) =>
    val s = new TransientRow(t, v)
    q.add(s)
  }
  val counterWindow = new QueueBasedWindow(q)

  val gaugeSamples = Seq(   8072000L->7419.00,
                            8082100L->5511.00,
                            8092196L->4614.00,
                            8102215L->3724.00,
                            8112223L->4909.00,
                            8122388L->4948.00,
                            8132570L->5000.00,
                            8142822L->3095.00,
                            8152858L->5102.00,
                            8163000L->8201.00)

  val q2 = new IndexedArrayQueue[TransientRow]()
  gaugeSamples.foreach { case (t, v) =>
    val s = new TransientRow(t, v)
    q2.add(s)
  }
  val gaugeWindow = new QueueBasedWindow(q2)

  val errorOk = 0.0000001

  // Basic test cases covered
  // TODO Extrapolation special cases not done

  it ("rate should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val expected = (q.last.value - q.head.value) / (q.last.timestamp - q.head.timestamp) * 1000
    val toEmit = new TransientRow
    RateFunction.apply(startTs,endTs, counterWindow, toEmit, queryConfig)
    Math.abs(toEmit.value - expected) should be < errorOk
  }

  it ("irate should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val prevSample = q(q.size - 2)
    val expected = (q.last.value - prevSample.value) / (q.last.timestamp - prevSample.timestamp) * 1000
    val toEmit = new TransientRow
    IRateFunction.apply(startTs, endTs, counterWindow, toEmit, queryConfig)
    Math.abs(toEmit.value - expected) should be < errorOk
  }

  it ("resets should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val expected = 4.0
    val toEmit = new TransientRow
    val q3 = new IndexedArrayQueue[TransientRow]()
    val gaugeWindowForReset = new QueueBasedWindow(q3)
    gaugeSamples.foreach { case (t, v) =>
      val s = new TransientRow(t, v)
      q3.add(s)
      ResetsFunction.addedToWindow(s, gaugeWindowForReset)
    }

    ResetsFunction.apply(startTs, endTs, gaugeWindowForReset, toEmit, queryConfig)
    Math.abs(toEmit.value - expected) should be < errorOk

    // Window sliding case
    val expected2 = 1
    var toEmit2 = new TransientRow

    // 3 resets at the beginning - so resets count should drop only by 3 (4 - 3 = 1) even though we are removing 5 items
    for (i <- 0 until 5) {
      toEmit2 = q3.remove
      ResetsFunction.removedFromWindow(toEmit2, gaugeWindowForReset)// old items being evicted for new window items
    }
    ResetsFunction.apply(startTs, endTs, gaugeWindow, toEmit2, queryConfig)
    Math.abs(toEmit2.value - expected2) should be < errorOk
  }

  it ("deriv should work when start and end are outside window") {
    val gaugeSamples = Seq(
      8072000L->4419.00,
      8082100L->4419.00,
      8092196L->4419.00,
      8102215L->4724.00,
      8112223L->4724.00,
      8122388L->4724.00,
      8132570L->5000.00,
      8142822L->5000.00,
      8152858L->5000.00,
      8163000L->5201.00)

    val expectedSamples = Seq(
      8092196L->0.00,
      8102215L->15.143392157475684,
      8112223L->15.232227023719313,
      8122388L->0.0,
      8132570L->13.568427882659712,
      8142822L->13.4914241262328,
      8152858L->0.0,
      8163000L->9.978695375995517
    )
    for (i <- 0 to gaugeSamples.size - 3) {
      val startTs = gaugeSamples(i)._1
      val endTs =   gaugeSamples(i + 2)._1
      val qDeriv = new IndexedArrayQueue[TransientRow]()
      for (j <- i until i + 3) {
        val s = new TransientRow(gaugeSamples(j)._1.toLong, gaugeSamples(j)._2)
        qDeriv.add(s)
      }

      val gaugeWindow = new QueueBasedWindow(qDeriv)

      val toEmit = new TransientRow
      DerivFunction.apply(startTs, endTs, gaugeWindow, toEmit, queryConfig)
      Math.abs(toEmit.value - expectedSamples(i)._2) should be < errorOk
    }
  }

  it ("increase should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val expected = (q.last.value - q.head.value) / (q.last.timestamp - q.head.timestamp) * (endTs - startTs)
    val toEmit = new TransientRow
    IncreaseFunction.apply(startTs,endTs, counterWindow, toEmit, queryConfig)
    Math.abs(toEmit.value - expected) should be < errorOk
  }

  it ("delta should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val expected = (q2.last.value - q2.head.value) / (q2.last.timestamp - q2.head.timestamp) * (endTs - startTs)
    val toEmit = new TransientRow
    DeltaFunction.apply(startTs,endTs, gaugeWindow, toEmit, queryConfig)
    Math.abs(toEmit.value - expected) should be < errorOk
  }

  it ("idelta should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val prevSample = q2(q2.size - 2)
    //val expected = q2.last.value - prevSample.value
    val expected = q2.last.value - prevSample.value
    val toEmit = new TransientRow
    IDeltaFunction.apply(startTs,endTs, gaugeWindow, toEmit, queryConfig)
    Math.abs(toEmit.value - expected) should be < errorOk
  }

}
