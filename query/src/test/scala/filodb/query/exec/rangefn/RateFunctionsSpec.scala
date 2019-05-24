package filodb.query.exec.rangefn

import scala.util.Random

import filodb.core.memstore.TimeSeriesPartition
import filodb.memory.format.TupleRowReader
import filodb.query.exec.{ChunkedWindowIteratorD, QueueBasedWindow, TransientRow}
import filodb.query.util.IndexedArrayQueue

class RateFunctionsSpec extends RawDataWindowingSpec {
  val rand = new Random()

  val counterSamples = Seq(  8072000L->4419.00,
                      8082100L->4511.00,
                      8092196L->4614.00,
                      8102215L->4724.00,
                      8112223L->4909.00,
                      8122388L->4948.00,
                      8132570L->5000.00,
                      8142822L->5095.00,
                      8152858L->5102.00,
                      8162999L->5201.00)

  val q = new IndexedArrayQueue[TransientRow]()
  counterSamples.foreach { case (t, v) =>
    val s = new TransientRow(t, v)
    q.add(s)
  }
  val counterWindow = new QueueBasedWindow(q)
  val counterRV = timeValueRV(counterSamples)

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

  it("rate should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val expected = (q.last.value - q.head.value) / (q.last.timestamp - q.head.timestamp) * 1000
    val toEmit = new TransientRow
    RateFunction.apply(startTs,endTs, counterWindow, toEmit, queryConfig)
    toEmit.value shouldEqual expected +- errorOk

    // One window, start=end=endTS
    val it = new ChunkedWindowIteratorD(counterRV, endTs, 10000, endTs, endTs - startTs,
                                        new ChunkedRateFunction, queryConfig)
    it.next.getDouble(1) shouldEqual expected +- errorOk
  }

  it("should compute rate correctly when reset occurs at chunk boundaries") {
    val chunk2Data = Seq(8173000L->325.00,
                         8183000L->511.00,
                         8193000L->614.00,
                         8203000L->724.00,
                         8213000L->909.00)
    val rv = timeValueRV(counterSamples)

    // Add data and chunkify chunk2Data
    val part = rv.partition.asInstanceOf[TimeSeriesPartition]
    part.numChunks shouldEqual 1
    val readers = chunk2Data.map { case (ts, d) => TupleRowReader((Some(ts), Some(d))) }
    readers.foreach { row => part.ingest(row, ingestBlockHolder) }
    part.switchBuffers(ingestBlockHolder, encode = true)
    part.numChunks shouldEqual 2

    val startTs = 8071950L
    val endTs =   8213070L
    val correction = q.last.value
    val expected = (chunk2Data.last._2 + correction - q.head.value) / (chunk2Data.last._1 - q.head.timestamp) * 1000

    // One window, start=end=endTS
    val it = new ChunkedWindowIteratorD(rv, endTs, 10000, endTs, endTs - startTs,
                                        new ChunkedRateFunction, queryConfig)
    it.next.getDouble(1) shouldEqual expected +- errorOk
  }

  // Also ensures that chunked rate works across chunk boundaries
  it("rate should work for variety of window and step sizes") {
    val data = (1 to 500).map(_ * 10 + rand.nextInt(10)).map(_.toDouble)
    val tuples = data.zipWithIndex.map { case (d, t) => (defaultStartTS + t * pubFreq, d) }
    val rv = timeValueRV(tuples)  // should be a couple chunks

    (0 until 10).foreach { x =>
      val windowSize = rand.nextInt(100) + 10
      val step = rand.nextInt(50) + 5
      info(s"  iteration $x  windowSize=$windowSize step=$step")

      val slidingRate = slidingWindowIt(data, rv, RateFunction, windowSize, step)
      val slidingResults = slidingRate.map(_.getDouble(1)).toBuffer

      val rateChunked = chunkedWindowIt(data, rv, new ChunkedRateFunction, windowSize, step)
      val resultRows = rateChunked.map { r => (r.getLong(0), r.getDouble(1)) }.toBuffer
      val rates = resultRows.map(_._2)

      // Since the input data and window sizes are randomized, it is not possible to precompute results
      // beforehand.  Coming up with a formula to figure out the right rate is really hard.
      // Thus we take an approach of comparing the sliding and chunked results to ensure they are identical.

      // val windowTime = (windowSize.toLong - 1) * pubFreq
      // val expected = tuples.sliding(windowSize, step).toBuffer
      //                      .zip(resultRows).map { case (w, (ts, _)) =>
      //   // For some reason rate is based on window, not timestamps  - so not w.last._1
      //   (w.last._2 - w.head._2) / (windowTime) * 1000
      //   // (w.last._2 - w.head._2) / (w.last._1 - w.head._1) * 1000
      // }
      rates shouldEqual slidingResults
    }
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

  it ("resets should work with empty windows and no resets data") {
    val startTs = 8071950L
    val endTs =   8163070L
    val toEmit = new TransientRow
    val q3 = new IndexedArrayQueue[TransientRow]()
    val gaugeWindowForReset = new QueueBasedWindow(q3)
    val resetsFunction = new ResetsFunction

    val counterSamples = Seq(   8072000L->1419.00,
      8082100L->2511.00,
      8092196L->3614.00,
      8102215L->4724.00,
      8112223L->5909.00,
      8122388L->6948.00,
      8132570L->7000.00,
      8142822L->8095.00,
      8152858L->9102.00,
      8163000L->9201.00)

    resetsFunction.apply(startTs, endTs, gaugeWindowForReset, toEmit, queryConfig)
    assert(toEmit.value.isNaN) // Empty window should return NaN

    counterSamples.foreach { case (t, v) =>
      val s = new TransientRow(t, v)
      q3.add(s)
      resetsFunction.addedToWindow(s, gaugeWindowForReset)
    }
    resetsFunction.apply(startTs, endTs, gaugeWindowForReset, toEmit, queryConfig)
    toEmit.value shouldEqual 0
  }

  it ("resets should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val expected = 4.0
    val toEmit = new TransientRow
    val q3 = new IndexedArrayQueue[TransientRow]()
    val gaugeWindowForReset = new QueueBasedWindow(q3)
    val resetsFunction = new ResetsFunction

    gaugeSamples.foreach { case (t, v) =>
      val s = new TransientRow(t, v)
      q3.add(s)
      resetsFunction.addedToWindow(s, gaugeWindowForReset)
    }

    resetsFunction.apply(startTs, endTs, gaugeWindowForReset, toEmit, queryConfig)
    Math.abs(toEmit.value - expected) should be < errorOk

    // Window sliding case
    val expected2 = 1
    var toEmit2 = new TransientRow

    // 3 resets at the beginning - so resets count should drop only by 3 (4 - 3 = 1) even though we are removing 5 items
    for (i <- 0 until 5) {
      toEmit2 = q3.remove
      resetsFunction.removedFromWindow(toEmit2, gaugeWindowForReset)// old items being evicted for new window items
    }
    resetsFunction.apply(startTs, endTs, gaugeWindow, toEmit2, queryConfig)
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
    toEmit.value shouldEqual expected +- errorOk

    // One window, start=end=endTS
    val it = new ChunkedWindowIteratorD(counterRV, endTs, 10000, endTs, endTs - startTs,
                                        new ChunkedIncreaseFunction, queryConfig)
    it.next.getDouble(1) shouldEqual expected +- errorOk
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
