package filodb.query.exec.rangefn

import scala.util.Random

import filodb.core.{MachineMetricsData, TestData}
import filodb.core.memstore.{TimeSeriesPartition, WriteBufferPool}
import filodb.core.metadata.Dataset
import filodb.memory.format.vectors.MutableHistogram
import filodb.query.exec.{ChunkedWindowIteratorD, ChunkedWindowIteratorH, QueueBasedWindow, TransientRow}
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
                            8162999L->8201.00)

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
    addChunkToRV(rv, chunk2Data)

    val startTs = 8071950L
    val endTs =   8213070L
    val correction = q.last.value
    val expected = (chunk2Data.last._2 + correction - q.head.value) / (chunk2Data.last._1 - q.head.timestamp) * 1000

    // One window, start=end=endTS
    val it = new ChunkedWindowIteratorD(rv, endTs, 10000, endTs, endTs - startTs,
                                        new ChunkedRateFunction, queryConfig)
    it.next.getDouble(1) shouldEqual expected +- errorOk
  }

  val resetChunk1 = Seq(8072000L->4419.00,
                        8082100L->4511.00,
                        8092196L->4614.00,
                        8102215L->4724.00,
                        8112223L->4909.00,
                        8122388L->948.00,
                        8132570L->1000.00,
                        8142822L->1095.00,
                        8152858L->1102.00,
                        8162999L->1201.00)
  val correction1 = resetChunk1(4)._2

  val resetChunk2 = Seq(8173000L->1325.00,
                        8183000L->1511.00,
                        8193000L->214.00,
                        8203000L->324.00,
                        8213000L->409.00)

  val corr2 = resetChunk2(1)._2

  it("should compute rate correctly when drops occur in middle of chunks") {
    // One drop in each chunk
    val rv = timeValueRV(resetChunk1)
    addChunkToRV(rv, resetChunk2)

    val startTs = 8071950L
    val endTs =   8213070L
    val corrections = correction1 + corr2
    val expected = (resetChunk2.last._2 + corrections - resetChunk1.head._2) /
                   (resetChunk2.last._1 - resetChunk1.head._1) * 1000

    // One window, start=end=endTS
    val it = new ChunkedWindowIteratorD(rv, endTs, 10000, endTs, endTs - startTs,
                                        new ChunkedRateFunction, queryConfig)
    it.next.getDouble(1) shouldEqual expected +- errorOk

    // Two drops in one chunk
    val rv2 = timeValueRV(resetChunk1 ++ resetChunk2)
    val it2 = new ChunkedWindowIteratorD(rv2, endTs, 10000, endTs, endTs - startTs,
                                         new ChunkedRateFunction, queryConfig)
    it2.next.getDouble(1) shouldEqual expected +- errorOk
  }

  it("should return NaN for rate when window only contains one sample") {
    val startTs = 8101215L
    val endTs =   8103215L

    val it = new ChunkedWindowIteratorD(counterRV, endTs, 10000, endTs, endTs - startTs,
                                        new ChunkedRateFunction, queryConfig)
    it.next.getDouble(1).isNaN shouldEqual true
  }

  it("should return rate of 0 when counter samples do not increase") {
    val startTs = 8071950L
    val endTs =   8163070L
    val flatSamples = counterSamples.map { case (t, v) => t -> counterSamples.head._2 }
    val flatRV = timeValueRV(flatSamples)

    // One window, start=end=endTS
    val it = new ChunkedWindowIteratorD(flatRV, endTs, 10000, endTs, endTs - startTs,
                                        new ChunkedRateFunction, queryConfig)
    it.next.getDouble(1) shouldEqual 0.0
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

  val promHistDS = Dataset("histogram", Seq("metric:string", "tags:map"),
                           Seq("timestamp:ts", "count:long", "sum:long", "h:hist:counter=true"))
  val histBufferPool = new WriteBufferPool(TestData.nativeMem, promHistDS.schema.data, TestData.storeConf)

  it("should compute rate for Histogram RVs") {
    val (data, rv) = MachineMetricsData.histogramRV(100000L, numSamples=10, pool=histBufferPool, ds=promHistDS)
    val startTs = 99500L
    val endTs =   161000L // just past 7th sample
    val lastTime = 160000L
    val headTime = 100000L
    val headHist = data(0)(3).asInstanceOf[MutableHistogram]
    val lastHist = data(6)(3).asInstanceOf[MutableHistogram]
    val expectedRates = (0 until headHist.numBuckets).map { b =>
      (lastHist.bucketValue(b) - headHist.bucketValue(b)) / (lastTime - headTime) * 1000
    }
    val expected = MutableHistogram(MachineMetricsData.histBucketScheme, expectedRates.toArray)

    // One window, start=end=endTS
    val it = new ChunkedWindowIteratorH(rv, endTs, 100000, endTs, endTs - startTs,
                                        new HistRateFunction, queryConfig)
    // Scheme should have remained the same
    val answer = it.next.getHistogram(1)
    answer.numBuckets shouldEqual expected.numBuckets

    // Have to compare each bucket with floating point error tolerance
    for { b <- 0 until expected.numBuckets } {
      answer.bucketTop(b) shouldEqual expected.bucketTop(b)
      answer.bucketValue(b) shouldEqual expected.bucketValue(b) +- errorOk
    }
  }

  it("should compute rate for Histogram RVs with drop") {
    val (data, rv) = MachineMetricsData.histogramRV(100000L, numSamples=7, pool=histBufferPool, ds=promHistDS)

    // Inject a few more samples with original data, which means a drop
    val part = rv.partition.asInstanceOf[TimeSeriesPartition]
    val dropData = data.map(d => (d.head.asInstanceOf[Long] + 70000L) +: d.drop(1))
    val container = MachineMetricsData.records(promHistDS, dropData).records
    val bh = MachineMetricsData.histIngestBH
    container.iterate(promHistDS.ingestionSchema).foreach { row => part.ingest(0, row, bh) }
    part.switchBuffers(bh, encode = true)


    val startTs = 99500L
    val endTs =   171000L // just past 8th sample, the first dropped one
    val lastTime = 170000L
    val headTime = 100000L
    val headHist = data(0)(3).asInstanceOf[MutableHistogram]
    val corrHist = data(6)(3).asInstanceOf[MutableHistogram]
    val lastHist = headHist.copy   // 8th sample == first sample + correction
    lastHist.add(corrHist)
    val expectedRates = (0 until headHist.numBuckets).map { b =>
      (lastHist.bucketValue(b) - headHist.bucketValue(b)) / (lastTime - headTime) * 1000
    }
    val expected = MutableHistogram(MachineMetricsData.histBucketScheme, expectedRates.toArray)

    // One window, start=end=endTS
    val it = new ChunkedWindowIteratorH(rv, endTs, 110000, endTs, endTs - startTs,
                                        new HistRateFunction, queryConfig)
    // Scheme should have remained the same
    val answer = it.next.getHistogram(1)
    answer.numBuckets shouldEqual expected.numBuckets

    // Have to compare each bucket with floating point error tolerance
    for { b <- 0 until expected.numBuckets } {
      answer.bucketTop(b) shouldEqual expected.bucketTop(b)
      answer.bucketValue(b) shouldEqual expected.bucketValue(b) +- errorOk
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
    toEmit.value shouldEqual expected +- errorOk

    // One window, start=end=endTS
    val gaugeRV = timeValueRV(gaugeSamples)
    val it = new ChunkedWindowIteratorD(gaugeRV, endTs, 10000, endTs, endTs - startTs,
                                        new ChunkedDeltaFunction, queryConfig)
    it.next.getDouble(1) shouldEqual expected +- errorOk
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
