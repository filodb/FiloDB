package filodb.query.exec.rangefn

import filodb.core.{MachineMetricsData, TestData}
import filodb.core.memstore.WriteBufferPool
import filodb.core.metadata.Dataset
import filodb.core.query.TransientRow
import filodb.memory.format.vectors.{LongHistogram, MutableHistogram}
import filodb.query.exec.{ChunkedWindowIteratorD, ChunkedWindowIteratorH, QueueBasedWindow}
import filodb.query.util.IndexedArrayQueue

import scala.util.Random

class PeriodicRateFunctionsSpec extends RawDataWindowingSpec {
  val rand = new Random()

  val deltaCounterSamples = Seq(8072000L->111.0,
                      8082100L->92.00,
                      8092196L->103.00,
                      8102215L->110.00,
                      8112223L->185.00,
                      8122388L->39.00,
                      8132570L->52.00,
                      8142822L->95.00,
                      8152858L->7.00,
                      8162999L->99.00)

  val qDelta = new IndexedArrayQueue[TransientRow]()
  deltaCounterSamples.foreach { case (t, v) =>
    val s = new TransientRow(t, v)
    qDelta.add(s)
  }
  val deltaDCounterWindow = new QueueBasedWindow(qDelta)
  val deltaCounterRV = timeValueRVPk(deltaCounterSamples)

  val errorOk = 0.0000001

  // Basic test cases covered
  it("rate over period-counter should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val expectedDelta = deltaCounterSamples.map(_._2).sum / (endTs - startTs) * 1000

    // One window, start=end=endTS
    val it = new ChunkedWindowIteratorD(deltaCounterRV, endTs, 10000, endTs, endTs - startTs,
                                        new RateOverDeltaChunkedFunctionD, querySession)
    it.next.getDouble(1) shouldEqual expectedDelta +- errorOk
  }

  it("should not return NaN for rate over period-counter when window only contains one sample") {
    val startTs = 8101215L
    val endTs =   8103215L

    val it = new ChunkedWindowIteratorD(deltaCounterRV, endTs, 10000, endTs, endTs - startTs,
                                        new RateOverDeltaChunkedFunctionD, querySession)
    it.next.getDouble(1).isNaN shouldEqual false
  }

  it("should not return rate of 0 when delta-counter samples do not increase") {
    val startTs = 8071950L
    val endTs =   8163070L
    val flatSamples = deltaCounterSamples.map { case (t, v) => t -> deltaCounterSamples.head._2 }
    val flatRV = timeValueRVPk(flatSamples)

    // One window, start=end=endTS
    val it = new ChunkedWindowIteratorD(flatRV, endTs, 10000, endTs, endTs - startTs,
                                        new RateOverDeltaChunkedFunctionD, querySession)
    it.next.getDouble(1) should not equal 0.0
  }

  // Also ensures that chunked rate works across chunk boundaries
  it("rate over period-counter should work for variety of window and step sizes") {
    val data = (1 to 500).map(_ => rand.nextInt(10)).map(_.toDouble)
    val tuples = data.zipWithIndex.map { case (d, t) => (defaultStartTS + t * pubFreq, d) }
    val rv = timeValueRVPk(tuples)  // should be a couple chunks

    (0 until 10).foreach { x =>
      val windowSize = rand.nextInt(100) + 10
      val step = rand.nextInt(50) + 5
      info(s"  iteration $x  windowSize=$windowSize step=$step")

      val slidingRate = slidingWindowIt(data, rv, new RateOverDeltaFunction, windowSize, step)
      val slidingResults = slidingRate.map{ r => (r.getLong(0), r.getDouble(1)) }.toBuffer
      slidingRate.close()

      val rateChunked = chunkedWindowIt(data, rv, new RateOverDeltaChunkedFunctionD, windowSize, step)
      val resultRows = rateChunked.map { r => (r.getLong(0), r.getDouble(1)) }.toBuffer
      val rates = resultRows.map(_._2)

      // Since the input data and window sizes are randomized, it is not possible to precompute results
      // beforehand.  Coming up with a formula to figure out the right rate is really hard.
      // Thus we take an approach of comparing the sliding and chunked results to ensure they are identical.

      // val windowTime = (windowSize.toLong - 1) * pubFreq
      // val expected = tuples.sliding(windowSize, step).toBuffer
      //                      .zip(resultRows).map { case (w, (ts, _)) =>
      //   // For some reason rate is based on window, not timestamps  - so not w.last._1
      //   (w.sum_of_sample_values) / (windowTime) * 1000
      //   // (w.sum_of_sample_values) / (w.last._1 - w.head._1) * 1000
      // }
      rates.zipWithIndex.foreach{case(rate, i) => rate shouldEqual slidingResults(i)._2 +- errorOk}
    }
  }

  val promHistDS = Dataset("histogram", Seq("metric:string", "tags:map"),
                           Seq("timestamp:ts", "count:long", "sum:long", "h:hist:counter=false"))
  val histBufferPool = new WriteBufferPool(TestData.nativeMem, promHistDS.schema.data, TestData.storeConf)

  it("should compute rate for DeltaHistogram RVs") {
    val (data, rv) = MachineMetricsData.histogramRV(100000L, numSamples=10, pool=histBufferPool, ds=promHistDS)
    val startTs = 99500L
    val endTs =   161000L // just past 7th sample
    val expectedRates = data
      .slice(0, 7)
      .map(_(3).asInstanceOf[LongHistogram].values) // buckets
      .reduce((b1, b2) => b1.zip(b2).map { case (x, y) => x + y }) // sum of respective buckets
      .map(_.toDouble / (endTs - startTs) * 1000) // rate

    val expected = MutableHistogram(MachineMetricsData.histBucketScheme, expectedRates.toArray)

    // One window, start=end=endTS
    val it = new ChunkedWindowIteratorH(rv, endTs, 100000, endTs, endTs - startTs,
                                        new RateOverDeltaChunkedFunctionH, querySession)
    // Scheme should have remained the same
    val answer = it.next.getHistogram(1)
    answer.numBuckets shouldEqual expected.numBuckets

    // Have to compare each bucket with floating point error tolerance
    for { b <- 0 until expected.numBuckets } {
      answer.bucketTop(b) shouldEqual expected.bucketTop(b)
      answer.bucketValue(b) shouldEqual expected.bucketValue(b) +- errorOk
    }
  }

  it ("irate over period-counters should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val prevSample = qDelta(qDelta.size - 2)
    val expected = (qDelta.last.value) / (qDelta.last.timestamp - prevSample.timestamp) * 1000
    val toEmit = new TransientRow
    IRatePeriodicFunction.apply(startTs, endTs, deltaDCounterWindow, toEmit, queryConfig)
    Math.abs(toEmit.value - expected) should be < errorOk
  }

  it ("increase over period-counters should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs = 8163070L
    val expectedDelta = deltaCounterSamples.map(_._2).sum

    // One window, start=end=endTS
    val it = new ChunkedWindowIteratorD(deltaCounterRV, endTs, 10000, endTs, endTs - startTs,
      new SumOverTimeChunkedFunctionD, querySession)
    it.next.getDouble(1) shouldEqual expectedDelta
  }
}
