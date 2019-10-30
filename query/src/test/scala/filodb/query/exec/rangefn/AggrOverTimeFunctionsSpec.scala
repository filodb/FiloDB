package filodb.query.exec.rangefn

import scala.util.Random
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import filodb.core.memstore.{TimeSeriesPartition, TimeSeriesPartitionSpec, WriteBufferPool}
import filodb.core.query.{RangeParams, RawDataRangeVector, TransientHistMaxRow, TransientHistRow, TransientRow}
import filodb.core.store.AllChunkScan
import filodb.core.{MetricsTestData, TestData, MachineMetricsData => MMD}
import filodb.memory._
import filodb.memory.format.{TupleRowReader, vectors => bv}
import filodb.query.QueryConfig
import filodb.query.exec._

/**
 * A common trait for windowing query tests which uses real chunks and real RawDataRangeVectors
 */
trait RawDataWindowingSpec extends FunSpec with Matchers with BeforeAndAfterAll {
  import MetricsTestData._

  private val blockStore = new PageAlignedBlockManager(100 * 1024 * 1024,
    new MemoryStats(Map("test"-> "test")), null, 16)
  val storeConf = TestData.storeConf.copy(maxChunksSize = 200)
  protected val ingestBlockHolder = new BlockMemFactory(blockStore, None, timeseriesSchema.data.blockMetaSize,
                                      MMD.dummyContext, true)
  protected val tsBufferPool = new WriteBufferPool(TestData.nativeMem, timeseriesSchema.data, storeConf)

  protected val ingestBlockHolder2 = new BlockMemFactory(blockStore, None, downsampleSchema.data.blockMetaSize,
                                      MMD.dummyContext, true)
  protected val tsBufferPool2 = new WriteBufferPool(TestData.nativeMem, downsampleSchema.data, storeConf)

  override def afterAll(): Unit = {
    blockStore.releaseBlocks()
  }

  def sumSquares(nn: Seq[Double]): Double = nn.map(n => n*n).sum.toDouble
  def avg(nn: Seq[Double]): Double = nn.sum.toDouble / nn.length
  def stdVar(nn: Seq[Double]): Double = sumSquares(nn)/nn.length - avg(nn)*avg(nn)

  val defaultStartTS = 100000L
  val pubFreq = 10000L

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))

  // windowSize and step are in number of elements of the data
  def numWindows(data: Seq[Any], windowSize: Int, step: Int): Int = data.sliding(windowSize, step).length

  // Creates a RawDataRangeVector using Prometheus time-value schema and a given chunk size etc.
  def timeValueRV(tuples: Seq[(Long, Double)]): RawDataRangeVector = {
    val part = TimeSeriesPartitionSpec.makePart(0, timeseriesDataset, bufferPool = tsBufferPool)
    val readers = tuples.map { case (ts, d) => TupleRowReader((Some(ts), Some(d))) }
    readers.foreach { row => part.ingest(0, row, ingestBlockHolder) }
    // Now flush and ingest the rest to ensure two separate chunks
    part.switchBuffers(ingestBlockHolder, encode = true)
    // part.encodeAndReleaseBuffers(ingestBlockHolder)
    RawDataRangeVector(null, part, AllChunkScan, Array(0, 1))
  }

  def timeValueRvDownsample(tuples: Seq[(Long, Double, Double, Double, Double, Double)],
                            colIds: Array[Int]): RawDataRangeVector = {
    val part = TimeSeriesPartitionSpec.makePart(0, downsampleDataset, bufferPool = tsBufferPool2)
    val readers = tuples.map { case (ts, d1, d2, d3, d4, d5) =>
      TupleRowReader((Some(ts), Some(d1), Some(d2), Some(d3), Some(d4), Some(d5)))
    }
    readers.foreach { row => part.ingest(0, row, ingestBlockHolder2) }
    // Now flush and ingest the rest to ensure two separate chunks
    part.switchBuffers(ingestBlockHolder2, encode = true)
    // part.encodeAndReleaseBuffers(ingestBlockHolder)
    RawDataRangeVector(null, part, AllChunkScan, colIds)
  }

  def timeValueRV(data: Seq[Double], startTS: Long = defaultStartTS): RawDataRangeVector = {
    val tuples = data.zipWithIndex.map { case (d, t) => (startTS + t * pubFreq, d) }
    timeValueRV(tuples)
  }

  // Adds more Time-Value tuples to a RawRangeVector as a new chunk
  def addChunkToRV(rv: RawDataRangeVector, tuples: Seq[(Long, Double)]): Unit = {
    val part = rv.partition.asInstanceOf[TimeSeriesPartition]
    val startingNumChunks = part.numChunks
    val readers = tuples.map { case (ts, d) => TupleRowReader((Some(ts), Some(d))) }
    readers.foreach { row => part.ingest(0, row, ingestBlockHolder) }
    part.switchBuffers(ingestBlockHolder, encode = true)
    part.numChunks shouldEqual (startingNumChunks + 1)
  }

  // Call this only after calling histogramRV
  def emptyAggHist: bv.MutableHistogram = bv.MutableHistogram.empty(MMD.histBucketScheme)

  // Designed explicitly to work with linearHistSeries records and histDataset from MachineMetricsData
  def histogramRV(numSamples: Int = 100, numBuckets: Int = 8): (Stream[Seq[Any]], RawDataRangeVector) =
    MMD.histogramRV(defaultStartTS, pubFreq, numSamples, numBuckets)

  def chunkedWindowIt(data: Seq[Double],
                      rv: RawDataRangeVector,
                      func: ChunkedRangeFunction[TransientRow],
                      windowSize: Int,
                      step: Int): ChunkedWindowIteratorD = {
    val windowTime = (windowSize.toLong - 1) * pubFreq
    val windowStartTS = defaultStartTS + windowTime
    val stepTimeMillis = step.toLong * pubFreq
    val windowEndTS = windowStartTS + (numWindows(data, windowSize, step) - 1) * stepTimeMillis
    new ChunkedWindowIteratorD(rv, windowStartTS, stepTimeMillis, windowEndTS, windowTime, func, queryConfig)
  }

  def chunkedWindowItHist[R <: TransientHistRow](data: Seq[Seq[Any]],
                          rv: RawDataRangeVector,
                          func: ChunkedRangeFunction[R],
                          windowSize: Int,
                          step: Int,
                          row: R): ChunkedWindowIteratorH = {
    val windowTime = (windowSize.toLong - 1) * pubFreq
    val windowStartTS = defaultStartTS + windowTime
    val stepTimeMillis = step.toLong * pubFreq
    val windowEndTS = windowStartTS + (numWindows(data, windowSize, step) - 1) * stepTimeMillis
    new ChunkedWindowIteratorH(rv, windowStartTS, stepTimeMillis, windowEndTS, windowTime,
                               func.asInstanceOf[ChunkedRangeFunction[TransientHistRow]], queryConfig, row)
  }

  def chunkedWindowItHist(data: Seq[Seq[Any]],
                          rv: RawDataRangeVector,
                          func: ChunkedRangeFunction[TransientHistRow],
                          windowSize: Int,
                          step: Int): ChunkedWindowIteratorH =
    chunkedWindowItHist(data, rv, func, windowSize, step, new TransientHistRow())

  def slidingWindowIt(data: Seq[Double],
                      rv: RawDataRangeVector,
                      func: RangeFunction,
                      windowSize: Int,
                      step: Int): SlidingWindowIterator = {
    val windowTime = (windowSize.toLong - 1) * pubFreq
    val windowStartTS = defaultStartTS + windowTime
    val stepTimeMillis = step.toLong * pubFreq
    val windowEndTS = windowStartTS + (numWindows(data, windowSize, step) - 1) * stepTimeMillis
    new SlidingWindowIterator(rv.rows, windowStartTS, stepTimeMillis, windowEndTS, windowTime, func, queryConfig)
  }
}

class AggrOverTimeFunctionsSpec extends RawDataWindowingSpec {
  val rand = new Random()

  // TODO: replace manual loops with ScalaCheck/properties checker
  val numIterations = 10

  it("should correctly aggregate sum_over_time using both chunked and sliding iterators") {
    val data = (1 to 240).map(_.toDouble)
    val rv = timeValueRV(data)
    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(100) + 10
      val step = rand.nextInt(75) + 5
      info(s"  iteration $x  windowSize=$windowSize step=$step")
      val slidingIt = slidingWindowIt(data, rv, new SumOverTimeFunction(), windowSize, step)
      val aggregated = slidingIt.map(_.getDouble(1)).toBuffer
      // drop first sample because of exclusive start
      aggregated shouldEqual data.sliding(windowSize, step).map(_.drop(1).sum).toBuffer

      val chunkedIt = chunkedWindowIt(data, rv, new SumOverTimeChunkedFunctionD(), windowSize, step)
      val aggregated2 = chunkedIt.map(_.getDouble(1)).toBuffer
      aggregated2 shouldEqual data.sliding(windowSize, step).map(_.drop(1).sum).toBuffer
    }
  }

  it("should correctly aggregate sum_over_time for histogram RVs") {
    val (data, rv) = histogramRV(numSamples = 150)
    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(50) + 10
      val step = rand.nextInt(50) + 5
      info(s"  iteration $x  windowSize=$windowSize step=$step")

      val chunkedIt = chunkedWindowItHist(data, rv, new SumOverTimeChunkedFunctionH(), windowSize, step)
      chunkedIt.zip(data.sliding(windowSize, step).map(_.drop(1))).foreach { case (aggRow, rawDataWindow) =>
        val aggHist = aggRow.getHistogram(1)
        val sumRawHist = rawDataWindow.map(_(3).asInstanceOf[bv.MutableHistogram])
                                      .foldLeft(emptyAggHist) { case (agg, h) => agg.add(h); agg }
        aggHist shouldEqual sumRawHist
      }
    }
  }

  // Goal of this is to verify aggregation functionality for diff time windows.
  // See PeriodicSampleMapperSpec for verifying integration of histograms with max
  it("should aggregate both max and hist for sum_over_time when max in schema") {
    val (data, rv) = MMD.histMaxRV(defaultStartTS, pubFreq, 150, 8)
    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(50) + 10
      val step = rand.nextInt(50) + 5
      info(s"  iteration $x  windowSize=$windowSize step=$step")

      val row = new TransientHistMaxRow()
      val chunkedIt = chunkedWindowItHist(data, rv, new SumAndMaxOverTimeFuncHD(3), windowSize, step, row)
      chunkedIt.zip(data.sliding(windowSize, step).map(_.drop(1))).foreach { case (aggRow, rawDataWindow) =>
        val aggHist = aggRow.getHistogram(1)
        val sumRawHist = rawDataWindow.map(_(4).asInstanceOf[bv.MutableHistogram])
                                      .foldLeft(emptyAggHist) { case (agg, h) => agg.add(h); agg }
        aggHist shouldEqual sumRawHist

        val maxMax = rawDataWindow.map(_(3).asInstanceOf[Double])
                                  .foldLeft(0.0) { case (agg, m) => Math.max(agg, m) }
        aggRow.getDouble(2) shouldEqual maxMax
      }
    }
  }

  it("should correctly aggregate min_over_time / max_over_time using both chunked and sliding iterators") {
    val data = (1 to 240).map(_.toDouble)
    val chunkSize = 40
    val rv = timeValueRV(data)
    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(100) + 10
      val step = rand.nextInt(75) + 5
      info(s"  iteration $x  windowSize=$windowSize step=$step")

      val minSlidingIt = slidingWindowIt(data, rv, new MinMaxOverTimeFunction(Ordering[Double].reverse), windowSize, step)
      val aggregated = minSlidingIt.map(_.getDouble(1)).toBuffer
      // drop first sample because of exclusive start
      aggregated shouldEqual data.sliding(windowSize, step).map(_.drop(1).min).toBuffer

      val minChunkedIt = chunkedWindowIt(data, rv, new MinOverTimeChunkedFunctionD(), windowSize, step)
      val aggregated2 = minChunkedIt.map(_.getDouble(1)).toBuffer
      aggregated2 shouldEqual data.sliding(windowSize, step).map(_.drop(1).min).toBuffer

      val maxSlidingIt = slidingWindowIt(data, rv, new MinMaxOverTimeFunction(Ordering[Double]), windowSize, step)
      val aggregated3 = maxSlidingIt.map(_.getDouble(1)).toBuffer
      // drop first sample because of exclusive start
      aggregated3 shouldEqual data.sliding(windowSize, step).map(_.drop(1).max).toBuffer

      val maxChunkedIt = chunkedWindowIt(data, rv, new MaxOverTimeChunkedFunctionD(), windowSize, step)
      val aggregated4 = maxChunkedIt.map(_.getDouble(1)).toBuffer
      aggregated4 shouldEqual data.sliding(windowSize, step).map(_.drop(1).max).toBuffer
    }
  }

  it("should aggregate count_over_time and avg_over_time using both chunked and sliding iterators") {
    val data = (1 to 500).map(_.toDouble)
    val chunkSize = 40
    val rv = timeValueRV(data)

    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(100) + 10
      val step = rand.nextInt(50) + 5
      info(s"  iteration $x  windowSize=$windowSize step=$step")

      val countSliding = slidingWindowIt(data, rv, new CountOverTimeFunction(), windowSize, step)
      val aggregated1 = countSliding.map(_.getDouble(1)).toBuffer
      aggregated1 shouldEqual data.sliding(windowSize, step).map(_.length - 1).toBuffer

      val countChunked = chunkedWindowIt(data, rv, new CountOverTimeChunkedFunction(), windowSize, step)
      val aggregated2 = countChunked.map(_.getDouble(1)).toBuffer
      aggregated2 shouldEqual data.sliding(windowSize, step).map(_.length - 1).toBuffer

      val avgSliding = slidingWindowIt(data, rv, new AvgOverTimeFunction(), windowSize, step)
      val aggregated3 = avgSliding.map(_.getDouble(1)).toBuffer
      aggregated3 shouldEqual data.sliding(windowSize, step).map(a => avg(a drop 1)).toBuffer

      val avgChunked = chunkedWindowIt(data, rv, new AvgOverTimeChunkedFunctionD(), windowSize, step)
      val aggregated4 = avgChunked.map(_.getDouble(1)).toBuffer
      aggregated4 shouldEqual data.sliding(windowSize, step).map(a => avg(a drop 1)).toBuffer

      val changesChunked = chunkedWindowIt(data, rv, new ChangesChunkedFunctionD(), windowSize, step)
      val aggregated5 = changesChunked.map(_.getDouble(1)).toBuffer
      aggregated5.drop(0) shouldEqual data.sliding(windowSize, step).map(_.length - 2).drop(0).toBuffer
    }
  }

  it("should aggregate var_over_time and stddev_over_time using both chunked and sliding iterators") {
    val data = (1 to 500).map(_.toDouble)
    val chunkSize = 40
    val rv = timeValueRV(data)

    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(100) + 10
      val step = rand.nextInt(50) + 5
      info(s"  iteration $x  windowSize=$windowSize step=$step")

      val varSlidingIt = slidingWindowIt(data, rv, new StdVarOverTimeFunction(), windowSize, step)
      val aggregated2 = varSlidingIt.map(_.getDouble(1)).toBuffer
      aggregated2 shouldEqual data.sliding(windowSize, step).map(a => stdVar(a drop 1)).toBuffer

      val stdDevSlidingIt = slidingWindowIt(data, rv, new StdDevOverTimeFunction(), windowSize, step)
      val aggregated3 = stdDevSlidingIt.map(_.getDouble(1)).toBuffer
      aggregated3 shouldEqual data.sliding(windowSize, step).map(d => Math.sqrt(stdVar(d drop 1))).toBuffer

      val varFunc = new StdVarOverTimeChunkedFunctionD()
      val windowIt4 = chunkedWindowIt(data, rv, varFunc, windowSize, step)
      val aggregated4 = windowIt4.map(_.getDouble(1)).toBuffer
      aggregated4 shouldEqual data.sliding(windowSize, step).map(a => stdVar(a drop 1)).toBuffer

      val devFunc = new StdDevOverTimeChunkedFunctionD()
      val windowIt5 = chunkedWindowIt(data, rv, devFunc, windowSize, step)
      val aggregated5 = windowIt5.map(_.getDouble(1)).toBuffer
      aggregated5 shouldEqual data.sliding(windowSize, step).map(d => Math.sqrt(stdVar(d drop 1))).toBuffer
    }
  }

  it("should correctly do changes") {
    var data = Seq(1.5, 2.5, 3.5, 4.5, 5.5)
    val rv = timeValueRV(data)
    val list = rv.rows.map(x => (x.getLong(0), x.getDouble(1))).toList

    val windowSize = 100
    val step = 20

    val chunkedIt = new ChunkedWindowIteratorD(rv, 100000, 20000, 150000, 30000,
      new ChangesChunkedFunctionD(), queryConfig)
    val aggregated = chunkedIt.map(x => (x.getLong(0), x.getDouble(1))).toList
    aggregated shouldEqual List((100000, 0.0), (120000, 2.0), (140000, 2.0))
  }

  it("should correctly calculate quantileovertime") {
    val twoSampleData = Seq(0.0, 1.0)
    val threeSampleData = Seq(1.0, 0.0, 2.0)
    val unevenSampleData = Seq(0.0, 1.0, 4.0)
    val  rangeParams = RangeParams(100,20, 500)

    val quantiles = Seq(0, 0.5, 0.75, 0.8, 1, -1, 2).map(x => StaticFuncArgs(x, rangeParams))
    val twoSampleDataResponses = Seq(0, 0.5, 0.75, 0.8, 1, Double.NegativeInfinity, Double.PositiveInfinity)
    val threeSampleDataResponses = Seq(0, 1, 1.5, 1.6, 2, Double.NegativeInfinity, Double.PositiveInfinity)
    val unevenSampleDataResponses = Seq(0, 1, 2.5, 2.8, 4, Double.NegativeInfinity, Double.PositiveInfinity)

    val n = quantiles.length
    for (i <- 0 until n) {
      var rv = timeValueRV(twoSampleData)
      val chunkedItTwoSample = new ChunkedWindowIteratorD(rv, 110000, 120000, 150000, 30000,
        new QuantileOverTimeChunkedFunctionD(Seq(quantiles(i))), queryConfig)
      val aggregated2 = chunkedItTwoSample.map(_.getDouble(1)).toBuffer
      aggregated2(0) shouldEqual twoSampleDataResponses(i) +- 0.0000000001

      rv = timeValueRV(threeSampleData)
      val chunkedItThreeSample = new ChunkedWindowIteratorD(rv, 120000, 20000, 130000, 50000,
        new QuantileOverTimeChunkedFunctionD(Seq(quantiles(i))), queryConfig)
      val aggregated3 = chunkedItThreeSample.map(_.getDouble(1)).toBuffer
      aggregated3(0) shouldEqual threeSampleDataResponses(i) +- 0.0000000001

      rv = timeValueRV(unevenSampleData)
      val chunkedItUnevenSample = new ChunkedWindowIteratorD(rv, 120000, 20000, 130000, 30000,
        new QuantileOverTimeChunkedFunctionD(Seq(quantiles(i))), queryConfig)
      val aggregatedUneven = chunkedItUnevenSample.map(_.getDouble(1)).toBuffer
      aggregatedUneven(0) shouldEqual unevenSampleDataResponses(i) +- 0.0000000001
    }
    val emptyData = Seq()
    var rv = timeValueRV(emptyData)
    val chunkedItNoSample = new ChunkedWindowIteratorD(rv, 110000, 120000, 150000, 30000,
      new QuantileOverTimeChunkedFunctionD(Seq(StaticFuncArgs(0.5, rangeParams))), queryConfig)
    val aggregatedEmpty = chunkedItNoSample.map(_.getDouble(1)).toBuffer
    aggregatedEmpty(0) isNaN

    def median(s: Seq[Double]): Double = {
      val (lower, upper) = s.sortWith(_<_).splitAt(s.size / 2)
      if (s.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head
    }

    val data = (1 to 500).map(_.toDouble)
    val rv2 = timeValueRV(data)
    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(100) + 10
      val step = rand.nextInt(50) + 5
      info(s"  iteration $x  windowSize=$windowSize step=$step")

      val minChunkedIt = chunkedWindowIt(data, rv2,  new QuantileOverTimeChunkedFunctionD
      (Seq(StaticFuncArgs(0.5, rangeParams))), windowSize, step)

      val aggregated2 = minChunkedIt.map(_.getDouble(1)).toBuffer
      aggregated2 shouldEqual data.sliding(windowSize, step).map(_.drop(1)).map(median).toBuffer
    }
  }
  
  it("should correctly do changes for DoubleVectorDataReader and DeltaDeltaDataReader when window has more " +
    "than one chunks") {
    val data1= (1 to 240).map(_.toDouble)
    val data2 : Seq[Double]= Seq[Double]( 1.1, 1.5, 2.5, 3.5, 4.5, 5.5)

    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(100) + 10
      val step = rand.nextInt(50) + 5
      info(s"  iteration $x  windowSize=$windowSize step=$step")
      // Append double data and shuffle so that it becomes DoubleVectorDataReader
      val data = scala.util.Random.shuffle(data2 ++ data1)

      val rv = timeValueRV(data)
      val list = rv.rows.map(x => (x.getLong(0), x.getDouble(1))).toList

      val stepTimeMillis = step.toLong * pubFreq
      val changesChunked = chunkedWindowIt(data, rv, new ChangesChunkedFunctionD(), windowSize, step)
      val aggregated2 = changesChunked.map(_.getDouble(1)).toBuffer

      data.sliding(windowSize, step).map(_.length - 2).toBuffer.zip(aggregated2).foreach {
        case (val1, val2) => if (val1 == -1) {
          val2.isNaN shouldEqual (true) // window does not have any element so changes will be NaN
        } else {
          val1 shouldEqual (val2)
        }
      }
    }
  }

  it("should correctly calculate holt winters") {
    val positiveTrendData2 = Seq(15900.0, 15920.0, 15940.0, 15960.0, 15980.0, 16000.0)
    val positiveTrendData3 = Seq(23850.0,23880.0,23910.0,23940.0,23970.0, 24000.0)
    val positiveTrendData4 = Seq(31800.0, 31840.0, 31880.0, 31920.0, 31960.0, 32000.0)

    val negativeTrendData2 = Seq(-15900.0, -15920.0, -15940.0, -15960.0, -15980.0, -16000.0)
    val params = Seq(StaticFuncArgs(0.01, RangeParams(100,20,500)), StaticFuncArgs(0.1, RangeParams(100,20,500)))

    def holt_winters(arr: Seq[Double]): Double = {
      val sf = 0.01
      val tf = 0.1
      var smoothedResult = Double.NaN
      var s0 = arr(0)
      val n = arr.length
      var b0 = Double.NaN
      if (n >= 2) {
        b0 = arr(1) - arr(0)
        for (i <- 1 until n) {
          smoothedResult  = sf*arr(i) + (1-sf)*(s0 + b0)
          b0 = tf*(smoothedResult - s0) + (1-tf)*b0
          s0 = smoothedResult

        }
        s0
      } else {
        Double.NaN
      }
    }


    var rv = timeValueRV(positiveTrendData2)
    val chunkedIt2 = new ChunkedWindowIteratorD(rv, 160000, 100000, 180000, 100000,
      new HoltWintersChunkedFunctionD(params), queryConfig)
    val aggregated2 = chunkedIt2.map(_.getDouble(1)).toBuffer
    aggregated2(0) shouldEqual holt_winters(positiveTrendData2)

    rv = timeValueRV(positiveTrendData3)
    val chunkedIt3 = new ChunkedWindowIteratorD(rv, 160000, 100000, 180000, 100000,
      new HoltWintersChunkedFunctionD(params), queryConfig)
    val aggregated3 = chunkedIt3.map(_.getDouble(1)).toBuffer
    aggregated3(0) shouldEqual holt_winters(positiveTrendData3)

    rv = timeValueRV(positiveTrendData4)
    val chunkedIt4 = new ChunkedWindowIteratorD(rv, 160000, 100000, 180000, 100000,
      new HoltWintersChunkedFunctionD(params), queryConfig)
    val aggregated4 = chunkedIt4.map(_.getDouble(1)).toBuffer
    aggregated4(0) shouldEqual holt_winters(positiveTrendData4)

    rv = timeValueRV(negativeTrendData2)
    val chunkedItNeg2 = new ChunkedWindowIteratorD(rv, 160000, 100000, 180000, 100000,
      new HoltWintersChunkedFunctionD(params), queryConfig)
    val aggregatedNeg2 = chunkedItNeg2.map(_.getDouble(1)).toBuffer
    aggregatedNeg2(0) shouldEqual holt_winters(negativeTrendData2)


    val data = (1 to 240).map(_.toDouble)
    val rv2 = timeValueRV(data)
    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(100) + 10
      val step = rand.nextInt(75) + 5
      info(s"  iteration $x  windowSize=$windowSize step=$step")

      val minChunkedIt = chunkedWindowIt(data, rv2, new HoltWintersChunkedFunctionD(params), windowSize, step)
      val aggregated2 = minChunkedIt.map(_.getDouble(1)).toBuffer
      val res = data.sliding(windowSize, step).map(_.drop(1)).map(holt_winters).toBuffer
      for (i <- res.indices) {
        if (res(i).isNaN) {
          println("Inside Nan")
          aggregated2(i).isNaN shouldEqual true
        } else {
          aggregated2(i) shouldBe (res(i) +- 0.0000000001)
        }
      }
    }
  }
}
