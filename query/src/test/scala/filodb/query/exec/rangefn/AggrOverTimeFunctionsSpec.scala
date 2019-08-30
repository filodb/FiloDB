package filodb.query.exec.rangefn

import scala.util.Random

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

import filodb.core.memstore.{TimeSeriesPartition, TimeSeriesPartitionSpec, WriteBufferPool}
import filodb.core.query.RawDataRangeVector
import filodb.core.store.AllChunkScan
import filodb.core.{MachineMetricsData => MMD, MetricsTestData, TestData}
import filodb.memory._
import filodb.memory.format.{vectors => bv, TupleRowReader}
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
  protected val ingestBlockHolder = new BlockMemFactory(blockStore, None, timeseriesDataset.blockMetaSize, true)
  protected val tsBufferPool = new WriteBufferPool(TestData.nativeMem, timeseriesDataset, storeConf)

  protected val ingestBlockHolder2 = new BlockMemFactory(blockStore, None, downsampleDataset.blockMetaSize, true)
  protected val tsBufferPool2 = new WriteBufferPool(TestData.nativeMem, downsampleDataset, storeConf)

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
    readers.foreach { row => part.ingest(row, ingestBlockHolder) }
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
    readers.foreach { row => part.ingest(row, ingestBlockHolder2) }
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
    readers.foreach { row => part.ingest(row, ingestBlockHolder) }
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

//      val slidingIt = slidingWindowIt(data, rv, new SumOverTimeFunction(), windowSize, step)
//      val aggregated = slidingIt.map(_.getDouble(1)).toBuffer
//      // drop first sample because of exclusive start
//      aggregated shouldEqual data.sliding(windowSize, step).map(_.drop(1).sum).toBuffer

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

//      val countSliding = slidingWindowIt(data, rv, new CountOverTimeFunction(), windowSize, step)
//      val aggregated1 = countSliding.map(_.getDouble(1)).toBuffer
//      aggregated1 shouldEqual data.sliding(windowSize, step).map(_.length - 1).toBuffer
//
//      val countChunked = chunkedWindowIt(data, rv, new CountOverTimeChunkedFunction(), windowSize, step)
//      val aggregated2 = countChunked.map(_.getDouble(1)).toBuffer
//      aggregated2 shouldEqual data.sliding(windowSize, step).map(_.length - 1).toBuffer
//
//      val avgSliding = slidingWindowIt(data, rv, new AvgOverTimeFunction(), windowSize, step)
//      val aggregated3 = avgSliding.map(_.getDouble(1)).toBuffer
//      aggregated3 shouldEqual data.sliding(windowSize, step).map(a => avg(a drop 1)).toBuffer

      val avgChunked = chunkedWindowIt(data, rv, new AvgOverTimeChunkedFunctionD(), windowSize, step)
      val aggregated4 = avgChunked.map(_.getDouble(1)).toBuffer
      aggregated4 shouldEqual data.sliding(windowSize, step).map(a => avg(a drop 1)).toBuffer
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
    //val data = (1 to 5).map(_.toDouble)
    var data = Seq(1, 2, 3, 4, 5).map(_.toDouble)
    val rv = timeValueRV(data)
    val list = rv.rows.map(x => (x.getLong(0), x.getDouble(1))).toList

    val windowSize = 100
    val step = 20

    val slidingIt = new SlidingWindowIterator(rv.rows, 100000, 20000, 150000, 30000,
      new ChangesFunction(), queryConfig)
    val chunkedIt = new ChunkedWindowIteratorD(rv, 100000, 20000, 150000, 30000,
      new ChangesChunkedFunctionD(), queryConfig)
    println("rv rows:" + rv.rows.map(x => (x.getLong(0), x.getDouble(1))).toList)
    val aggregated1 = slidingIt.map(x => (x.getLong(0), x.getDouble(1))).toList
    val aggregated2 = chunkedIt.map(x => (x.getLong(0), x.getDouble(1))).toList
    val expectedResult = List((100000, 0.0), (120000, 2.0), (140000, 2.0))
    aggregated1 shouldEqual(expectedResult)
    aggregated2 shouldEqual(expectedResult)
  }

  it("should correctly do changes when values is NaN") {
    var data = Seq(Double.NaN, 1d, 2d, 3d, 4d, 5d)
    val rv = timeValueRV(data)
    val windowSize = 100
    val step = 20

    val slidingIt = slidingWindowIt(data, rv, new ChangesFunction(), windowSize, step)
    val aggregated = slidingIt.map(_.getDouble(1)).toBuffer

  }

  it("should yield NaN when changes is done on data having NaN  after a value") {
    var data = Seq(0, 5, Double.NaN)
    val rv = timeValueRV(data)
    val windowSize = 100
    val step = 20

    val slidingIt = new SlidingWindowIterator(rv.rows, 100000, 20000, 150000, 30000,
      new ChangesFunction(), queryConfig)
    val chunkedIt = new ChunkedWindowIteratorD(rv, 100000, 20000, 150000, 30000,
      new ChangesChunkedFunctionD(), queryConfig)

    val chunkedIt1 = new ChunkedWindowIteratorD(rv, 100000, 20000, 150000, 30000,
      new SumOverTimeChunkedFunctionD(), queryConfig)
    println("rv rows:" + rv.rows.map(x => (x.getLong(0), x.getDouble(1))).toList)
    val aggregated1 = slidingIt.map(x => (x.getLong(0), x.getDouble(1))).toList
    val aggregated2 = chunkedIt.map(x => (x.getLong(0), x.getDouble(1))).toList
    val aggregatedSum = chunkedIt1.map(x => (x.getLong(0), x.getDouble(1))).toList

    println(s"aggregated1:  ${aggregated1}")
    println(s"aggregated2:  ${aggregated2}")
    println(s"aggregatedSum:  ${aggregatedSum}")
    aggregated1(0)._2 shouldEqual(0.0)
    aggregated1(1)._2.isNaN shouldEqual(true)
    aggregated2(0)._2 shouldEqual(0.0)
    aggregated2(1)._2.isNaN shouldEqual(true)
  }

  it("should yield NaN when changes is done on data having 2 or more consecutive NaN's after a value") {
    var data = Seq(0, 5, 6, Double.NaN, Double.NaN, Double.NaN, Double.NaN)
    val rv = timeValueRV(data)
    val windowSize = 100
    val step = 20

    val slidingIt = new SlidingWindowIterator(rv.rows, 100000, 10000, 160000, 20000, new ChangesFunction(), queryConfig)
    val aggregated = slidingIt.map(x => (x.getLong(0), x.getDouble(1))).toList
    aggregated(6)._2.isNaN shouldEqual(true)
    aggregated(5)._2.isNaN shouldEqual(true)
    aggregated(4)._2.isNaN shouldEqual(true)
    aggregated(3)._2.isNaN shouldEqual(true)
  }

}
