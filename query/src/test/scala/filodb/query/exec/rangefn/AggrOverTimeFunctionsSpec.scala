package filodb.query.exec.rangefn

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import filodb.core.{MetricsTestData, QueryTimeoutException, TestData, MachineMetricsData => MMD}
import filodb.core.memstore.{TimeSeriesPartition, TimeSeriesPartitionSpec, WriteBufferPool}
import filodb.core.query._
import filodb.core.store.AllChunkScan
import filodb.core.MachineMetricsData.defaultPartKey
import filodb.memory._
import filodb.memory.data.ChunkMap
import filodb.memory.format.{TupleRowReader, vectors => bv}
import filodb.memory.BinaryRegion.NativePointer
import filodb.query.exec._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
 * A common trait for windowing query tests which uses real chunks and real RawDataRangeVectors
 */
trait RawDataWindowingSpec extends AnyFunSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll {
  import MetricsTestData._

  private val blockStore = new PageAlignedBlockManager(100 * 1024 * 1024,
    new MemoryStats(Map("test"-> "test")), null, 16)
  val storeConf = TestData.storeConf.copy(maxChunksSize = 200)
  protected val ingestBlockHolder = new BlockMemFactory(blockStore, None, timeseriesSchema.data.blockMetaSize,
                                      MMD.dummyContext, true)
  protected val tsBufferPool = new WriteBufferPool(TestData.nativeMem,
                                        timeseriesDatasetWithMetric.schema.data, storeConf)

  protected val ingestBlockHolder2 = new BlockMemFactory(blockStore, None, downsampleSchema.data.blockMetaSize,
                                      MMD.dummyContext, true)
  protected val tsBufferPool2 = new WriteBufferPool(TestData.nativeMem, downsampleSchema.data, storeConf)

  after {
    ChunkMap.validateNoSharedLocks(true)
  }

  override def afterAll(): Unit = {
    blockStore.releaseBlocks()
  }

  // Below function computes summation of the squared values of the sequence
  // Limitation : If the sequence contains combination of NaN and not NaN values, the computed summed squared value will be NaN
  def sumSquares(nn: Seq[Double]): Double = nn.map(n => n*n).sum.toDouble

  // Below function computes average of the sequence
  // Limitation : If the sequence contains combination of NaN and not NaN values, the computed average will be NaN
  def avg(nn: Seq[Double]): Double = nn.sum.toDouble / nn.length

  // Below function computes stdVar of the sequence
  // Limitation : If the sequence contains combination ofNaN and not NaN values, the standard variation will be NaN
  def stdVar(nn: Seq[Double]): Double = sumSquares(nn)/nn.length - avg(nn)*avg(nn)

  // Below function count no. of non-nan values in the sequence
  def countNonNaN(arr: Seq[Double]): Int = {
    var count = 0
    var currPos = 0
    val length = arr.length
    if (length > 0) {
      while (currPos < length) {
        if(!arr(currPos).isNaN) {
          count += 1
        }
        currPos += 1
      }
    }
    count
  }

  // Below function computes sum of the sequence when the sequence contains combination of NaN and not NaN values
  // sum = summation of non-nan values.
  def sumWithNaN(arr: Seq[Double]): Double = {
    var currPos = 0
    var length = arr.length
    var sum = Double.NaN
    var count = 0
    if (length > 0) {
      while (currPos < length) {
        if(!arr(currPos).isNaN) {
          if (sum.isNaN) sum = 0d
          sum += arr(currPos)
          count +=1
        }
        currPos += 1
      }
    }
    sum
  }

  // Below function computes average of the sequence when the sequence contains combination of NaN and not NaN values
  // avg = summation of non-nan values/no. of non-nan values.
  def avgWithNaN(arr: Seq[Double]): Double = {
    sumWithNaN(arr)/countNonNaN(arr)
  }

  // Below function computes the summation of the squared values of the sequence when the sequence contains combination of NaN and not NaN values
  // squaredSum = summation of squares of non-nan values.
  def sumSquaresWithNaN(arr: Seq[Double]): Double = {
    var currPos = 0
    var sumSquares = Double.NaN
    val length = arr.length
    if (length > 0) {
      while (currPos < length) {
        if(!arr(currPos).isNaN) {
          if (sumSquares.isNaN) sumSquares = 0d
          sumSquares += arr(currPos) * arr(currPos)
        }
        currPos += 1
      }
    }
    sumSquares
  }


  // Below function computes stdVar of the sequence when the sequence contains combination of NaN and non-NaN values
  // standard variation = (summation of squares of non-nan values/no. of non-nan values) - square of avg of non-nan values
  def stdVarWithNaN(arr: Seq[Double]): Double = {
    val avg = avgWithNaN(arr)
    (sumSquaresWithNaN(arr) / countNonNaN(arr)) - (avg * avg)
  }

  // Below function computes zscore for the last Sample of the input sequence.
  // zscore = (lastSampleValue - avg(sequence))/stddev(sequence)
  def z_score(arr: Seq[Double]): Double = {
    var zscore = Double.NaN
    if (arr.length > 0) {
      if (!arr.last.isNaN) {
        val av = avgWithNaN(arr)
        val sd = Math.sqrt(stdVarWithNaN(arr))
        zscore = (arr.last - av) / sd
      }
    }
    zscore
  }

  val defaultStartTS = 100000L
  val pubFreq = 10000L

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)

  // windowSize and step are in number of elements of the data
  def numWindows(data: Seq[Any], windowSize: Int, step: Int): Int = data.sliding(windowSize, step).length

  // Creates a RawDataRangeVector using Prometheus time-value schema and a given chunk size etc.
  def timeValueRVPk(tuples: Seq[(Long, Double)],
                    partKey: NativePointer = defaultPartKey): RawDataRangeVector = {
    val part = TimeSeriesPartitionSpec.makePart(0, timeseriesDatasetWithMetric, partKey, bufferPool = tsBufferPool)
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
    timeValueRVPk(tuples)
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
  def histogramRV(numSamples: Int = 100, numBuckets: Int = 8, infBucket: Boolean = false):
  (Stream[Seq[Any]], RawDataRangeVector) =
    MMD.histogramRV(defaultStartTS, pubFreq, numSamples, numBuckets, infBucket)

  def chunkedWindowIt(data: Seq[Double],
                      rv: RawDataRangeVector,
                      func: ChunkedRangeFunction[TransientRow],
                      windowSize: Int,
                      step: Int): ChunkedWindowIteratorD = {
    val windowTime = (windowSize.toLong - 1) * pubFreq
    val windowStartTS = defaultStartTS + windowTime
    val stepTimeMillis = step.toLong * pubFreq
    val windowEndTS = windowStartTS + (numWindows(data, windowSize, step) - 1) * stepTimeMillis
    new ChunkedWindowIteratorD(rv, windowStartTS, stepTimeMillis, windowEndTS, windowTime, func, querySession)
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
                               func.asInstanceOf[ChunkedRangeFunction[TransientHistRow]], querySession, row)
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
      info(s"iteration $x windowSize=$windowSize step=$step")
      val slidingIt = slidingWindowIt(data, rv, new SumOverTimeFunction(), windowSize, step)
      val aggregated = slidingIt.map(_.getDouble(1)).toBuffer
      slidingIt.close()
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
      info(s"iteration $x windowSize=$windowSize step=$step")

      val chunkedIt = chunkedWindowItHist(data, rv, new SumOverTimeChunkedFunctionH(), windowSize, step)
      chunkedIt.zip(data.sliding(windowSize, step).map(_.drop(1))).foreach { case (aggRow, rawDataWindow) =>
        val aggHist = aggRow.getHistogram(1)
        val sumRawHist = rawDataWindow.map(_(3).asInstanceOf[bv.LongHistogram])
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
      info(s"iteration $x windowSize=$windowSize step=$step")

      val row = new TransientHistMaxRow()
      val chunkedIt = chunkedWindowItHist(data, rv, new SumAndMaxOverTimeFuncHD(3), windowSize, step, row)
      chunkedIt.zip(data.sliding(windowSize, step).map(_.drop(1))).foreach { case (aggRow, rawDataWindow) =>
        val aggHist = aggRow.getHistogram(1)
        val sumRawHist = rawDataWindow.map(_(4).asInstanceOf[bv.LongHistogram])
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
      info(s"iteration $x windowSize=$windowSize step=$step")

      val minSlidingIt = slidingWindowIt(data, rv, new MinMaxOverTimeFunction(Ordering[Double].reverse), windowSize, step)
      val aggregated = minSlidingIt.map(_.getDouble(1)).toBuffer
      minSlidingIt.close()
      // drop first sample because of exclusive start
      aggregated shouldEqual data.sliding(windowSize, step).map(_.drop(1).min).toBuffer

      val minChunkedIt = chunkedWindowIt(data, rv, new MinOverTimeChunkedFunctionD(), windowSize, step)
      val aggregated2 = minChunkedIt.map(_.getDouble(1)).toBuffer
      aggregated2 shouldEqual data.sliding(windowSize, step).map(_.drop(1).min).toBuffer

      val maxSlidingIt = slidingWindowIt(data, rv, new MinMaxOverTimeFunction(Ordering[Double]), windowSize, step)
      val aggregated3 = maxSlidingIt.map(_.getDouble(1)).toBuffer
      maxSlidingIt.close()
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
      info(s"iteration $x windowSize=$windowSize step=$step")

      val countSliding = slidingWindowIt(data, rv, new CountOverTimeFunction(), windowSize, step)
      val aggregated1 = countSliding.map(_.getDouble(1)).toBuffer
      countSliding.close()
      aggregated1 shouldEqual data.sliding(windowSize, step).map(_.length - 1).toBuffer

      val countChunked = chunkedWindowIt(data, rv, new CountOverTimeChunkedFunction(), windowSize, step)
      val aggregated2 = countChunked.map(_.getDouble(1)).toBuffer
      aggregated2 shouldEqual data.sliding(windowSize, step).map(_.length - 1).toBuffer

      val avgSliding = slidingWindowIt(data, rv, new AvgOverTimeFunction(), windowSize, step)
      val aggregated3 = avgSliding.map(_.getDouble(1)).toBuffer
      avgSliding.close()
      aggregated3 shouldEqual data.sliding(windowSize, step).map(a => avg(a drop 1)).toBuffer

      // In sample_data2, there are no NaN's, that's why using avg function is fine
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
      info(s"iteration $x windowSize=$windowSize step=$step")

      val varSlidingIt = slidingWindowIt(data, rv, new StdVarOverTimeFunction(), windowSize, step)
      val aggregated2 = varSlidingIt.map(_.getDouble(1)).toBuffer
      varSlidingIt.close()
      aggregated2 shouldEqual data.sliding(windowSize, step).map(a => stdVar(a drop 1)).toBuffer

      val stdDevSlidingIt = slidingWindowIt(data, rv, new StdDevOverTimeFunction(), windowSize, step)
      val aggregated3 = stdDevSlidingIt.map(_.getDouble(1)).toBuffer
      stdDevSlidingIt.close()
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
      new ChangesChunkedFunctionD(), querySession)
    val aggregated = chunkedIt.map(x => (x.getLong(0), x.getDouble(1))).toList
    aggregated shouldEqual List((100000, 0.0), (120000, 2.0), (140000, 2.0))
  }

  it("should correctly calculate quantileovertime") {
    val twoSampleData = Seq(0.0, 1.0)
    val threeSampleData = Seq(1.0, 0.0, 2.0)
    val unevenSampleData = Seq(0.0, 1.0, 4.0)
    val rangeParams = RangeParams(100, 20, 500)

    val quantiles = Seq(0, 0.5, 0.75, 0.8, 1, -1, 2).map(x => StaticFuncArgs(x, rangeParams))
    val twoSampleDataResponses = Seq(0, 0.5, 0.75, 0.8, 1, Double.NegativeInfinity, Double.PositiveInfinity)
    val threeSampleDataResponses = Seq(0, 1, 1.5, 1.6, 2, Double.NegativeInfinity, Double.PositiveInfinity)
    val unevenSampleDataResponses = Seq(0, 1, 2.5, 2.8, 4, Double.NegativeInfinity, Double.PositiveInfinity)

    val n = quantiles.length
    for (i <- 0 until n) {
      var rv = timeValueRV(twoSampleData)
      val chunkedItTwoSample = new ChunkedWindowIteratorD(rv, 110000, 120000, 150000, 30000,
        new QuantileOverTimeChunkedFunctionD(Seq(quantiles(i))), querySession)
      val aggregated2 = chunkedItTwoSample.map(_.getDouble(1)).toBuffer
      aggregated2(0) shouldEqual twoSampleDataResponses(i) +- 0.0000000001

      rv = timeValueRV(threeSampleData)
      val chunkedItThreeSample = new ChunkedWindowIteratorD(rv, 120000, 20000, 130000, 50000,
        new QuantileOverTimeChunkedFunctionD(Seq(quantiles(i))), querySession)
      val aggregated3 = chunkedItThreeSample.map(_.getDouble(1)).toBuffer
      aggregated3(0) shouldEqual threeSampleDataResponses(i) +- 0.0000000001

      rv = timeValueRV(unevenSampleData)
      val chunkedItUnevenSample = new ChunkedWindowIteratorD(rv, 120000, 20000, 130000, 30000,
        new QuantileOverTimeChunkedFunctionD(Seq(quantiles(i))), querySession)
      val aggregatedUneven = chunkedItUnevenSample.map(_.getDouble(1)).toBuffer
      aggregatedUneven(0) shouldEqual unevenSampleDataResponses(i) +- 0.0000000001
    }
    val emptyData = Seq()
    var rv = timeValueRVPk(emptyData)
    val chunkedItNoSample = new ChunkedWindowIteratorD(rv, 110000, 120000, 150000, 30000,
      new QuantileOverTimeChunkedFunctionD(Seq(StaticFuncArgs(0.5, rangeParams))), querySession)
    val aggregatedEmpty = chunkedItNoSample.map(_.getDouble(1)).toBuffer
    aggregatedEmpty(0) isNaN

    def median(s: Seq[Double]): Double = {
      val (lower, upper) = s.sortWith(_ < _).splitAt(s.size / 2)
      if (s.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head
    }

    val data = (1 to 500).map(_.toDouble)
    val rv2 = timeValueRV(data)
    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(100) + 10
      val step = rand.nextInt(50) + 5
      info(s"iteration $x windowSize=$windowSize step=$step")

      val minChunkedIt = chunkedWindowIt(data, rv2, new QuantileOverTimeChunkedFunctionD
      (Seq(StaticFuncArgs(0.5, rangeParams))), windowSize, step)

      val aggregated2 = minChunkedIt.map(_.getDouble(1)).toBuffer
      aggregated2 shouldEqual data.sliding(windowSize, step).map(_.drop(1)).map(median).toBuffer
    }
  }

  it("should correctly do changes for DoubleVectorDataReader and DeltaDeltaDataReader when window has more " +
    "than one chunks") {
    val data1 = (1 to 240).map(_.toDouble)
    val data2: Seq[Double] = Seq[Double](1.1, 1.5, 2.5, 3.5, 4.5, 5.5)

    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(100) + 10
      val step = rand.nextInt(50) + 5
      info(s"iteration $x windowSize=$windowSize step=$step")
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
    val positiveTrendData3 = Seq(23850.0, 23880.0, 23910.0, 23940.0, 23970.0, 24000.0)
    val positiveTrendData4 = Seq(31800.0, 31840.0, 31880.0, 31920.0, 31960.0, 32000.0)

    val negativeTrendData2 = Seq(-15900.0, -15920.0, -15940.0, -15960.0, -15980.0, -16000.0)
    val params = Seq(StaticFuncArgs(0.01, RangeParams(100, 20, 500)), StaticFuncArgs(0.1, RangeParams(100, 20, 500)))

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
          smoothedResult = sf * arr(i) + (1 - sf) * (s0 + b0)
          b0 = tf * (smoothedResult - s0) + (1 - tf) * b0
          s0 = smoothedResult

        }
        s0
      } else {
        Double.NaN
      }
    }

    var rv = timeValueRV(positiveTrendData2)
    val chunkedIt2 = new ChunkedWindowIteratorD(rv, 160000, 100000, 180000, 100000,
      new HoltWintersChunkedFunctionD(params), querySession)
    val aggregated2 = chunkedIt2.map(_.getDouble(1)).toBuffer
    aggregated2(0) shouldEqual holt_winters(positiveTrendData2)

    rv = timeValueRV(positiveTrendData3)
    val chunkedIt3 = new ChunkedWindowIteratorD(rv, 160000, 100000, 180000, 100000,
      new HoltWintersChunkedFunctionD(params), querySession)
    val aggregated3 = chunkedIt3.map(_.getDouble(1)).toBuffer
    aggregated3(0) shouldEqual holt_winters(positiveTrendData3)

    rv = timeValueRV(positiveTrendData4)
    val chunkedIt4 = new ChunkedWindowIteratorD(rv, 160000, 100000, 180000, 100000,
      new HoltWintersChunkedFunctionD(params), querySession)
    val aggregated4 = chunkedIt4.map(_.getDouble(1)).toBuffer
    aggregated4(0) shouldEqual holt_winters(positiveTrendData4)

    rv = timeValueRV(negativeTrendData2)
    val chunkedItNeg2 = new ChunkedWindowIteratorD(rv, 160000, 100000, 180000, 100000,
      new HoltWintersChunkedFunctionD(params), querySession)
    val aggregatedNeg2 = chunkedItNeg2.map(_.getDouble(1)).toBuffer
    aggregatedNeg2(0) shouldEqual holt_winters(negativeTrendData2)


    val data = (1 to 240).map(_.toDouble)
    val rv2 = timeValueRV(data)
    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(100) + 10
      val step = rand.nextInt(75) + 5
      info(s"iteration $x windowSize=$windowSize step=$step")

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

  it("should correctly calculate predict_linear") {
    val data = (1 to 500).map(_.toDouble)
    val rv2 = timeValueRV(data)
    val duration = 50
    val params = Seq(StaticFuncArgs(50, RangeParams(100, 20, 500)))

    def predict_linear(s: Seq[Double], interceptTime: Long, startTime: Long): Double = {
      val n = s.length.toDouble
      var sumY = 0.0
      var sumX = 0.0
      var sumXY = 0.0
      var sumX2 = 0.0
      var x = 0.0
      if (n >= 2) {
        for (i <- 0 until n.toInt) {
          x = (startTime + i * pubFreq - interceptTime) / 1000.0
          sumY += s(i)
          sumX += x
          sumXY += x * s(i)
          sumX2 += x * x
        }
        val covXY = sumXY - (sumX * sumY) / n.toDouble
        val varX = sumX2 - (sumX * sumX) / n.toDouble
        val slope = covXY.toDouble / varX.toDouble
        val intercept = sumY / n - (slope * sumX) / n.toDouble
        slope * duration + intercept
      } else {
        Double.NaN
      }
    }

    val step = rand.nextInt(50) + 5
    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(100) + 10
      info(s"iteration $x windowSize=$windowSize step=$step")
      val ChunkedIt = chunkedWindowIt(data, rv2, new PredictLinearChunkedFunctionD(params), windowSize, step)
      val aggregated2 = ChunkedIt.map(_.getDouble(1)).toBuffer
      var res = new ArrayBuffer[Double]
      var startTime = defaultStartTS + pubFreq
      var endTime = startTime + (windowSize - 2) * pubFreq
      for (item <- data.sliding(windowSize, step).map(_.drop(1))) {
        res += predict_linear(item, endTime, startTime)
        startTime += step * pubFreq
        endTime += step * pubFreq
      }
      aggregated2 shouldEqual (res)
    }
  }

  it("it should correctly calculate zscore") {
    val data = (1 to 500).map(_.toDouble)
    val rv = timeValueRV(data)

    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(100) + 10
      val step = rand.nextInt(50) + 5
      info(s"iteration $x windowSize=$windowSize step=$step")

      val chunkedIt = chunkedWindowIt(data, rv, new ZScoreChunkedFunctionD(), windowSize, step)
      val aggregated = chunkedIt.map(_.getDouble(1)).toBuffer
      aggregated shouldEqual data.sliding(windowSize, step).map(d => z_score(d drop 1)).toBuffer
    }
  }

  it("it should correctly calculate sum_over_time, avg_over_time, stddev_over_time & " +
    "zscore when the sequence contains NaNs or is empty") {
    val test_data = Seq(
      Seq(15900.0, 15920.0, 15940.0, 15960.0, 15980.0, 16000.0, 16020.0),
      Seq(-15900.0, -15920.0, -15940.0, -15960.0, -15980.0, -16000.0),
      Seq(15900.0, 15920.0, 15940.0, 15960.0, 15980.0, 16000.0, Double.NaN),
      Seq(23850.0, 23880.0, 23910.0, 23940.0, 23970.0, 24000.0),
      Seq(31800.0, 31840.0, 31880.0, 31920.0, 31960.0, 32000.0),
      Seq(31800.0, 31840.0, 31880.0, Double.NaN, 31920.0, 31960.0, 32000.0),
      Seq(Double.NaN, 31800.0, 31840.0, 31880.0, 31920.0, 31960.0, 32000.0),
      Seq(Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN),
      Seq()
    )

    for (data <- test_data) {
      val rv = timeValueRV(data)

      // sum_over_time
      val chunkedItSumOverTime = new ChunkedWindowIteratorD(rv, 160000, 100000, 180000, 100000, new SumOverTimeChunkedFunctionD(), querySession)
      val aggregatedSumOverTime = chunkedItSumOverTime.map(_.getDouble(1)).toBuffer
      if (aggregatedSumOverTime(0).isNaN) aggregatedSumOverTime(0).isNaN shouldBe true
      else aggregatedSumOverTime(0) shouldBe sumWithNaN(data)

      // avg_over_time
      val chunkedItAvgOverTime = new ChunkedWindowIteratorD(rv, 160000, 100000, 180000, 100000, new AvgOverTimeChunkedFunctionD(), querySession)
      val aggregatedAvgOverTime = chunkedItAvgOverTime.map(_.getDouble(1)).toBuffer
      if (aggregatedAvgOverTime(0).isNaN) aggregatedAvgOverTime(0).isNaN shouldBe true
      else aggregatedAvgOverTime(0) shouldBe avgWithNaN(data)

      // stdvar_over_time
      val chunkedItStdVarOverTime = new ChunkedWindowIteratorD(rv, 160000, 100000, 180000, 100000, new StdVarOverTimeChunkedFunctionD(), querySession)
      val aggregatedStdVarOverTime = chunkedItStdVarOverTime.map(_.getDouble(1)).toBuffer
      if (aggregatedStdVarOverTime(0).isNaN) aggregatedStdVarOverTime(0).isNaN shouldBe true
      else aggregatedStdVarOverTime(0) shouldBe stdVarWithNaN(data)

      // stddev_over_time
      val chunkedItStdDevOverTime = new ChunkedWindowIteratorD(rv, 160000, 100000, 180000, 100000, new StdDevOverTimeChunkedFunctionD(), querySession)
      val aggregatedStdDevOverTime = chunkedItStdDevOverTime.map(_.getDouble(1)).toBuffer
      if (aggregatedStdDevOverTime(0).isNaN) aggregatedStdDevOverTime(0).isNaN shouldBe true
      else aggregatedStdDevOverTime(0) shouldBe Math.sqrt(stdVarWithNaN(data))

      // zscore
      val chunkedItZscore = new ChunkedWindowIteratorD(rv, 160000, 100000, 180000, 100000, new ZScoreChunkedFunctionD(), querySession)
      val aggregatedZscore = chunkedItZscore.map(_.getDouble(1)).toBuffer
      if (aggregatedZscore(0).isNaN) aggregatedZscore(0).isNaN shouldBe true
      else aggregatedZscore(0) shouldBe z_score(data)
    }
  }

  it("should throw QueryTimeoutException when query processing time is greater than timeout") {
    the[QueryTimeoutException] thrownBy {
      val data = Seq(1.5, 2.5, 3.5, 4.5, 5.5)
      val rv = timeValueRV(data)
      val chunkedIt = new ChunkedWindowIteratorD(rv, 100000, 20000, 150000, 30000,
        new ChangesChunkedFunctionD(),
        QuerySession(QueryContext(submitTime = System.currentTimeMillis() - 180000), queryConfig))
      val aggregated = chunkedIt.map(x => (x.getLong(0), x.getDouble(1))).toList
      aggregated shouldEqual List((100000, 0.0), (120000, 2.0), (140000, 2.0))
    } should have message "Query timeout in filodb.core.store.WindowedChunkIterator after 180 seconds"
  }

  it("should return 0 for changes on constant value") {
    val data = List.fill(1000)(1.586365307E9)
    val startTS = 1599071100L
    val tuples = data.zipWithIndex.map { case (d, t) => (startTS + t * 15, d) }
    val rv = timeValueRVPk(tuples)
    val chunkedIt = new ChunkedWindowIteratorD(rv, 1599073500L, 300000,  1599678300L, 10800000,
      new ChangesChunkedFunctionD(), querySession)
    val aggregated = chunkedIt.map(x => (x.getLong(0), x.getDouble(1))).toList
    aggregated.foreach(x => x._2 shouldEqual(0))
  }
}
