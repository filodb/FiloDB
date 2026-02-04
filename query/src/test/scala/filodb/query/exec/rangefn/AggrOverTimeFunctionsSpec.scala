package filodb.query.exec.rangefn

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import com.typesafe.config.ConfigFactory
import debox.Buffer
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import filodb.core.{MetricsTestData, QueryTimeoutException, TestData, MachineMetricsData => MMD}
import filodb.core.memstore.{TimeSeriesPartition, TimeSeriesPartitionSpec, WriteBufferPool}
import filodb.core.query._
import filodb.core.store.AllChunkScan
import filodb.core.MachineMetricsData.defaultPartKey
import filodb.memory._
import filodb.memory.data.ChunkMap
import filodb.memory.format.{TupleRowReader, vectors => bv}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.memory.BinaryRegion.NativePointer
import filodb.memory.format.vectors.MutableHistogram
import filodb.query.exec._

/**
 * A common trait for windowing query tests which uses real chunks and real RawDataRangeVectors
 */
trait RawDataWindowingSpec extends AnyFunSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll {
  import MetricsTestData._

  val evictionLock = new EvictionLock
  private val blockStore = new PageAlignedBlockManager(100 * 1024 * 1024,
    new MemoryStats(Map("test"-> "test")), null, 16, evictionLock)
  val storeConf = TestData.storeConf.copy(maxChunksSize = 200)
  protected val ingestBlockHolder = new BlockMemFactory(blockStore, timeseriesSchema.data.blockMetaSize,
                                      MMD.dummyContext, true)
  protected val tsBufferPool = new WriteBufferPool(TestData.nativeMem,
                                        timeseriesDatasetWithMetric.schema.data, storeConf)

  protected val ingestBlockHolder2 = new BlockMemFactory(blockStore, downsampleSchema.data.blockMetaSize,
                                      MMD.dummyContext, true)
  protected val tsBufferPool2 = new WriteBufferPool(TestData.nativeMem, downsampleSchema.data, storeConf)

  after {
    ChunkMap.validateNoSharedLocks(getClass().toString(), true)
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
  val queryConfig = QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)

  // windowSize and step are in number of elements of the data
  def numWindows(data: Seq[Any], windowSize: Int, step: Int): Int = data.sliding(windowSize, step).length

  // Creates a RawDataRangeVector using Prometheus time-value schema and a given chunk size etc.
  def timeValueRVPk(tuples: Seq[(Long, Double)],
                    partKey: NativePointer = defaultPartKey): RawDataRangeVector = {
    val part = TimeSeriesPartitionSpec.makePart(0, timeseriesDatasetWithMetric, partKey, bufferPool = tsBufferPool)
    val readers = tuples.map { case (ts, d) => TupleRowReader((Some(ts), Some(d))) }
    readers.foreach { row => part.ingest(0, row, ingestBlockHolder, createChunkAtFlushBoundary = false,
      flushIntervalMillis = Option.empty, acceptDuplicateSamples = false) }
    // Now flush and ingest the rest to ensure two separate chunks
    part.switchBuffers(ingestBlockHolder, encode = true)
    // part.encodeAndReleaseBuffers(ingestBlockHolder)
    RawDataRangeVector(null, part, AllChunkScan, Array(0, 1), new AtomicLong(), new AtomicLong(), Long.MaxValue, "query-id")
  }

  def timeValueRvDownsample(tuples: Seq[(Long, Double, Double, Double, Double, Double)],
                            colIds: Array[Int]): RawDataRangeVector = {
    val part = TimeSeriesPartitionSpec.makePart(0, downsampleDataset, bufferPool = tsBufferPool2)
    val readers = tuples.map { case (ts, d1, d2, d3, d4, d5) =>
      TupleRowReader((Some(ts), Some(d1), Some(d2), Some(d3), Some(d4), Some(d5)))
    }
    readers.foreach { row => part.ingest(0, row, ingestBlockHolder2, createChunkAtFlushBoundary = false,
      flushIntervalMillis = Option.empty, acceptDuplicateSamples = false) }
    // Now flush and ingest the rest to ensure two separate chunks
    part.switchBuffers(ingestBlockHolder2, encode = true)
    // part.encodeAndReleaseBuffers(ingestBlockHolder)
    RawDataRangeVector(null, part, AllChunkScan, colIds, new AtomicLong(), new AtomicLong(), Long.MaxValue, "query-id")
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
    readers.foreach { row => part.ingest(0, row, ingestBlockHolder, createChunkAtFlushBoundary = false,
      flushIntervalMillis = Option.empty, acceptDuplicateSamples = false) }
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

  def slidingWindowItH(data: Seq[Seq[Any]],
                       rv: RawDataRangeVector,
                       func: RangeFunction[TransientHistRow],
                       windowSize: Int,
                       step: Int): SlidingWindowIterator[TransientHistRow] = {
    val windowTime = (windowSize.toLong - 1) * pubFreq
    val windowStartTS = defaultStartTS + windowTime
    val stepTimeMillis = step.toLong * pubFreq
    val windowEndTS = windowStartTS + (numWindows(data, windowSize, step) - 1) * stepTimeMillis
    new SlidingWindowIterator(rv.rows(), windowStartTS, stepTimeMillis, windowEndTS, windowTime,
      func, queryConfig, false, new TransientHistRow())
  }

  def chunkedWindowItHist(data: Seq[Seq[Any]],
                          rv: RawDataRangeVector,
                          func: ChunkedRangeFunction[TransientHistRow],
                          windowSize: Int,
                          step: Int): ChunkedWindowIteratorH =
    chunkedWindowItHist(data, rv, func, windowSize, step, new TransientHistRow())

  def slidingWindowIt(data: Seq[Double],
                      rv: RawDataRangeVector,
                      func: RangeFunction[TransientRow],
                      windowSize: Int,
                      step: Int): SlidingWindowIterator[TransientRow] = {
    val windowTime = (windowSize.toLong - 1) * pubFreq
    val windowStartTS = defaultStartTS + windowTime
    val stepTimeMillis = step.toLong * pubFreq
    val windowEndTS = windowStartTS + (numWindows(data, windowSize, step) - 1) * stepTimeMillis
    new SlidingWindowIterator[TransientRow](rv.rows(), windowStartTS, stepTimeMillis, windowEndTS, windowTime,
      func, queryConfig, false, new TransientRow())
  }
}

class AggrOverTimeFunctionsSpec extends RawDataWindowingSpec {
  val rand = new Random()
  val errorOk = 0.0000001

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
      // do not drop first sample because of inclusive start
      aggregated shouldEqual data.sliding(windowSize, step).map(_.sum).toBuffer

      val chunkedIt = chunkedWindowIt(data, rv, new SumOverTimeChunkedFunctionD(), windowSize, step)
      val aggregated2 = chunkedIt.map(_.getDouble(1)).toBuffer
      aggregated2 shouldEqual data.sliding(windowSize, step).map(_.sum).toBuffer
    }
  }

  it("should correctly aggregate sum_over_time for histogram RVs") {
    val (data, rv) = histogramRV(numSamples = 150)
    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(50) + 10
      val step = rand.nextInt(50) + 5
      info(s"iteration $x windowSize=$windowSize step=$step")

      val chunkedIt = chunkedWindowItHist(data, rv, new SumOverTimeChunkedFunctionH(), windowSize, step)
      chunkedIt.zip(data.sliding(windowSize, step)).foreach { case (aggRow, rawDataWindow) =>
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
    val (data, rv) = MMD.histMaxMinRV(defaultStartTS, pubFreq, 150, 8)
    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(50) + 10
      val step = rand.nextInt(50) + 5
      info(s"iteration $x windowSize=$windowSize step=$step")

      val row = new TransientHistMaxMinRow()
      val chunkedIt = chunkedWindowItHist(data, rv, new SumAndMaxOverTimeFuncHD(5), windowSize, step, row)
      chunkedIt.zip(data.sliding(windowSize, step)).foreach { case (aggRow, rawDataWindow) =>
        val aggHist = aggRow.getHistogram(1)
        val sumRawHist = rawDataWindow.map(_(3).asInstanceOf[bv.LongHistogram])
                                      .foldLeft(emptyAggHist) { case (agg, h) => agg.add(h); agg }
        aggHist shouldEqual sumRawHist

        val maxMax = rawDataWindow.map(_(5).asInstanceOf[Double])
                                  .foldLeft(0.0) { case (agg, m) => Math.max(agg, m) }
        aggRow.getDouble(2) shouldEqual maxMax
      }
    }
  }

  // Goal of this is to verify histogram rate functionality for diff time windows.
  // See PeriodicSampleMapperSpec for verifying integration of histograms with min and max
  it("should aggregate both max/min and hist for rate when min/max is in schema") {
    val (data, rv) = MMD.histMaxMinRV(defaultStartTS, pubFreq, 150, 8)
    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(50) + 10
      val step = rand.nextInt(50) + 5
      info(s"iteration $x windowSize=$windowSize step=$step")

      val row = new TransientHistMaxMinRow()
      val chunkedIt = chunkedWindowItHist(data, rv, new DeltaRateAndMinMaxOverTimeFuncHD(5, 4), windowSize, step, row)
      chunkedIt.zip(data.sliding(windowSize, step)).foreach { case (aggRow, rawDataWindow) =>
        val aggHist = aggRow.getHistogram(1)
        val sumRawHist = rawDataWindow.map(_(3).asInstanceOf[bv.LongHistogram])
          .foldLeft(emptyAggHist) { case (agg, h) => agg.add(h); agg }
        aggHist.asInstanceOf[MutableHistogram].values // what we got
          .zip(sumRawHist.values.map(_/(windowSize-1)/10)) // expected
          .foreach { case (a, b) =>
            a shouldEqual b +- 0.0000001
          }

        val maxMax = rawDataWindow.map(_(5).asInstanceOf[Double])
          .foldLeft(0.0) { case (agg, m) => Math.max(agg, m) }
        aggRow.getDouble(2) shouldEqual maxMax

        val minMin = rawDataWindow.map(_(4).asInstanceOf[Double])
          .foldLeft(Double.MaxValue) { case (agg, m) => Math.min(agg, m) }
        aggRow.getDouble(3) shouldEqual minMin

      }
    }
  }

  it("should correctly identify outliers with mad using sliding iterators for all three boundsCheck settings") {
    val data = (0 until 200).map { i =>
      if ((i+1) % 40 == 0) -2.3d
      else if ((i+1) % 20 == 0) 2.3d
      else rand.nextDouble()
    }
    val rv = timeValueRV(data)
    val rangeParams = RangeParams(100, 20, 500)
    val windowSize = 20
    val step = 5

    // only lower bounds
    val lowerAnomalies = data.sliding(windowSize, step).map { k =>
      if (k.last < 0) k.last else Double.NaN
    }.toBuffer

    val minSlidingIt3 = slidingWindowIt(data, rv, new LastOverTimeIsMadOutlierFunction(Seq(StaticFuncArgs(4, rangeParams), StaticFuncArgs(0.0, rangeParams))), windowSize, step)
    val result3 = minSlidingIt3.map(_.getDouble(1)).toBuffer
    minSlidingIt3.close()

    println(s"expected: $lowerAnomalies")
    println(s"result: $result3")
    result3.zip(lowerAnomalies).foreach { case (r, a) =>
      if (a.isNaN) r.isNaN shouldEqual true
      else r shouldEqual a
    }

    // both upper and lower bounds
    val allAnomalies = data.sliding(windowSize, step).map { k =>
      if (k.last > 1.0 || k.last < 0) k.last else Double.NaN
    }.toBuffer

    val minSlidingIt1 = slidingWindowIt(data, rv, new LastOverTimeIsMadOutlierFunction(Seq(StaticFuncArgs(4, rangeParams), StaticFuncArgs(1.0, rangeParams))), windowSize, step)
    val result1 = minSlidingIt1.map(_.getDouble(1)).toBuffer
    minSlidingIt1.close()
    println(s"result1: $result1")
    println(s"allAnomalies: $allAnomalies")
    result1.zip(allAnomalies).foreach { case (r, a) =>
      if (a.isNaN) r.isNaN shouldEqual true
      else r shouldEqual a
    }

    // only upper bounds
    val upperAnomalies = data.sliding(windowSize, step).map { k =>
      if (k.last > 1.0) k.last else Double.NaN
    }.toBuffer

    val minSlidingIt2 = slidingWindowIt(data, rv, new LastOverTimeIsMadOutlierFunction(Seq(StaticFuncArgs(4, rangeParams), StaticFuncArgs(2.0, rangeParams))), windowSize, step)
    val result2 = minSlidingIt2.map(_.getDouble(1)).toBuffer
    minSlidingIt2.close()
    result2.zip(upperAnomalies).foreach { case (r, a) =>
      if (a.isNaN) r.isNaN shouldEqual true
      else r shouldEqual a
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
      // do not drop first sample because of inclusive start
      aggregated shouldEqual data.sliding(windowSize, step).map(_.min).toBuffer

      val minChunkedIt = chunkedWindowIt(data, rv, new MinOverTimeChunkedFunctionD(), windowSize, step)
      val aggregated2 = minChunkedIt.map(_.getDouble(1)).toBuffer
      aggregated2 shouldEqual data.sliding(windowSize, step).map(_.min).toBuffer

      val maxSlidingIt = slidingWindowIt(data, rv, new MinMaxOverTimeFunction(Ordering[Double]), windowSize, step)
      val aggregated3 = maxSlidingIt.map(_.getDouble(1)).toBuffer
      maxSlidingIt.close()
      // do not drop first sample because of inclusive start
      aggregated3 shouldEqual data.sliding(windowSize, step).map(_.max).toBuffer

      val maxChunkedIt = chunkedWindowIt(data, rv, new MaxOverTimeChunkedFunctionD(), windowSize, step)
      val aggregated4 = maxChunkedIt.map(_.getDouble(1)).toBuffer
      aggregated4 shouldEqual data.sliding(windowSize, step).map(_.max).toBuffer
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
      aggregated1 shouldEqual data.sliding(windowSize, step).map(_.length).toBuffer

      val countChunked = chunkedWindowIt(data, rv, new CountOverTimeChunkedFunction(), windowSize, step)
      val aggregated2 = countChunked.map(_.getDouble(1)).toBuffer
      aggregated2 shouldEqual data.sliding(windowSize, step).map(_.length).toBuffer

      val avgSliding = slidingWindowIt(data, rv, new AvgOverTimeFunction(), windowSize, step)
      val aggregated3 = avgSliding.map(_.getDouble(1)).toBuffer
      avgSliding.close()
      aggregated3 shouldEqual data.sliding(windowSize, step).map(a => avg(a)).toBuffer

      // In sample_data2, there are no NaN's, that's why using avg function is fine
      val avgChunked = chunkedWindowIt(data, rv, new AvgOverTimeChunkedFunctionD(), windowSize, step)
      val aggregated4 = avgChunked.map(_.getDouble(1)).toBuffer
      aggregated4 shouldEqual data.sliding(windowSize, step).map(a => avg(a)).toBuffer

      val changesChunked = chunkedWindowIt(data, rv, new ChangesChunkedFunctionD(), windowSize, step)
      val aggregated5 = changesChunked.map(_.getDouble(1)).toBuffer
      aggregated5 shouldEqual data.sliding(windowSize, step).map(_.length - 1).toBuffer
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
      aggregated2 shouldEqual data.sliding(windowSize, step).map(a => stdVar(a)).toBuffer

      val stdDevSlidingIt = slidingWindowIt(data, rv, new StdDevOverTimeFunction(), windowSize, step)
      val aggregated3 = stdDevSlidingIt.map(_.getDouble(1)).toBuffer
      stdDevSlidingIt.close()
      aggregated3 shouldEqual data.sliding(windowSize, step).map(d => Math.sqrt(stdVar(d))).toBuffer

      val varFunc = new StdVarOverTimeChunkedFunctionD()
      val windowIt4 = chunkedWindowIt(data, rv, varFunc, windowSize, step)
      val aggregated4 = windowIt4.map(_.getDouble(1)).toBuffer
      aggregated4 shouldEqual data.sliding(windowSize, step).map(a => stdVar(a)).toBuffer

      val devFunc = new StdDevOverTimeChunkedFunctionD()
      val windowIt5 = chunkedWindowIt(data, rv, devFunc, windowSize, step)
      val aggregated5 = windowIt5.map(_.getDouble(1)).toBuffer
      aggregated5 shouldEqual data.sliding(windowSize, step).map(d => Math.sqrt(stdVar(d))).toBuffer
    }
  }

  it("should correctly do changes") {
    var data = Seq(1.5, 2.5, 3.5, 4.5, 5.5)
    val rv = timeValueRV(data)
    val list = rv.rows().map(x => (x.getLong(0), x.getDouble(1))).toList

    val windowSize = 100
    val step = 20

    val chunkedIt = new ChunkedWindowIteratorD(rv, 100000, 20000, 150000, 30000,
      new ChangesChunkedFunctionD(), querySession)
    val aggregated = chunkedIt.map(x => (x.getLong(0), x.getDouble(1))).toList
    aggregated shouldEqual List((100000, 0.0), (120000, 2.0), (140000, 3.0))
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
      aggregated2 shouldEqual data.sliding(windowSize, step).map(median).toBuffer
    }
  }

  it("should correctly calculate medianabsolutedeviationovertime") {
    val sampleData = Seq(9.0, 6.0, 4.0, 1.0, 1.0, 2.0, 2.0)
    var rv = timeValueRV(sampleData)
    val chunkedItSample = new ChunkedWindowIteratorD(rv, 170000, 10000, 170000, 100000,
        new MedianAbsoluteDeviationOverTimeChunkedFunctionD(), querySession)
    val aggregated2 = chunkedItSample.map(_.getDouble(1)).toBuffer
      aggregated2(0) shouldEqual 1.0 +- 0.0000000001

    val emptyData = Seq()
    rv = timeValueRVPk(emptyData)
    val chunkedItNoSample = new ChunkedWindowIteratorD(rv, 110000, 120000, 150000, 30000,
      new MedianAbsoluteDeviationOverTimeChunkedFunctionD(), querySession)
    val aggregatedEmpty = chunkedItNoSample.map(_.getDouble(1)).toBuffer
    aggregatedEmpty(0) isNaN

    def median(s: Seq[Double]): Double = {
      val (lower, upper) = s.sortWith(_ < _).splitAt(s.size / 2)
      if (s.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head
    }

    def mad(s: Seq[Double]) : Double = {
      val medianVal = median(s)
      val diffFromMedians: Buffer[Double] = Buffer.ofSize(s.length)
      var iter = s.iterator
      while (iter.hasNext) {
        diffFromMedians.append(Math.abs(iter.next()-medianVal))
      }
      diffFromMedians.sort(spire.algebra.Order.fromOrdering[Double])
      val (weight, upperIndex, lowerIndex) = QuantileOverTimeFunction.calculateRank(0.5, diffFromMedians.length)
      iter = diffFromMedians.iterator
      diffFromMedians(lowerIndex) * (1 - weight) + diffFromMedians(upperIndex) * weight
    }

    val data = (1 to 500).map(_.toDouble)
    val rv2 = timeValueRV(data)
    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(100) + 10
      val step = rand.nextInt(50) + 5
      info(s"iteration $x windowSize=$windowSize step=$step")

      val minChunkedIt = chunkedWindowIt(data, rv2, new MedianAbsoluteDeviationOverTimeChunkedFunctionD
      (), windowSize, step)

      val aggregated2 = minChunkedIt.map(_.getDouble(1)).toBuffer
      aggregated2 shouldEqual data.sliding(windowSize, step).map(mad).toBuffer
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
      val list = rv.rows().map(x => (x.getLong(0), x.getDouble(1))).toList

      val stepTimeMillis = step.toLong * pubFreq
      val changesChunked = chunkedWindowIt(data, rv, new ChangesChunkedFunctionD(), windowSize, step)
      val aggregated2 = changesChunked.map(_.getDouble(1)).toBuffer

      data.sliding(windowSize, step).map(_.length - 1).toBuffer.zip(aggregated2).foreach {
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
      aggregated shouldEqual data.sliding(windowSize, step).map(d => z_score(d)).toBuffer
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

      // present over time
      val chunkedItPresentOverTime = new ChunkedWindowIteratorD(rv, 160000, 100000, 180000, 100000, new PresentOverTimeChunkedFunctionD(), querySession)
      val aggregatedPresentOverTime = chunkedItPresentOverTime.map(_.getDouble(1)).toBuffer
      if (aggregatedPresentOverTime(0).isNaN) aggregatedPresentOverTime(0).isNaN shouldBe true
      else aggregatedPresentOverTime(0) shouldBe 1
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
  it("should correctly aggregate sum_over_time for histogram RVs using sliding window iterator") {
    val (data, rv) = histogramRV(numSamples = 150)
    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(50) + 10
      val step = rand.nextInt(50) + 5
      info(s"iteration $x windowSize=$windowSize step=$step")

      val slidingIt = slidingWindowItH(data, rv, new SumOverTimeFunctionH(), windowSize, step)
      slidingIt.zip(data.sliding(windowSize, step)).foreach { case (aggRow, rawDataWindow) =>
        val aggHist = aggRow.getHistogram(1)
        val sumRawHist = rawDataWindow.map(_(3).asInstanceOf[bv.LongHistogram])
          .foldLeft(emptyAggHist) { case (agg, h) => agg.add(h); agg }
        aggHist shouldEqual sumRawHist
      }
      slidingIt.close()
    }
  }

  it("should handle edge cases for SumOverTimeFunctionH") {
    // Test with single histogram
    val (singleData, singleRv) = histogramRV(numSamples = 1)
    val singleIt = slidingWindowItH(singleData, singleRv, new SumOverTimeFunctionH(), 1, 1)
    val singleResult = singleIt.next()
    val expectedSingle = singleData.head(3).asInstanceOf[bv.LongHistogram]
    singleResult.getHistogram(1) shouldEqual expectedSingle
    singleIt.close()
  }

  it("should correctly handle window removal for SumOverTimeFunctionH") {
    val (data, rv) = histogramRV(numSamples = 50)
    val windowSize = 10
    val step = 1

    val slidingIt = slidingWindowItH(data, rv, new SumOverTimeFunctionH(), windowSize, step)

    // Verify that each result matches the expected sum for that window
    slidingIt.zip(data.sliding(windowSize, step)).foreach { case (aggRow, rawDataWindow) =>
      val aggHist = aggRow.getHistogram(1)
      val expectedHist = rawDataWindow.map(_(3).asInstanceOf[bv.LongHistogram])
        .foldLeft(emptyAggHist) { case (agg, h) => agg.add(h); agg }
      aggHist shouldEqual expectedHist
    }
  }

  it("should handle histograms with different bucket sizes for SumOverTimeFunctionH") {
    // Test with different bucket configurations
    val bucketSizes = Seq(4, 8, 16)
    bucketSizes.foreach { numBuckets =>
      val (data, rv) = histogramRV(numSamples = 30, numBuckets = numBuckets)
      val windowSize = 5
      val step = 2

      val slidingIt = slidingWindowItH(data, rv, new SumOverTimeFunctionH(), windowSize, step)
      slidingIt.zip(data.sliding(windowSize, step)).foreach { case (aggRow, rawDataWindow) =>
        val aggHist = aggRow.getHistogram(1)
        val expectedHist = rawDataWindow.map(_(3).asInstanceOf[bv.LongHistogram])
          .foldLeft(emptyAggHist) { case (agg, h) => agg.add(h); agg }
        aggHist shouldEqual expectedHist
        aggHist.numBuckets shouldEqual numBuckets
      }
      slidingIt.close()
    }
  }

  it("should produce same results as SumOverTimeChunkedFunctionH for identical data") {
    val (data, rv1) = histogramRV(numSamples = 100)
    val (_, rv2) = histogramRV(numSamples = 100) // Create second RV with same data

    val windowSize = 20
    val step = 10

    // Test sliding window function
    val slidingIt = slidingWindowItH(data, rv1, new SumOverTimeFunctionH(), windowSize, step)
    // Test chunked function
    val chunkedIt = chunkedWindowItHist(data, rv2, new SumOverTimeChunkedFunctionH(), windowSize, step)
    val chunkedResults = chunkedIt.map(_.getHistogram(1)).toList
    // Results should be identical
    slidingIt.zip(chunkedResults.iterator).foreach { case (sliding, chunked) =>
      sliding.getHistogram(1) shouldEqual chunked
    }
  }

  it("AvgOverDeltaFunctionH should calculate average of delta histogram values") {
    import filodb.query.util.IndexedArrayQueue
    import filodb.query.exec.QueueBasedWindow
    
    val buckets = bv.GeometricBuckets(2.0, 2.0, 4)
    
    // Create histogram samples with different values for averaging
    val histSamples = Seq(
      8072000L -> Array(100.0, 200.0, 300.0, 400.0),   // First sample
      8082100L -> Array(50.0, 150.0, 250.0, 350.0),    // Second sample
      8092196L -> Array(150.0, 250.0, 350.0, 450.0)    // Third sample
    )
    
    val qHist = new IndexedArrayQueue[TransientHistRow]()
    val avgOverDeltaFunc = new AvgOverDeltaFunctionH()
    val histWindow = new QueueBasedWindow(qHist)
    
    histSamples.foreach { case (t, bucketValues) =>
      val hist = bv.MutableHistogram(buckets, bucketValues)
      val row = new TransientHistRow(t, hist)
      qHist.add(row)
      avgOverDeltaFunc.addedToWindow(row, histWindow)
    }
    
    val startTs = 8071950L
    val endTs = 8092250L
    val toEmit = new TransientHistRow
    
    avgOverDeltaFunc.apply(startTs, endTs, histWindow, toEmit, queryConfig)
    
    val result = toEmit.value
    result.numBuckets shouldEqual 4
    
    // Expected averages: sum/count = [300, 600, 900, 1200]/3 = [100, 200, 300, 400]
    val expectedAvgs = Array(100.0, 200.0, 300.0, 400.0)
    for (b <- 0 until result.numBuckets) {
      result.bucketValue(b) shouldEqual expectedAvgs(b) +- errorOk
    }
  }

  it("AvgOverDeltaFunctionH should handle empty window") {
    import filodb.query.util.IndexedArrayQueue
    import filodb.query.exec.QueueBasedWindow
    
    val qHist = new IndexedArrayQueue[TransientHistRow]()
    val avgOverDeltaFunc = new AvgOverDeltaFunctionH()
    val histWindow = new QueueBasedWindow(qHist)
    
    val startTs = 8071950L
    val endTs = 8083070L
    val toEmit = new TransientHistRow
    
    avgOverDeltaFunc.apply(startTs, endTs, histWindow, toEmit, queryConfig)
    
    // Should return empty histogram for empty window
    toEmit.value shouldEqual bv.HistogramWithBuckets.empty
  }

  it("AvgOverDeltaFunctionH should handle single histogram sample") {
    import filodb.query.util.IndexedArrayQueue
    import filodb.query.exec.QueueBasedWindow
    
    val buckets = bv.GeometricBuckets(2.0, 2.0, 3)
    val qHist = new IndexedArrayQueue[TransientHistRow]()
    val avgOverDeltaFunc = new AvgOverDeltaFunctionH()
    val histWindow = new QueueBasedWindow(qHist)
    
    // Add single sample
    val hist = bv.MutableHistogram(buckets, Array(100.0, 200.0, 300.0))
    val row = new TransientHistRow(8072000L, hist)
    qHist.add(row)
    avgOverDeltaFunc.addedToWindow(row, histWindow)
    
    val startTs = 8071950L
    val endTs = 8083070L
    val toEmit = new TransientHistRow
    
    avgOverDeltaFunc.apply(startTs, endTs, histWindow, toEmit, queryConfig)
    
    val result = toEmit.value
    result.numBuckets shouldEqual 3
    
    // Should return same values as input (average of single value is the value itself)
    for (b <- 0 until result.numBuckets) {
      result.bucketValue(b) shouldEqual hist.bucketValue(b) +- errorOk
    }
  }

  it("SumAndMaxOverTimeFunctionHD should correctly track sum and max with sliding window") {
    import filodb.query.util.IndexedArrayQueue
    import filodb.query.exec.QueueBasedWindow
    
    val buckets = bv.GeometricBuckets(2.0, 2.0, 3)
    val qHist = new IndexedArrayQueue[TransientHistRow]()
    val sumMaxFunc = new SumAndMaxOverTimeFunctionHD()
    val histWindow = new QueueBasedWindow(qHist)
    
    // Create histogram samples with different max values
    val histSamples = Seq(
      (100000L, Array(10.0, 20.0, 30.0), 5.5),   // timestamp, histogram values, max value
      (110000L, Array(15.0, 25.0, 35.0), 8.2),
      (120000L, Array(5.0, 15.0, 25.0), 3.1),
      (130000L, Array(20.0, 30.0, 40.0), 9.7)
    )
    
    // Add samples to window
    histSamples.foreach { case (ts, bucketValues, maxVal) =>
      val hist = bv.MutableHistogram(buckets, bucketValues)
      val row = new TransientHistMaxMinRow()
      row.setValues(ts, hist)
      row.setDouble(2, maxVal)  // Set max value at index 2
      qHist.add(row)
      sumMaxFunc.addedToWindow(row, histWindow)
    }
    
    val toEmit = new TransientHistMaxMinRow
    sumMaxFunc.apply(100000L, 130000L, histWindow, toEmit, queryConfig)
    
    // Check histogram sum
    val resultHist = toEmit.value
    resultHist.numBuckets shouldEqual 3
    val expectedSum = Array(50.0, 90.0, 130.0)  // Sum of all bucket values
    for (b <- 0 until resultHist.numBuckets) {
      resultHist.bucketValue(b) shouldEqual expectedSum(b) +- errorOk
    }
    
    // Check max value (should be 9.7, the highest among all max values)
    toEmit.getDouble(2) shouldEqual 9.7 +- errorOk
  }

  it("SumAndMaxOverTimeFunctionHD should handle window removal correctly") {
    import filodb.query.util.IndexedArrayQueue
    import filodb.query.exec.QueueBasedWindow
    
    val buckets = bv.GeometricBuckets(2.0, 2.0, 3)
    val qHist = new IndexedArrayQueue[TransientHistRow]()
    val sumMaxFunc = new SumAndMaxOverTimeFunctionHD()
    val histWindow = new QueueBasedWindow(qHist)
    
    // Add first sample with highest max
    val hist1 = bv.MutableHistogram(buckets, Array(10.0, 20.0, 30.0))
    val row1 = new TransientHistMaxMinRow(10)
    row1.setValues(100000L, hist1)
    qHist.add(row1)
    sumMaxFunc.addedToWindow(row1, histWindow)
    
    // Add second sample with lower max
    val hist2 = bv.MutableHistogram(buckets, Array(5.0, 10.0, 15.0))
    val row2 = new TransientHistMaxMinRow(7.5)
    row2.setValues(110000L, hist2)
    qHist.add(row2)
    sumMaxFunc.addedToWindow(row2, histWindow)
    
    // Verify initial state
    val toEmit1 = new TransientHistMaxMinRow()
    sumMaxFunc.apply(100000L, 110000L, histWindow, toEmit1, queryConfig)
    toEmit1.getDouble(2) shouldEqual 10.0 +- errorOk  // Should have highest max
    
    // Remove the first sample (with highest max)
    qHist.remove()
    sumMaxFunc.removedFromWindow(row1, histWindow)
    
    // Verify max is updated after removal
    val toEmit2 = new TransientHistMaxMinRow
    sumMaxFunc.apply(110000L, 110000L, histWindow, toEmit2, queryConfig)
    toEmit2.getDouble(2) shouldEqual 7.5 +- errorOk  // Should now have the remaining max
  }

  it("SumAndMaxOverTimeFunctionHD should handle NaN max values") {
    import filodb.query.util.IndexedArrayQueue
    import filodb.query.exec.QueueBasedWindow
    
    val buckets = bv.GeometricBuckets(2.0, 2.0, 3)
    val qHist = new IndexedArrayQueue[TransientHistRow]()
    val sumMaxFunc = new SumAndMaxOverTimeFunctionHD()
    val histWindow = new QueueBasedWindow(qHist)
    
    // Add sample with NaN max value (should be ignored)
    val hist1 = bv.MutableHistogram(buckets, Array(10.0, 20.0, 30.0))
    val row1 = new TransientHistMaxMinRow(Double.NaN)
    row1.setValues(100000L, hist1)
    qHist.add(row1)
    sumMaxFunc.addedToWindow(row1, histWindow)
    
    // Add sample with valid max value
    val hist2 = bv.MutableHistogram(buckets, Array(5.0, 10.0, 15.0))
    val row2 = new TransientHistMaxMinRow(6.5)
    row2.setValues(110000L, hist2)
    qHist.add(row2)
    sumMaxFunc.addedToWindow(row2, histWindow)
    
    val toEmit = new TransientHistMaxMinRow
    sumMaxFunc.apply(100000L, 110000L, histWindow, toEmit, queryConfig)
    
    // Should ignore NaN and return the valid max value
    toEmit.getDouble(2) shouldEqual 6.5 +- errorOk
    
    // Histogram sum should still work correctly (NaN doesn't affect histogram aggregation)
    val resultHist = toEmit.value
    val expectedSum = Array(15.0, 30.0, 45.0)  // Sum of both histograms
    for (b <- 0 until resultHist.numBuckets) {
      resultHist.bucketValue(b) shouldEqual expectedSum(b) +- errorOk
    }
  }

  it("RateAndMaxMinOverTimeFunctionHD should correctly calculate rate and track max/min") {
    import filodb.query.util.IndexedArrayQueue
    import filodb.query.exec.QueueBasedWindow
    
    val buckets = bv.GeometricBuckets(2.0, 2.0, 3)
    val qHist = new IndexedArrayQueue[TransientHistRow]()
    val rateMaxMinFunc = new RateAndMaxMinOverTimeFunctionHD()
    val histWindow = new QueueBasedWindow(qHist)
    
    // Create delta histogram samples with min/max values
    val histSamples = Seq(
      (100000L, Array(10.0, 20.0, 30.0), 8.5, 2.1),   // timestamp, histogram values, max, min
      (110000L, Array(15.0, 25.0, 35.0), 12.3, 1.8),
      (120000L, Array(5.0, 15.0, 25.0), 6.7, 3.2),
      (130000L, Array(20.0, 30.0, 40.0), 15.4, 1.5)
    )
    
    // Add samples to window
    histSamples.foreach { case (ts, bucketValues, maxVal, minVal) =>
      val hist = bv.MutableHistogram(buckets, bucketValues)
      val row = new TransientHistMaxMinRow(maxVal, minVal)
      row.setValues(ts, hist)
      qHist.add(row)
      rateMaxMinFunc.addedToWindow(row, histWindow)
    }
    
    val startTs = 99000L
    val endTs = 131000L
    val timeDelta = endTs - startTs  // 32000ms = 32 seconds
    val toEmit = new TransientHistMaxMinRow
    
    rateMaxMinFunc.apply(startTs, endTs, histWindow, toEmit, queryConfig)
    
    // Check histogram rate calculation
    val resultHist = toEmit.value
    resultHist.numBuckets shouldEqual 3
    val expectedSum = Array(50.0, 90.0, 130.0)  // Sum of all bucket values
    for (b <- 0 until resultHist.numBuckets) {
      val expectedRate = expectedSum(b) / (timeDelta / 1000.0)
      resultHist.bucketValue(b) shouldEqual expectedRate +- errorOk
    }
    
    // Check max value (should be 15.4, the highest among all max values)
    toEmit.getDouble(2) shouldEqual 15.4 +- errorOk
    
    // Check min value (should be 1.5, the lowest among all min values)
    toEmit.getDouble(3) shouldEqual 1.5 +- errorOk
  }

  it("RateAndMaxMinOverTimeFunctionHD should handle window removal correctly") {
    import filodb.query.util.IndexedArrayQueue
    import filodb.query.exec.QueueBasedWindow
    
    val buckets = bv.GeometricBuckets(2.0, 2.0, 3)
    val qHist = new IndexedArrayQueue[TransientHistRow]()
    val rateMaxMinFunc = new RateAndMaxMinOverTimeFunctionHD()
    val histWindow = new QueueBasedWindow(qHist)
    
    // Add first sample with extreme max and min values
    val hist1 = bv.MutableHistogram(buckets, Array(10.0, 20.0, 30.0))
    val row1 = new TransientHistMaxMinRow(20.0, 1.0)  // highest max, lowest min
    row1.setValues(100000L, hist1)
    qHist.add(row1)
    rateMaxMinFunc.addedToWindow(row1, histWindow)
    
    // Add second sample with moderate values
    val hist2 = bv.MutableHistogram(buckets, Array(5.0, 10.0, 15.0))
    val row2 = new TransientHistMaxMinRow(12.5, 5.5)
    row2.setValues(110000L, hist2)
    qHist.add(row2)
    rateMaxMinFunc.addedToWindow(row2, histWindow)
    
    // Add third sample with different values
    val hist3 = bv.MutableHistogram(buckets, Array(8.0, 12.0, 18.0))
    val row3 = new TransientHistMaxMinRow(15.0, 3.0)
    row3.setValues(120000L, hist3)
    qHist.add(row3)
    rateMaxMinFunc.addedToWindow(row3, histWindow)
    
    // Verify initial state (all three samples)
    val toEmit1 = new TransientHistMaxMinRow
    rateMaxMinFunc.apply(99000L, 121000L, histWindow, toEmit1, queryConfig)
    toEmit1.getDouble(2) shouldEqual 20.0 +- errorOk  // Should have highest max
    toEmit1.getDouble(3) shouldEqual 1.0 +- errorOk   // Should have lowest min
    
    // Remove the first sample (with extreme values)
    qHist.remove()
    rateMaxMinFunc.removedFromWindow(row1, histWindow)
    
    // Verify values are updated after removal
    val toEmit2 = new TransientHistMaxMinRow
    rateMaxMinFunc.apply(110000L, 121000L, histWindow, toEmit2, queryConfig)
    toEmit2.getDouble(2) shouldEqual 15.0 +- errorOk  // Should now have second highest max
    toEmit2.getDouble(3) shouldEqual 3.0 +- errorOk   // Should now have second lowest min
  }

  it("RateAndMaxMinOverTimeFunctionHD should handle NaN max/min values") {
    import filodb.query.util.IndexedArrayQueue
    import filodb.query.exec.QueueBasedWindow
    
    val buckets = bv.GeometricBuckets(2.0, 2.0, 3)
    val qHist = new IndexedArrayQueue[TransientHistRow]()
    val rateMaxMinFunc = new RateAndMaxMinOverTimeFunctionHD()
    val histWindow = new QueueBasedWindow(qHist)
    
    // Add sample with NaN max/min values (should be ignored)
    val hist1 = bv.MutableHistogram(buckets, Array(10.0, 20.0, 30.0))
    val row1 = new TransientHistMaxMinRow(Double.NaN, Double.NaN)
    row1.setValues(100000L, hist1)
    qHist.add(row1)
    rateMaxMinFunc.addedToWindow(row1, histWindow)
    
    // Add sample with valid max/min values
    val hist2 = bv.MutableHistogram(buckets, Array(5.0, 10.0, 15.0))
    val row2 = new TransientHistMaxMinRow(8.5, 2.3)
    row2.setValues(110000L, hist2)
    qHist.add(row2)
    rateMaxMinFunc.addedToWindow(row2, histWindow)
    
    // Add another sample with valid values
    val hist3 = bv.MutableHistogram(buckets, Array(8.0, 12.0, 18.0))
    val row3 = new TransientHistMaxMinRow(6.2, 4.1)
    row3.setValues(120000L, hist3)
    qHist.add(row3)
    rateMaxMinFunc.addedToWindow(row3, histWindow)
    
    val startTs = 99000L
    val endTs = 121000L
    val toEmit = new TransientHistMaxMinRow
    
    rateMaxMinFunc.apply(startTs, endTs, histWindow, toEmit, queryConfig)
    
    // Should ignore NaN and track valid max/min values
    toEmit.getDouble(2) shouldEqual 8.5 +- errorOk  // Max of valid values
    toEmit.getDouble(3) shouldEqual 2.3 +- errorOk  // Min of valid values
    
    // Histogram rate calculation should still work correctly
    val resultHist = toEmit.value
    val expectedSum = Array(23.0, 42.0, 63.0)  // Sum of all histograms
    val timeDelta = endTs - startTs
    for (b <- 0 until resultHist.numBuckets) {
      val expectedRate = expectedSum(b) / (timeDelta / 1000.0)
      resultHist.bucketValue(b) shouldEqual expectedRate +- errorOk
    }
  }

  it("should compute rate for cumulative histogram with min/max using CumulativeHistRateAndMinMaxFunction") {
    // Create cumulative histogram data with max/min (similar to OTEL cumulative histograms)
    // Generate 10 samples starting at 100000L with 10000L frequency
    val (data, rv) = MMD.cumulativeHistMaxMinRV(100000L, pubFreq = 10000L, numSamples = 10, numBuckets = 8)

    // Define window boundaries - use samples 0 through 6 (7 samples total)
    val startTs = 99500L      // Just before first sample at 100000L
    val endTs = 161000L       // Just past 7th sample at 160000L
    val headTime = 100000L    // First sample timestamp
    val lastTime = 160000L    // Last (7th) sample timestamp

    // Extract first and last histograms from data
    val headHist = data(0)(3).asInstanceOf[bv.LongHistogram]
    val lastHist = data(6)(3).asInstanceOf[bv.LongHistogram]

    // Calculate expected rates: (last - first) / timeDelta
    // Note: For cumulative histograms, values always increase
    val timeDelta = (lastTime - headTime) / 1000.0  // Convert to seconds
    val expectedRates = (0 until headHist.numBuckets).map { b =>
      (lastHist.bucketValue(b).toDouble - headHist.bucketValue(b)) / timeDelta
    }
    val expectedHist = bv.MutableHistogram(MMD.histBucketScheme, expectedRates.toArray)

    // Max and min are constant across all samples (realistic OTEL-style observed values)
    // Values match the bucket scheme: GeometricBuckets(2.0, 2.0, 8) has tops [2, 4, 8, 16, 32, 64, 128, 256]
    val expectedMax = 200.0  // In bucket 7: 128 < 200 <= 256
    val expectedMin = 1.0    // In bucket 0: 0 < 1 <= 2

    // Create ONE window iterator with the specified range
    val row = new TransientHistMaxMinRow()
    val it = new ChunkedWindowIteratorH(rv, endTs, 100000, endTs, endTs - startTs,
                 new CumulativeHistRateAndMinMaxFunction(5, 4).asInstanceOf[ChunkedRangeFunction[TransientHistRow]],
                 querySession, row)

    // Get the result
    val answer = it.next
    val answerHist = answer.getHistogram(1)

    // Verify histogram structure
    answerHist.numBuckets shouldEqual expectedHist.numBuckets

    // Compare each bucket rate with tolerance
    for { b <- 0 until expectedHist.numBuckets } {
      answerHist.bucketTop(b) shouldEqual expectedHist.bucketTop(b)
      answerHist.bucketValue(b) shouldEqual expectedHist.bucketValue(b) +- errorOk
    }

    // Verify max and min aggregations
    answer.getDouble(2) shouldEqual expectedMax +- errorOk
    answer.getDouble(3) shouldEqual expectedMin +- errorOk
  }

  it("should handle counter resets in cumulative histograms correctly") {
    // Test that CumulativeHistRateAndMinMaxFunction properly handles counter resets
    // by using the counter correction mechanism
    val (initialData, rv) = MMD.cumulativeHistMaxMinRV(100000L, pubFreq=10000L, numSamples=7)

    // Inject samples that simulate a counter reset (values go back to low numbers)
    val part = rv.partition.asInstanceOf[TimeSeriesPartition]
    val resetData = initialData.take(3).map { d =>
      // Create new timestamp (70000ms later) but with reset histogram values
      val originalSeq = d.asInstanceOf[Seq[Any]]
      val newTime = originalSeq(0).asInstanceOf[Long] + 70000L
      Seq(newTime) ++ originalSeq.drop(1)
    }

    val container = MMD.records(MMD.cumulativeHistMaxMinDS, resetData).records
    val bh = MMD.cumulativeHistMaxMinBH
    container.iterate(MMD.cumulativeHistMaxMinDS.ingestionSchema).foreach { row =>
      part.ingest(0, row, bh, createChunkAtFlushBoundary = false,
                  flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
    }
    part.switchBuffers(bh, encode = true)

    val startTs = 99500L
    val endTs = 171000L  // Past the reset point
    val windowTime = endTs - startTs

    val row = new TransientHistMaxMinRow()
    val it = new ChunkedWindowIteratorH(rv, endTs, 110000, endTs, windowTime,
                                        new CumulativeHistRateAndMinMaxFunction(5, 4).asInstanceOf[ChunkedRangeFunction[TransientHistRow]], querySession, row)

    // Should handle the reset and produce valid rate
    val result = it.next
    val resultHist = result.getHistogram(1)

    // Verify histogram rate is calculated correctly despite reset
    // The counter correction should have been applied
    resultHist.numBuckets shouldEqual MMD.histBucketScheme.numBuckets

    // All bucket rates should be non-negative (no negative rates from reset)
    for (b <- 0 until resultHist.numBuckets) {
      resultHist.bucketValue(b) should be >= 0.0
    }

    // Max and min should be the constant values from histMaxMin
    // Even with counter resets, the max/min fields remain constant across all samples
    result.getDouble(2) shouldEqual 200.0 +- errorOk  // Max
    result.getDouble(3) shouldEqual 1.0 +- errorOk    // Min
  }

  it("should produce same results as composed functions for cumulative histograms") {
    // Verify that CumulativeHistRateAndMinMaxFunction composition produces correct results
    val (data, rv) = MMD.cumulativeHistMaxMinRV(defaultStartTS, pubFreq, 50, 8)

    val windowSize = 20
    val step = 5

    val row = new TransientHistMaxMinRow()
    val chunkedIt = chunkedWindowItHist(data, rv, new CumulativeHistRateAndMinMaxFunction(5, 4), windowSize, step, row)

    var windowCount = 0
    chunkedIt.foreach { aggRow =>
      windowCount += 1

      // Verify all fields are populated
      aggRow.getLong(0) should be > 0L  // Timestamp

      val hist = aggRow.getHistogram(1)
      hist.numBuckets shouldEqual 8  // Should match input histogram

      // Max and min should be the constant values from histMaxMin
      val maxVal = aggRow.getDouble(2)
      val minVal = aggRow.getDouble(3)

      maxVal shouldEqual 200.0 +- errorOk  // Constant max across all samples
      minVal shouldEqual 1.0 +- errorOk    // Constant min across all samples
    }

    // Should have produced multiple windows
    windowCount should be > 0
  }

  it("should handle empty windows gracefully for cumulative histograms") {
    // Test edge case with very small dataset
    val (data, rv) = MMD.cumulativeHistMaxMinRV(defaultStartTS, pubFreq, numSamples = 2)

    val windowSize = 10
    val step = 5

    val row = new TransientHistMaxMinRow()
    val chunkedIt = chunkedWindowItHist(data, rv, new CumulativeHistRateAndMinMaxFunction(5, 4), windowSize, step, row)

    // Should handle gracefully even with insufficient data
    val results = chunkedIt.toList
    results.foreach { result =>
      // Result should be valid even if limited data
      result.getLong(0) should be > 0L
    }
  }

  it("should compute histogram_quantile on rate of cumulative histogram with min/max") {
    // Create cumulative histogram with min/max - use samples 0 through 6 (7 samples)
    val (data, rv) = MMD.cumulativeHistMaxMinRV(100000L, pubFreq = 10000L, numSamples = 10, numBuckets = 8)

    // Define window for rate calculation
    val startTs = 99500L
    val endTs = 161000L
    val windowTime = endTs - startTs
    val headTime = 100000L    // First sample timestamp
    val lastTime = 160000L    // Last (7th) sample timestamp

    // Extract first and last histograms from data to calculate expected rate
    val headHist = data(0)(3).asInstanceOf[bv.LongHistogram]
    val lastHist = data(6)(3).asInstanceOf[bv.LongHistogram]

    // Calculate expected rate histogram: (last - first) / timeDelta * 1000
    val timeDelta = (lastTime - headTime) / 1000.0
    val expectedRates = (0 until headHist.numBuckets).map { b =>
      (lastHist.bucketValue(b).toDouble - headHist.bucketValue(b)) / timeDelta
    }
    val expectedRateHist = bv.MutableHistogram(MMD.histBucketScheme, expectedRates.toArray)

    // Calculate rate using CumulativeHistRateAndMinMaxFunction
    val row = new TransientHistMaxMinRow()
    val it = new ChunkedWindowIteratorH(rv, endTs, 100000, endTs, windowTime,
                 new CumulativeHistRateAndMinMaxFunction(5, 4).asInstanceOf[ChunkedRangeFunction[TransientHistRow]],
                 querySession, row)

    // Collect rate histogram results
    val rateResults = it.toList
    rateResults.size shouldEqual 1

    val rateRow = rateResults.head
    val rateHist = rateRow.getHistogram(1)

    // Verify histogram structure
    rateHist.numBuckets shouldEqual 8
    // Verify bucket scheme matches by comparing bucket tops
    for (i <- 0 until rateHist.numBuckets) {
      rateHist.bucketTop(i) shouldEqual MMD.histBucketScheme.bucketTop(i)
    }

    // Test various quantiles on the rate histogram and verify against expected values
    val quantiles = Seq(0.5, 0.9, 0.95, 0.99)
    quantiles.foreach { q =>
      // Calculate quantile from both actual and expected histograms
      val actualQuantile = rateHist.quantile(q)
      val expectedQuantile = expectedRateHist.quantile(q)

      // Verify quantiles match
      if (!expectedQuantile.isNaN) {
        actualQuantile shouldEqual expectedQuantile +- errorOk

        // Also verify quantile is within bucket range
        actualQuantile should be >= 0.0
        actualQuantile should be <= rateHist.bucketTop(rateHist.numBuckets - 1)
      } else {
        actualQuantile.isNaN shouldEqual true
      }
    }

    // Verify max and min columns are correct
    rateRow.getDouble(2) shouldEqual 200.0 +- errorOk  // Max
    rateRow.getDouble(3) shouldEqual 1.0 +- errorOk    // Min
  }

  it("should compute histogram quantile on rate of cumulative histogram with max min inputs") {
    // Create cumulative histogram with min/max - use samples 0 through 6 (7 samples)
    val (data, rv) = MMD.cumulativeHistMaxMinRV(100000L, pubFreq = 10000L, numSamples = 10, numBuckets = 8)

    // Define window for rate calculation
    val startTs = 99500L
    val endTs = 161000L
    val windowTime = endTs - startTs
    val headTime = 100000L    // First sample timestamp
    val lastTime = 160000L    // Last (7th) sample timestamp

    // Extract first and last histograms from data to calculate expected rate
    val headHist = data(0)(3).asInstanceOf[bv.LongHistogram]
    val lastHist = data(6)(3).asInstanceOf[bv.LongHistogram]

    // Calculate expected rate histogram
    val timeDelta = (lastTime - headTime) / 1000.0
    val expectedRates = (0 until headHist.numBuckets).map { b =>
      (lastHist.bucketValue(b).toDouble - headHist.bucketValue(b)) / timeDelta
    }
    val expectedRateHist = bv.MutableHistogram(MMD.histBucketScheme, expectedRates.toArray)

    // Calculate rate using CumulativeHistRateAndMinMaxFunction
    val row = new TransientHistMaxMinRow()
    val it = new ChunkedWindowIteratorH(rv, endTs, 100000, endTs, windowTime,
                 new CumulativeHistRateAndMinMaxFunction(5, 4).asInstanceOf[ChunkedRangeFunction[TransientHistRow]],
                 querySession, row)

    // Collect rate results
    val rateResults = it.toList
    rateResults.size shouldEqual 1

    val rateRow = rateResults.head
    val rateHist = rateRow.getHistogram(1)
    val maxVal = rateRow.getDouble(2)  // Max column
    val minVal = rateRow.getDouble(3)  // Min column

    // Verify max and min are the constant values
    maxVal shouldEqual 200.0 +- errorOk
    minVal shouldEqual 1.0 +- errorOk

    // Test histogram_max_quantile calculation with various quantiles using min/max bounds
    val quantiles = Seq(0.5, 0.9, 0.95, 0.99, 0.999)
    quantiles.foreach { q =>
      // Calculate bounded quantile with max/min inputs
      val actualBounded = rateHist.quantile(q, minVal, maxVal)
      val expectedBounded = expectedRateHist.quantile(q, minVal, maxVal)

      // Verify bounded quantile matches expected and respects constraints
      if (!expectedBounded.isNaN) {
        actualBounded shouldEqual expectedBounded +- errorOk
        actualBounded should be <= maxVal
        actualBounded should be >= minVal
      } else {
        actualBounded.isNaN shouldEqual true
      }
    }
  }

  it("should compute multiple quantiles correctly on cumulative histogram rate") {
    // Create cumulative histogram with min/max - use samples 0 through 6 (7 samples)
    val (data, rv) = MMD.cumulativeHistMaxMinRV(100000L, pubFreq = 10000L, numSamples = 10, numBuckets = 8)

    // Define window for rate calculation
    val startTs = 99500L
    val endTs = 161000L
    val windowTime = endTs - startTs
    val headTime = 100000L
    val lastTime = 160000L

    // Extract first and last histograms to calculate expected rate
    val headHist = data(0)(3).asInstanceOf[bv.LongHistogram]
    val lastHist = data(6)(3).asInstanceOf[bv.LongHistogram]

    // Calculate expected rate histogram
    val timeDelta = (lastTime - headTime) / 1000.0
    val expectedRates = (0 until headHist.numBuckets).map { b =>
      (lastHist.bucketValue(b).toDouble - headHist.bucketValue(b)) / timeDelta
    }
    val expectedRateHist = bv.MutableHistogram(MMD.histBucketScheme, expectedRates.toArray)

    // Calculate rate using CumulativeHistRateAndMinMaxFunction
    val row = new TransientHistMaxMinRow()
    val it = new ChunkedWindowIteratorH(rv, endTs, 100000, endTs, windowTime,
                 new CumulativeHistRateAndMinMaxFunction(5, 4).asInstanceOf[ChunkedRangeFunction[TransientHistRow]],
                 querySession, row)

    // Collect rate results
    val rateResults = it.toList
    val rateRow = rateResults.head
    val rateHist = rateRow.getHistogram(1)

    // Calculate various quantiles from both actual and expected histograms
    val quantiles = Seq(0.5, 0.75, 0.9, 0.95, 0.99)
    val actualResults = quantiles.map { q => (q, rateHist.quantile(q)) }
    val expectedResults = quantiles.map { q => (q, expectedRateHist.quantile(q)) }

    // Verify actual matches expected
    actualResults.zip(expectedResults).foreach { case ((q1, actual), (q2, expected)) =>
      q1 shouldEqual q2
      if (!expected.isNaN) {
        actual shouldEqual expected +- errorOk
      } else {
        actual.isNaN shouldEqual true
      }
    }

    // Verify quantiles are monotonically increasing
    actualResults.sliding(2).foreach { case Seq((q1, v1), (q2, v2)) =>
      if (!v1.isNaN && !v2.isNaN) {
        v2 should be >= v1  // Higher quantile should have higher or equal value
      }
    }

    // Verify each quantile is within valid range
    actualResults.foreach { case (q, value) =>
      if (!value.isNaN) {
        value should be >= 0.0  // Rate should be non-negative for counters
        value should be <= rateHist.bucketTop(rateHist.numBuckets - 1)
      }
    }
  }

  it("should constrain quantile by max value when interpolating in top bucket") {
    // Create custom cumulative histogram data with distribution heavily skewed to top bucket
    // This ensures high quantiles fall in top bucket where max constraint is applied
    val startTS = 100000L
    val endTS = 200000L
    val maxVal = 200.0
    val minVal = 1.0

    // Create bucket scheme explicitly (same as GeometricBuckets(2.0, 2.0, 8))
    val bucketScheme = bv.GeometricBuckets(2.0, 2.0, 8)

    // Create custom histogram data: first sample with low values, second with high concentration in top bucket
    // First histogram: [1, 2, 3, 4, 5, 10, 50, 100] - gradual increase
    // Second histogram: [2, 4, 6, 8, 10, 20, 100, 1100] - top bucket jumps by 1000
    val firstBuckets = Array[Long](1, 2, 3, 4, 5, 10, 50, 100)
    val secondBuckets = Array[Long](2, 4, 6, 8, 10, 20, 100, 1100)

    val histData = Stream(
      Seq(startTS, 161L, 161L, bv.LongHistogram(bucketScheme, firstBuckets), minVal, maxVal,
          "request-latency", MMD.extraTags ++ Map("_ws_".utf8 -> "demo".utf8, "_ns_".utf8 -> "testapp".utf8, "dc".utf8 -> "0".utf8)),
      Seq(endTS, 1161L, 1161L, bv.LongHistogram(bucketScheme, secondBuckets), minVal, maxVal,
          "request-latency", MMD.extraTags ++ Map("_ws_".utf8 -> "demo".utf8, "_ns_".utf8 -> "testapp".utf8, "dc".utf8 -> "0".utf8))
    )

    // Use the same pattern as cumulativeHistMaxMinRV
    val container = MMD.records(MMD.cumulativeHistMaxMinDS, histData).records
    val bufferPool = new WriteBufferPool(TestData.nativeMem, MMD.cumulativeHistMaxMinDS.schema.data, storeConf)
    val part = TimeSeriesPartitionSpec.makePart(0, MMD.cumulativeHistMaxMinDS, partKey=MMD.histPartKey,
                                                bufferPool=bufferPool)
    container.iterate(MMD.cumulativeHistMaxMinDS.ingestionSchema).foreach { row =>
      part.ingest(0, row, MMD.cumulativeHistMaxMinBH, false, Option.empty, false)
    }
    part.switchBuffers(MMD.cumulativeHistMaxMinBH, encode = true)

    val rv = RawDataRangeVector(null, part, AllChunkScan, Array(0, 3, 5, 4),
                                new AtomicLong, new AtomicLong, Long.MaxValue, "query-id")

    // Calculate rate using CumulativeHistRateAndMinMaxFunction through full pipeline
    val windowTime = endTS - startTS
    val row = new TransientHistMaxMinRow()
    val it = new ChunkedWindowIteratorH(rv, endTS, 100000, endTS, windowTime,
                 new CumulativeHistRateAndMinMaxFunction(5, 4).asInstanceOf[ChunkedRangeFunction[TransientHistRow]],
                 querySession, row)

    val rateResults = it.toList
    rateResults.size shouldEqual 1

    val rateRow = rateResults.head
    val rateHist = rateRow.getHistogram(1)
    val extractedMax = rateRow.getDouble(2)
    val extractedMin = rateRow.getDouble(3)

    // Verify max/min are preserved through the pipeline
    extractedMax shouldEqual maxVal +- errorOk
    extractedMin shouldEqual minVal +- errorOk

    // Top bucket rate should be (1100-100)/100 = 10 per second, creating strong concentration
    val timeDelta = (endTS - startTS) / 1000.0
    val expectedTopBucketRate = (secondBuckets.last - firstBuckets.last).toDouble / timeDelta
    info(s"Top bucket rate: $expectedTopBucketRate per second")

    // Test high quantiles where interpolation falls in top bucket
    val highQuantiles = Seq(0.95, 0.99, 0.995, 0.999)

    highQuantiles.foreach { q =>
      val unboundedQuantile = rateHist.quantile(q)
      val boundedQuantile = rateHist.quantile(q, extractedMin, extractedMax)

      info(s"q=$q: unbounded=$unboundedQuantile, bounded=$boundedQuantile")

      // Bounded quantile must always respect min/max constraints
      if (!boundedQuantile.isNaN) {
        boundedQuantile should be <= maxVal
        boundedQuantile should be >= minVal
      }

      // When unbounded exceeds max, verify clamping occurred
      if (!unboundedQuantile.isNaN && unboundedQuantile > maxVal) {
        boundedQuantile should be < unboundedQuantile
        info(s"  ✓ Max constraint applied: $unboundedQuantile clamped to $boundedQuantile")
      }
    }
  }

  it("should constrain quantile by min value when interpolating in bottom bucket") {
    // Create custom cumulative histogram data with distribution heavily skewed to bottom bucket
    // This ensures low quantiles fall in bottom bucket where min constraint is applied
    val startTS = 100000L
    val endTS = 200000L
    val maxVal = 200.0
    val minVal = 1.0

    // Create bucket scheme explicitly (same as GeometricBuckets(2.0, 2.0, 8))
    val bucketScheme = bv.GeometricBuckets(2.0, 2.0, 8)

    // Create custom histogram data: first sample with heavy concentration in bottom bucket
    // First histogram: [1000, 50, 10, 5, 4, 3, 2, 1] - 1000 samples in bucket 0 (0 < x <= 2)
    // Second histogram: [2000, 100, 20, 10, 8, 6, 4, 2] - bottom bucket jumps by 1000
    val firstBuckets = Array[Long](1000, 50, 10, 5, 4, 3, 2, 1)
    val secondBuckets = Array[Long](2000, 100, 20, 10, 8, 6, 4, 2)

    val histData = Stream(
      Seq(startTS, 1075L, 1075L, bv.LongHistogram(bucketScheme, firstBuckets), minVal, maxVal,
          "request-latency", MMD.extraTags ++ Map("_ws_".utf8 -> "demo".utf8, "_ns_".utf8 -> "testapp".utf8, "dc".utf8 -> "0".utf8)),
      Seq(endTS, 2150L, 2150L, bv.LongHistogram(bucketScheme, secondBuckets), minVal, maxVal,
          "request-latency", MMD.extraTags ++ Map("_ws_".utf8 -> "demo".utf8, "_ns_".utf8 -> "testapp".utf8, "dc".utf8 -> "0".utf8))
    )

    // Use the same pattern as cumulativeHistMaxMinRV
    val container = MMD.records(MMD.cumulativeHistMaxMinDS, histData).records
    val bufferPool = new WriteBufferPool(TestData.nativeMem, MMD.cumulativeHistMaxMinDS.schema.data, storeConf)
    val part = TimeSeriesPartitionSpec.makePart(0, MMD.cumulativeHistMaxMinDS, partKey=MMD.histPartKey,
                                                bufferPool=bufferPool)
    container.iterate(MMD.cumulativeHistMaxMinDS.ingestionSchema).foreach { row =>
      part.ingest(0, row, MMD.cumulativeHistMaxMinBH, false, Option.empty, false)
    }
    part.switchBuffers(MMD.cumulativeHistMaxMinBH, encode = true)

    val rv = RawDataRangeVector(null, part, AllChunkScan, Array(0, 3, 5, 4),
                                new AtomicLong, new AtomicLong, Long.MaxValue, "query-id")

    // Calculate rate using CumulativeHistRateAndMinMaxFunction through full pipeline
    val windowTime = endTS - startTS
    val row = new TransientHistMaxMinRow()
    val it = new ChunkedWindowIteratorH(rv, endTS, 100000, endTS, windowTime,
                 new CumulativeHistRateAndMinMaxFunction(5, 4).asInstanceOf[ChunkedRangeFunction[TransientHistRow]],
                 querySession, row)

    val rateResults = it.toList
    rateResults.size shouldEqual 1

    val rateRow = rateResults.head
    val rateHist = rateRow.getHistogram(1)
    val extractedMax = rateRow.getDouble(2)
    val extractedMin = rateRow.getDouble(3)

    // Verify max/min are preserved through the pipeline
    extractedMax shouldEqual maxVal +- errorOk
    extractedMin shouldEqual minVal +- errorOk

    // Bottom bucket rate should be (2000-1000)/100 = 10 per second, creating strong concentration
    val timeDelta = (endTS - startTS) / 1000.0
    val expectedBottomBucketRate = (secondBuckets.head - firstBuckets.head).toDouble / timeDelta
    info(s"Bottom bucket rate: $expectedBottomBucketRate per second")

    // Test low quantiles where interpolation falls in bottom bucket
    val lowQuantiles = Seq(0.001, 0.005, 0.01, 0.05, 0.5, 0.75, 0.9, 0.95, 0.99)

    lowQuantiles.foreach { q =>
      val unboundedQuantile = rateHist.quantile(q)
      val boundedQuantile = rateHist.quantile(q, extractedMin, extractedMax)

      info(s"q=$q: unbounded=$unboundedQuantile, bounded=$boundedQuantile")

      // Bounded quantile must always respect min/max constraints
      if (!boundedQuantile.isNaN) {
        boundedQuantile should be <= maxVal
        boundedQuantile should be >= minVal
      }

      // When unbounded goes below min, verify clamping occurred
      if (!unboundedQuantile.isNaN && unboundedQuantile < minVal) {
        boundedQuantile should be > unboundedQuantile
        info(s"  ✓ Min constraint applied: $unboundedQuantile clamped to $boundedQuantile")
      }
    }
  }

}
