package filodb.query.exec.rangefn

import scala.collection.mutable
import scala.util.Random

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

import filodb.core.memstore.TimeSeriesPartitionSpec
import filodb.core.query.RawDataRangeVector
import filodb.core.store.AllChunkScan
import filodb.core.MetricsTestData
import filodb.memory._
import filodb.memory.format.TupleRowReader
import filodb.query.{QueryConfig, RangeFunctionId}
import filodb.query.exec.{ChunkedWindowIterator, QueueBasedWindow, SlidingWindowIterator, TransientRow}
import filodb.query.util.IndexedArrayQueue

/**
 * A common trait for windowing query tests which uses real chunks and real RawDataRangeVectors
 */
trait RawDataWindowingSpec extends FunSpec with Matchers with BeforeAndAfterAll {
  import MetricsTestData._

  private val blockStore = new PageAlignedBlockManager(100 * 1024 * 1024,
    new MemoryStats(Map("test"-> "test")), null, 1)
  protected val ingestBlockHolder = new BlockMemFactory(blockStore, None, timeseriesDataset.blockMetaSize, true)

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
  def numWindows(data: Seq[Double], windowSize: Int, step: Int): Int = data.sliding(windowSize, step).length

  // Creates a RawDataRangeVector using Prometheus time-value schema and a given chunk size etc.
  def timeValueRV(data: Seq[Double], chunkSize: Int = 100, startTS: Long = defaultStartTS): RawDataRangeVector = {
    val part = TimeSeriesPartitionSpec.makePart(0, timeseriesDataset)
    val readers = data.zipWithIndex.map { case (d, t) => TupleRowReader((Some(startTS + t * pubFreq), Some(d))) }
    readers.grouped(chunkSize).foreach { rowGroup =>
      rowGroup.foreach { row => part.ingest(row, ingestBlockHolder) }
      // Now flush and ingest the rest to ensure two separate chunks
      part.switchBuffers(ingestBlockHolder)
      part.encodeAndReleaseBuffers(ingestBlockHolder)
    }
    RawDataRangeVector(null, part, AllChunkScan, Array(0, 1))
  }

  def chunkedWindowIt(data: Seq[Double],
                      rv: RawDataRangeVector,
                      func: ChunkedRangeFunction,
                      windowSize: Int,
                      step: Int): ChunkedWindowIterator = {
    val windowTime = (windowSize.toLong - 1) * pubFreq
    val windowStartTS = defaultStartTS + windowTime
    val stepTimeMillis = step.toLong * pubFreq
    val windowEndTS = windowStartTS + (numWindows(data, windowSize, step) - 1) * stepTimeMillis
    new ChunkedWindowIterator(rv, windowStartTS, stepTimeMillis, windowEndTS, windowTime, func, queryConfig)()
  }

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

  it ("aggregation functions should work correctly on a sliding window") {
    val sum = RangeFunction(Some(RangeFunctionId.SumOverTime), useChunked = false)
    val count = RangeFunction(Some(RangeFunctionId.CountOverTime), useChunked = false)
    val avg = RangeFunction(Some(RangeFunctionId.AvgOverTime), useChunked = false)
    val min = RangeFunction(Some(RangeFunctionId.MinOverTime), useChunked = false)
    val max = RangeFunction(Some(RangeFunctionId.MaxOverTime), useChunked = false)

    val fns = Array(sum, count, avg, min, max)

    val samples = Array.fill(1000) { rand.nextInt(1000).toDouble }
    val validationQueue = new mutable.Queue[Double]()
    var added = 0
    var removed = 0
    val dummyWindow = new QueueBasedWindow(new IndexedArrayQueue[TransientRow]())
    val toEmit = new TransientRow()

    while (removed < samples.size) {
      val addTimes = rand.nextInt(10)
      for { i <- 0 until addTimes } {
        if (added < samples.size) {
          validationQueue.enqueue(samples(added))
          fns.foreach(_.addedToWindow(new TransientRow(added.toLong, samples(added)), dummyWindow))
          added += 1
        }
      }

      if (validationQueue.nonEmpty) {
        sum.apply(0, 0, dummyWindow, toEmit, queryConfig)
        toEmit.value shouldEqual validationQueue.sum

        min.apply(0, 0, dummyWindow, toEmit, queryConfig)
        toEmit.value shouldEqual validationQueue.min

        max.apply(0, 0, dummyWindow, toEmit, queryConfig)
        toEmit.value shouldEqual validationQueue.max

        count.apply(0, 0, dummyWindow, toEmit, queryConfig)
        toEmit.value shouldEqual validationQueue.size.toDouble

        avg.apply(0, 0, dummyWindow, toEmit, queryConfig)
        toEmit.value shouldEqual (validationQueue.sum / validationQueue.size)
      }

      val removeTimes = rand.nextInt(validationQueue.size + 1)
      for { i <- 0 until removeTimes } {
        if (removed < samples.size) {
          validationQueue.dequeue()
          fns.foreach(_.removedFromWindow(new TransientRow(removed.toLong, samples(removed)), dummyWindow))
          removed += 1
        }
      }
    }
  }

  // TODO: replace manual loops with ScalaCheck/properties checker
  val numIterations = 10

  it("should correctly aggregate sum_over_time using both chunked and sliding iterators") {
    val data = (1 to 120).map(_.toDouble)
    val chunkSize = 40
    val rv = timeValueRV(data, chunkSize)
    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(30) + 10
      val step = rand.nextInt(15) + 5
      info(s"  iteration $x  windowSize=$windowSize step=$step")

      val sumFunc = new SumOverTimeChunkedFunction()
      val slidingIt = slidingWindowIt(data, rv, sumFunc, windowSize, step)
      // TODO: enable this and debug why the sliding window iterator isn't working
      // val aggregated = slidingIt.map(_.getDouble(1)).toBuffer
      // aggregated shouldEqual data.sliding(windowSize, step).map(_.sum).toBuffer

      val chunkedIt = chunkedWindowIt(data, rv, sumFunc, windowSize, step)
      val aggregated2 = chunkedIt.map(_.getDouble(1)).toBuffer
      aggregated2 shouldEqual data.sliding(windowSize, step).map(_.sum).toBuffer
    }
  }

  it("should aggregate using ChunkedRangeFunction / ChunkedWindowIterator") {
    val data = (1 to 120).map(_.toDouble)
    val chunkSize = 40
    val rv = timeValueRV(data, chunkSize)

    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(30) + 10
      val step = rand.nextInt(15) + 5
      info(s"  iteration $x  windowSize=$windowSize step=$step")

      val countFunc = new CountOverTimeChunkedFunction()
      val windowIt2 = chunkedWindowIt(data, rv, countFunc, windowSize, step)
      val aggregated2 = windowIt2.map(_.getDouble(1)).toBuffer
      aggregated2 shouldEqual data.sliding(windowSize, step).map(_.length).toBuffer

      val avgFunc = new AvgOverTimeChunkedFunctionD()
      val windowIt3 = chunkedWindowIt(data, rv, avgFunc, windowSize, step)
      val aggregated3 = windowIt3.map(_.getDouble(1)).toBuffer
      aggregated3 shouldEqual data.sliding(windowSize, step).map(avg).toBuffer

      val varFunc = new StdVarOverTimeChunkedFunctionD()
      val windowIt4 = chunkedWindowIt(data, rv, varFunc, windowSize, step)
      val aggregated4 = windowIt4.map(_.getDouble(1)).toBuffer
      aggregated4 shouldEqual data.sliding(windowSize, step).map(stdVar).toBuffer

      val devFunc = new StdDevOverTimeChunkedFunctionD()
      val windowIt5 = chunkedWindowIt(data, rv, devFunc, windowSize, step)
      val aggregated5 = windowIt5.map(_.getDouble(1)).toBuffer
      aggregated5 shouldEqual data.sliding(windowSize, step).map(d => Math.sqrt(stdVar(d))).toBuffer
    }
  }

}
