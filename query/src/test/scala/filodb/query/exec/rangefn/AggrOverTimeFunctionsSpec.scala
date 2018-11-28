package filodb.query.exec.rangefn

import scala.collection.mutable
import scala.util.Random

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}

import filodb.core.memstore.TimeSeriesPartitionSpec
import filodb.core.query.RawDataRangeVector
import filodb.core.store.AllChunkScan
import filodb.core.MachineMetricsData
import filodb.memory._
import filodb.memory.format.SeqRowReader
import filodb.query.{QueryConfig, RangeFunctionId}
import filodb.query.exec.{ChunkedWindowIterator, QueueBasedWindow, TransientRow}
import filodb.query.util.IndexedArrayQueue

class AggrOverTimeFunctionsSpec extends FunSpec with Matchers {
  import MachineMetricsData._

  val rand = new Random()
  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))

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

  /**
   * Raw data used for the Chunked window iterator tests
   * timestamp    value
   * 100000L        1.0  // Start of first window: 80001 -> 110000
   * 101000         2.0
   * 102000         3.0
   * 103000         4.0
   * 104000         5.0
   * 105000         6.0
   * 106000         7.0
   * 107000         8.0
   * 108000         9.0
   * 109000        10.0
   * 110000        11.0
   *
   * 111000        12.0  // Second window: 110001 -> 1400000
   * 112000        13.0
   * ....
   * 140000        41.0
   *
   * The second window spans multiple chunks.
   */
  private val blockStore = new PageAlignedBlockManager(100 * 1024 * 1024,
    new MemoryStats(Map("test"-> "test")), null, 1)
  protected val ingestBlockHolder = new BlockMemFactory(blockStore, None, dataset1.blockMetaSize, true)

  it("should aggregate using ChunkedRangeFunction / ChunkedWindowIterator") {
    val part = TimeSeriesPartitionSpec.makePart(0, dataset1)
    val data = linearMultiSeries().take(50).map(SeqRowReader)
    data.take(25).foreach { d => part.ingest(d, ingestBlockHolder) }
    // Now flush and ingest the rest to ensure two separate chunks
    part.switchBuffers(ingestBlockHolder)
    part.encodeAndReleaseBuffers(ingestBlockHolder)
    data.drop(25).foreach { d => part.ingest(d, ingestBlockHolder) }

    val rv = RawDataRangeVector(null, part, AllChunkScan, Array(0, 1))

    val sumFunc = new SumOverTimeChunkedFunction()
    val windowIt = new ChunkedWindowIterator(rv, 110000L, 30000L, 150000L, 30000L, sumFunc, queryConfig)()
    val aggregated = windowIt.map(_.getDouble(1)).toBuffer
    aggregated shouldEqual Seq((1 to 11).sum, (12 to 41).sum)
  }

}
