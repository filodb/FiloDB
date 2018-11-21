package filodb.query.exec.rangefn

import scala.collection.mutable
import scala.util.Random

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}

import filodb.core.memstore.TimeSeriesPartitionSpec
import filodb.core.query.RawDataRangeVector
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

    val sum = RangeFunction(Some(RangeFunctionId.SumOverTime))
    val count = RangeFunction(Some(RangeFunctionId.CountOverTime))
    val avg = RangeFunction(Some(RangeFunctionId.AvgOverTime))
    val min = RangeFunction(Some(RangeFunctionId.MinOverTime))
    val max = RangeFunction(Some(RangeFunctionId.MaxOverTime))

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

  private val blockStore = new PageAlignedBlockManager(100 * 1024 * 1024,
    new MemoryStats(Map("test"-> "test")), null, 1)
  protected val ingestBlockHolder = new BlockMemFactory(blockStore, None, dataset1.blockMetaSize, true)

  it("should aggregate using ChunkedRangeFunction / ChunkedWindowIterator") {
    val part = TimeSeriesPartitionSpec.makePart(0, dataset1)
    val data = linearMultiSeries().take(50).map(SeqRowReader)
    data.foreach { d => part.ingest(d, ingestBlockHolder) }

    val rv = RawDataRangeVector(null, part, null, Array(0, 1))

    val sumFunc = new SumOverTimeChunkedFunction()
    val windowIt = new ChunkedWindowIterator(rv, 110000L, 30000L, 150000L, 30000L, sumFunc, null)
    val aggregated = windowIt.map(_.getDouble(1)).toBuffer
    aggregated(0) shouldEqual ((1 to 11).sum)
  }

}
