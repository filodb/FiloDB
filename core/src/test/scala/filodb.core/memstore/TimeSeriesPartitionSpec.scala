package filodb.core.memstore

import filodb.core._
import filodb.core.store._
import filodb.memory.impl.PageAlignedBlockManager
import filodb.memory.{BlockHolder, BlockManager}

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}

class TimeSeriesPartitionSpec extends FunSpec with Matchers with ScalaFutures {
  import MachineMetricsData._

  import monix.execution.Scheduler.Implicits.global
  private val blockStore = new PageAlignedBlockManager(100 * 1024 * 1024)
  protected implicit val blockHolder = new BlockHolder(blockStore,BlockManager.reclaimAnyPolicy)


  it("should be able to read immediately after ingesting rows") {
    val part = new TimeSeriesPartition(dataset, defaultPartKey, 0, chunksToKeep = 3, maxChunkSize = 10)
    val data = singleSeriesReaders().take(5)
    part.ingest(data(0), 1000L)
    part.numChunks shouldEqual 1
    part.latestChunkLen shouldEqual 1
    val minData = data.map(_.getDouble(1)).take(1)
    val chunk1 = part.readers(LastSampleChunkScan, Array(1)).toSeq.head
    chunk1.vectors(0).toSeq should equal (minData)
  }

  it("should ingest rows, flush, and be able to ingest new rows") {
    val part = new TimeSeriesPartition(dataset, defaultPartKey, 0, chunksToKeep = 3, maxChunkSize = 10)
    val data = singleSeriesReaders().take(11)
    val minData = data.map(_.getDouble(1))
    data.zipWithIndex.foreach { case (r, i) => part.ingest(r, 1000L + i) }

    // there should be a frozen chunk of 10 records plus 1 record in currently appending chunks
    part.numChunks shouldEqual 2
    part.latestChunkLen shouldEqual 1
    val chunks = part.streamReaders(AllChunkScan, Array(1))
                     .map(_.vectors(0).toSeq).toListL.runAsync
    chunks.futureValue should equal (Seq(minData take 10, minData drop 10))
  }

  it("should remove old chunks when # chunks > chunksToKeep") {
    val part = new TimeSeriesPartition(dataset, defaultPartKey, 0, chunksToKeep = 2, maxChunkSize = 10)
    val data = singleSeriesReaders().take(21)   // one more than needed to kick old chunks out
    val minData = data.map(_.getDouble(1))

    // First ingest 20 rows. This should fill up and finalize 2 chunks.  Both chunks should be kept.
    data.zipWithIndex.take(20).foreach { case (r, i) => part.ingest(r, 1000L + i) }
    part.numChunks shouldEqual 2
    val ids1 = part.newestChunkIds(2).toBuffer
    ids1 should have length (2)
    val chunks = part.readers(AllChunkScan, Array(1)).map(_.vectors(0).toSeq).toSeq
    chunks shouldEqual Seq(minData take 10, minData drop 10 take 10)

    // Now ingest one more.  This should start creating a new set of chunks which means old one gotta go
    part.ingest(data.last, 10000L)
    part.numChunks shouldEqual 2
    val ids2 = part.newestChunkIds(2).toBuffer
    ids2 should not equal (ids1)
    val chunks2 = part.readers(AllChunkScan, Array(1)).map(_.vectors(0).toSeq).toSeq
    chunks2 shouldEqual Seq(minData drop 10 take 10, minData drop 20)
  }

  it("should skip ingesting rows when offset < flushedWatermark") {
    val part = new TimeSeriesPartition(dataset, defaultPartKey, 0, chunksToKeep = 3, maxChunkSize = 10)
    part.flushedWatermark = 500L
    val data = singleSeriesReaders().take(10)
    val minData = data.map(_.getDouble(1))
    // First 6 will be skipped, offsets 0, 100, 200, 300, 400, 500
    data.zipWithIndex.foreach { case (r, i) => part.ingest(r, i * 100) }

    part.numChunks shouldEqual 1
    val chunks = part.readers(LastSampleChunkScan, Array(1))
                     .map(_.vectors(0).toSeq).toSeq
    chunks shouldEqual Seq(minData drop 6)
  }
}