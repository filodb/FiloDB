package filodb.core.memstore

import scala.concurrent.Future

import filodb.core._
import filodb.core.store._
import filodb.memory.impl.PageAlignedBlockManager
import filodb.memory.{BlockHolder, BlockManager}

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span, Seconds}
import org.scalatest.{FunSpec, Matchers}

class TimeSeriesPartitionSpec extends FunSpec with Matchers with ScalaFutures {
  import MachineMetricsData._

  implicit override val patienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(50, Millis))

  import monix.execution.Scheduler.Implicits.global
  private val blockStore = new PageAlignedBlockManager(100 * 1024 * 1024)
  protected implicit val blockHolder = new BlockHolder(blockStore,BlockManager.reclaimAnyPolicy)

  private val bufferPool = new WriteBufferPool(dataset, 10, 50)

  it("should be able to read immediately after ingesting rows") {
    val part = new TimeSeriesPartition(dataset, defaultPartKey, 0, bufferPool)
    val data = singleSeriesReaders().take(5)
    part.ingest(data(0), 1000L)
    part.numChunks shouldEqual 1
    part.latestChunkLen shouldEqual 1
    val minData = data.map(_.getDouble(1)).take(1)
    val chunk1 = part.readers(LastSampleChunkScan, Array(1)).toSeq.head
    chunk1.vectors(0).toSeq should equal (minData)
  }

  it("should be able to ingest new rows while flush() executing concurrently") {
    val part = new TimeSeriesPartition(dataset, defaultPartKey, 0, bufferPool)
    val data = singleSeriesReaders().take(11)
    val minData = data.map(_.getDouble(1))
    data.take(10).zipWithIndex.foreach { case (r, i) => part.ingest(r, 1000L + i) }

    val origPoolSize = bufferPool.poolSize

    // First 10 rows ingested. Now flush in a separate Future while ingesting the remaining row
    part.switchBuffers()
    bufferPool.poolSize shouldEqual (origPoolSize - 1)

    val flushFut = Future(part.flush())
    data.drop(10).zipWithIndex.foreach { case (r, i) => part.ingest(r, 1100L + i) }
    val chunkSetOpt = flushFut.futureValue

    // After flush, the old writebuffers should be returned to pool
    bufferPool.poolSize shouldEqual origPoolSize

    // there should be a frozen chunk of 10 records plus 1 record in currently appending chunks
    part.numChunks shouldEqual 2
    part.latestChunkLen shouldEqual 1
    val chunks = part.streamReaders(AllChunkScan, Array(1))
                     .map(_.vectors(0).toSeq).toListL.runAsync
    chunks.futureValue should equal (Seq(minData take 10, minData drop 10))

    chunkSetOpt.isDefined shouldEqual true
    chunkSetOpt.get.info.numRows shouldEqual 10
    chunkSetOpt.get.chunks should have length (5)
  }

  // TimeSeriesPartition does not remove old chunks automatically.  This is waiting for reclaim stuff to be done.
  // Maybe enable once reclaim stuff is done.

  // it("should remove old chunks when # chunks > chunksToKeep") {
  //   val part = new TimeSeriesPartition(dataset, defaultPartKey, 0, bufferPool)
  //   val data = singleSeriesReaders().take(21)   // one more than needed to kick old chunks out
  //   val minData = data.map(_.getDouble(1))

  //   // First ingest 20 rows. This should fill up and finalize 2 chunks.  Both chunks should be kept.
  //   data.zipWithIndex.take(20).foreach { case (r, i) => part.ingest(r, 1000L + i) }
  //   part.numChunks shouldEqual 2
  //   val ids1 = part.newestChunkIds(2).toBuffer
  //   ids1 should have length (2)
  //   val chunks = part.readers(AllChunkScan, Array(1)).map(_.vectors(0).toSeq).toSeq
  //   chunks shouldEqual Seq(minData take 10, minData drop 10 take 10)

  //   // Now ingest one more.  This should start creating a new set of chunks which means old one gotta go
  //   part.ingest(data.last, 10000L)
  //   part.numChunks shouldEqual 2
  //   val ids2 = part.newestChunkIds(2).toBuffer
  //   ids2 should not equal (ids1)
  //   val chunks2 = part.readers(AllChunkScan, Array(1)).map(_.vectors(0).toSeq).toSeq
  //   chunks2 shouldEqual Seq(minData drop 10 take 10, minData drop 20)
  // }

  it("should not switch buffers and flush when current chunks are empty") {
    val part = new TimeSeriesPartition(dataset, defaultPartKey, 0, bufferPool)
    val data = singleSeriesReaders().take(11)
    data.zipWithIndex.foreach { case (r, i) => part.ingest(r, 1000L + i) }
    part.numChunks shouldEqual 1
    part.latestChunkLen shouldEqual 11

    // Now, switch buffers and flush.  Now we have two chunks
    part.switchBuffers()
    part.flush().isDefined shouldEqual true
    part.numChunks shouldEqual 2
    part.latestChunkLen shouldEqual 0

    // Now, switch buffers again without ingesting more data.  Clearly there are no rows, no switch, and no flush.
    part.switchBuffers()
    part.numChunks shouldEqual 2
    part.flush() shouldEqual None

    val minData = data.map(_.getDouble(1))
    val chunk1 = part.readers(LastSampleChunkScan, Array(1)).toSeq.head
    chunk1.vectors(0).toSeq should equal (minData)
  }
}