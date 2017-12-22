package filodb.core.memstore

import scala.concurrent.Future

import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._
import filodb.core.store._
import filodb.memory.{BlockHolder, NativeMemoryManager, PageAlignedBlockManager}

class TimeSeriesPartitionSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  import MachineMetricsData._

  implicit override val patienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(50, Millis))

  import monix.execution.Scheduler.Implicits.global

  // implemented by concrete test sub class
  val colStore: ColumnStore = new NullColumnStore()

  private val blockStore = new PageAlignedBlockManager(100 * 1024 * 1024)
  val memFactory = new NativeMemoryManager(10 * 1024 * 1024)
  protected val bufferPool = new WriteBufferPool(memFactory, dataset1, 10, 50)

  before {
    colStore.truncate(dataset1.ref).futureValue
  }

  it("should be able to read immediately after ingesting rows") {
    val part = new TimeSeriesPartition(dataset1, defaultPartKey, 0, colStore, bufferPool,
      new TimeSeriesShardStats(dataset1.ref, 0))
    val data = singleSeriesReaders().take(5)
    part.ingest(data(0), 1000L)
    part.numChunks shouldEqual 1
    part.latestChunkLen shouldEqual 1
    val minData = data.map(_.getDouble(1)).take(1)
    val chunk1 = part.readers(LastSampleChunkScan, Array(1)).toSeq.head
    chunk1.vectors(0).toSeq should equal (minData)
  }

  it("should be able to ingest new rows while flush() executing concurrently") {
    val part = new TimeSeriesPartition(dataset1, defaultPartKey, 0, colStore, bufferPool,
      new TimeSeriesShardStats(dataset1.ref, 0))
    val data = singleSeriesReaders().take(11)
    val minData = data.map(_.getDouble(1))
    data.take(10).zipWithIndex.foreach { case (r, i) => part.ingest(r, 1000L + i) }

    val origPoolSize = bufferPool.poolSize

    // First 10 rows ingested. Now flush in a separate Future while ingesting the remaining row
    part.switchBuffers()
    bufferPool.poolSize shouldEqual (origPoolSize - 1)
    val blockHolder = new BlockHolder(blockStore)
    val flushFut = Future(part.makeFlushChunks(blockHolder))
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

  it("should be reclaim blocks and evict flush chunks upon reclaim") {
     val part = new TimeSeriesPartition(dataset1, defaultPartKey, 0, colStore, bufferPool,
                                        new TimeSeriesShardStats(dataset1.ref, 0))
     val data = singleSeriesReaders().take(11)
     val minData = data.map(_.getDouble(1))
     data.take(10).zipWithIndex.foreach { case (r, i) => part.ingest(r, 1000L + i) }

     val origPoolSize = bufferPool.poolSize

     // First 10 rows ingested. Now flush in a separate Future while ingesting the remaining row
     part.switchBuffers()
     bufferPool.poolSize shouldEqual (origPoolSize - 1)
     val blockHolder = new BlockHolder(blockStore)
     val flushFut = Future(part.makeFlushChunks(blockHolder))
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

     blockHolder.markUsedBlocksReclaimable()
     blockHolder.blockGroup.foreach(_.reclaim())
     //should now be only 1 the unflushed chunk instead of 2
     val readers = part.readers(AllChunkScan,Array(1))
     readers.toSeq.length should be(1)
 }


  it("should not switch buffers and flush when current chunks are empty") {
    val part = new TimeSeriesPartition(dataset1, defaultPartKey, 0, colStore, bufferPool,
      new TimeSeriesShardStats(dataset1.ref, 0))
    val data = singleSeriesReaders().take(11)
    data.zipWithIndex.foreach { case (r, i) => part.ingest(r, 1000L + i) }
    part.numChunks shouldEqual 1
    part.latestChunkLen shouldEqual 11

    // Now, switch buffers and flush.  Now we have two chunks
    part.switchBuffers()
    val blockHolder = new BlockHolder(blockStore)
    part.makeFlushChunks(blockHolder).isDefined shouldEqual true
    part.numChunks shouldEqual 2
    part.latestChunkLen shouldEqual 0

    // Now, switch buffers again without ingesting more data.  Clearly there are no rows, no switch, and no flush.
    part.switchBuffers()
    part.numChunks shouldEqual 2
    part.makeFlushChunks(blockHolder) shouldEqual None

    val minData = data.map(_.getDouble(1))
    val chunk1 = part.readers(LastSampleChunkScan, Array(1)).toSeq.head
    chunk1.vectors(0).toSeq should equal (minData)
  }


}