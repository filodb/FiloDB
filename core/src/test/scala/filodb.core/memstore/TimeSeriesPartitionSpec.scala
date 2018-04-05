package filodb.core.memstore

import scala.concurrent.Future

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._
import filodb.core.store._
import filodb.memory._
import filodb.memory.format.UnsafeUtils

class TimeSeriesPartitionSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  import MachineMetricsData._

  implicit override val patienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(50, Millis))

  import monix.execution.Scheduler.Implicits.global
  import TimeSeriesShard.BlockMetaAllocSize

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val chunkRetentionHours = 72
  // implemented by concrete test sub class
  val colStore: ColumnStore = new NullColumnStore()

  var part: TimeSeriesPartition = null

  val reclaimer = new ReclaimListener {
    def onReclaim(metaAddr: Long, numBytes: Int): Unit = {
      assert(numBytes == BlockMetaAllocSize)
      val partID = UnsafeUtils.getInt(metaAddr)
      val chunkID = UnsafeUtils.getLong(metaAddr + 4)
      part.removeChunksAt(chunkID)
    }
  }

  private val blockStore = new PageAlignedBlockManager(100 * 1024 * 1024,
    new MemoryStats(Map("test"-> "test")), reclaimer, 1, chunkRetentionHours)
  val memFactory = new NativeMemoryManager(10 * 1024 * 1024)
  val maxChunkSize = 100
  protected val bufferPool = new WriteBufferPool(memFactory, dataset1, maxChunkSize, 50)
  protected val pagedChunkStore = new DemandPagedChunkStore(dataset1, blockStore,
                                    BlockMetaAllocSize, chunkRetentionHours, 1)
  private val ingestBlockHolder = new BlockMemFactory(blockStore, None, BlockMetaAllocSize, true)

  before {
    colStore.truncate(dataset1.ref).futureValue
  }

  it("should be able to read immediately after ingesting rows") {
    part = new TimeSeriesPartition(0, dataset1, defaultPartKey, 0, colStore, bufferPool,
          false, pagedChunkStore, new TimeSeriesShardStats(dataset1.ref, 0))
    val data = singleSeriesReaders().take(5)
    part.ingest(data(0), 1000L, ingestBlockHolder)
    part.numChunks shouldEqual 1
    part.appendingChunkLen shouldEqual 1
    part.unflushedChunksets shouldEqual 1
    val minData = data.map(_.getDouble(1)).take(1)
    val chunk1 = part.readers(LastSampleChunkScan, Array(1)).toSeq.head
    chunk1.vectors(0).toSeq should equal (minData)
  }

  it("should be able to ingest new rows while flush() executing concurrently") {
    part = new TimeSeriesPartition(0, dataset1, defaultPartKey, 0, colStore, bufferPool,
                    false, pagedChunkStore, new TimeSeriesShardStats(dataset1.ref, 0))
    val data = singleSeriesReaders().take(11)
    val minData = data.map(_.getDouble(1))
    data.take(10).zipWithIndex.foreach { case (r, i) => part.ingest(r, 1000L + i, ingestBlockHolder) }

    val origPoolSize = bufferPool.poolSize

    // First 10 rows ingested. Now flush in a separate Future while ingesting the remaining row
    part.switchBuffers(ingestBlockHolder)
    // After switchBuffers, currentChunks should be null, pool size the same (nothing new allocated yet)
    bufferPool.poolSize shouldEqual origPoolSize
    part.appendingChunkLen shouldEqual 0

    // Before flush happens, should be able to read all chunks
    part.unflushedChunksets shouldEqual 1
    part.numChunks shouldEqual 1
    val chunks1 = part.readers(AllChunkScan, Array(1)).map(_.vectors(0).toSeq).toBuffer
    chunks1 shouldEqual Seq(minData take 10)

    val blockHolder = new BlockMemFactory(blockStore, None, BlockMetaAllocSize)
    val flushFut = Future(part.makeFlushChunks(blockHolder))
    data.drop(10).zipWithIndex.foreach { case (r, i) => part.ingest(r, 1100L + i, ingestBlockHolder) }
    val chunkSets = flushFut.futureValue.toSeq

    // After flush, the old writebuffers should be returned to pool, but new one allocated for ingesting
    bufferPool.poolSize shouldEqual origPoolSize

    // there should be a frozen chunk of 10 records plus 1 record in currently appending chunks
    part.numChunks shouldEqual 2
    part.unflushedChunksets shouldEqual 2
    part.appendingChunkLen shouldEqual 1
    val chunks = part.streamReaders(AllChunkScan, Array(1))
                     .map(_.vectors(0).toSeq).toListL.runAsync
    chunks.futureValue should equal (Seq(minData take 10, minData drop 10))

    chunkSets should have length (1)
    chunkSets.head.info.numRows shouldEqual 10
    chunkSets.head.chunks should have length (5)

    chunkSets.head.invokeFlushListener()    // update newestFlushedID
    part.unflushedChunksets shouldEqual 1
  }

  it("should reclaim blocks and evict flushed chunks properly upon reclaim") {
     part = new TimeSeriesPartition(0, dataset1, defaultPartKey, 0, colStore, bufferPool,
                        false, pagedChunkStore, new TimeSeriesShardStats(dataset1.ref, 0))
     val data = singleSeriesReaders().take(21)
     val minData = data.map(_.getDouble(1))
     data.take(10).zipWithIndex.foreach { case (r, i) => part.ingest(r, 1000L + i, ingestBlockHolder) }

     val origPoolSize = bufferPool.poolSize

     // First 10 rows ingested. Now flush in a separate Future while ingesting 6 more rows
     part.switchBuffers(ingestBlockHolder)
     bufferPool.poolSize shouldEqual origPoolSize    // current chunks become null, no new allocation yet
     val blockHolder = new BlockMemFactory(blockStore, None, BlockMetaAllocSize)
     val flushFut = Future(part.makeFlushChunks(blockHolder))
     data.drop(10).take(6).zipWithIndex.foreach { case (r, i) => part.ingest(r, 1100L + i, ingestBlockHolder) }
     val chunkSets = flushFut.futureValue.toSeq

     // After flush, the old writebuffers should be returned to pool, but new one allocated too
     bufferPool.poolSize shouldEqual origPoolSize

     // there should be a frozen chunk of 10 records plus 6 records in currently appending chunks
     part.numChunks shouldEqual 2
     part.appendingChunkLen shouldEqual 6
     val chunks = part.streamReaders(AllChunkScan, Array(1))
       .map(_.vectors(0).toSeq).toListL.runAsync
     chunks.futureValue.toSet shouldEqual Seq(minData take 10, minData drop 10 take 6).toSet

     chunkSets should have length (1)
     chunkSets.head.info.numRows shouldEqual 10
     chunkSets.head.chunks should have length (5)

    part.unflushedChunksets shouldEqual 2
    chunkSets.head.invokeFlushListener()    // update newestFlushedID
    part.unflushedChunksets shouldEqual 1

     val currBlock = blockHolder.currentBlock.get() // hang on to these; we'll later test reclaiming them manually
     blockHolder.markUsedBlocksReclaimable()

     // Now, switch buffers and flush again, ingesting 5 more rows
     // There should now be 3 chunks total, the current write buffers plus the two flushed ones
     part.switchBuffers(ingestBlockHolder)
     val holder2 = new BlockMemFactory(blockStore, None, BlockMetaAllocSize)
     val flushFut2 = Future(part.makeFlushChunks(holder2))
     data.drop(16).zipWithIndex.foreach { case (r, i) => part.ingest(r, 1100L + i, ingestBlockHolder) }
     val chunkSets2 = flushFut2.futureValue.toSeq

     part.numChunks shouldEqual 3
     part.appendingChunkLen shouldEqual 5
     chunkSets2 should have length (1)
     chunkSets2.head.info.numRows shouldEqual 6

     val chunks2 = part.readers(AllChunkScan, Array(1)).map(_.vectors(0).toSeq).toSeq
     chunks2 shouldEqual Seq(minData take 10, minData drop 10 take 6, minData drop 16)

     // Reclaim earliest group of flushed chunks.  Make sure write buffers + latest flushed chunks still there.
    currBlock.reclaim(forced = true)
     val readers = part.readers(AllChunkScan, Array(1)).toSeq
     readers.length shouldEqual 2
     readers.map(_.vectors(0).toSeq) shouldEqual Seq(minData drop 10 take 6, minData drop 16)
 }

  it("should not switch buffers and flush when current chunks are empty") {
    part = new TimeSeriesPartition(0, dataset1, defaultPartKey, 0, colStore, bufferPool, false,
              pagedChunkStore, new TimeSeriesShardStats(dataset1.ref, 0))
    val data = singleSeriesReaders().take(11)
    data.zipWithIndex.foreach { case (r, i) => part.ingest(r, 1000L + i, ingestBlockHolder) }
    part.numChunks shouldEqual 1
    part.appendingChunkLen shouldEqual 11

    // Now, switch buffers and flush.  Appenders will be empty.
    part.switchBuffers(ingestBlockHolder)
    val blockHolder = new BlockMemFactory(blockStore, None, BlockMetaAllocSize)
    val chunkSets = part.makeFlushChunks(blockHolder)
    chunkSets.isEmpty shouldEqual false
    part.numChunks shouldEqual 1
    part.appendingChunkLen shouldEqual 0

    chunkSets.foreach(_.invokeFlushListener())    // update newestFlushedID
    part.unflushedChunksets shouldEqual 0    // No new data

    // Now, switch buffers again without ingesting more data.  Clearly there are no rows, no switch, and no flush.
    part.switchBuffers(ingestBlockHolder)
    part.numChunks shouldEqual 1
    part.makeFlushChunks(blockHolder).isEmpty shouldEqual true

    val minData = data.map(_.getDouble(1))
    val chunk1 = part.readers(LastSampleChunkScan, Array(1)).toSeq.head
    chunk1.vectors(0).toSeq should equal (minData)
  }

  it("should automatically use new write buffers and encode old one when write buffers overflow") {
    // Ingest 10 less than maxChunkSize
    val origPoolSize = bufferPool.poolSize

    part = new TimeSeriesPartition(0, dataset1, defaultPartKey, 0, colStore, bufferPool,
          false, pagedChunkStore, new TimeSeriesShardStats(dataset1.ref, 0))
    val data = singleSeriesReaders().take(maxChunkSize + 10)
    data.take(maxChunkSize - 10).zipWithIndex.foreach { case (r, i) => part.ingest(r, 1000L + i, ingestBlockHolder) }
    part.numChunks shouldEqual 1
    part.appendingChunkLen shouldEqual (maxChunkSize - 10)
    part.unflushedChunksets shouldEqual 1

    bufferPool.poolSize shouldEqual (origPoolSize - 1)

    // Now ingest 20 more.  Verify new chunks encoded.  10 rows after switch at 100. Verify can read everything.
    data.drop(maxChunkSize - 10).zipWithIndex.foreach { case (r, i) => part.ingest(r, 1000L + i, ingestBlockHolder) }
    part.numChunks shouldEqual 2
    part.appendingChunkLen shouldEqual 10
    part.unflushedChunksets shouldEqual 2
    bufferPool.poolSize shouldEqual (origPoolSize - 1)

    val minData = data.map(_.getDouble(1)) drop 100
    val chunk1 = part.readers(LastSampleChunkScan, Array(1)).toSeq.head
    chunk1.vectors(0).toSeq should equal (minData)

    // Now simulate a flush, verify that both chunksets flushed
    // Now, switch buffers and flush.  Appenders will be empty.
    part.switchBuffers(ingestBlockHolder)
    val blockHolder = new BlockMemFactory(blockStore, None, BlockMetaAllocSize)
    val chunkSets = part.makeFlushChunks(blockHolder).toSeq
    chunkSets should have length (2)
    part.numChunks shouldEqual 2
    part.appendingChunkLen shouldEqual 0
    chunkSets.map(_.info.numRows) shouldEqual Seq(100, 10)
  }
}