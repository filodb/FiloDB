package filodb.core.memstore

import java.util.concurrent.TimeUnit

import scala.concurrent.Future

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._
import filodb.core.store._
import filodb.memory.{BlockMemFactory, MemoryStats, NativeMemoryManager, PageAlignedBlockManager}

class TimeSeriesPartitionSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  import MachineMetricsData._

  implicit override val patienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(50, Millis))

  import monix.execution.Scheduler.Implicits.global

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val chunkRetentionHours = config.getDuration("memstore.demand-paged-chunk-retention-period", TimeUnit.HOURS).toInt
  // implemented by concrete test sub class
  val colStore: ColumnStore = new NullColumnStore()

  private val blockStore = new PageAlignedBlockManager(100 * 1024 * 1024,
    new MemoryStats(Map("test"-> "test")), 1, chunkRetentionHours)
  val memFactory = new NativeMemoryManager(10 * 1024 * 1024)
  protected val bufferPool = new WriteBufferPool(memFactory, dataset1, 10, 50)
  protected val pagedChunkStore = new DemandPagedChunkStore(dataset1, blockStore, chunkRetentionHours, 1)

  before {
    colStore.truncate(dataset1.ref).futureValue
  }

  it("should be able to read immediately after ingesting rows") {
    val part = new TimeSeriesPartition(dataset1, defaultPartKey, 0, colStore, bufferPool, config,
          false, pagedChunkStore, new TimeSeriesShardStats(dataset1.ref, 0))
    val data = singleSeriesReaders().take(5)
    part.ingest(data(0), 1000L)
    part.numChunks shouldEqual 1
    part.latestChunkLen shouldEqual 1
    val minData = data.map(_.getDouble(1)).take(1)
    val chunk1 = part.readers(LastSampleChunkScan, Array(1)).toSeq.head
    chunk1.vectors(0).toSeq should equal (minData)
  }

  it("should be able to ingest new rows while flush() executing concurrently") {
    val part = new TimeSeriesPartition(dataset1, defaultPartKey, 0, colStore, bufferPool, config,
                    false, pagedChunkStore, new TimeSeriesShardStats(dataset1.ref, 0))
    val data = singleSeriesReaders().take(11)
    val minData = data.map(_.getDouble(1))
    data.take(10).zipWithIndex.foreach { case (r, i) => part.ingest(r, 1000L + i) }

    val origPoolSize = bufferPool.poolSize

    // First 10 rows ingested. Now flush in a separate Future while ingesting the remaining row
    part.switchBuffers()
    // After switchBuffers, currentChunks should be null, pool size the same (nothing new allocated yet)
    bufferPool.poolSize shouldEqual origPoolSize
    part.latestChunkLen shouldEqual 0

    // Before flush happens, should be able to read all chunks
    part.numChunks shouldEqual 1
    val chunks1 = part.readers(AllChunkScan, Array(1)).map(_.vectors(0).toSeq).toBuffer
    chunks1 shouldEqual Seq(minData take 10)

    val blockHolder = new BlockMemFactory(blockStore, None)
    val flushFut = Future(part.makeFlushChunks(blockHolder))
    data.drop(10).zipWithIndex.foreach { case (r, i) => part.ingest(r, 1100L + i) }
    val chunkSetOpt = flushFut.futureValue

    // After flush, the old writebuffers should be returned to pool, but new one allocated for ingesting
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

  it("should reclaim blocks and evict flushed chunks properly upon reclaim") {
     val part = new TimeSeriesPartition(dataset1, defaultPartKey, 0, colStore, bufferPool, config,
                        false, pagedChunkStore, new TimeSeriesShardStats(dataset1.ref, 0))
     val data = singleSeriesReaders().take(21)
     val minData = data.map(_.getDouble(1))
     data.take(10).zipWithIndex.foreach { case (r, i) => part.ingest(r, 1000L + i) }

     val origPoolSize = bufferPool.poolSize

     // First 10 rows ingested. Now flush in a separate Future while ingesting 6 more rows
     part.switchBuffers()
     bufferPool.poolSize shouldEqual origPoolSize    // current chunks become null, no new allocation yet
     val blockHolder = new BlockMemFactory(blockStore, None)
     val flushFut = Future(part.makeFlushChunks(blockHolder))
     data.drop(10).take(6).zipWithIndex.foreach { case (r, i) => part.ingest(r, 1100L + i) }
     val chunkSetOpt = flushFut.futureValue

     // After flush, the old writebuffers should be returned to pool, but new one allocated too
     bufferPool.poolSize shouldEqual origPoolSize

     // there should be a frozen chunk of 10 records plus 6 records in currently appending chunks
     part.numChunks shouldEqual 2
     part.latestChunkLen shouldEqual 6
     val chunks = part.streamReaders(AllChunkScan, Array(1))
       .map(_.vectors(0).toSeq).toListL.runAsync
     chunks.futureValue.toSet shouldEqual Seq(minData take 10, minData drop 10 take 6).toSet

     chunkSetOpt.isDefined shouldEqual true
     chunkSetOpt.get.info.numRows shouldEqual 10
     chunkSetOpt.get.chunks should have length (5)

     val currBlock = blockHolder.currentBlock.get() // hang on to these; we'll later test reclaiming them manually
     blockHolder.markUsedBlocksReclaimable()

     // Now, switch buffers and flush again, ingesting 5 more rows
     // There should now be 3 chunks total, the current write buffers plus the two flushed ones
     part.switchBuffers()
     val holder2 = new BlockMemFactory(blockStore, None)
     val flushFut2 = Future(part.makeFlushChunks(holder2))
     data.drop(16).zipWithIndex.foreach { case (r, i) => part.ingest(r, 1100L + i) }
     val chunkSetOpt2 = flushFut2.futureValue

     part.numChunks shouldEqual 3
     part.latestChunkLen shouldEqual 5
     chunkSetOpt2.isDefined shouldEqual true
     chunkSetOpt2.get.info.numRows shouldEqual 6

     val chunks2 = part.readers(AllChunkScan, Array(1)).map(_.vectors(0).toSeq).toSeq
     chunks2 shouldEqual Seq(minData take 10, minData drop 10 take 6, minData drop 16)

     // Reclaim earliest group of flushed chunks.  Make sure write buffers + latest flushed chunks still there.
    currBlock.reclaim(forced = true)
     val readers = part.readers(AllChunkScan, Array(1)).toSeq
     readers.length shouldEqual 2
     readers.map(_.vectors(0).toSeq) shouldEqual Seq(minData drop 10 take 6, minData drop 16)
 }

  it("should not switch buffers and flush when current chunks are empty") {
    val part = new TimeSeriesPartition(dataset1, defaultPartKey, 0, colStore, bufferPool, config, false,
              pagedChunkStore, new TimeSeriesShardStats(dataset1.ref, 0))
    val data = singleSeriesReaders().take(11)
    data.zipWithIndex.foreach { case (r, i) => part.ingest(r, 1000L + i) }
    part.numChunks shouldEqual 1
    part.latestChunkLen shouldEqual 11

    // Now, switch buffers and flush.  Now we have two chunks
    part.switchBuffers()
    val blockHolder = new BlockMemFactory(blockStore, None)
    part.makeFlushChunks(blockHolder).isDefined shouldEqual true
    part.numChunks shouldEqual 1
    part.latestChunkLen shouldEqual 0

    // Now, switch buffers again without ingesting more data.  Clearly there are no rows, no switch, and no flush.
    part.switchBuffers()
    part.numChunks shouldEqual 1
    part.makeFlushChunks(blockHolder) shouldEqual None

    val minData = data.map(_.getDouble(1))
    val chunk1 = part.readers(LastSampleChunkScan, Array(1)).toSeq.head
    chunk1.vectors(0).toSeq should equal (minData)
  }


}