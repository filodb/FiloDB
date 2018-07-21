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

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val chunkRetentionHours = 72
  // implemented by concrete test sub class
  val colStore: ColumnStore = new NullColumnStore()

  var part: TimeSeriesPartition = null

  val reclaimer = new ReclaimListener {
    def onReclaim(metaAddr: Long, numBytes: Int): Unit = {
      assert(numBytes == dataset1.blockMetaSize)
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
  protected val ingestBlockHolder = new BlockMemFactory(blockStore, None, dataset1.blockMetaSize, true)

  before {
    colStore.truncate(dataset1.ref).futureValue
  }

  it("should be able to read immediately after ingesting one row") {
    part = new TimeSeriesPartition(0, dataset1, defaultPartKey, 0, bufferPool,
          new TimeSeriesShardStats(dataset1.ref, 0))
    val data = singleSeriesReaders().take(5)
    part.ingest(data(0), ingestBlockHolder)   // just one row
    part.numChunks shouldEqual 1
    part.appendingChunkLen shouldEqual 1
    part.unflushedChunksets shouldEqual 1
    val minData = data.map(_.getDouble(1)).take(1)
    val iterator = part.timeRangeRows(LastSampleChunkScan, Array(1))
    iterator.map(_.getDouble(0)).toSeq shouldEqual minData
  }

  it("should be able to ingest new rows while flush() executing concurrently") {
    part = new TimeSeriesPartition(0, dataset1, defaultPartKey, 0, bufferPool,
                    new TimeSeriesShardStats(dataset1.ref, 0))
    val data = singleSeriesReaders().take(11)
    val minData = data.map(_.getDouble(1))
    data.take(10).zipWithIndex.foreach { case (r, i) => part.ingest(r, ingestBlockHolder) }

    val origPoolSize = bufferPool.poolSize

    // First 10 rows ingested. Now flush in a separate Future while ingesting the remaining row
    part.switchBuffers(ingestBlockHolder)
    // After switchBuffers, currentChunks should be null, pool size the same (nothing new allocated yet)
    bufferPool.poolSize shouldEqual origPoolSize
    part.appendingChunkLen shouldEqual 0

    // Before flush happens, should be able to read all chunks
    part.unflushedChunksets shouldEqual 1
    part.numChunks shouldEqual 1
    val data1 = part.timeRangeRows(AllChunkScan, Array(1)).map(_.getDouble(0)).toBuffer
    data1 shouldEqual (minData take 10)

    val blockHolder = new BlockMemFactory(blockStore, None, dataset1.blockMetaSize)
    val flushFut = Future(part.makeFlushChunks(blockHolder))
    data.drop(10).zipWithIndex.foreach { case (r, i) => part.ingest(r, ingestBlockHolder) }
    val chunkSets = flushFut.futureValue.toSeq

    // After flush, the old writebuffers should be returned to pool, but new one allocated for ingesting
    bufferPool.poolSize shouldEqual origPoolSize

    // there should be a frozen chunk of 10 records plus 1 record in currently appending chunks
    part.numChunks shouldEqual 2
    part.unflushedChunksets shouldEqual 2
    part.appendingChunkLen shouldEqual 1
    val initTS = data(0).getLong(0)
    val readIt = part.timeRangeRows(initTS, initTS + 500000, Array(1)).map(_.getDouble(0))
    readIt.toBuffer shouldEqual minData

    chunkSets should have length (1)
    chunkSets.head.info.numRows shouldEqual 10
    chunkSets.head.chunks should have length (5)

    chunkSets.head.invokeFlushListener()    // update newestFlushedID
    part.unflushedChunksets shouldEqual 1
  }

  it("should be able to read a time range of ingested data") (pending)

  it("should reclaim blocks and evict flushed chunks properly upon reclaim") {
     part = new TimeSeriesPartition(0, dataset1, defaultPartKey, 0, bufferPool,
                        new TimeSeriesShardStats(dataset1.ref, 0))
     val data = singleSeriesReaders().take(21)
     val minData = data.map(_.getDouble(1))
     data.take(10).zipWithIndex.foreach { case (r, i) => part.ingest(r, ingestBlockHolder) }

     val origPoolSize = bufferPool.poolSize

     // First 10 rows ingested. Now flush in a separate Future while ingesting 6 more rows
     part.switchBuffers(ingestBlockHolder)
     bufferPool.poolSize shouldEqual origPoolSize    // current chunks become null, no new allocation yet
     val blockHolder = new BlockMemFactory(blockStore, None, dataset1.blockMetaSize)
     val flushFut = Future(part.makeFlushChunks(blockHolder))
     data.drop(10).take(6).zipWithIndex.foreach { case (r, i) => part.ingest(r, ingestBlockHolder) }
     val chunkSets = flushFut.futureValue.toSeq

     // After flush, the old writebuffers should be returned to pool, but new one allocated too
     bufferPool.poolSize shouldEqual origPoolSize

     // there should be a frozen chunk of 10 records plus 6 records in currently appending chunks
     part.numChunks shouldEqual 2
     part.appendingChunkLen shouldEqual 6
    val initTS = data(0).getLong(0)
    val readIt = part.timeRangeRows(initTS, initTS + 500000, Array(1)).map(_.getDouble(0))
    // readIt.toBuffer shouldEqual Seq(minData take 10, minData drop 10 take 6).toSet
    readIt.toBuffer shouldEqual (minData take 16)

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
     val holder2 = new BlockMemFactory(blockStore, None, dataset1.blockMetaSize)
     val flushFut2 = Future(part.makeFlushChunks(holder2))
     data.drop(16).zipWithIndex.foreach { case (r, i) => part.ingest(r, ingestBlockHolder) }
     val chunkSets2 = flushFut2.futureValue.toSeq

     part.numChunks shouldEqual 3
     part.appendingChunkLen shouldEqual 5
     chunkSets2 should have length (1)
     chunkSets2.head.info.numRows shouldEqual 6

     val data2 = part.timeRangeRows(AllChunkScan, Array(1)).map(_.getDouble(0)).toSeq
     data2 shouldEqual minData

     // Reclaim earliest group of flushed chunks.  Make sure write buffers + latest flushed chunks still there.
    currBlock.reclaim(forced = true)
     val data3 = part.timeRangeRows(AllChunkScan, Array(1)).map(_.getDouble(0))
     data3.toBuffer shouldEqual (minData drop 10)
 }

  it("should not switch buffers and flush when current chunks are empty") {
    part = new TimeSeriesPartition(0, dataset1, defaultPartKey, 0, bufferPool,
              new TimeSeriesShardStats(dataset1.ref, 0))
    val data = singleSeriesReaders().take(11)
    data.zipWithIndex.foreach { case (r, i) => part.ingest(r, ingestBlockHolder) }
    part.numChunks shouldEqual 1
    part.appendingChunkLen shouldEqual 11

    // Now, switch buffers and flush.  Appenders will be empty.
    part.switchBuffers(ingestBlockHolder)
    val blockHolder = new BlockMemFactory(blockStore, None, dataset1.blockMetaSize)
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
    val allData = part.timeRangeRows(LastSampleChunkScan, Array(1)).map(_.getDouble(0))
    allData.toSeq shouldEqual minData
  }

  it("should reset metadata correctly when recycling old write buffers") {
    // Ingest data into 10 TSPartitions and switch and encode all of them.  Now WriteBuffers poolsize should be
    // down 10 and then back up.
    val stats = new TimeSeriesShardStats(dataset1.ref, 0)
    val data = singleSeriesReaders().take(10).toBuffer
    val origPoolSize = bufferPool.poolSize
    val partitions = (0 to 9).map { partNo =>
      new TimeSeriesPartition(partNo, dataset1, defaultPartKey, 0, bufferPool, stats)
    }
    (0 to 9).foreach { i =>
      data.foreach { case d => partitions(i).ingest(d, ingestBlockHolder) }
      partitions(i).numChunks shouldEqual 1
      partitions(i).appendingChunkLen shouldEqual 10
      partitions(i).switchBuffers(ingestBlockHolder, true)
    }

    bufferPool.poolSize shouldEqual origPoolSize

    // Do this 4 more times so that we get old recycled metadata back
    (0 until 4).foreach { n =>
      (0 to 9).foreach { i =>
      data.foreach { case d => partitions(i).ingest(d, ingestBlockHolder) }
        partitions(i).appendingChunkLen shouldEqual 10
        partitions(i).switchBuffers(ingestBlockHolder, true)
      }
    }

    // Now ingest again but don't switch buffers.  Ensure appendingChunkLen is appropriate.
    (0 to 9).foreach { i =>
      data.foreach { case d => partitions(i).ingest(d, ingestBlockHolder) }
      partitions(i).appendingChunkLen shouldEqual 10
    }
  }

  it("should automatically use new write buffers and encode old one when write buffers overflow") {
    // Ingest 10 less than maxChunkSize
    val origPoolSize = bufferPool.poolSize

    part = new TimeSeriesPartition(0, dataset1, defaultPartKey, 0, bufferPool,
          new TimeSeriesShardStats(dataset1.ref, 0))
    val data = singleSeriesReaders().take(maxChunkSize + 10)
    data.take(maxChunkSize - 10).zipWithIndex.foreach { case (r, i) => part.ingest(r, ingestBlockHolder) }
    part.numChunks shouldEqual 1
    part.appendingChunkLen shouldEqual (maxChunkSize - 10)
    part.unflushedChunksets shouldEqual 1

    bufferPool.poolSize shouldEqual (origPoolSize - 1)

    // Now ingest 20 more.  Verify new chunks encoded.  10 rows after switch at 100. Verify can read everything.
    data.drop(maxChunkSize - 10).zipWithIndex.foreach { case (r, i) => part.ingest(r, ingestBlockHolder) }
    part.numChunks shouldEqual 2
    part.appendingChunkLen shouldEqual 10
    part.unflushedChunksets shouldEqual 2
    bufferPool.poolSize shouldEqual (origPoolSize - 1)

    val minData = data.map(_.getDouble(1)) drop 100
    val readData1 = part.timeRangeRows(LastSampleChunkScan, Array(1)).map(_.getDouble(0))
    readData1.toBuffer shouldEqual minData

    // Now simulate a flush, verify that both chunksets flushed
    // Now, switch buffers and flush.  Appenders will be empty.
    part.switchBuffers(ingestBlockHolder)
    val blockHolder = new BlockMemFactory(blockStore, None, dataset1.blockMetaSize)
    val chunkSets = part.makeFlushChunks(blockHolder).toSeq
    chunkSets should have length (2)
    part.numChunks shouldEqual 2
    part.appendingChunkLen shouldEqual 0
    chunkSets.map(_.info.numRows) shouldEqual Seq(100, 10)
  }
}