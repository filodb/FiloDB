package filodb.core.memstore

import scala.concurrent.Future

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._
import filodb.core.metadata.Dataset
import filodb.core.store._
import filodb.memory._
import filodb.memory.data.{OffheapLFSortedIDMap, OffheapLFSortedIDMapMutator}
import filodb.memory.format.UnsafeUtils

object TimeSeriesPartitionSpec {
  import MachineMetricsData._
  import BinaryRegion.NativePointer

  val memFactory = new NativeMemoryManager(10 * 1024 * 1024)
  val offheapInfoMapKlass = new OffheapLFSortedIDMapMutator(memFactory, classOf[TimeSeriesPartition])
  val maxChunkSize = 100
  protected val myBufferPool = new WriteBufferPool(memFactory, dataset1, maxChunkSize, 50)

  def makePart(partNo: Int, dataset: Dataset,
               partKey: NativePointer = defaultPartKey,
               bufferPool: WriteBufferPool = myBufferPool): TimeSeriesPartition = {
    val infoMapAddr = OffheapLFSortedIDMap.allocNew(memFactory, 40)
    new TimeSeriesPartition(partNo, dataset, partKey, 0, bufferPool,
          new TimeSeriesShardStats(dataset.ref, 0), infoMapAddr, offheapInfoMapKlass)
  }
}

trait MemFactoryCleanupTest extends FunSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll {
  override def afterAll(): Unit = {
    super.afterAll()
    TimeSeriesPartitionSpec.memFactory.shutdown()
  }
}

class TimeSeriesPartitionSpec extends MemFactoryCleanupTest with ScalaFutures {
  import MachineMetricsData._
  import TimeSeriesPartitionSpec._

  implicit override val patienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(50, Millis))

  import monix.execution.Scheduler.Implicits.global

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
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
    new MemoryStats(Map("test"-> "test")), reclaimer, 1)
  protected val ingestBlockHolder = new BlockMemFactory(blockStore, None, dataset1.blockMetaSize, true)

  before {
    colStore.truncate(dataset1.ref).futureValue
  }

  it("should be able to read immediately after ingesting one row") {
    part = makePart(0, dataset1)
    val data = singleSeriesReaders().take(5)
    part.ingest(data(0), ingestBlockHolder)   // just one row
    part.numChunks shouldEqual 1
    part.appendingChunkLen shouldEqual 1
    part.unflushedChunksets shouldEqual 1
    val minData = data.map(_.getDouble(1)).take(1)
    val iterator = part.timeRangeRows(WriteBufferChunkScan, Array(1))
    iterator.map(_.getDouble(0)).toSeq shouldEqual minData
  }

  it("should be able to ingest new rows while flush() executing concurrently") {
    part = makePart(0, dataset1)
    val data = singleSeriesReaders().take(11)
    val minData = data.map(_.getDouble(1))
    val initTS = data(0).getLong(0)
    data.take(10).zipWithIndex.foreach { case (r, i) => part.ingest(r, ingestBlockHolder) }

    val origPoolSize = myBufferPool.poolSize

    // First 10 rows ingested. Now flush in a separate Future while ingesting the remaining row
    part.switchBuffers(ingestBlockHolder)
    // After switchBuffers, currentChunks should be null, pool size the same (nothing new allocated yet)
    myBufferPool.poolSize shouldEqual origPoolSize
    part.appendingChunkLen shouldEqual 0

    // Before flush happens, should be able to read all chunks
    part.unflushedChunksets shouldEqual 1
    part.numChunks shouldEqual 1
    val infos1 = part.infos(AllChunkScan).toBuffer
    infos1 should have length 1
    infos1.head.startTime shouldEqual initTS
    val data1 = part.timeRangeRows(AllChunkScan, Array(1)).map(_.getDouble(0)).toBuffer
    data1 shouldEqual (minData take 10)

    val blockHolder = new BlockMemFactory(blockStore, None, dataset1.blockMetaSize)
    // Task needs to fully iterate over the chunks, to release the shared lock.
    val flushFut = Future(part.makeFlushChunks(blockHolder).toBuffer)
    data.drop(10).zipWithIndex.foreach { case (r, i) => part.ingest(r, ingestBlockHolder) }
    val chunkSets = flushFut.futureValue

    // After flush, the old writebuffers should be returned to pool, but new one allocated for ingesting
    myBufferPool.poolSize shouldEqual origPoolSize

    // there should be a frozen chunk of 10 records plus 1 record in currently appending chunks
    part.numChunks shouldEqual 2
    part.unflushedChunksets shouldEqual 2
    part.appendingChunkLen shouldEqual 1
    val infos2 = part.infos(AllChunkScan).toBuffer
    infos2 should have length 2
    infos2.head.startTime shouldEqual initTS
    infos2.last.startTime shouldEqual data(10).getLong(0)

    val readIt = part.timeRangeRows(initTS, initTS + 500000, Array(1)).map(_.getDouble(0))
    readIt.toBuffer shouldEqual minData

    chunkSets should have length (1)
    chunkSets.head.info.numRows shouldEqual 10
    chunkSets.head.chunks should have length (5)

    chunkSets.head.invokeFlushListener()    // update newestFlushedID
    part.unflushedChunksets shouldEqual 1
  }

  it("should be able to read a time range of ingested data") {
    part = makePart(0, dataset1)
    val data = singleSeriesReaders().take(11)
    val initTS = data(0).getLong(0)
    val appendingTS = data.last.getLong(0)
    val minData = data.map(_.getDouble(1))
    data.take(10).foreach { r => part.ingest(r, ingestBlockHolder) }

    // First 10 rows ingested. Now flush in a separate Future while ingesting the remaining row
    part.switchBuffers(ingestBlockHolder)
    part.appendingChunkLen shouldEqual 0

    val blockHolder = new BlockMemFactory(blockStore, None, dataset1.blockMetaSize)
    // Task needs to fully iterate over the chunks, to release the shared lock.
    val flushFut = Future(part.makeFlushChunks(blockHolder).toBuffer)
    data.drop(10).zipWithIndex.foreach { case (r, i) => part.ingest(r, ingestBlockHolder) }

    // there should be a frozen chunk of 10 records plus 1 record in currently appending chunks
    part.numChunks shouldEqual 2
    part.appendingChunkLen shouldEqual 1

    // Flushed chunk:  initTS -> initTS + 9000 (1000 ms per tick)
    // Read from flushed chunk only
    val readIt = part.timeRangeRows(RowKeyChunkScan(initTS, initTS + 9000), Array(0, 1)).map(_.getDouble(1))
    readIt.toBuffer shouldEqual minData.take(10)

    val infos1 = part.infos(initTS, initTS + 9000)
    infos1.hasNext shouldEqual true
    val info1 = infos1.nextInfo
    info1.numRows shouldEqual 10
    info1.startTime shouldEqual initTS

    val readIt2 = part.timeRangeRows(RowKeyChunkScan(initTS + 1000, initTS + 7000), Array(0, 1)).map(_.getDouble(1))
    readIt2.toBuffer shouldEqual minData.drop(1).take(7)

    // Read from appending chunk only:  initTS + 10000
    val readIt3 = part.timeRangeRows(RowKeyChunkScan(appendingTS, appendingTS + 3000), Array(0, 1)).map(_.getDouble(1))
    readIt3.toBuffer shouldEqual minData.drop(10)

    // Try to read from before flushed to part of flushed chunk
    val readIt4 = part.timeRangeRows(RowKeyChunkScan(initTS - 7000, initTS + 3000), Array(0, 1)).map(_.getDouble(1))
    readIt4.toBuffer shouldEqual minData.take(4)

    // both flushed and appending chunk
    val readIt5 = part.timeRangeRows(RowKeyChunkScan(initTS + 7000, initTS + 14000), Array(0, 1)).map(_.getDouble(1))
    readIt5.toBuffer shouldEqual minData.drop(7)

    // No data: past appending chunk
    val readIt6 = part.timeRangeRows(RowKeyChunkScan(initTS + 20000, initTS + 24000), Array(0, 1)).map(_.getDouble(1))
    readIt6.toBuffer shouldEqual Nil

    // No data: before initTS
    val readIt7 = part.timeRangeRows(RowKeyChunkScan(initTS - 9000, initTS - 900), Array(0, 1)).map(_.getDouble(1))
    readIt7.toBuffer shouldEqual Nil
  }

  it("should reclaim blocks and evict flushed chunks properly upon reclaim") {
     part = makePart(0, dataset1)
     val data = singleSeriesReaders().take(21)
     val minData = data.map(_.getDouble(1))
     data.take(10).zipWithIndex.foreach { case (r, i) => part.ingest(r, ingestBlockHolder) }

     val origPoolSize = myBufferPool.poolSize

     // First 10 rows ingested. Now flush in a separate Future while ingesting 6 more rows
     part.switchBuffers(ingestBlockHolder)
     myBufferPool.poolSize shouldEqual origPoolSize    // current chunks become null, no new allocation yet
     val blockHolder = new BlockMemFactory(blockStore, None, dataset1.blockMetaSize)
     // Task needs to fully iterate over the chunks, to release the shared lock.
     val flushFut = Future(part.makeFlushChunks(blockHolder).toBuffer)
     data.drop(10).take(6).zipWithIndex.foreach { case (r, i) => part.ingest(r, ingestBlockHolder) }
     val chunkSets = flushFut.futureValue

     // After flush, the old writebuffers should be returned to pool, but new one allocated too
     myBufferPool.poolSize shouldEqual origPoolSize

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
     // Task needs to fully iterate over the chunks, to release the shared lock.
     val flushFut2 = Future(part.makeFlushChunks(holder2).toBuffer)
     data.drop(16).zipWithIndex.foreach { case (r, i) => part.ingest(r, ingestBlockHolder) }
     val chunkSets2 = flushFut2.futureValue

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
    part = makePart(0, dataset1)
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
    val allData = part.timeRangeRows(InMemoryChunkScan, Array(1)).map(_.getDouble(0))
    allData.toSeq shouldEqual minData
  }

  it("should reset metadata correctly when recycling old write buffers") {
    // Ingest data into 10 TSPartitions and switch and encode all of them.  Now WriteBuffers poolsize should be
    // down 10 and then back up.
    val data = singleSeriesReaders().take(10).toBuffer
    val moreData = singleSeriesReaders().drop(10).take(50).toBuffer
    val origPoolSize = myBufferPool.poolSize
    val partitions = (0 to 9).map { partNo =>
      makePart(partNo, dataset1)
    }
    (0 to 9).foreach { i =>
      data.foreach { case d => partitions(i).ingest(d, ingestBlockHolder) }
      partitions(i).numChunks shouldEqual 1
      partitions(i).appendingChunkLen shouldEqual 10
      partitions(i).switchBuffers(ingestBlockHolder, true)
    }

    myBufferPool.poolSize shouldEqual origPoolSize

    // Do this 4 more times so that we get old recycled metadata back
    (0 until 4).foreach { n =>
      (0 to 9).foreach { i =>
        moreData.drop(n*10).take(10).foreach { case d => partitions(i).ingest(d, ingestBlockHolder) }
        partitions(i).appendingChunkLen shouldEqual 10
        partitions(i).switchBuffers(ingestBlockHolder, true)
      }
    }

    // Now ingest again but don't switch buffers.  Ensure appendingChunkLen is appropriate.
    (0 to 9).foreach { i =>
      moreData.drop(40).foreach { case d => partitions(i).ingest(d, ingestBlockHolder) }
      partitions(i).appendingChunkLen shouldEqual 10
    }
  }

  it("should automatically use new write buffers and encode old one when write buffers overflow") {
    // Ingest 10 less than maxChunkSize
    val origPoolSize = myBufferPool.poolSize

    part = makePart(0, dataset1)
    val data = singleSeriesReaders().take(maxChunkSize + 10)
    data.take(maxChunkSize - 10).zipWithIndex.foreach { case (r, i) => part.ingest(r, ingestBlockHolder) }
    part.numChunks shouldEqual 1
    part.appendingChunkLen shouldEqual (maxChunkSize - 10)
    part.unflushedChunksets shouldEqual 1

    myBufferPool.poolSize shouldEqual (origPoolSize - 1)

    // Now ingest 20 more.  Verify new chunks encoded.  10 rows after switch at 100. Verify can read everything.
    data.drop(maxChunkSize - 10).zipWithIndex.foreach { case (r, i) => part.ingest(r, ingestBlockHolder) }
    part.numChunks shouldEqual 2
    part.appendingChunkLen shouldEqual 10
    part.unflushedChunksets shouldEqual 2
    myBufferPool.poolSize shouldEqual (origPoolSize - 1)

    val minData = data.map(_.getDouble(1)) drop 100
    val readData1 = part.timeRangeRows(WriteBufferChunkScan, Array(1)).map(_.getDouble(0))
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

  it("should drop out of time order ingested samples") {
    part = makePart(0, dataset1)
    val data = singleSeriesReaders().take(12)
    val minData = data.map(_.getDouble(1))

    // Ingest first 5, then: 8th, 6th, 7th, 9th, 10th
    data.take(5).foreach { r => part.ingest(r, ingestBlockHolder) }
    part.ingest(data(7), ingestBlockHolder)
    part.ingest(data(5), ingestBlockHolder)
    part.ingest(data(6), ingestBlockHolder)
    part.ingest(data(8), ingestBlockHolder)
    part.ingest(data(9), ingestBlockHolder)

    // Try ingesting old sample now at the end.  Verify that end time of chunkInfo is not incorrectly changed.
    part.ingest(data(2), ingestBlockHolder)
    // 8 of first 10 ingested, 2 should be dropped.  Switch buffers, and try ingesting out of order again.
    part.appendingChunkLen shouldEqual 8
    part.infoLast.numRows shouldEqual 8
    part.infoLast.endTime shouldEqual data(9).getLong(0)

    part.switchBuffers(ingestBlockHolder)
    part.appendingChunkLen shouldEqual 0

    // Now try ingesting an old smaple again at first element of next chunk.
    part.ingest(data(8), ingestBlockHolder)   // This one should be dropped
    part.appendingChunkLen shouldEqual 0
    part.ingest(data(10), ingestBlockHolder)
    part.ingest(data(11), ingestBlockHolder)

    // there should be a frozen chunk of 10 records plus 2 records in currently appending chunks
    part.numChunks shouldEqual 2
    part.appendingChunkLen shouldEqual 2
    val readData1 = part.timeRangeRows(AllChunkScan, Array(1)).map(_.getDouble(0))
    readData1.toBuffer shouldEqual (minData take 5) ++ (minData drop 7)
    val timestamps = data.map(_.getLong(0))
    val readData2 = part.timeRangeRows(AllChunkScan, Array(0)).map(_.getLong(0))
    readData2.toBuffer shouldEqual (timestamps take 5) ++ (timestamps drop 7)
  }

}