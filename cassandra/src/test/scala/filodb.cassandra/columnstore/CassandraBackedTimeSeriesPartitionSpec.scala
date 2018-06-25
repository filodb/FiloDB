package filodb.cassandra.columnstore

import scala.concurrent.duration._
import scala.language.postfixOps

import monix.reactive.Observable
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import filodb.core.TestData
import filodb.core.memstore.{TimeSeriesPartition, TimeSeriesPartitionSpec, TimeSeriesShardStats}
import filodb.core.MachineMetricsData.{dataset1, defaultPartKey, singleSeriesData}
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.store.{AllChunkScan, ChunkSet, RowKeyChunkScan}
import filodb.memory.format.TupleRowReader

class CassandraBackedTimeSeriesPartitionSpec extends TimeSeriesPartitionSpec with BeforeAndAfterAll {

  import monix.execution.Scheduler.Implicits.global
  override val colStore = new CassandraColumnStore(config, global)
  val timeout = Timeout(30 seconds)

  override def beforeAll(): Unit = {
    super.beforeAll()
    colStore.initialize(dataset1.ref).futureValue(timeout)
  }

  it("should be able to load from persistent store when partition is empty") {
    val now = System.currentTimeMillis()
    val data = singleSeriesData(now, 1000).map(TupleRowReader).take(40) // generate for each second
    val chunks: Observable[ChunkSet] = TestData.toChunkSetStream(dataset1,
      defaultPartKey, data, 10) // 10 rows per chunk, 4 chunks will be created

    // first write chunks to persistent store
    colStore.write(dataset1, chunks).futureValue

    // Ingest a few rows.
    val part = new TimeSeriesPartition(0, dataset1, defaultPartKey, 0, colStore, bufferPool,
          pagedChunkStore, new TimeSeriesShardStats(dataset1.ref, 0))

    // now query the persistence backed store for a sub interval without ingesting data explicitly
    val start = BinaryRecord(dataset1, Seq(now))
    val end = BinaryRecord(dataset1, Seq(now + 20000 - 100)) // query for 2 chunks
    val scan = RowKeyChunkScan(start, end)
    val colIds = Array(0, 1)
    val readers1 = part.readers(scan, colIds).toList
    // we should see 20 rows in two chunks.  No data ingested means no chunks for write buffer
    readers1.size shouldBe 2
    readers1.map(_.rowIterator().size).sum shouldEqual 20
    val readTuples = readers1.flatMap(_.rowIterator().map(r => (r.getLong(0), r.getDouble(1))))
    val ingestedTuples = data.take(20).toList.map(r => (r.getLong(0), r.getDouble(1)))
    readTuples shouldEqual ingestedTuples
  }

  it("should be able to load from persistent store to answer queries") {
    val now = System.currentTimeMillis()
    val data = singleSeriesData(now, 1000).map(TupleRowReader).take(45) // generate for each second
    val chunks: Observable[ChunkSet] = TestData.toChunkSetStream(dataset1,
      defaultPartKey, data take 40, 10) // 10 rows per chunk, 4 chunks will be created

    // first write chunks to persistent store - except last 5 samples
    colStore.write(dataset1, chunks).futureValue

    // Ingest the last 5 rows (everything not persisted)
    val part = new TimeSeriesPartition(0, dataset1, defaultPartKey, 0, colStore, bufferPool,
          pagedChunkStore, new TimeSeriesShardStats(dataset1.ref, 0))
    (data drop 40).foreach(r => part.ingest(r, ingestBlockHolder))

    // now query the persistence backed store for a sub interval.  Should pull out 2 chunks, 3 total (including buffers)
    val start = BinaryRecord(dataset1, Seq(now + 20000))
    val end = BinaryRecord(dataset1, Seq(now + 45000))   // query for recent 2 chunks + unpersisted data
    val scan = RowKeyChunkScan(start, end)
    val colIds = Array(0, 1)
    val readers1 = part.readers(scan, colIds).toList
    // we should see 25 rows in 3 chunks
    readers1.size shouldBe 3
    readers1.map(_.rowIterator().size).sum shouldEqual 25
    val readTuples = readers1.flatMap(_.rowIterator().map(r => (r.getLong(0), r.getDouble(1))))
    val ingestedTuples = data.drop(20).toList.map(r => (r.getLong(0), r.getDouble(1)))
    readTuples shouldEqual ingestedTuples

    // now query all chunks
    val readers2 = part.readers(AllChunkScan, colIds).toList
    // we should see exactly 4 chunks, each with 10 rows (since we didn't ingest new data)
    readers2.size shouldEqual 5
    readers2.map(_.rowIterator().size).sum shouldEqual 45 // since it may include rows from earlier runs

    // now test streamReaders
    val readers3 = part.streamReaders(scan, colIds).toListL.runAsync.futureValue
    // we should see 25 rows in 3 chunks
    readers3.size shouldBe 3
    readers3.map(_.rowIterator().size).sum shouldEqual 25
  }

  // TODO test a query that involves both memory and cassandra

  // TODO test a query that does not involve all columns

}