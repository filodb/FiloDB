package filodb.cassandra.columnstore

import com.typesafe.config.ConfigFactory
import monix.reactive.Observable

import filodb.core.TestData
import filodb.core.memstore.{TimeSeriesPartition, TimeSeriesPartitionSpec}
import filodb.core.MachineMetricsData.{dataset1, defaultPartKey, singleSeriesData}
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.store.{AllChunkScan, ChunkSet, RowKeyChunkScan}
import filodb.memory.format.TupleRowReader

class CassandraBackedTimeSeriesPartitionSpec extends TimeSeriesPartitionSpec {

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  import monix.execution.Scheduler.Implicits.global
  override val colStore = new CassandraColumnStore(config, global)
  colStore.initialize(dataset1.ref)

  it("should be able to load from persistent store to answer queries") {

    val now = System.currentTimeMillis()
    val data = singleSeriesData(now, 1000).map(TupleRowReader).take(40) // generate for each second
    val chunks: Observable[ChunkSet] = TestData.toChunkSetStream(dataset1,
      defaultPartKey, data, 10) // 10 rows per chunk

    // first write chunks to persistent store
    colStore.write(dataset1, chunks).futureValue

    val part = new TimeSeriesPartition(dataset1, defaultPartKey, 0, colStore, bufferPool)

    // now query the persistence backed store for a sub interval without ingesting data explicitly
    val start: BinaryRecord = BinaryRecord(dataset1, Seq(now))
    val end: BinaryRecord = BinaryRecord(dataset1, Seq(now + 20000 - 100)) // query for 2 chunks
    val scan = RowKeyChunkScan(start, end)
    val colIds = Array(0,1)
    val readers1 = part.readers(scan, colIds).toList
    // we should see 20 rows in two chunks, then add one for the latest unencoded chunk
    readers1.size shouldBe 3
    readers1.map(_.rowIterator().size).sum shouldEqual 20
    val readTuples = readers1.flatMap(_.rowIterator().map(r => (r.getLong(0), r.getDouble(1))))
    val ingestedTuples = data.take(20).toList.map(r => (r.getLong(0), r.getDouble(1)))
    readTuples shouldEqual ingestedTuples

    // now query all chunks
    val readers2 = part.readers(AllChunkScan, colIds).toList
    // we should see at least 4 chunks, each with 10 rows
    readers2.size should be > 4
    readers2.map(_.rowIterator().size).sum shouldEqual 40 // since it may include rows from earlier runs

    // now test streamReaders
    val readers3 = part.streamReaders(scan, colIds).toListL.runAsync.futureValue
    // we should see 20 rows in two chunks, then add one for the latest unencoded chunk
    readers3.size shouldBe 3
    readers3.map(_.rowIterator().size).sum shouldEqual 20

  }

}