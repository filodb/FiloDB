package filodb.core.memstore

import com.typesafe.config.ConfigFactory
import monix.reactive.Observable
import org.velvia.filo.{BinaryVector, ZeroCopyUTF8String}

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}
import org.scalatest.concurrent.ScalaFutures

import filodb.core._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.query.{ColumnFilter, Filter, AggregationFunction}
import filodb.core.store.{FilteredPartitionScan, SinglePartitionScan, QuerySpec}

class TimeSeriesMemStoreSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  import monix.execution.Scheduler.Implicits.global
  import MachineMetricsData._
  import ZeroCopyUTF8String._

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val memStore = new TimeSeriesMemStore(config)

  after {
    memStore.reset()
  }

  // Look mama!  Real-time time series ingestion and querying across multiple partitions!
  it("should ingest into multiple series and be able to query across all partitions in real time") {
    memStore.setup(projection1, 0)
    val data = records(multiSeriesData()).take(20)   // 2 records per series x 10 series
    memStore.ingest(projection1.datasetRef, 0, data)

    memStore.numPartitions(projection1.datasetRef, 0) should equal (10)
    memStore.indexNames(projection1.datasetRef).toSeq should equal (Seq(("series", 0)))

    val minSet = data.map(_.data.getDouble(1)).toSet
    val split = memStore.getScanSplits(projection1.datasetRef, 1).head
    val q = memStore.scanRows(projection1, Seq(schema(1)), 0, FilteredPartitionScan(split))
    q.map(_.getDouble(0)).toSet should equal (minSet)

    // query the series name string column as well
    val q2 = memStore.scanRows(projection1, schemaWithSeries.takeRight(1), 0, FilteredPartitionScan(split))
    q2.map(_.filoUTF8String(0)).toSet should equal (data.map(_.partition.filoUTF8String(0)).toSet)
  }

  it("should ingest into multiple series and aggregate across partitions") {
    memStore.setup(projection1, 1)
    val data = records(linearMultiSeries()).take(20)   // 2 records per series x 10 series
    memStore.ingest(projection1.datasetRef, 1, data)

    // NOTE: ingesting into wrong shard should give an error
    intercept[IllegalArgumentException] {
      memStore.ingest(projection1.datasetRef, 0, data)
    }

    val split = memStore.getScanSplits(projection1.datasetRef, 1).head
    val query = QuerySpec("min", AggregationFunction.Sum)
    val agg1 = memStore.aggregate(projection1, 0, query, FilteredPartitionScan(split))
                       .get.runAsync.futureValue
    agg1.result should equal (Array((1 to 20).map(_.toDouble).sum))
  }

  it("should ingest map/tags column as partition key and aggregate") {
    memStore.setup(projection2, 0)
    val data = records(withMap(linearMultiSeries())).take(20)   // 2 records per series x 10 series
    memStore.ingest(projection2.datasetRef, 0, data)

    val split = memStore.getScanSplits(projection2.datasetRef, 1).head
    val query = QuerySpec("min", AggregationFunction.Sum)
    val filter = ColumnFilter("n", Filter.Equals("2".utf8))
    val agg1 = memStore.aggregate(projection2, 0, query, FilteredPartitionScan(split, Seq(filter)))
                       .get.runAsync.futureValue
    agg1.result should equal (Array(3 + 8 + 13 + 18))
  }

  it("should be able to handle nonexistent partition keys") {
    memStore.setup(projection1, 0)

    val q = memStore.scanRows(projection1, Seq(schema(1)), 0, SinglePartitionScan(BinaryRecord.empty))
    q.toBuffer.length should equal (0)
  }

  it("should ingest into multiple series and be able to query on one partition in real time") {
    memStore.setup(projection1, 0)
    val data = records(multiSeriesData()).take(20)   // 2 records per series x 10 series
    memStore.ingest(projection1.datasetRef, 0, data)

    val minSeries0 = data(0).data.getDouble(1)
    val partKey0 = projection1.partKey(data(0).partition)
    val q = memStore.scanRows(projection1, Seq(schema(1)), 0, SinglePartitionScan(partKey0))
    q.map(_.getDouble(0)).toSeq.head should equal (minSeries0)

    val minSeries1 = data(1).data.getDouble(1)
    val partKey1 = projection1.partKey("Series 1")
    val q2 = memStore.scanRows(projection1, Seq(schema(1)), 0, SinglePartitionScan(partKey1))
    q2.map(_.getDouble(0)).toSeq.head should equal (minSeries1)
  }

  it("should query on multiple partitions using filters") {
    memStore.setup(projection1, 0)
    val data = records(linearMultiSeries()).take(20)   // 2 records per series x 10 series
    memStore.ingest(projection1.datasetRef, 0, data)

    val filter = ColumnFilter("series", Filter.In(Set("Series 1".utf8, "Series 2".utf8)))
    val split = memStore.getScanSplits(projection1.datasetRef, 1).head
    val q2 = memStore.scanRows(projection1, Seq(schema(1)), 0, FilteredPartitionScan(split, Seq(filter)))
    q2.map(_.getDouble(0)).toSeq should equal (Seq(2.0, 12.0, 3.0, 13.0))
  }

  it("should ingest into multiple shards, getScanSplits, query, get index info from shards") {
    memStore.setup(projection2, 0)
    memStore.setup(projection2, 1)
    val data = records(withMap(linearMultiSeries())).take(20)   // 2 records per series x 10 series
    memStore.ingest(projection2.datasetRef, 0, data)
    val data2 = records(withMap(linearMultiSeries(200000L, 6), 6)).take(20)   // 5 series only
    memStore.ingest(projection2.datasetRef, 1, data2)

    memStore.activeShards(projection1.datasetRef) should equal (Seq(0, 1))
    memStore.numRowsIngested(projection1.datasetRef, 0) should equal (20L)

    val splits = memStore.getScanSplits(projection2.datasetRef, 1)
    splits should have length (2)

    memStore.indexNames(projection2.datasetRef).toList should equal (
      Seq(("n", 0), ("series", 0), ("n", 1), ("series", 1)))

    val query = QuerySpec("min", AggregationFunction.Sum)
    val filter = ColumnFilter("n", Filter.Equals("2".utf8))
    val agg1 = memStore.aggregate(projection2, 0, query, FilteredPartitionScan(splits.head, Seq(filter)))
                       .get.runAsync.futureValue
    agg1.result should equal (Array(3 + 8 + 13 + 18))

    val agg2 = memStore.aggregate(projection2, 0, query, FilteredPartitionScan(splits.last, Seq(filter)))
                       .get.runAsync.futureValue
    agg2.result should equal (Array(3 + 9 + 15))
  }

  it("should ingestStream into multiple shards") {
    memStore.setup(projection1, 0)
    memStore.setup(projection1, 1)
    memStore.activeShards(projection1.datasetRef) should equal (Seq(0, 1))

    val stream = Observable.fromIterable(records(linearMultiSeries()).take(100).grouped(5).toSeq)
    memStore.ingestStream(projection1.datasetRef, 0, stream)(ex => throw ex)
    memStore.ingestStream(projection1.datasetRef, 1, stream)(ex => throw ex)

    Thread sleep 1000
    val splits = memStore.getScanSplits(projection1.datasetRef, 1)
    val query = QuerySpec("min", AggregationFunction.Sum)
    val agg1 = memStore.aggregate(projection1, 0, query, FilteredPartitionScan(splits.head))
                       .get.runAsync.futureValue
    agg1.result should equal (Array((1 to 100).map(_.toDouble).sum))

    val agg2 = memStore.aggregate(projection1, 0, query, FilteredPartitionScan(splits.last))
                       .get.runAsync.futureValue
    agg2.result should equal (Array((1 to 100).map(_.toDouble).sum))
  }

  it("should handle errors from ingestStream") {
    memStore.setup(projection1, 0)
    var err: Throwable = null

    val errStream = Observable.fromIterable(records(linearMultiSeries()).take(100).grouped(5).toSeq)
                              .endWithError(new NumberFormatException)
    memStore.ingestStream(projection1.datasetRef, 0, errStream) { ex => err = ex }

    Thread sleep 500
    err shouldBe a[NumberFormatException]
  }

  it("should ingest into multiple series and flush older chunks") (pending)
}