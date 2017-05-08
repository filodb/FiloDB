package filodb.core.memstore

import com.typesafe.config.ConfigFactory
import org.velvia.filo.{BinaryVector, ZeroCopyUTF8String}

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}
import org.scalatest.concurrent.ScalaFutures

import filodb.core._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.store.{FilteredPartitionScan, SinglePartitionScan}

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
    memStore.setup(projection1)
    val data = mapper(multiSeriesData()).take(20)   // 2 records per series x 10 series
    val rows = data.zipWithIndex.map { case (reader, n) => RowWithOffset(reader, n) }
    memStore.ingest(projection1.datasetRef, rows)

    memStore.indexNames(projection1.datasetRef).toSeq should equal (Seq("series"))

    val minSet = data.map(_.getDouble(1)).toSet
    val split = memStore.getScanSplits(projection1.datasetRef, 1).head
    val q = memStore.scanRows(projection1, Seq(schema(1)), 0, FilteredPartitionScan(split))
    q.map(_.getDouble(0)).toSet should equal (minSet)

    // query the series name string column as well
    val q2 = memStore.scanRows(projection1, schemaWithSeries.takeRight(1), 0, FilteredPartitionScan(split))
    q2.map(_.filoUTF8String(0)).toSet should equal (data.map(_.filoUTF8String(5)).toSet)
  }

  it("should be able to handle nonexistent partition keys") {
    memStore.setup(projection1)

    val q = memStore.scanRows(projection1, Seq(schema(1)), 0, SinglePartitionScan(BinaryRecord.empty))
    q.toBuffer.length should equal (0)
  }

  it("should ingest into multiple series and be able to query on one partition in real time") {
    memStore.setup(projection1)
    val data = mapper(multiSeriesData()).take(20)   // 2 records per series x 10 series
    val rows = data.zipWithIndex.map { case (reader, n) => RowWithOffset(reader, n) }
    memStore.ingest(projection1.datasetRef, rows)

    val minSeries0 = data(0).getDouble(1)
    val partKey0 = projection1.partKey(data(0).filoUTF8String(5))
    val q = memStore.scanRows(projection1, Seq(schema(1)), 0, SinglePartitionScan(partKey0))
    q.map(_.getDouble(0)).toSeq.head should equal (minSeries0)

    val minSeries1 = data(1).getDouble(1)
    val partKey1 = projection1.partKey("Series 1")
    val q2 = memStore.scanRows(projection1, Seq(schema(1)), 0, SinglePartitionScan(partKey1))
    q2.map(_.getDouble(0)).toSeq.head should equal (minSeries1)
  }

  it("should query on multiple partitions using filters") {
    memStore.setup(projection1)
    val data = mapper(linearMultiSeries()).take(20)   // 2 records per series x 10 series
    val rows = data.zipWithIndex.map { case (reader, n) => RowWithOffset(reader, n) }
    memStore.ingest(projection1.datasetRef, rows)

    val filter = ColumnFilter("series", Filter.In(Set("Series 1".utf8, "Series 2".utf8)))
    val split = memStore.getScanSplits(projection1.datasetRef, 1).head
    val q2 = memStore.scanRows(projection1, Seq(schema(1)), 0, FilteredPartitionScan(split, Seq(filter)))
    q2.map(_.getDouble(0)).toSeq should equal (Seq(2.0, 12.0, 3.0, 13.0))
  }

  it("should ingest into multiple series and flush older chunks") (pending)
}