package filodb.core.query

import com.typesafe.config.ConfigFactory
import org.velvia.filo.BinaryVector

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}
import org.scalatest.concurrent.ScalaFutures

import filodb.core._
import filodb.core.memstore.{IngestRecord, TimeSeriesMemStore}
import filodb.core.store.{QuerySpec, FilteredPartitionScan, RowKeyChunkScan}

class TimeGroupingAggregateSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  import monix.execution.Scheduler.Implicits.global
  import MachineMetricsData._

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val memStore = new TimeSeriesMemStore(config)

  after {
    memStore.reset()
  }

  it("should filter and aggregate across time buckets and series") {
    memStore.setup(projection1, 0)
    val data = records(linearMultiSeries()).take(30)   // 3 records per series x 10 series
    memStore.ingest(projection1.datasetRef, 0, data)

    val split = memStore.getScanSplits(projection1.datasetRef, 1).head
    val timeScan = RowKeyChunkScan(projection1, Seq(110000L), Seq(130000L))
    val query = QuerySpec("min", AggregationFunction.TimeGroupMin, Seq("2"))
    val agg1 = memStore.aggregate(projection1, 0, query, FilteredPartitionScan(split), timeScan)
                       .get.runAsync.futureValue
    agg1 shouldBe an [ArrayAggregate[_]]
    agg1.result should equal (Array(11.0, 21.0))

    val query2 = query.copy(aggregateFunc = AggregationFunction.TimeGroupMax)
    val agg2 = memStore.aggregate(projection1, 0, query2, FilteredPartitionScan(split), timeScan)
                       .get.runAsync.futureValue
    agg2.result should equal (Array(20.0, 30.0))

    val query3 = query.copy(aggregateFunc = AggregationFunction.TimeGroupAvg)
    val agg3 = memStore.aggregate(projection1, 0, query3, FilteredPartitionScan(split), timeScan)
                       .get.runAsync.futureValue
    agg3.result should equal (Array(15.5, 25.5))
  }

  it("should aggregate with default number of buckets when no argument passed in") {
    memStore.setup(projection1, 0)
    val data = records(linearMultiSeries()).take(30)   // 3 records per series x 10 series
    memStore.ingest(projection1.datasetRef, 0, data)

    val split = memStore.getScanSplits(projection1.datasetRef, 1).head
    val timeScan = RowKeyChunkScan(projection1, Seq(110000L), Seq(130000L))
    val query = QuerySpec("min", AggregationFunction.TimeGroupMin)
    val agg1 = memStore.aggregate(projection1, 0, query, FilteredPartitionScan(split), timeScan)
                       .get.runAsync.futureValue
    agg1 shouldBe an [ArrayAggregate[_]]
  }

  it("should return error when using time-based functions on non-time series dataset/projection") {
    import GdeltTestData._

    memStore.setup(projection4, 0)   // string / multi-column row key, no timestamp
    memStore.ingest(projection4.datasetRef, 0, records(projection4, readers))

    val split = memStore.getScanSplits(projection1.datasetRef, 1).head
    val query = QuerySpec("AvgTone", AggregationFunction.TimeGroupMin)
    val agg1 = memStore.aggregate(projection4, 0, query, FilteredPartitionScan(split))
    agg1.isBad shouldEqual true
    agg1.swap.get shouldEqual NoTimestampColumn
  }
}