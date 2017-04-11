package filodb.core.query

import com.typesafe.config.ConfigFactory
import org.velvia.filo.BinaryVector

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}
import org.scalatest.concurrent.ScalaFutures

import filodb.core._
import filodb.core.memstore.{RowWithOffset, TimeSeriesMemStore}
import filodb.core.store.FilteredPartitionScan

class TimeGroupingAggregateSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  import monix.execution.Scheduler.Implicits.global
  import MachineMetricsData._

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val memStore = new TimeSeriesMemStore(config)

  after {
    memStore.reset()
  }

  it("should filter and aggregate across time buckets and series") {
    memStore.setup(projection1)
    val data = mapper(linearMultiSeries()).take(30)   // 3 records per series x 10 series
    val rows = data.zipWithIndex.map { case (reader, n) => RowWithOffset(reader, n) }
    memStore.ingest(projection1.datasetRef, rows)

    val split = memStore.getScanSplits(projection1.datasetRef, 1).head
    val aggregator = new TimeGroupingMinDoubleAgg(0, 1, 110000L, 130000L, 2)
    val agg1 = memStore.readChunks(projection1, schema take 2, 0, FilteredPartitionScan(split))
                       .foldLeftL(aggregator.asInstanceOf[Aggregate[Double]])(_ add _)
                       .runAsync.futureValue
    agg1 should equal (aggregator)
    agg1.result should equal (Array(11.0, 21.0))

    val aggregator2 = new TimeGroupingMaxDoubleAgg(0, 1, 110000L, 130000L, 2)
    val agg2 = memStore.readChunks(projection1, schema take 2, 0, FilteredPartitionScan(split))
                       .foldLeftL(aggregator2.asInstanceOf[Aggregate[Double]])(_ add _)
                       .runAsync.futureValue
    agg2.result should equal (Array(20.0, 30.0))

    val aggregator3 = new TimeGroupingAvgDoubleAgg(0, 1, 110000L, 130000L, 2)
    val agg3 = memStore.readChunks(projection1, schema take 2, 0, FilteredPartitionScan(split))
                       .foldLeftL(aggregator3.asInstanceOf[Aggregate[Double]])(_ add _)
                       .runAsync.futureValue
    agg3.result should equal (Array(15.5, 25.5))
  }
}