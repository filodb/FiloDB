package filodb.core.query

import com.typesafe.config.ConfigFactory
import org.velvia.filo.BinaryVector

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}
import org.scalatest.concurrent.ScalaFutures

import filodb.core._
import filodb.core.metadata._
import filodb.core.memstore.{RowWithOffset, TimeSeriesMemStore}
import filodb.core.store.{QuerySpec, FilteredPartitionScan}

class CombinerSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  import monix.execution.Scheduler.Implicits.global
  import MachineMetricsData._

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val memStore = new TimeSeriesMemStore(config)

  after {
    memStore.reset()
  }

  val split = memStore.getScanSplits(projection1.datasetRef, 1).head

  describe("Histogram") {
    val baseQuery = QuerySpec(AggregationFunction.Sum, Seq("min"),
                              CombinerFunction.Histogram, Seq("2000"))
    it("should invalidate queries with invalid Combiner/histo args") {
      memStore.setup(projection1)

      // No args
      val query1 = baseQuery.copy(combinerArgs = Nil)
      val agg1 = memStore.aggregate(projection1, 0, query1, FilteredPartitionScan(split))
      agg1.isBad should equal (true)
      agg1.swap.get should equal (WrongNumberArguments(0, 1))

      // Too many args
      val query2 = baseQuery.copy(combinerArgs = Seq("one", "two", "three"))
      val agg2 = memStore.aggregate(projection1, 0, query2, FilteredPartitionScan(split))
      agg2.swap.get should equal (WrongNumberArguments(3, 1))

      // First arg not a double
      val query3 = baseQuery.copy(combinerArgs = Seq("1abc2", "10"))
      val agg3 = memStore.aggregate(projection1, 0, query3, FilteredPartitionScan(split))
      agg3.swap.get shouldBe a[BadArgument]

      // Second arg not an integer
      val query4 = baseQuery.copy(combinerArgs = Seq("1E6", "1.1"))
      val agg4 = memStore.aggregate(projection1, 0, query4, FilteredPartitionScan(split))
      agg4.swap.get shouldBe a[BadArgument]
    }

    it("should invalidate histo queries where aggregation is not single number") {
      val query = baseQuery.copy(aggregateFunc = AggregationFunction.TimeGroupMin,
                                 aggregateArgs = Seq("timestamp", "min", "110000", "130000", "2"))
      val agg1 = memStore.aggregate(projection1, 0, query, FilteredPartitionScan(split))
      agg1.swap.get shouldBe an[InvalidAggregator]
    }

    it("should compute histogram correctly") {
      memStore.setup(projection1)
      val data = mapper(linearMultiSeries()).take(30)   // 3 records per series x 10 series
      val rows = data.zipWithIndex.map { case (reader, n) => RowWithOffset(reader, n) }
      memStore.ingest(projection1.datasetRef, rows)

      val agg = memStore.aggregate(projection1, 0, baseQuery, FilteredPartitionScan(split))
                  .get.runAsync.futureValue
      agg shouldBe a[HistogramAggregate]
      val histAgg = agg.asInstanceOf[HistogramAggregate]
      histAgg.counts.size should equal (10)
      histAgg.counts should equal (Array(0, 0, 0, 0, 4, 6, 0, 0, 0, 0))
    }
  }
}