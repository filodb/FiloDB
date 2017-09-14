package filodb.core.query

import com.typesafe.config.ConfigFactory
import monix.reactive.Observable
import org.velvia.filo.BinaryVector

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}
import org.scalatest.concurrent.ScalaFutures

import filodb.core._
import filodb.core.metadata._
import filodb.core.memstore.{IngestRecord, TimeSeriesMemStore}
import filodb.core.store.{QuerySpec, FilteredPartitionScan, RowKeyChunkScan}

class CombinerSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  import monix.execution.Scheduler.Implicits.global
  import MachineMetricsData._

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val memStore = new TimeSeriesMemStore(config)

  after {
    memStore.reset()
  }

  describe("Simple and ListCombiners") {
    it("should use ListCombiner for base functions which don't combine eg Last") {
      memStore.setup(projection1, 0)
      val data = records(linearMultiSeries()).take(30)   // 3 records per series x 10 series
      memStore.ingest(projection1.datasetRef, 0, data)

      val split = memStore.getScanSplits(projection1.datasetRef, 1).head
      val query = QuerySpec("min", AggregationFunction.Last, Nil, CombinerFunction.Simple, Nil)
      val agg1 = memStore.aggregate(projection1, 0, query, FilteredPartitionScan(split))
                   .get.runAsync.futureValue
      agg1 shouldBe a[ListAggregate[_]]
      val listAgg = agg1.asInstanceOf[ListAggregate[DoubleSeriesValues]]
      listAgg.values.length should be (10)
      listAgg.values.head.points.length should be (1)
      listAgg.values.head.points.head.timestamp should be (120000L)
    }
  }

  describe("Histogram") {
    val baseQuery = QuerySpec("min", AggregationFunction.Sum, Nil,
                              CombinerFunction.Histogram, Seq("2000"))
    it("should invalidate queries with invalid Combiner/histo args") {
      memStore.setup(projection1, 0)
      val split = memStore.getScanSplits(projection1.datasetRef, 1).head

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
      memStore.setup(projection1, 0)
      val split = memStore.getScanSplits(projection1.datasetRef, 1).head
      val query = baseQuery.copy(aggregateFunc = AggregationFunction.TimeGroupMin,
                                 aggregateArgs = Seq("2"))
      val timeScan = RowKeyChunkScan(projection1, Seq(110000L), Seq(130000L))
      val agg1 = memStore.aggregate(projection1, 0, query, FilteredPartitionScan(split), timeScan)
      agg1.swap.get shouldBe an[InvalidAggregator]
    }

    it("should compute histogram correctly") {
      memStore.setup(projection1, 0)
      val data = records(linearMultiSeries()).take(30)   // 3 records per series x 10 series
      memStore.ingest(projection1.datasetRef, 0, data)

      val split = memStore.getScanSplits(projection1.datasetRef, 1).head
      val agg = memStore.aggregate(projection1, 0, baseQuery, FilteredPartitionScan(split))
                  .get.runAsync.futureValue
      agg shouldBe a[HistogramAggregate]
      val histAgg = agg.asInstanceOf[HistogramAggregate]
      histAgg.counts.size should equal (10)
      histAgg.counts should equal (Array(0, 0, 0, 0, 4, 6, 0, 0, 0, 0))
    }

    it("should be able to combine two HistogramAggregates") {
      val buckets = HistogramAggregate.geometricBuckets(5000, 8)
      val combiner1 = new HistogramCombiner(buckets)
      val combiner2 = new HistogramCombiner(buckets)
      val values = Seq(1.5, 50.0, 2000.0, 5.6, 7.1, 99.9, 555.5, 128, 256, 71, 890)
                     .map(DoubleAggregate)
      val hist1 = combiner1.fold(Observable.fromIterable(values take 5)).runAsync.futureValue
      val hist2 = combiner2.fold(Observable.fromIterable(values drop 5)).runAsync.futureValue
      hist1.counts should equal (Array(1, 2, 0, 1, 0, 0, 0, 1))
      hist2.counts should equal (Array(0, 0, 0, 0, 3, 2, 1, 0))

      val finalHist = combiner1.combine(hist1, hist2)
      finalHist.counts should equal (Array(1, 2, 0, 1, 3, 2, 1, 1))
    }
  }
}