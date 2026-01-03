package filodb.coordinator.flight

import com.typesafe.config.ConfigFactory
import org.apache.arrow.flight.{FlightServer, Location}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core.MachineMetricsData.records
import filodb.core.MetricsTestData.{timeSeriesData, timeseriesDatasetWithMetric}
import filodb.core.TestData
import filodb.core.memstore.TimeSeriesMemStore
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.core.store.{InMemoryMetaStore, NullColumnStore, TimeRangeChunkScan}
import filodb.memory.format.ZeroCopyUTF8String.StringToUTF8
import filodb.query.AggregationOperator.Count
import filodb.query.exec._
import filodb.query.{BinaryOperator, Cardinality, QueryResult}

class FlightQueryProducerSpec  extends AnyFunSpec with Matchers with BeforeAndAfter
                                                  with BeforeAndAfterAll with ScalaFutures {
  System.setProperty("arrow.memory.debug.allocator", "true") // allows debugging of memory leaks - look into logs
  implicit val s = monix.execution.Scheduler.Implicits.global
  val config = ConfigFactory.parseString("""
                                           |filodb.memstore.max-partitions-on-heap-per-shard = 1100
                                           |filodb.memstore.ensure-block-memory-headroom-percent = 10
                                           |filodb.memstore.ensure-tsp-count-headroom-percent = 10
                                           |filodb.memstore.ensure-native-memory-headroom-percent = 10
                                           |filodb.memstore.index-updates-publishing-enabled = true
                                           |  """.stripMargin)
    .withFallback(ConfigFactory.load("application_test.conf")).resolve()

  val memStore = new TimeSeriesMemStore(config.getConfig("filodb"), new NullColumnStore, new NullColumnStore, new InMemoryMetaStore())
  implicit override val patienceConfig = PatienceConfig(timeout = Span(5, Seconds), interval = Span(50, Millis))

  val allocator = FlightAllocator.rootAllocator.newChildAllocator("FlightQueryProducerSpec", 0, 100000)
  val querySession = QuerySession(QueryContext(), QueryConfig.unitTestingQueryConfig, queryAllocator = Some(allocator))
  val location = Location.forGrpcInsecure("localhost", 38815)

  memStore.setup(timeseriesDatasetWithMetric.ref, Schemas(timeseriesDatasetWithMetric.schema), 0, TestData.storeConf, 1)
  val rawData = timeSeriesData(Map("host".utf8 -> s"host1".utf8, "region".utf8 -> s"region1".utf8)).take(1000) ++
    timeSeriesData(Map("host".utf8 -> s"host2".utf8, "region".utf8 -> s"region1".utf8)).take(1000)
  val data = records(timeseriesDatasetWithMetric, rawData)
  memStore.ingest(timeseriesDatasetWithMetric.ref, 0, data)
  memStore.refreshIndexForTesting(timeseriesDatasetWithMetric.ref)

  val server = FlightServer.builder(allocator, location,
    new FiloDBFlightProducer(memStore, allocator, location, config)).build()
  server.start()

  after {
    querySession.close()
    allocator.close()
  }

  override def afterAll(): Unit = {
    memStore.shutdown()
    server.shutdown()
    // dont close these since there can be other tests using it, but enabling them will help find leaks within this test
//    FlightClientManager.global.shutdown()
//    FlightAllocator.rootAllocator.close()
  }

  describe("Execution of queries") {

    val chunkScanMethod = TimeRangeChunkScan(0, 100000)

    val filters = Seq(ColumnFilter("region", Filter.Equals("region1")))
    val mspe1 = MultiSchemaPartitionsExec(
      QueryContext(),
      SingleClusterFlightPlanDispatcher(location, "test"),
      timeseriesDatasetWithMetric.ref,
      0,
      filters,
      chunkScanMethod, timeseriesDatasetWithMetric.schema.partition.options.metricColumn)
    mspe1.addRangeVectorTransformer(PeriodicSamplesMapper(0, 1000, 100000, None, None))

    val mspe2 = MultiSchemaPartitionsExec(
      QueryContext(),
      SingleClusterFlightPlanDispatcher(location, "test"),
      timeseriesDatasetWithMetric.ref,
      0,
      filters,
      chunkScanMethod, timeseriesDatasetWithMetric.schema.partition.options.metricColumn)
    mspe2.addRangeVectorTransformer(PeriodicSamplesMapper(0, 1000, 100000, None, None))

    val bje = BinaryJoinExec(
      QueryContext(),
      SingleClusterFlightPlanDispatcher(location, "test"),
      Seq(mspe1),
      Seq(mspe2),
      BinaryOperator.ADD,
      Cardinality.OneToOne,
      on = None,
      ignoring = Nil,
      include = Nil,
      metricColumn = "_metric_",
      Some(RvRange(0, 1000, 100000)))

    it("should be able to run a LocalPartitionDistConcatExec query plan over flight server") {

      /*
      Construct the following query plan by hand.
      It is two mspe plans with two time series each joined with plus operator

      ---E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on SingleClusterFlightPlanDispatcher(Location{uri=grpc+tcp://localhost:8815},test)
      ----T~PeriodicSamplesMapper(start=0, step=1000, end=100000, window=None, functionId=None, rawSource=true, offsetMs=None)
      -----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(0,100000), filters=List(ColumnFilter(region,Equals(region1))), colName=None, schema=None) on SingleClusterFlightPlanDispatcher(Location{uri=grpc+tcp://localhost:8815},test)
      ----T~PeriodicSamplesMapper(start=0, step=1000, end=100000, window=None, functionId=None, rawSource=true, offsetMs=None)
      -----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(0,100000), filters=List(ColumnFilter(region,Equals(region1))), colName=None, schema=None) on SingleClusterFlightPlanDispatcher(Location{uri=grpc+tcp://localhost:8815},test)
      */

      val dce = LocalPartitionDistConcatExec(
        QueryContext(),
        SingleClusterFlightPlanDispatcher(location, "test"),
        Seq(mspe1, mspe2)
      )

      val allocatedMemBeforeQuery = FlightAllocator.rootAllocator.getAllocatedMemory
      val qRes2 = dce.dispatcher.dispatch(ExecPlanWithClientParams(dce, ClientParams(60000), querySession),
        UnsupportedChunkSource()).runToFuture.futureValue.asInstanceOf[QueryResult]
      val rvRows2 = qRes2.result.map { rv =>
        val rows = rv.rows().map(r => (r.getLong(0))).toList
        rv.asInstanceOf[ArrowSerializedRangeVector].close()
        rows
      }
      rvRows2 shouldEqual List((0 to 100000 by 1000).toList, (0 to 100000 by 1000).toList,
                               (0 to 100000 by 1000).toList, (0 to 100000 by 1000).toList)

      qRes2.result.map(_.key.toString) shouldEqual
        List("/shard:0/b2[schema=schemaID:60110  _metric_=cpu_usage,tags={host: host1, region: region1}] [grp3]",
             "/shard:0/b2[schema=schemaID:60110  _metric_=cpu_usage,tags={host: host2, region: region1}] [grp0]",
             "/shard:0/b2[schema=schemaID:60110  _metric_=cpu_usage,tags={host: host1, region: region1}] [grp3]",
             "/shard:0/b2[schema=schemaID:60110  _metric_=cpu_usage,tags={host: host2, region: region1}] [grp0]")

      FlightAllocator.rootAllocator.getAllocatedMemory shouldEqual allocatedMemBeforeQuery
    }

    it("should be able to run a binary join query plan over flight server") {

      /*
      Construct the following query plan by hand.
      It is two mspe plans with two time series each joined with plus operator

      ---E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on SingleClusterFlightPlanDispatcher(Location{uri=grpc+tcp://localhost:8815},test)
      ----T~PeriodicSamplesMapper(start=0, step=1000, end=100000, window=None, functionId=None, rawSource=true, offsetMs=None)
      -----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(0,100000), filters=List(ColumnFilter(region,Equals(region1))), colName=None, schema=None) on SingleClusterFlightPlanDispatcher(Location{uri=grpc+tcp://localhost:8815},test)
      ----T~PeriodicSamplesMapper(start=0, step=1000, end=100000, window=None, functionId=None, rawSource=true, offsetMs=None)
      -----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(0,100000), filters=List(ColumnFilter(region,Equals(region1))), colName=None, schema=None) on SingleClusterFlightPlanDispatcher(Location{uri=grpc+tcp://localhost:8815},test)
      */

      val allocatedMemBeforeQuery = FlightAllocator.rootAllocator.getAllocatedMemory
      val qRes2 = bje.dispatcher.dispatch(ExecPlanWithClientParams(bje, ClientParams(60000), querySession),
        UnsupportedChunkSource()).runToFuture.futureValue.asInstanceOf[QueryResult]
      val rvRows2 = qRes2.result.map { rv =>
        val rows = rv.rows().map(r => (r.getLong(0))).toList
        rv.asInstanceOf[ArrowSerializedRangeVector].close()
        rows
      }
      rvRows2 shouldEqual List((0 to 100000 by 1000).toList, (0 to 100000 by 1000).toList)

      qRes2.result.map(_.key.toString) shouldEqual
        List("/shard:/Map(host -> host2, region -> region1)", "/shard:/Map(host -> host1, region -> region1)")

      FlightAllocator.rootAllocator.getAllocatedMemory shouldEqual allocatedMemBeforeQuery
    }

    it("should be able to do aggregation on top of binary join query plan over flight server") {

      val allocatedMemBeforeQuery = FlightAllocator.rootAllocator.getAllocatedMemory
      /*
      Add to the previous query plan with an aggregation on top.
      It is two mspe plans with two time series each joined with plus operator. Then a count aggregation on top.

      T~AggregatePresenter(aggrOp=Count, aggrParams=List(), rangeParams=RangeParams(0,1,100))
      -E~LocalPartitionReduceAggregateExec(aggrOp=Count, aggrParams=List()) on SingleClusterFlightPlanDispatcher(Location{uri=grpc+tcp://localhost:8815},test)
      --T~AggregateMapReduce(aggrOp=Count, aggrParams=List(), without=List(), by=List())
      ---E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on SingleClusterFlightPlanDispatcher(Location{uri=grpc+tcp://localhost:8815},test)
      ----T~PeriodicSamplesMapper(start=0, step=1000, end=100000, window=None, functionId=None, rawSource=true, offsetMs=None)
      -----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(0,100000), filters=List(ColumnFilter(region,Equals(region1))), colName=None, schema=None) on SingleClusterFlightPlanDispatcher(Location{uri=grpc+tcp://localhost:8815},test)
      ----T~PeriodicSamplesMapper(start=0, step=1000, end=100000, window=None, functionId=None, rawSource=true, offsetMs=None)
      -----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=0, chunkMethod=TimeRangeChunkScan(0,100000), filters=List(ColumnFilter(region,Equals(region1))), colName=None, schema=None) on SingleClusterFlightPlanDispatcher(Location{uri=grpc+tcp://localhost:8815},test)
      */

      bje.addRangeVectorTransformer(AggregateMapReduce(Count, Nil, None, Nil))

      val agg = LocalPartitionReduceAggregateExec(
        QueryContext(),
        SingleClusterFlightPlanDispatcher(location, "test"),
        Seq(bje),
        Count,
        Nil)
      agg.addRangeVectorTransformer(AggregatePresenter(Count, Nil, RangeParams(0, 1, 100), Nil))

      val qRes = agg.dispatcher.dispatch(ExecPlanWithClientParams(agg, ClientParams(60000), querySession),
        UnsupportedChunkSource()).runToFuture.futureValue.asInstanceOf[QueryResult]
      val rvRows = qRes.result.map { rv =>
        val rows = rv.rows().map(r => (r.getLong(0), r.getDouble(1))).toList
        rv.asInstanceOf[ArrowSerializedRangeVector].close()
        rows
      }
      rvRows shouldEqual List((0 to 100000 by 1000).map(_ -> 2).toList)

      qRes.result.map(_.key.toString) shouldEqual
        List("/shard:/Map()")

      FlightAllocator.rootAllocator.getAllocatedMemory shouldEqual allocatedMemBeforeQuery
    }

    it("should be able to do label values queries") {
      val allocatedMemBeforeQuery = FlightAllocator.rootAllocator.getAllocatedMemory
      val lve = LabelValuesExec(
        QueryContext(),
        SingleClusterFlightPlanDispatcher(location, "test"),
        timeseriesDatasetWithMetric.ref,
        0,
        filters,
        Seq("host"),
        0, 100000)

      val lvdce = LabelValuesDistConcatExec(
        QueryContext(),
        SingleClusterFlightPlanDispatcher(location, "test"),
        Seq(lve)
      )

      val qRes3 = lvdce.dispatcher.dispatch(ExecPlanWithClientParams(lvdce, ClientParams(60000), querySession),
        UnsupportedChunkSource()).runToFuture.futureValue.asInstanceOf[QueryResult]

      val rvRows3 = qRes3.result.map { rv =>
        val rows = rv.rows().map(_.getString(0)).toList
        rv.asInstanceOf[ArrowSerializedRangeVector].close()
        rows
      }
      rvRows3 shouldEqual List(List("host1", "host2"))

//      println(allocator.toVerboseString)
      FlightAllocator.rootAllocator.getAllocatedMemory shouldEqual allocatedMemBeforeQuery
    }

    it("should be able to do part keys queries") {
      val allocatedMemBeforeQuery = FlightAllocator.rootAllocator.getAllocatedMemory
      val pke = PartKeysExec(
        QueryContext(),
        SingleClusterFlightPlanDispatcher(location, "test"),
        timeseriesDatasetWithMetric.ref,
        0,
        filters,
        false,
        0, 100000)

      val pkdce = PartKeysDistConcatExec(
        QueryContext(),
        SingleClusterFlightPlanDispatcher(location, "test"),
        Seq(pke)
      )

      val qRes4 = pkdce.dispatcher.dispatch(ExecPlanWithClientParams(pkdce, ClientParams(60000), querySession),
        UnsupportedChunkSource()).runToFuture.futureValue.asInstanceOf[QueryResult]

      val rvRows4 = qRes4.result.map { rv =>
        val rows = rv.rows().map(_.getAny(0).asInstanceOf[Map[String, String]]).toList
        rv.asInstanceOf[ArrowSerializedRangeVector].close()
        rows
      }
      rvRows4 shouldEqual List(List(
        Map("_metric_" -> "cpu_usage", "_type_" -> "schemaID:60110", "host" -> "host2", "region" -> "region1"),
        Map("_metric_" -> "cpu_usage", "_type_" -> "schemaID:60110", "host" -> "host1", "region" -> "region1")))

      // println(allocator.toVerboseString)
      FlightAllocator.rootAllocator.getAllocatedMemory shouldEqual allocatedMemBeforeQuery
    }
  }
}
