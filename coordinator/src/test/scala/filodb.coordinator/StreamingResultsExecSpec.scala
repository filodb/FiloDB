package filodb.coordinator

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler.Implicits.global
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SomeData, TimeSeriesMemStore}
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.core.store.{InMemoryMetaStore, NullColumnStore, TimeRangeChunkScan}
import filodb.core.{DatasetRef, TestData}
import filodb.memory.MemFactory
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.query._
import filodb.query.exec.{AggregateMapReduce, ClientParams, ExecPlanWithClientParams, InProcessPlanDispatcher, LocalPartitionReduceAggregateExec, MultiSchemaPartitionsExec}

class StreamingResultsExecSpec extends AnyFunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  import Schemas.promCounter
  import ZeroCopyUTF8String._
  import filodb.core.{MachineMetricsData => MMD}

  val dsRef = DatasetRef("raw-metrics")
  val builder = new RecordBuilder(MemFactory.onHeapFactory)

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val allConfig = ConfigFactory.parseString("filodb.query.streaming-query-results-enabled = true")
    .withFallback(ConfigFactory.load("application_test.conf")).resolve()

  val filodbConfig = allConfig.getConfig("filodb")
  val queryConfig = QueryConfig(filodbConfig.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)
  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(filodbConfig, new NullColumnStore, new InMemoryMetaStore(), Some(policy))
  val inProcessPlanDispatcher = InProcessPlanDispatcher(queryConfig)

  val metric = "http_req_total"
  val partKeyLabelValues = Map("job" -> "myCoolService", "instance" -> "someHost:8787", "host" -> "host-1")
  val partKeyKVWithMetric = partKeyLabelValues ++ Map("_metric_" -> metric)
  val partTagsUTF8 = partKeyLabelValues.map { case (k, v) => (k.utf8, v.utf8) }
  val now = System.currentTimeMillis()
  val numRawSamples = 1000
  val reportingInterval = 10000
  val tuples = (numRawSamples until 0).by(-1).map { n =>
    (now - n * reportingInterval, n.toDouble)
  }
  val tupleCounts = (numRawSamples until 0).by(-1).map { n =>
    (now - n * reportingInterval, 2.0)
  }

  val schemas = Schemas(promCounter.partition,
                        Map(promCounter.name -> promCounter,
                            "histogram" -> MMD.histDataset.schema,
                            Schemas.dsGauge.name -> Schemas.dsGauge))

  // NOTE: due to max-chunk-size in storeConf = 100, this will make (numRawSamples / 100) chunks
  // Be sure to reset the builder; it is in an Object so static and shared amongst tests
  builder.reset()
  tuples.map { t => SeqRowReader(Seq(t._1, t._2, metric.utf8, partTagsUTF8)) }
        .foreach(builder.addFromReader(_, promCounter))
  val container = builder.allContainers.head

  val mmdTuples = MMD.linearMultiSeries().take(100)
  val mmdSomeData = MMD.records(MMD.dataset1, mmdTuples)
  val histData = MMD.linearHistSeries().take(100)
  val system = ActorSystemHolder.createActorSystem("testActorSystem", allConfig)

  implicit val execTimeout = 5.seconds

  override def beforeAll(): Unit = {
    memStore.setup(dsRef, schemas, 0, TestData.storeConf, 2)
    memStore.ingest(dsRef, 0, SomeData(container, 0))
    memStore.ingest(dsRef, 0, MMD.records(MMD.histDataset, histData))

    // set up shard, but do not ingest data to simulate an empty shard
    memStore.setup(dsRef, schemas, 1, TestData.storeConf, 2)

    memStore.setup(MMD.dataset1.ref, Schemas(MMD.schema1), 0, TestData.storeConf, 1)
    memStore.ingest(MMD.dataset1.ref, 0, mmdSomeData)

    memStore.refreshIndexForTesting(dsRef)
    memStore.refreshIndexForTesting(MMD.dataset1.ref)
    memStore.refreshIndexForTesting(MMD.histMaxMinDS.ref)
  }

  override def afterAll(): Unit = {
    memStore.shutdown()
    // TODO ActorSystemHolder.system.shutdown
  }

  it("should read raw samples from Memstore using IntervalSelector in result streaming mode") {
    import ZeroCopyUTF8String._
    val filters = Seq(ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)),
      ColumnFilter("instance", Filter.NotEquals("SomeJob".utf8)))
    // read from an interval of 100000ms, resulting in 11 samples
    val startTime = now - numRawSamples * reportingInterval
    val endTime = now - (numRawSamples - 10) * reportingInterval

    val execPlan = MultiSchemaPartitionsExec(QueryContext(), inProcessPlanDispatcher,
      dsRef, 0, filters, TimeRangeChunkScan(startTime, endTime), "_metric_")

    querySession.queryStats.clear() // so this can be run as a standalone test

    val resp = execPlan.executeStreaming(memStore, querySession).toListL.runToFuture.futureValue
    resp.size shouldEqual 3
    val dataRead = resp(1).asInstanceOf[StreamQueryResult].result(0).rows().map(r => (r.getLong(0), r.getDouble(1))).toList
    dataRead shouldEqual tuples.take(11)
    val partKeyRead = resp(1).asInstanceOf[StreamQueryResult].result(0).key.labelValues.map(lv => (lv._1.asNewString, lv._2.asNewString))
    partKeyRead shouldEqual partKeyKVWithMetric
    resp(2).asInstanceOf[StreamQueryResultFooter].queryStats.getResultBytesCounter().get() shouldEqual 297
    resp(2).asInstanceOf[StreamQueryResultFooter].queryStats.getCpuNanosCounter().get() > 0 shouldEqual true
    resp(2).asInstanceOf[StreamQueryResultFooter].queryStats.getDataBytesScannedCounter().get() shouldEqual 48
    resp(2).asInstanceOf[StreamQueryResultFooter].queryStats.getTimeSeriesScannedCounter().get() shouldEqual 1

  }

  it("should execute Aggregation Exec Plans from Memstore in result streaming mode") {
    import ZeroCopyUTF8String._
    querySession.queryStats.clear()
    val filters = Seq(ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)),
      ColumnFilter("instance", Filter.NotEquals("SomeJob".utf8)))
    // read from an interval of 100000ms, resulting in 11 samples
    val startTime = now - numRawSamples * reportingInterval
    val endTime = now - (numRawSamples - 10) * reportingInterval
    val queryContext = QueryContext()

    val mspExec = MultiSchemaPartitionsExec(queryContext, inProcessPlanDispatcher,
      dsRef, 0, filters, TimeRangeChunkScan(startTime, endTime), "_metric_")
    mspExec.addRangeVectorTransformer(AggregateMapReduce(AggregationOperator.Count, Nil))

    val reducerExec = LocalPartitionReduceAggregateExec(queryContext, inProcessPlanDispatcher,
                                                    Seq(mspExec, mspExec), AggregationOperator.Count, Nil)

    val resp = reducerExec.executeStreaming(memStore, querySession).toListL.runToFuture.futureValue
    resp.size shouldEqual 3
    val dataRead = resp(1).asInstanceOf[StreamQueryResult].result(0).rows().map(r => (r.getLong(0), r.getDouble(1))).toList
    dataRead shouldEqual tupleCounts.take(11)
    val partKeyRead = resp(1).asInstanceOf[StreamQueryResult].result(0).key.labelValues.map(lv => (lv._1.asNewString, lv._2.asNewString))
    partKeyRead shouldEqual Map()
    resp(2).asInstanceOf[StreamQueryResultFooter].queryStats.getResultBytesCounter().get() shouldEqual 660
    resp(2).asInstanceOf[StreamQueryResultFooter].queryStats.getCpuNanosCounter().get() > 0 shouldEqual true
    resp(2).asInstanceOf[StreamQueryResultFooter].queryStats.getDataBytesScannedCounter().get() shouldEqual 96
    resp(2).asInstanceOf[StreamQueryResultFooter].queryStats.getTimeSeriesScannedCounter().get() shouldEqual 2
  }

  ignore("should execute Aggregation Exec Plans from Memstore in result streaming mode using actor plan dispatcher") {

    val queryRef = system.actorOf(QueryActor.props(memStore, MMD.dataset1, schemas,
                    new ShardMapper(0), 0))
    val actorPlanDispatcher = ActorPlanDispatcher(queryRef, "testCluster")

    import ZeroCopyUTF8String._
    querySession.queryStats.clear()
    val filters = Seq(ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)),
      ColumnFilter("instance", Filter.NotEquals("SomeJob".utf8)))
    // read from an interval of 100000ms, resulting in 11 samples
    val startTime = now - numRawSamples * reportingInterval
    val endTime = now - (numRawSamples - 10) * reportingInterval
    val queryContext = QueryContext()

    val mspExec1 = MultiSchemaPartitionsExec(queryContext, actorPlanDispatcher,
      dsRef, 0, filters, TimeRangeChunkScan(startTime, endTime), "_metric_")
    mspExec1.addRangeVectorTransformer(AggregateMapReduce(AggregationOperator.Count, Nil))

    val mspExec2 = MultiSchemaPartitionsExec(queryContext, actorPlanDispatcher,
      dsRef, 0, filters, TimeRangeChunkScan(startTime, endTime), "_metric_")
    mspExec2.addRangeVectorTransformer(AggregateMapReduce(AggregationOperator.Count, Nil))

    val reducerExec = LocalPartitionReduceAggregateExec(queryContext, actorPlanDispatcher,
      Seq(mspExec1, mspExec2), AggregationOperator.Count, Nil)

    println(s"Executing ${reducerExec.printTree()}")

    val resp = actorPlanDispatcher.dispatchStreaming(
      ExecPlanWithClientParams(reducerExec, ClientParams(5000)), memStore).toListL.runToFuture.futureValue
    resp.size shouldEqual 3
    val dataRead = resp(1).asInstanceOf[StreamQueryResult].result(0).rows().map(r => (r.getLong(0), r.getDouble(1))).toList
    dataRead shouldEqual tupleCounts.take(11)
    val partKeyRead = resp(1).asInstanceOf[StreamQueryResult].result(0).key.labelValues.map(lv => (lv._1.asNewString, lv._2.asNewString))
    partKeyRead shouldEqual Map()
    // Assertion fails on github PRB, this value comes as 880
    resp(2).asInstanceOf[StreamQueryResultFooter].queryStats.getResultBytesCounter().get() shouldEqual 660
    resp(2).asInstanceOf[StreamQueryResultFooter].queryStats.getCpuNanosCounter().get() > 0 shouldEqual true
    resp(2).asInstanceOf[StreamQueryResultFooter].queryStats.getDataBytesScannedCounter().get() shouldEqual 96
    resp(2).asInstanceOf[StreamQueryResultFooter].queryStats.getTimeSeriesScannedCounter().get() shouldEqual 2
  }


}

