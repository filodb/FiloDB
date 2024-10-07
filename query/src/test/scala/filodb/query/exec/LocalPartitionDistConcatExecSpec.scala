package filodb.query.exec

import com.typesafe.config.ConfigFactory
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SomeData, TimeSeriesMemStore}
import filodb.core.metadata.Column.ColumnType.{DoubleColumn, TimestampColumn}
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.core.store.{AllChunkScan, ChunkSource, InMemoryMetaStore, NullColumnStore, TimeRangeChunkScan}
import filodb.core.{DatasetRef, GlobalConfig, TestData}
import filodb.memory.MemFactory
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.query._
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._

object LocalPartitionDistConcatExecSpec {
  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)
  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlanWithClientParams, source: ChunkSource)
                         (implicit sched: Scheduler): Task[QueryResponse] = {
      plan.execPlan.execute(source, querySession)(global)
    }

    override def clusterName: String = ???

    override def isLocalCall: Boolean = ???

    override def dispatchStreaming(plan: ExecPlanWithClientParams,
                                   source: ChunkSource)(implicit sched: Scheduler): Observable[StreamQueryResponse] = ???
  }

  val dsRef = DatasetRef("raw-metrics")
  val dummyPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher, dsRef, 0, Nil, AllChunkScan, "_metric_")

  val builder = new RecordBuilder(MemFactory.onHeapFactory)
}


class LocalPartitionDistConcatExecSpec extends AnyFunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  import LocalPartitionDistConcatExecSpec._
  import Schemas.promCounter
  import ZeroCopyUTF8String._
  import filodb.core.{MachineMetricsData => MMD}

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))

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

  val mmdBuilder = new RecordBuilder(MemFactory.onHeapFactory)
  val mmdTuples = MMD.linearMultiSeries().take(100)
  val mmdSomeData = MMD.records(MMD.dataset1, mmdTuples)
  val histData = MMD.linearHistSeries().take(100)
  val histMaxMinData = MMD.histMaxMin(histData)

  val histDataDisabledWS = MMD.linearHistSeries(ws = GlobalConfig.workspacesDisabledForMaxMin.get.head).take(100)
  val histMaxMinDataDisabledWS = MMD.histMaxMin(histDataDisabledWS)

  implicit val execTimeout = 5.seconds

  override def beforeAll(): Unit = {
    memStore.setup(dsRef, schemas, 0, TestData.storeConf, 2)
    memStore.ingest(dsRef, 0, SomeData(container, 0))
    memStore.ingest(dsRef, 0, MMD.records(MMD.histDataset, histData))

    // set up shard, but do not ingest data to simulate an empty shard
    memStore.setup(dsRef, schemas, 1, TestData.storeConf, 2)

    memStore.setup(MMD.dataset1.ref, Schemas(MMD.schema1), 0, TestData.storeConf, 1)
    memStore.ingest(MMD.dataset1.ref, 0, mmdSomeData)
    memStore.setup(MMD.histMaxMinDS.ref, Schemas(MMD.histMaxMinDS.schema), 0, TestData.storeConf, 1)
    memStore.ingest(MMD.histMaxMinDS.ref, 0, MMD.records(MMD.histMaxMinDS, histMaxMinData))
    memStore.ingest(MMD.histMaxMinDS.ref, 0, MMD.records(MMD.histMaxMinDS, histMaxMinDataDisabledWS))

    memStore.refreshIndexForTesting(dsRef)
    memStore.refreshIndexForTesting(MMD.dataset1.ref)
    memStore.refreshIndexForTesting(MMD.histMaxMinDS.ref)
  }

  override def afterAll(): Unit = {
    memStore.shutdown()
  }

  it("should be able to hande complex query like sum(topk + sum)") {
    val startTime = now - numRawSamples * reportingInterval
    val endTime = now - (numRawSamples - 10) * reportingInterval
    val filters = Seq(ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)))
    val leafExecPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher,
      dsRef, 0, filters, TimeRangeChunkScan(startTime, endTime), "_metric_")
    val transformer = PeriodicSamplesMapper(startTime, reportingInterval, endTime, None, None, QueryContext())
    leafExecPlan.addRangeVectorTransformer(transformer)

    val topK = LocalPartitionReduceAggregateExec(QueryContext(), dummyDispatcher,
      Array[ExecPlan](leafExecPlan), AggregationOperator.TopK, Seq(1.0))
    val sum = LocalPartitionReduceAggregateExec(QueryContext(), dummyDispatcher,
      Array[ExecPlan](leafExecPlan), AggregationOperator.Sum, Seq(0))
    val binaryjoin1 = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Seq(topK), Seq(sum), BinaryOperator.ADD, Cardinality.OneToOne,
      None, Nil, Nil, "__name__", Some(RvRange(startMs = 0, endMs = 19, stepMs = 1)))
    val binaryjoin2 = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Seq(sum), Seq(topK), BinaryOperator.ADD, Cardinality.OneToOne,
      None, Nil, Nil, "__name__", Some(RvRange(startMs = 0, endMs = 19, stepMs = 1)))
    val execPlan = LocalPartitionReduceAggregateExec(QueryContext(), dummyDispatcher,
      Seq(binaryjoin1, binaryjoin2), AggregationOperator.Sum, Seq(0))


    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, DoubleColumn)
    result.resultSchema.fixedVectorLen.nonEmpty shouldEqual true
    result.resultSchema.fixedVectorLen.get shouldEqual 11
  }


  it("should be able to hande complex query like sum(count + sum)") {
    val startTime = now - numRawSamples * reportingInterval
    val endTime = now - (numRawSamples - 10) * reportingInterval
    val filters = Seq(ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)))
    val leafExecPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher,
      dsRef, 0, filters, TimeRangeChunkScan(startTime, endTime), "_metric_")
    val transformer = PeriodicSamplesMapper(startTime, reportingInterval, endTime, None, None, QueryContext())
    leafExecPlan.addRangeVectorTransformer(transformer)

    val count = LocalPartitionReduceAggregateExec(QueryContext(), dummyDispatcher,
      Array[ExecPlan](leafExecPlan), AggregationOperator.Count, Seq(1.0))
    val sum = LocalPartitionReduceAggregateExec(QueryContext(), dummyDispatcher,
      Array[ExecPlan](leafExecPlan), AggregationOperator.Sum, Seq(0))
    val binaryjoin1 = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Seq(count), Seq(sum), BinaryOperator.ADD, Cardinality.OneToOne,
      None, Nil, Nil, "__name__", Some(RvRange(startMs = 0, endMs = 19, stepMs = 1)))
    val binaryjoin2 = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Seq(sum), Seq(count), BinaryOperator.ADD, Cardinality.OneToOne,
      None, Nil, Nil, "__name__", Some(RvRange(startMs = 0, endMs = 19, stepMs = 1)))
    val execPlan = LocalPartitionReduceAggregateExec(QueryContext(), dummyDispatcher,
      Seq(binaryjoin1, binaryjoin2), AggregationOperator.Sum, Seq(0))


    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, DoubleColumn)
    result.resultSchema.fixedVectorLen.nonEmpty shouldEqual true
    result.resultSchema.fixedVectorLen.get shouldEqual 11
  }
}
