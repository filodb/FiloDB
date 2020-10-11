package filodb.query.exec

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.core.store.{AllChunkScan, InMemoryMetaStore, NullColumnStore}
import filodb.core.{DatasetRef, TestData}
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SomeData, TimeSeriesMemStore}
import filodb.core.metadata.Column.ColumnType.{DoubleColumn, TimestampColumn}
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.memory.MemFactory
import filodb.query.{QueryResponse, QueryResult}

object SplitLocalPartitionDistConcatExecSpec {
  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlan)
                         (implicit sched: Scheduler): Task[QueryResponse] = ???
  }

  val dsRef = DatasetRef("raw-metrics")
  val dummyPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher, dsRef, 0, Nil, AllChunkScan)

  val builder = new RecordBuilder(MemFactory.onHeapFactory)
}

class SplitLocalPartitionDistConcatExecSpec extends AnyFunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  import ZeroCopyUTF8String._
  import filodb.core.{MachineMetricsData => MMD}
  import Schemas.promCounter
  import SplitLocalPartitionDistConcatExecSpec._

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)
  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))

  val metric = "http_req_total"
  val partKeyLabelValues = Map("job" -> "myCoolService", "instance" -> "someHost:8787")
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

  implicit val execTimeout = 5.seconds

  override def beforeAll(): Unit = {
    memStore.setup(dsRef, schemas, 0, TestData.storeConf)
    memStore.ingest(dsRef, 0, SomeData(container, 0))

    memStore.setup(MMD.dataset1.ref, Schemas(MMD.schema1), 0, TestData.storeConf)
    memStore.ingest(MMD.dataset1.ref, 0, mmdSomeData)

    memStore.refreshIndexForTesting(dsRef)
    memStore.refreshIndexForTesting(MMD.dataset1.ref)
  }

  override def afterAll(): Unit = {
    memStore.shutdown()
  }

  it ("should stitch the child exec results with different splits, result schema should have fixedVectorLen of both results") {
    import ZeroCopyUTF8String._

    val filters = Seq (ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val step = reportingInterval*100
    val start1 = now - numRawSamples * reportingInterval + step
    val end1 = start1 + step * 3 // 4 steps
    val start2 = end1 + step
    val end2 = now // 6 steps
    val dispacher = DummyDispatcher(memStore, querySession)

    val execPlan1 = MultiSchemaPartitionsExec(QueryContext(), dispacher,
      dsRef, 0, filters, AllChunkScan)
    execPlan1.addRangeVectorTransformer(new PeriodicSamplesMapper(start1, step, end1, Some(reportingInterval * 100),
      Some(InternalRangeFunction.SumOverTime), QueryContext()))

    val execPlan2 = MultiSchemaPartitionsExec(QueryContext(), dispacher,
      dsRef, 0, filters, AllChunkScan)
    execPlan2.addRangeVectorTransformer(new PeriodicSamplesMapper(start2, step, end2, Some(reportingInterval * 100),
      Some(InternalRangeFunction.SumOverTime), QueryContext()))

    val distConcatExec = SplitLocalPartitionDistConcatExec(QueryContext(), dispacher, Seq(execPlan1, execPlan2))

    val resp = distConcatExec.execute(memStore, querySession).runAsync.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, DoubleColumn)
    result.result.size shouldEqual 1
    val partKeyRead = result.result(0).key.labelValues.map(lv => (lv._1.asNewString, lv._2.asNewString))
    partKeyRead shouldEqual partKeyKVWithMetric
    result.resultSchema.fixedVectorLen shouldEqual Some(4) // first schema vectorLength
  }
}
