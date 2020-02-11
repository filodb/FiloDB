package filodb.query.exec

import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import com.typesafe.config.{Config, ConfigFactory}
import monix.eval.Task
import monix.execution.Scheduler
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import filodb.core.MetricsTestData.{builder, timeseriesDataset, timeseriesSchema}
import filodb.core.TestData
import filodb.core.binaryrecord2.{RecordBuilder, RecordContainer}
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SomeData, TimeSeriesMemStore}
import filodb.core.metadata.{Column, Dataset, Schemas}
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.store.{AllChunkScan, InMemoryMetaStore, NullColumnStore}
import filodb.memory.MemFactory
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.query._
import kamon.Kamon

// So, this is effectively a test for NonLeafExecPlan
class InProcessPlanDispatcherSpec extends FunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  import ZeroCopyUTF8String._
  import filodb.core.{MachineMetricsData => MMD}

  override def beforeAll(): Unit = {
    memStore.setup(timeseriesDataset.ref, Schemas(timeseriesSchema), 0, TestData.storeConf)
    memStore.ingest(timeseriesDataset.ref, 0, SomeData(container, 0))
    memStore.setup(MMD.dataset1.ref, Schemas(MMD.schema1), 0, TestData.storeConf)
    memStore.ingest(MMD.dataset1.ref, 0, mmdSomeData)
    memStore.setup(MMD.histDataset.ref, Schemas(MMD.histDataset.schema), 0, TestData.storeConf)
    memStore.ingest(MMD.histDataset.ref, 0, MMD.records(MMD.histDataset, histData))
    memStore.setup(MMD.histMaxDS.ref, Schemas(MMD.histMaxDS.schema), 0, TestData.storeConf)
    memStore.ingest(MMD.histMaxDS.ref, 0, MMD.records(MMD.histMaxDS, histMaxData))
    memStore.refreshIndexForTesting(timeseriesDataset.ref)
    memStore.refreshIndexForTesting(MMD.dataset1.ref)
    memStore.refreshIndexForTesting(MMD.histDataset.ref)
    memStore.refreshIndexForTesting(MMD.histMaxDS.ref)
  }

  override def afterAll(): Unit = {
    memStore.shutdown()
  }

  val queryId: String = "InProcessPlanDispatcherSpec"

  implicit val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))
  implicit val scheduler: Scheduler = Scheduler(executor)
  implicit val timeout: FiniteDuration = FiniteDuration(5, TimeUnit.SECONDS)
  implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(30, Seconds),
    interval = Span(250, Millis))

  val dataset: Dataset = timeseriesDataset

  val config: Config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))

  val partKeyLabelValues: Map[String, String] =
    Map("__name__"->"http_req_total", "job"->"myCoolService", "instance"->"someHost:8787")
  val partTagsUTF8: Map[ZeroCopyUTF8String, ZeroCopyUTF8String] =
    partKeyLabelValues.map { case (k, v) => (k.utf8, v.utf8) }
  val now: Long = System.currentTimeMillis()
  val numRawSamples = 1000
  val reportingInterval = 10000
  val tuples: immutable.IndexedSeq[(Long, Double)] = (numRawSamples until 0).by(-1).map { n =>
    (now - n * reportingInterval, n.toDouble)
  }

  // NOTE: due to max-chunk-size in storeConf = 100, this will make (numRawSamples / 100) chunks
  // Be sure to reset the builder; it is in an Object so static and shared amongst tests
  builder.reset()
  tuples.map { t => SeqRowReader(Seq(t._1, t._2, partTagsUTF8)) }.foreach(builder.addFromReader(_, timeseriesSchema))
  val container: RecordContainer = builder.allContainers.head

  val mmdBuilder = new RecordBuilder(MemFactory.onHeapFactory)
  val mmdTuples: Stream[Seq[Any]] = MMD.linearMultiSeries().take(100)
  val mmdSomeData: SomeData = MMD.records(MMD.dataset1, mmdTuples)
  val histData: Stream[Seq[Any]] = MMD.linearHistSeries().take(100)
  val histMaxData: Stream[Seq[Any]] = MMD.histMax(histData)

  it ("inprocess dispatcher should execute and return monix task which in turn should return QueryResult") {
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val dispatcher: PlanDispatcher = InProcessPlanDispatcher

    val dummyDispatcher = DummyDispatcher(memStore, queryConfig)

    val execPlan1 = MultiSchemaPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher,
      timeseriesDataset.ref, 0, filters, AllChunkScan)
    val execPlan2 = MultiSchemaPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher,
      timeseriesDataset.ref, 0, filters, AllChunkScan)

    val sep = StitchRvsExec(queryId, dispatcher, Seq(execPlan1, execPlan2))
    val result = dispatcher.dispatch(sep, Kamon.currentSpan()).runAsync.futureValue

    result match {
      case e: QueryError => throw e.t
      case r: QueryResult =>
        r.result.size shouldEqual 1
        r.result.head.numRows shouldEqual Some(numRawSamples)
    }
  }

  import Column.ColumnType._

  it ("inprocess dispatcher should work when a child plan returns no time series") {
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val emptyFilters = Seq (ColumnFilter("__name__", Filter.Equals("nonsense".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val dispatcher: PlanDispatcher = InProcessPlanDispatcher

    val dummyDispatcher = DummyDispatcher(memStore, queryConfig)

    val execPlan1 = MultiSchemaPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher,
      timeseriesDataset.ref, 0, filters, AllChunkScan)
    val execPlan2 = MultiSchemaPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher,
      timeseriesDataset.ref, 0, emptyFilters, AllChunkScan)

    val sep = StitchRvsExec(queryId, dispatcher, Seq(execPlan1, execPlan2))
    val result = dispatcher.dispatch(sep, Kamon.currentSpan()).runAsync.futureValue

    result match {
      case e: QueryError => throw e.t
      case r: QueryResult =>
        r.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, DoubleColumn)
        r.result.size shouldEqual 1
        r.result.head.numRows shouldEqual Some(numRawSamples)
    }

    // Switch the order and make sure it's OK if the first result doesn't have any data
    val sep2 = StitchRvsExec(queryId, dispatcher, Seq(execPlan2, execPlan1))
    val result2 = dispatcher.dispatch(sep2, Kamon.currentSpan()).runAsync.futureValue

    result2 match {
      case e: QueryError => throw e.t
      case r: QueryResult =>
        r.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, DoubleColumn)
        r.result.size shouldEqual 1
        r.result.head.numRows shouldEqual Some(numRawSamples)
    }

    // Two children none of which returns data
    val sep3 = StitchRvsExec(queryId, dispatcher, Seq(execPlan2, execPlan2))
    val result3 = dispatcher.dispatch(sep3, Kamon.currentSpan()).runAsync.futureValue

    result3 match {
      case e: QueryError => throw e.t
      case r: QueryResult =>
        r.resultSchema.columns.map(_.colType) shouldEqual Nil
        r.result.size shouldEqual 0
    }
  }
}

case class DummyDispatcher(memStore: TimeSeriesMemStore, queryConfig: QueryConfig) extends PlanDispatcher {
  // run locally withing any check.
  override def dispatch(plan: ExecPlan, span: kamon.trace.Span)
                       (implicit sched: Scheduler,
                        timeout: FiniteDuration): Task[QueryResponse] = {
    plan.execute(memStore, queryConfig)
  }
}