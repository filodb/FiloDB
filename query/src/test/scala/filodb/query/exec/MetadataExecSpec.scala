package filodb.query.exec

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core.MetricsTestData._
import filodb.core.TestData
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SomeData, TimeSeriesMemStore}
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.store.{InMemoryMetaStore, NullColumnStore}
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.query._

class MetadataExecSpec extends FunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  import ZeroCopyUTF8String._

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))

  val partKeyLabelValues = Map("__name__"->"http_req_total", "instance"->"someHost:8787", "job"->"myCoolService")
  val jobQueryResult1 = "myCoolService"

  val partTagsUTF8 = partKeyLabelValues.map { case (k, v) => (k.utf8, v.utf8) }
  val now = System.currentTimeMillis()
  val numRawSamples = 1000
  val reportingInterval = 10000
  val tuples = (numRawSamples until 0).by(-1).map { n =>
    (now - n * reportingInterval, n.toDouble)
  }

  // NOTE: due to max-chunk-size in storeConf = 100, this will make (numRawSamples / 100) chunks
  // Be sure to reset the builder; it is in an Object so static and shared amongst tests
  builder.reset()
  tuples.map { t => SeqRowReader(Seq(t._1, t._2, partTagsUTF8)) }.foreach(builder.addFromReader)
  val container = builder.allContainers.head

  implicit val execTimeout = 5.seconds

  override def beforeAll(): Unit = {
    memStore.setup(timeseriesDataset, 0, TestData.storeConf)
    memStore.ingest(timeseriesDataset.ref, 0, SomeData(container, 0))
    memStore.commitIndexForTesting(timeseriesDataset.ref)
  }

  override def afterAll(): Unit = {
    memStore.shutdown()
  }

  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlan)
                         (implicit sched: ExecutionContext,
                          timeout: FiniteDuration): Task[QueryResponse] = ???
  }

  it ("should read the job names from timeseriesindex matching the columnfilters") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
                       ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val execPlan = LabelValuesExec("someQueryId", now, numRawSamples, dummyDispatcher,
      timeseriesDataset.ref, 0, filters, "job", 10)

    val resp = execPlan.execute(memStore, timeseriesDataset, queryConfig).runAsync.futureValue
    val result = resp match {
      case QueryResult(id, _, response) => {
        val rv = response(0)
        rv.rows.size shouldEqual 1
        val record = rv.rows.next()
        record.filoUTF8String(0).toString
      }
    }
    result shouldEqual jobQueryResult1
  }


  it ("should not return any rows for wrong column filters") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total1".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val execPlan = PartKeysExec("someQueryId", now, numRawSamples, dummyDispatcher,
      timeseriesDataset.ref, 0, filters, now-5000, now)

    val resp = execPlan.execute(memStore, timeseriesDataset, queryConfig).runAsync.futureValue
    resp match {
      case QueryResult(_, _, results) => results.size shouldEqual 1
        results(0).rows.size shouldEqual 0
    }
  }

  it ("should read the label names/values from timeseriesindex matching the columnfilters") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val execPlan = PartKeysExec("someQueryId", now, numRawSamples, dummyDispatcher,
      timeseriesDataset.ref, 0, filters, now-5000, now)

    val resp = execPlan.execute(memStore, timeseriesDataset, queryConfig).runAsync.futureValue
    val result = resp match {
      case QueryResult(id, _, response) => {
        response.size shouldEqual 1
        val record = response(0).rows.next()
        val schema = response(0).schema
        schema.toStringPairs(record.getBlobBase(0), record.getBlobOffset(0))
      }
    }
    result shouldEqual partKeyLabelValues.toSeq
  }

}

