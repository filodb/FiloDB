package filodb.query.exec

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core.{query, TestData}
import filodb.core.MetricsTestData._
import filodb.core.binaryrecord2.BinaryRecordRowReader
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SomeData, TimeSeriesMemStore}
import filodb.core.query.{ColumnFilter, Filter, SerializableRangeVector}
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

  val partKeyLabelValues = Seq(
    Map("__name__"->"http_req_total", "instance"->"someHost:8787", "job"->"myCoolService"),
    Map("__name__"->"http_resp_time", "instance"->"someHost:8787", "job"->"myCoolService")
  )

  val jobQueryResult1 = ArrayBuffer(("job", "myCoolService"))

  val partTagsUTF8s = partKeyLabelValues.map { _.map { case (k, v) => (k.utf8, v.utf8) }}
  val now = System.currentTimeMillis()
  val numRawSamples = 1000
  val reportingInterval = 10000
  val limit = 2
  val tuples = (numRawSamples until 0).by(-1).map { n =>
    (now - n * reportingInterval, n.toDouble)
  }

  // NOTE: due to max-chunk-size in storeConf = 100, this will make (numRawSamples / 100) chunks
  // Be sure to reset the builder; it is in an Object so static and shared amongst tests
  builder.reset()
  partTagsUTF8s.map( partTagsUTF8 => tuples.map { t => SeqRowReader(Seq(t._1, t._2, partTagsUTF8)) }
    .foreach(builder.addFromReader))
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
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Task[QueryResponse] = ???
  }

  it ("should read the job names from timeseriesindex matching the columnfilters") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
                       ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val execPlan = LabelValuesExec("someQueryId", now, limit, dummyDispatcher,
      timeseriesDataset.ref, 0, filters, Seq("job"), 10)

    val resp = execPlan.execute(memStore, timeseriesDataset, queryConfig).runAsync.futureValue
    val result = resp match {
      case QueryResult(id, _, response) => {
        val rv = response(0)
        rv.rows.size shouldEqual 1
        val record = rv.rows.next().asInstanceOf[BinaryRecordRowReader]
        rv.asInstanceOf[query.SerializableRangeVector].schema.toStringPairs(record.recordBase, record.recordOffset)
      }
    }
    result shouldEqual jobQueryResult1
  }


  it ("should not return any rows for wrong column filters") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total1".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val execPlan = PartKeysExec("someQueryId", now, limit, dummyDispatcher,
      timeseriesDataset.ref, 0, filters, now-5000, now)

    val resp = execPlan.execute(memStore, timeseriesDataset, queryConfig).runAsync.futureValue
    resp match {
      case QueryResult(_, _, results) => results.size shouldEqual 1
        results(0).rows.size shouldEqual 0
    }
  }

  it ("should read the label names/values from timeseriesindex matching the columnfilters") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val execPlan = PartKeysExec("someQueryId", now, limit, dummyDispatcher,
      timeseriesDataset.ref, 0, filters, now-5000, now)

    val resp = execPlan.execute(memStore, timeseriesDataset, queryConfig).runAsync.futureValue
    val result = resp match {
      case QueryResult(id, _, response) => {
        response.size shouldEqual 1
        response(0).rows.map (row => response(0).asInstanceOf[SerializableRangeVector]
          .schema.toStringPairs(row.getBlobBase(0),
          row.getBlobOffset(0)).toMap).toList
      }
    }
    result shouldEqual partKeyLabelValues
  }

  it ("should return one matching row (limit 1)") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    //Reducing limit results in truncated metadata response
    val execPlan = PartKeysExec("someQueryId", now, limit - 1, dummyDispatcher,
      timeseriesDataset.ref, 0, filters, now-5000, now)

    val resp = execPlan.execute(memStore, timeseriesDataset, queryConfig).runAsync.futureValue
    val result = resp match {
      case QueryResult(id, _, response) => {
        response.size shouldEqual 1
        response(0).rows.map (row => response(0).asInstanceOf[query.SerializableRangeVector]
          .schema.toStringPairs(row.getBlobBase(0),
          row.getBlobOffset(0)).toMap).toList
      }
    }
    result shouldEqual List(partKeyLabelValues(0))
  }

}

