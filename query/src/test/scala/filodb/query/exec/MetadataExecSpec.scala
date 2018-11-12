package filodb.query.exec

import scala.collection.mutable.ArrayBuffer
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
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SomeData, TimeSeriesMemStore}
import filodb.core.metadata.Column.ColumnType.{PartitionKeyColumn, StringColumn}
import filodb.core.query.{ColumnFilter, Filter, RecordList, SeqMapConsumer}
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

  val partKeyLabelValues = Map("__name__"->"http_req_total", "job"->"myCoolService", "instance"->"someHost:8787")
  val jobQueryResult1 = List("myCoolService")
  val jobQueryResult2 = ArrayBuffer(("__name__".utf8, "http_req_total".utf8),
    ("instance".utf8, "someHost:8787".utf8),
    ("job".utf8, "myCoolService".utf8))

  val partTagsUTF8 = partKeyLabelValues.map { case (k, v) => (k.utf8, v.utf8) }
  val now = System.currentTimeMillis()
  val numRawSamples = 1000
  val reportingInterval = 10000
  val tuples = (numRawSamples until 0).by(-1).map { n =>
    (now - n * reportingInterval, n.toDouble)
  }

  // NOTE: due to max-chunk-size in storeConf = 100, this will make (numRawSamples / 100) chunks
  tuples.map { t => SeqRowReader(Seq(t._1, t._2, partTagsUTF8)) }.foreach(builder.addFromReader)
  val container = builder.allContainers.head

  implicit val execTimeout = 5.seconds

  override def beforeAll(): Unit = {
    memStore.setup(timeseriesDataset, 0, TestData.storeConf)
    memStore.ingest(timeseriesDataset.ref, 0, SomeData(container, 0))
    memStore.commitIndexForTesting(timeseriesDataset.ref)
  }

  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: BaseExecPlan)
                         (implicit sched: ExecutionContext,
                          timeout: FiniteDuration): Task[QueryResponse] = ???
  }

  it ("should read the job names from timeseriesindex matching the columnfilters") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
                       ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val execPlan = LabelValuesExecLeafPlan("someQueryId", now, numRawSamples, dummyDispatcher,
      timeseriesDataset.ref, 0, filters, "job")

    val resp = execPlan.execute(memStore, timeseriesDataset, queryConfig).runAsync.futureValue
    val result = resp match {
      case RecordListResult(id, response) => {
        response.rows.size shouldEqual 1
        val recordList = response.asInstanceOf[RecordList]
        val record = response.rows.next()
        val seqMapConsumer = new SeqMapConsumer()
        recordList.schema.columns.map(column => column.colType match {
          case StringColumn => record.getString(0)
          case _ => ???
        })
      }
    }
    result shouldEqual jobQueryResult1
  }


  it ("should not return any rows for wrong column filters") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total1".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val execPlan = SeriesKeyExecLeafPlan("someQueryId", now, numRawSamples, dummyDispatcher,
      timeseriesDataset.ref, 0, filters, now-5000, now, Seq.empty)

    val resp = execPlan.execute(memStore, timeseriesDataset, queryConfig).runAsync.futureValue
    val result = resp.asInstanceOf[RecordListResult]
    result.result.rows.size shouldEqual 0
  }

  it ("should read the label names/values from timeseriesindex matching the columnfilters") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val execPlan = SeriesKeyExecLeafPlan("someQueryId", now, numRawSamples, dummyDispatcher,
      timeseriesDataset.ref, 0, filters, now-5000, now, Seq.empty)

    val resp = execPlan.execute(memStore, timeseriesDataset, queryConfig).runAsync.futureValue
    val result = resp match {
      case RecordListResult(id, response) => {
        response.rows.size shouldEqual 1
        val recordList = response.asInstanceOf[RecordList]
        val record = response.rows.next()
        val seqMapConsumer = new SeqMapConsumer()
        recordList.schema.columns.map(column => column.colType match {
          case StringColumn => record.getString(0)
          case PartitionKeyColumn => new RecordSchema(Seq(PartitionKeyColumn)).consumeMapItems(record.getBlobBase(0),
            record.getBlobOffset(0), 0, seqMapConsumer)
            seqMapConsumer.pairs
          case _ => ???
        })
      }
    }
    result shouldEqual List(jobQueryResult2)
  }

}

