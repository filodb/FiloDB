package filodb.core.memstore

import scala.collection.mutable.ArrayBuffer

import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler.Implicits.global
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core.MetricsTestData.{builder, timeseriesDataset}
import filodb.core.TestData
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.Column.ColumnType.{BinaryRecordColumn, StringColumn}
import filodb.core.query.{ColumnFilter, ColumnInfo, Filter, SeqMapConsumer}
import filodb.core.store.{InMemoryMetaStore, NullColumnStore}
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}

class TimeSeriesMemStoreForMetadataSpec extends FunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  import ZeroCopyUTF8String._

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))

  val partKeyLabelValues = Map("__name__"->"http_req_total", "job"->"myCoolService", "instance"->"someHost:8787")
  val metadataKeyLabelValues = Map("ignore" -> "ignore")
  val jobQueryResult1 = Map(("job".utf8, "myCoolService".utf8))
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

  override def beforeAll(): Unit = {
    memStore.setup(timeseriesDataset, 0, TestData.storeConf)
    memStore.ingest(timeseriesDataset.ref, 0, SomeData(container, 0))
    memStore.commitIndexForTesting(timeseriesDataset.ref)
  }

  it ("should read the metadata") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val metadata = memStore.partKeysWithFilters(timeseriesDataset.ref, 0, filters, now, now - 5000, 10)
    val schema = new RecordSchema(Seq(ColumnInfo("brc", ColumnType.BinaryRecordColumn)))
    val seqMapConsumer = new SeqMapConsumer()
    val record = metadata.next()
    val result = (schema.columnTypes.map(columnType => columnType match {
      case StringColumn => record.toString
      case BinaryRecordColumn => schema.consumeMapItems(record.partKeyBase, record.partKeyOffset, 0, seqMapConsumer)
        seqMapConsumer.pairs
      case _ => ???
    }))
    result.head shouldEqual jobQueryResult2
  }

  it ("should read the metadata label values for instance") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val metadata = memStore.indexValuesWithFilters(timeseriesDataset.ref, 0,
      filters, "instance", now, now - 5000, 10)

    metadata.hasNext shouldEqual true
    metadata.next.toString shouldEqual "someHost:8787"
  }

}
