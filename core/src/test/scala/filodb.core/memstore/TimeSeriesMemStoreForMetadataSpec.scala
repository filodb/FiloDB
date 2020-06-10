package filodb.core.memstore

import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler.Implicits.global
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core.MetricsTestData.{builder, timeseriesDataset, timeseriesSchema}
import filodb.core.TestData
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.store.{InMemoryMetaStore, NullColumnStore}
import filodb.core.binaryrecord2.RecordContainer
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}

class TimeSeriesMemStoreForMetadataSpec extends FunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  import ZeroCopyUTF8String._

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))

  val metadataKeyLabelValues = Map("ignore" -> "ignore")
  val jobQueryResult1 = Map(("job".utf8, "myCoolService".utf8))
  val jobQueryResult2 = Map(("job".utf8, "myCoolService".utf8),
    ("id".utf8, "0".utf8),
    ("__name__".utf8, "http_req_total".utf8),
    ("_type_".utf8 -> "schemaID:35859".utf8),
    ("instance".utf8, "someHost:8787".utf8)
    )

  val now = System.currentTimeMillis()
  val numRawSamples = 1000
  val reportingInterval = 10000
  val container = createRecordContainer(0, 10)

  override def beforeAll(): Unit = {
    memStore.setup(timeseriesDataset.ref, Schemas(timeseriesSchema), 0, TestData.storeConf)
    memStore.ingest(timeseriesDataset.ref, 0, SomeData(container, 0))
    memStore.refreshIndexForTesting(timeseriesDataset.ref)
  }

  def createRecordContainer(start: Int, end: Int): RecordContainer = {
    for(i <- start until end) {
      val partKey = Map("__name__".utf8 -> "http_req_total".utf8,
        "job".utf8 -> "myCoolService".utf8,
        "instance".utf8 -> "someHost:8787".utf8,
        "id".utf8 -> i.toString.utf8)
      builder.addFromReader(SeqRowReader(Seq(now - i * reportingInterval, 10.0, partKey)),
        timeseriesSchema)
    }
    builder.allContainers.head
  }

  it ("should search the metadata of in-memory partitions") {
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)),
        ColumnFilter("id", Filter.Equals("0".utf8)))
    val metadata = memStore.partKeysWithFilters(timeseriesDataset.ref, 0, filters, false, now, now - 5000, 10)
    val tsPartData = metadata.next()
    tsPartData shouldEqual jobQueryResult2
  }

  it("should search the metadata of evicted partitions") {

    //Evict partition "0"
    val shard = memStore.getShardE(timeseriesDataset.ref, 0)
    val blockFactory = shard.overflowBlockFactory
    val part = shard.partitions.get(0)
    part.switchBuffers(blockFactory, encode = true)
    shard.updatePartEndTimeInIndex(part, part.timestampOfLatestSample)
    memStore.refreshIndexForTesting(timeseriesDataset.ref)
    val endTime = part.timestampOfLatestSample
    val startTime = part.earliestTime

    memStore.ingest(timeseriesDataset.ref, 0, SomeData(createRecordContainer(10, 20), 0))
    memStore.refreshIndexForTesting(timeseriesDataset.ref)

    //Search metadata after eviction
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)),
      ColumnFilter("id", Filter.Equals("0".utf8)))
    val metadata = memStore.partKeysWithFilters(timeseriesDataset.ref, 0, filters, true, endTime, endTime - 5000, 10)
    val tsPartData = metadata.next()
    val jobQueryResult = jobQueryResult2 ++
      Map(("_firstSampleTime_".utf8, startTime.toString.utf8), ("_lastSampleTime_".utf8, endTime.toString.utf8))
    tsPartData shouldEqual jobQueryResult
  }

  it ("should read the metadata label values for instance") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val metadata = memStore.labelValuesWithFilters(timeseriesDataset.ref, 0,
      filters, Seq("instance"), now, now - 5000, 10)

    metadata.hasNext shouldEqual true
    metadata.next shouldEqual Map("instance".utf8 -> "someHost:8787".utf8)
  }

}
