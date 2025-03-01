package filodb.core.memstore

import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler.Implicits.global
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import filodb.core.MetricsTestData.{builder, timeseriesDataset, timeseriesSchema}
import filodb.core.TestData
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter, QueryContext, QuerySession}
import filodb.core.store.{InMemoryMetaStore, NullColumnStore}
import filodb.core.binaryrecord2.RecordContainer
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class TimeSeriesMemStoreForMetadataSpec extends AnyFunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
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
    memStore.setup(timeseriesDataset.ref, Schemas(timeseriesSchema), 0, TestData.storeConf, 1)
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
    val blockFactory = shard.blockFactoryPool.checkoutForOverflow(0)
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
      filters, Seq("instance"), now, now - 5000, QuerySession.makeForTestingOnly, 10)

    metadata.hasNext shouldEqual true
    metadata.next shouldEqual Map("instance".utf8 -> "someHost:8787".utf8)
  }

  it ("should read the metadata label values for multiple labels: instance, _type_") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val metadata = memStore.labelValuesWithFilters(timeseriesDataset.ref, 0,
      filters, labelNames = Seq("instance", "_type_"), now, now - 5000, QuerySession.makeForTestingOnly, 10)

    metadata.hasNext shouldEqual true
    metadata.next shouldEqual Map("instance".utf8 -> "someHost:8787".utf8, "_type_".utf8 -> "schemaID:35859".utf8)
  }


  it ("should return expected values for isCorrectPartitionForCardinalityQuery") {
    memStore.isCorrectPartitionForCardinalityQuery(QueryContext(),"") shouldEqual true
    memStore.isCorrectPartitionForCardinalityQuery(QueryContext(),"partition1") shouldEqual true
    val traceInfo = Map(memStore.FILODB_PARTITION_KEY -> "partition1")
    memStore.isCorrectPartitionForCardinalityQuery(QueryContext(traceInfo = traceInfo),
      "partition1") shouldEqual true

    memStore.isCorrectPartitionForCardinalityQuery(QueryContext(traceInfo = traceInfo),
      "partition2") shouldEqual false

    val traceInfo2 = Map("randomKey" -> "partition1")
    memStore.isCorrectPartitionForCardinalityQuery(QueryContext(traceInfo = traceInfo2),
      "partition1") shouldEqual true
    memStore.isCorrectPartitionForCardinalityQuery(QueryContext(traceInfo = traceInfo2),
      "") shouldEqual true

    val traceInfo3 = Map(memStore.FILODB_PARTITION_KEY -> "")
    memStore.isCorrectPartitionForCardinalityQuery(QueryContext(traceInfo = traceInfo2),
      "partition1") shouldEqual true
    memStore.isCorrectPartitionForCardinalityQuery(QueryContext(traceInfo = traceInfo2),
      "") shouldEqual true
  }

  it ("should throw assertion error when partition doesn't match for scanTsCardinalities") {
    val traceInfo1 = Map(memStore.FILODB_PARTITION_KEY -> "test-partition2")
    the[IllegalArgumentException] thrownBy  memStore.scanTsCardinalities(
      QueryContext(traceInfo = traceInfo1),timeseriesDataset.ref,Seq(1,2,3),Seq("testws","testns"),3)
  }

  it("should not throw error when partition match for scanTsCardinalities") {
    val traceInfo1 = Map(memStore.FILODB_PARTITION_KEY -> "test-partition")
    noException should be thrownBy memStore.scanTsCardinalities(
      QueryContext(traceInfo = traceInfo1), timeseriesDataset.ref, Seq(1, 2, 3), Seq("testws", "testns"), 3)

    noException should be thrownBy memStore.scanTsCardinalities(
      QueryContext(), timeseriesDataset.ref, Seq(1, 2, 3), Seq("testws", "testns"), 3)
  }

  it("should not throw error when empty QueryContext() for scanTsCardinalities") {
    noException should be thrownBy memStore.scanTsCardinalities(
      QueryContext(), timeseriesDataset.ref, Seq(1, 2, 3), Seq("testws", "testns"), 3)
  }

}
