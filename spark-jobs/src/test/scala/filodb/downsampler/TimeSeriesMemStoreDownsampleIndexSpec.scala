package filodb.downsampler

import com.typesafe.config.ConfigFactory
import monix.execution.ExecutionModel.BatchedExecution
import monix.reactive.Observable
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import filodb.cassandra.DefaultFiloSessionProvider
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.downsample.DownsampleConfig
import filodb.core.metadata.Dataset
import filodb.core.DatasetRef
import filodb.core.store.StoreConfig
import filodb.core.binaryrecord2.{RecordBuilder, RecordContainer, RecordSchema}
import filodb.core.memstore.{SomeData, TimeSeriesMemStore}
import filodb.core.metadata.Schemas
import filodb.core.store.{InMemoryMetaStore, PartKeyRecord}
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}
import filodb.memory.MemFactory

/**
  * Tests TimeSeriesMemStore with DownsamplableOnDemandPagingShard writing partition keys
  * to both raw and downsample column stores.
  *
  * This test creates its own Cassandra column stores
  */
class TimeSeriesMemStoreDownsampleIndexSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with ScalaFutures {
  implicit val s = monix.execution.Scheduler.Implicits.global

  implicit override val patienceConfig = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  // Configuration that enables chunk-downsampler and write-downsample-index-by-raw-ingesting-store
  val baseConf = ConfigFactory.load("application_test.conf")
  val testConfig = ConfigFactory.parseString(
    """
      |filodb {
      |  inline-dataset-configs = [
      |    {
      |      dataset = "prometheus"
      |      schema = "gauge"
      |      sourceconfig {
      |        store {
      |          flush-interval = 1h
      |          shard-mem-size = 100 MB
      |        }
      |        downsample {
      |          # Resolutions for downsampled data ** in ascending order **
      |          resolutions = [ 1 minute, 5 minutes ]
      |          # Retention of downsampled data for the corresponding resolution
      |          ttls = [ 30 days, 183 days ]
      |          # Raw schemas from which to downsample
      |          raw-schema-names = [ "gauge", "untyped"]
      |        }
      |      }
      |    }
      |  ]
      |  cassandra {
      |    hosts = localhost
      |    hosts = ${?CASSANDRA_HOST}
      |    port = 9042
      |    port = ${?CASSANDRA_NATIVE_TRANSPORT_PORT}
      |    partition-list-num-groups = 1
      |    create-tables-enabled = true
      |  }
      |  downsampler {
      |    chunk-downsampler-enabled = true
      |    write-downsample-index-by-raw-ingesting-store = true
      |  }
      |  memstore {
      |    max-partitions-on-heap-per-shard = 1100
      |    ensure-block-memory-headroom-percent = 10
      |    ensure-tsp-count-headroom-percent = 10
      |    ensure-native-memory-headroom-percent = 10
      |    index-updates-publishing-enabled = true
      |  }
      |  schemas {
      |    gauge {
      |      # Each column def is of name:type format.  Type may be ts,long,double,string,int
      |      # The first column must be ts or long
      |      columns = ["timestamp:ts", "value:double:detectDrops=false"]
      |
      |      # Default column to query using PromQL
      |      value-column = "value"
      |
      |      # Downsampling configuration.  See doc/downsampling.md
      |      downsamplers = [ "tTime(0)", "dMin(1)", "dMax(1)", "dSum(1)", "dCount(1)", "dAvg(1)" ]
      |
      |      # The marker implemention that determines row numbers at which to downsample
      |      downsample-period-marker = "time(0)"
      |
      |      # If downsamplers are defined, then the downsample schema must also be defined
      |      downsample-schema = "ds-gauge"
      |    }
      |    ds-gauge {
      |      columns = [ "timestamp:ts", "min:double", "max:double", "sum:double", "count:double", "avg:double" ]
      |      value-column = "avg"
      |      downsamplers = []
      |    }
      |  }
      |}
      |""".stripMargin)
    .withFallback(baseConf).resolve().getConfig("filodb")

  // Create Cassandra session and column stores
  lazy val session = new DefaultFiloSessionProvider(testConfig.getConfig("cassandra")).session
  lazy val rawColStore = new CassandraColumnStore(testConfig, s, session)
  lazy val downsampleColStore = new CassandraColumnStore(testConfig, s, session, true)
  lazy val metaStore = new InMemoryMetaStore()

  val schemas : Schemas = Schemas.fromConfig(testConfig).get

  val datasetConfig = testConfig.getConfigList("inline-dataset-configs").get(0)
  val sourceConfig = datasetConfig.getConfig("sourceconfig")
  val downsampleConfig: DownsampleConfig = DownsampleConfig.downsampleConfigFromSource(sourceConfig)
  val storeConfig : StoreConfig = StoreConfig(sourceConfig.getConfig("store"))

  // Create Dataset from config
  val rawDataset = Dataset(
    datasetConfig.getString("dataset"),     // "prometheus"
    schemas.schemas("gauge")                // The schema object
  )
  val datasetRef = rawDataset.ref
  val downsampleRefs : Seq[DatasetRef] = downsampleConfig.downsampleDatasetRefs(datasetRef.dataset)
  val downsample5MinDatasetRef = downsampleRefs.last

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Initialize raw column store for metrics1
    rawColStore.initialize(datasetRef, 1, ConfigFactory.empty).futureValue
    rawColStore.truncate(datasetRef, 1).futureValue

    // Initialize downsample column stores for all resolutions
    downsampleRefs.foreach { dsRef =>
      downsampleColStore.initialize(dsRef, 1, ConfigFactory.empty).futureValue
      downsampleColStore.truncate(dsRef, 1).futureValue
    }
  }

  override def afterAll(): Unit = {
    session.close()
    super.afterAll()
  }

  // Everything increments by 1 for simple predictability and testing
  def linearMultiSeries(
    startTs: Long = 100000L, numSeries: Int = 10, timeStep: Int = 1000,
    seriesPrefix: String = "Series "
  ): Stream[Seq[Any]] = {
    Stream.from(0).map { n =>
      Seq(
        startTs + n * timeStep,
        (1 + n).toDouble,
        seriesPrefix + (n % numSeries),
        Map(ZeroCopyUTF8String("some_tag") -> ZeroCopyUTF8String("some_tag_value"))
      )
    }
  }

  def records(
    ds: Dataset, stream: Stream[Seq[Any]], offset: Int = 0,
    ingestionTimeMillis: Long = System.currentTimeMillis()
  ): SomeData = {
    val builder = new RecordBuilder(MemFactory.onHeapFactory) {
      override def currentTimeMillis: Long = ingestionTimeMillis
    }
    stream.foreach { row =>
      builder.startNewRecord(ds.schema)
      row.foreach { thing => builder.addSlowly(thing) }
      builder.endRecord()
    }
    val containers: Seq[RecordContainer] = builder.allContainers
    containers.zipWithIndex.map { case (container, i) => SomeData(container, i + offset) }.head
  }

  def groupedRecords(ds: Dataset, stream: Stream[Seq[Any]], n: Int = 100, groupSize: Int = 5,
                     ingestionTimeStep: Long = 40000, ingestionTimeStart: Long = 0,
                     offset: Int = 0): Seq[SomeData] = {
    val i : Iterator[Stream[Seq[Any]]] = stream.take(n).grouped(groupSize)
    i.toSeq.zipWithIndex.map {
      case (group: Stream[Seq[Any]], i: Int) =>
        records(ds, group, offset + i, ingestionTimeStart + i * ingestionTimeStep)
    }
  }

  it("should ingest data and write partition keys to both raw and downsample stores") {
    // Create TimeSeriesMemStore with real Cassandra stores
    val memStore = new TimeSeriesMemStore(
      testConfig,
      rawColStore,
      downsampleColStore,
      metaStore
    )

    // Setup shard with downsample config
    memStore.setup(datasetRef, schemas, 0, storeConfig, 1, downsampleConfig)

    // ORIGINAL: Create ingestion stream with enough data to trigger a flush
    // groupedRecords creates batches with 40-second increments
    // flushInterval is 1 hour = 3600000ms
    // We need 90+ batches to cross the flush boundary at batch 90 (3600000ms)
    val stream = Observable.fromIterable(
      groupedRecords(rawDataset, linearMultiSeries(), n = 1500) // 1500 records / 5 per batch = 300 batches
    ).executeWithModel(BatchedExecution(5))

    // Start ingestion - this should trigger a flush at batch 90
    memStore.startIngestion(datasetRef, 0, stream, s).futureValue

    // Helper to extract metric name from partition key
    def pkMetricName(pkr: PartKeyRecord): String = {
      val strPairs = schemas.part.binSchema.toStringPairs(pkr.partKey, UnsafeUtils.arayOffset)
      strPairs.find(p => p._1 == "_metric_").get._2
    }

    // Verify partition keys were written to RAW column store
    val rawPartKeys = rawColStore.scanPartKeys(datasetRef, 0)
      .toListL.runToFuture.futureValue

    rawPartKeys should not be empty
    val rawMetrics = rawPartKeys.map(pkMetricName).toSet
    // Should contain "Series 0" through "Series 9" (10 series from linearMultiSeries)
    rawMetrics should contain ("Series 0")
    rawMetrics should contain ("Series 9")
    rawMetrics.size shouldEqual 10

    // Verify partition keys were written to DOWNSAMPLE column store
    // Check the 5-minute resolution downsample dataset
    val dsRef5m = downsampleRefs.find(_.dataset.contains("_ds_5")).get

    val downsamplePartKeys = downsampleColStore.scanPartKeys(dsRef5m, 0)
      .toListL.runToFuture.futureValue

    downsamplePartKeys should not be empty
    val dsMetrics = downsamplePartKeys.map(pkMetricName).toSet

    // Downsample store should have the same partition keys as raw
    dsMetrics shouldEqual rawMetrics

    // Verify schema IDs match
    rawPartKeys.foreach { pk =>
      val schemaId = RecordSchema.schemaID(pk.partKey, UnsafeUtils.arayOffset)
      val schema = schemas(schemaId)
      schema should not be Schemas.UnknownSchema
      // For linearMultiSeries, all partitions use gauge schema
      schema.data.name shouldEqual "gauge"
    }

    downsamplePartKeys.foreach { pk =>
      val schemaId = RecordSchema.schemaID(pk.partKey, UnsafeUtils.arayOffset)
      val schema = schemas(schemaId)
      schema should not be Schemas.UnknownSchema
      // Downsample schema should be dsGauge
      schema.data.name shouldEqual "ds-gauge"
    }

    // Cleanup
    memStore.shutdown()
  }

}
