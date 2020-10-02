package filodb.cassandra.columnstore

import scala.concurrent.Future
import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler
import monix.reactive.Observable
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import filodb.cassandra.DefaultFiloSessionProvider
import filodb.core.{MachineMetricsData, TestData}
import filodb.core.binaryrecord2.{BinaryRecordRowReader, RecordBuilder}
import filodb.core.downsample.OffHeapMemory
import filodb.core.memstore._
import filodb.core.memstore.FiloSchedulers.QuerySchedName
import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.query.{ColumnFilter, PlannerParam, QueryConfig, QueryContext, QuerySession}
import filodb.core.query.Filter.Equals
import filodb.core.store.{InMemoryMetaStore, PartKeyRecord, StoreConfig, TimeRangeChunkScan}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query.{QueryResponse, QueryResult}
import filodb.query.exec.{InProcessPlanDispatcher, MultiSchemaPartitionsExec}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class OdpSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")

  implicit val s = monix.execution.Scheduler.Implicits.global
  lazy val session = new DefaultFiloSessionProvider(config.getConfig("cassandra")).session
  lazy val colStore = new CassandraColumnStore(config, s, session)

  val rawDataStoreConfig = StoreConfig(ConfigFactory.parseString( """
                                                                    |flush-interval = 1h
                                                                    |shard-mem-size = 1MB
                                                                    |ingestion-buffer-mem-size = 30MB
                                                                  """.stripMargin))

  val offheapMem = new OffHeapMemory(Seq(Schemas.gauge),
    Map.empty, 100, rawDataStoreConfig)
  val schemas = Schemas(Schemas.gauge)

  val dataset = Dataset("prometheus", Schemas.gauge)
  val gaugeName = "my_gauge"
  var gaugePartKeyBytes: Array[Byte] = _
  val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8, "_ns_".utf8 -> "my_ns".utf8)
  val shardStats = new TimeSeriesShardStats(dataset.ref, -1)

  val firstSampleTime = 74373042000L
  val numSamples = 100
  val queryScheduler = Scheduler.fixedPool(s"$QuerySchedName", 3)

  // First create the tables in C*
  override def beforeAll(): Unit = {
    super.beforeAll()
    colStore.initialize(dataset.ref, 1).futureValue
    colStore.truncate(dataset.ref, 1).futureValue
  }

  override def afterAll(): Unit = {
    super.afterAll()
    queryScheduler.shutdown()
    offheapMem.free()
  }

  it ("should write gauge data to cassandra") {
    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.gauge, gaugeName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.gauge, partKey,
      0, offheapMem.bufferPools(Schemas.gauge.schemaHash), shardStats,
      offheapMem.nativeMemoryManager, 1)

    gaugePartKeyBytes = part.partKeyBytes

    val rawSamples = Stream.from(0).map { i =>
      Seq(firstSampleTime + i, i.toDouble, gaugeName, seriesTags)
    }.take(numSamples)

    MachineMetricsData.records(dataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.gauge.ingestionSchema, base, offset)
      part.ingest( System.currentTimeMillis(), rr, offheapMem.blockMemFactory)
      part.switchBuffers(offheapMem.blockMemFactory, true)
    }
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    colStore.write(dataset.ref, Observable.fromIterator(chunks)).futureValue
    val pk = PartKeyRecord(gaugePartKeyBytes, firstSampleTime, firstSampleTime + numSamples, Some(150))
    colStore.writePartKeys(dataset.ref, 0, Observable.now(pk), 259200, 34).futureValue
  }

  it ("should be able to do full ODP for non concurrent queries") {
    val policy = new FixedMaxPartitionsEvictionPolicy(20)
    val memStore = new TimeSeriesMemStore(config, colStore, new InMemoryMetaStore(), Some(policy))
    try {
      memStore.setup(dataset.ref, schemas, 0, TestData.storeConf)
      memStore.recoverIndex(dataset.ref, 0).futureValue
      memStore.refreshIndexForTesting(dataset.ref)

      val rvs = query(memStore).futureValue.asInstanceOf[QueryResult]
      rvs.result.size shouldEqual 1
      rvs.result.head.rows.toList.size shouldEqual numSamples
    } finally {
      memStore.shutdown()
    }
  }

  it ("should be able to do full ODP for concurrent queries") {
    val policy = new FixedMaxPartitionsEvictionPolicy(20)
    val memStore = new TimeSeriesMemStore(config, colStore, new InMemoryMetaStore(), Some(policy))
    try {
      memStore.setup(dataset.ref, schemas, 0, TestData.storeConf)
      memStore.recoverIndex(dataset.ref, 0).futureValue
      memStore.refreshIndexForTesting(dataset.ref)

      // issue 2 concurrent queries
      val res = (0 to 1).map(_ => query(memStore))

      // all queries should result in all chunks
      res.foreach { r =>
        val rvs = r.futureValue.asInstanceOf[QueryResult]
        rvs.result.size shouldEqual 1
        rvs.result.head.rows.toList.size shouldEqual numSamples
      }
    } finally {
      memStore.shutdown()
    }
  }

  it ("should be able to do partial ODP for non concurrent queries") {
    val policy = new FixedMaxPartitionsEvictionPolicy(20)
    val memStore = new TimeSeriesMemStore(config, colStore, new InMemoryMetaStore(), Some(policy))
    try {
      memStore.setup(dataset.ref, schemas, 0, TestData.storeConf)
      memStore.recoverIndex(dataset.ref, 0).futureValue
      memStore.refreshIndexForTesting(dataset.ref)

      // ingrest some more samples to trigger partial odp
      val rawSamples = Stream.from(0).map { i =>
        Seq(firstSampleTime + numSamples + i, i.toDouble, gaugeName, seriesTags)
      }.take(numSamples)

      memStore.ingest(dataset.ref, 0, SomeData(MachineMetricsData.records(dataset, rawSamples).records, 300))

      val rvs = query(memStore).futureValue.asInstanceOf[QueryResult]
      rvs.result.size shouldEqual 1
      rvs.result.head.rows.toList.size shouldEqual numSamples * 2
    } finally {
      memStore.shutdown()
    }
  }

  it ("should be able to do partial ODP for concurrent queries") {
    val policy = new FixedMaxPartitionsEvictionPolicy(20)
    val memStore = new TimeSeriesMemStore(config, colStore, new InMemoryMetaStore(), Some(policy))
    try {
      memStore.setup(dataset.ref, schemas, 0, TestData.storeConf)
      memStore.recoverIndex(dataset.ref, 0).futureValue
      memStore.refreshIndexForTesting(dataset.ref)

      // ingrest some more samples to trigger partial odp
      val rawSamples = Stream.from(0).map { i =>
        Seq(firstSampleTime + numSamples + i, i.toDouble, gaugeName, seriesTags)
      }.take(numSamples)

      memStore.ingest(dataset.ref, 0, SomeData(MachineMetricsData.records(dataset, rawSamples).records, 300))

      // issue 2 concurrent queries
      val res = (0 to 1).map(_ => query(memStore))

      // all queries should result in all chunks
      res.foreach { r =>
        val rvs = r.futureValue.asInstanceOf[QueryResult]
        rvs.result.size shouldEqual 1
        rvs.result.head.rows.toList.size shouldEqual numSamples * 2
      }
    } finally {
      memStore.shutdown()
    }
  }

  def query(memStore: TimeSeriesMemStore): Future[QueryResponse] = {
    val colFilters = seriesTags.map { case (t, v) => ColumnFilter(t.toString, Equals(v.toString)) }.toSeq
    val queryFilters = colFilters :+ ColumnFilter("_metric_", Equals(gaugeName))
    val exec = MultiSchemaPartitionsExec(QueryContext(plannerParam = PlannerParam(sampleLimit = numSamples * 2)),
      InProcessPlanDispatcher, dataset.ref, 0, queryFilters, TimeRangeChunkScan(firstSampleTime, firstSampleTime + 2 *
        numSamples))
    val queryConfig = new QueryConfig(config.getConfig("query"))
    val querySession = QuerySession(QueryContext(), queryConfig)
    exec.execute(memStore, querySession)(queryScheduler).runAsync(queryScheduler)
  }
}

