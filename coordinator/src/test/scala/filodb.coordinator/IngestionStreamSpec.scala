package filodb.coordinator

import scala.concurrent.duration._

import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.BeforeAndAfterEach

import filodb.core._
import filodb.core.metadata.{Dataset, RichProjection}

object IngestionStreamSpec extends ActorSpecConfig

// This is really an end to end ingestion test, it's what a client talking to a FiloDB node would do.
// Most of the tests use the automated DatasetSetup where the coordinators set up the IngestionStream, but
// some set them up manually by invoking the factories directly.
class IngestionStreamSpec extends ActorTest(IngestionStreamSpec.getNewSystem)
  with ScalaFutures with BeforeAndAfterEach {
  import akka.testkit._
  import IngestionCommands._
  import GdeltTestData._
  import NodeClusterActor._

  import sources.{CsvStream, CsvStreamFactory}

  val settings = CsvStream.CsvStreamSettings()

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  // TODO: get rid of memtable config, memtable not in write path anymore
  val config = ConfigFactory.parseString("""filodb.memtable.write.interval = 500 ms
                                           |filodb.memtable.filo.chunksize = 70
                                           |filodb.memtable.max-rows-per-table = 70""".stripMargin)
                            .withFallback(ConfigFactory.load("application_test.conf"))
                            .getConfig("filodb")

  private val within = 5.seconds.dilated
  private val cluster = FilodbCluster(system)
  private val coordinatorActor = cluster.coordinatorActor
  cluster.join()
  private val clusterActor = cluster.clusterSingletonProxy("worker", withManager = true)
  private val memStore = cluster.memStore
  private val metaStore = cluster.metaStore

  metaStore.initialize().futureValue
  metaStore.clearAllData().futureValue
  val ref = projection6.datasetRef
  metaStore.newDataset(dataset6).futureValue shouldEqual Success
  schema.foreach { col => metaStore.newColumn(col, ref).futureValue shouldEqual Success }

  val dataset33 = dataset3.withName("gdelt2")
  metaStore.newDataset(dataset33).futureValue shouldEqual Success
  val proj2 = RichProjection(dataset33, schema)
  val ref2 = proj2.datasetRef
  schema.foreach { col => metaStore.newColumn(col, ref2).futureValue shouldEqual Success }

  private var shardMap: ShardMapper = ShardMapper.empty

  override def afterEach(): Unit = {
    memStore.reset()
    coordinatorActor ! NodeProtocol.ResetState
    clusterActor ! NodeProtocol.ResetState
    receiveWhile(within) {
      case NodeProtocol.StateReset =>
      case IngestionStopped(_, 0) =>
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    cluster.shutdown()
  }

  val sampleReader = new java.io.InputStreamReader(getClass.getResourceAsStream("/GDELT-sample-test.csv"))
  val headerCols = CsvStream.getHeaderColumns(sampleReader)

  def setup(dataset: Dataset, resource: String, rowsToRead: Int = 5, source: Option[IngestionSource]): SetupDataset = {
    val config = ConfigFactory.parseString(s"""header = true
                                           batch-size = $rowsToRead
                                           resource = $resource
                                           """)

    val datasetRef = dataset.projections.head.dataset
    val ingestionSource = source.getOrElse(IngestionSource(classOf[CsvStreamFactory].getName, config))
    val command = SetupDataset(datasetRef, headerCols, DatasetResourceSpec(1, 1), ingestionSource)
    clusterActor ! command
    expectMsg(within, DatasetVerified)

    clusterActor ! SubscribeShardUpdates(command.ref)
    expectMsgPF(within) {
      case CurrentShardSnapshot(ds, mapper) =>
        shardMap = mapper
        shardMap.numShards shouldEqual command.resources.numShards
    }
    expectMsg(IngestionStarted(command.ref, 0, coordinatorActor))

    command
  }

  // It's pretty hard to get an IngestionStream to fail when reading the stream itself, as no real parsing
  // happens until the MemStoreCoordActor ingests.   When the failure occurs, the cluster state is updated
  // but then we need to query for it.
 it("should fail if cannot parse input RowReader during coordinator ingestion") {
    setup(dataset33, "/GDELT-sample-test-errors.csv", rowsToRead = 5, None)
    expectMsgPF(within) {
      case IngestionError(ds, shard, ex) =>
        ds shouldBe ref2
        ex shouldBe a[NumberFormatException]
    }

    // Sending this will intentionally raise: java.lang.IllegalArgumentException: dataset gdelt / shard 0 not setup
    // Note: this relies on the MemStoreCoordActor not disappearing after errors.
    coordinatorActor ! GetIngestionStats(ref2, 0)
    expectMsg(MemStoreCoordActor.IngestionStatus(50))
  }

  // TODO: Simulate more failures.  Maybe simulate I/O failure or use a custom source
  // where we can inject failures?

  it("should not start ingestion, raise a IngestionError vs IngestionStopped, if incorrect shard is sent for the creation of the stream") {
    val command = setup(dataset6, "/GDELT-sample-test.csv", rowsToRead = 5, None)
    val commandS = IngestionCommands.DatasetSetup(dataset6, headerCols, 0, noOpSource)

    val invalidShard = 10
    coordinatorActor ! StartShardIngestion(command.ref, invalidShard, None)
    expectMsgPF(within) {
      case IngestionError(_, shard, reason) => shard shouldEqual invalidShard
    }
    expectNoMsg()
  }

  it("should start and stop cleanly") {
    import MemStoreCoordActor.IngestionStatus

    val batchSize = 100
    val command = setup(dataset6, "/GDELT-sample-test.csv", rowsToRead = batchSize, None)

    import system.dispatcher
    import akka.pattern.ask
    implicit val timeout: Timeout = cluster.settings.InitializationTimeout

    val func = (coordinatorActor ? GetIngestionStats(command.ref, 0)).mapTo[IngestionStatus]
    awaitCond(func.futureValue.rowsIngested == batchSize - 1)

    coordinatorActor ! StopShardIngestion(command.ref, 0)
    expectMsg(IngestionStopped(command.ref, 0))
  }

  it("should ingest all rows directly into MemStore") {
    // Also need a way to probably unregister datasets from NodeClusterActor.
    // this functionality already is built into the shard actor: shardActor ! RemoveSubscription(ref)
    setup(dataset33, "/GDELT-sample-test.csv", rowsToRead = 5, None)
    coordinatorActor ! GetIngestionStats(DatasetRef(dataset33.name), 0)
    expectMsg(MemStoreCoordActor.IngestionStatus(99))
  }

  it("should ingest all rows using routeToShards and ProtocolActor") {
    val projection = projection6
    val resource = "/GDELT-sample-test-200.csv"
    val batchSize = 10

    // Empty ingestion source - we're going to pump in records ourselves
    setup(projection.dataset, resource, batchSize, Some(noOpSource))

    val config = ConfigFactory.parseString(s"""header = true
                                           batch-size = $batchSize
                                           resource = $resource
                                           """)
    val stream = (new CsvStreamFactory).create(config, projection, 0)
    val protocolActor = system.actorOf(IngestProtocol.props(clusterActor, projection.datasetRef))

    import cluster.ec

    stream.routeToShards(shardMap, projection, protocolActor)

    Thread sleep 1000 // time to accumulate 199 below
    coordinatorActor ! GetIngestionStats(projection.datasetRef, 0)
    expectMsg(MemStoreCoordActor.IngestionStatus(199))
  }
}