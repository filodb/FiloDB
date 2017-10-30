package filodb.coordinator

import scala.concurrent.duration._

import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._

object IngestionStreamSpec extends ActorSpecConfig

// This is really an end to end ingestion test, it's what a client talking to a FiloDB node would do.
// Most of the tests use the automated DatasetSetup where the coordinators set up the IngestionStream, but
// some set them up manually by invoking the factories directly.
class IngestionStreamSpec extends ActorTest(IngestionStreamSpec.getNewSystem)
  with ScalaFutures with BeforeAndAfterEach {

  import akka.testkit._

  import IngestionCommands._
  import NodeClusterActor._
  import GdeltTestData._

  import sources.CsvStreamFactory

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
  cluster.join()

  private val coordinatorActor = cluster.coordinatorActor
  private val clusterActor = cluster.clusterSingletonProxy("worker", withManager = true)
  private val memStore = cluster.memStore
  private val metaStore = cluster.metaStore
  private val dataset33 = dataset3.copy(name = "gdelt2")

  private var shardMap: ShardMapper = ShardMapper.default

  override def beforeAll(): Unit = {
    metaStore.initialize().futureValue
    metaStore.clearAllData().futureValue
    metaStore.newDataset(dataset6).futureValue shouldEqual Success
    metaStore.newDataset(dataset33).futureValue shouldEqual Success
  }

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

  def setup(ref: DatasetRef, resource: String, rowsToRead: Int = 5, source: Option[IngestionSource]): SetupDataset = {
    val config = ConfigFactory.parseString(s"""header = true
                                           batch-size = $rowsToRead
                                           resource = $resource
                                           noflush = true
                                           chunk-duration = 1 hour
                                           """)

    val ingestionSource = source.getOrElse(IngestionSource(classOf[CsvStreamFactory].getName, config))
    val command = SetupDataset(ref, DatasetResourceSpec(1, 1), ingestionSource)
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
  // happens until the IngestionActor ingests.   When the failure occurs, the cluster state is updated
  // but then we need to query for it.
 it("should fail if cannot parse input RowReader during coordinator ingestion") {
   setup(dataset33.ref, "/GDELT-sample-test-errors.csv", rowsToRead = 5, None)
   expectMsgPF(within) {
     case IngestionError(ds, shard, ex) =>
       ds shouldBe dataset33.ref
       ex shouldBe a[NumberFormatException]
   }

    expectMsg(IngestionStopped(dataset33.ref, 0))

    // NOTE: right now ingestion errors do not cause IngestionActor to disappear.  Should it?
    coordinatorActor ! GetIngestionStats(dataset33.ref)
    expectMsg(IngestionActor.IngestionStatus(50))
  }

  // TODO: Simulate more failures.  Maybe simulate I/O failure or use a custom source
  // where we can inject failures?

  it("should not start ingestion, raise a IngestionError vs IngestionStopped, " +
    "if incorrect shard is sent for the creation of the stream") {
    val command = setup(dataset6.ref, "/GDELT-sample-test.csv", rowsToRead = 5, None)

    val invalidShard = 10
    coordinatorActor ! StartShardIngestion(command.ref, invalidShard, None)
    // We don't know exact order of IngestionStopped vs IngestionError
    (0 to 1).foreach { n =>
      expectMsgPF(within) {
        case IngestionError(_, shard, reason) => shard shouldEqual invalidShard
        case IngestionStopped(dataset6.ref, 0) =>
      }
    }
  }

  it("should start and stop cleanly") {
    import IngestionActor.IngestionStatus

    val batchSize = 100
    val command = setup(dataset6.ref, "/GDELT-sample-test.csv", rowsToRead = batchSize, None)

    import akka.pattern.ask
    implicit val timeout: Timeout = cluster.settings.InitializationTimeout

    // Wait for all messages to be ingested
    expectMsg(IngestionStopped(command.ref, 0))

    val func = (coordinatorActor ? GetIngestionStats(command.ref)).mapTo[IngestionStatus]
    awaitCond(func.futureValue.rowsIngested == batchSize - 1)
  }

  it("should ingest all rows directly into MemStore") {
    // Also need a way to probably unregister datasets from NodeClusterActor.
    // this functionality already is built into the shard actor: shardActor ! RemoveSubscription(ref)
    setup(dataset33.ref, "/GDELT-sample-test.csv", rowsToRead = 5, None)

    // Wait for all messages to be ingested
    expectMsg(IngestionStopped(dataset33.ref, 0))

    coordinatorActor ! GetIngestionStats(DatasetRef(dataset33.name))
    expectMsg(IngestionActor.IngestionStatus(99))
  }

  it("should ingest all rows using routeToShards and ProtocolActor") {
    val resource = "/GDELT-sample-test-200.csv"
    val batchSize = 10

    // Empty ingestion source - we're going to pump in records ourselves
    setup(dataset6.ref, resource, batchSize, Some(noOpSource))

    val config = ConfigFactory.parseString(s"""header = true
                                           batch-size = $batchSize
                                           resource = $resource
                                           """)
    val stream = (new CsvStreamFactory).create(config, dataset6, 0)
    val protocolActor = system.actorOf(IngestProtocol.props(clusterActor, dataset6.ref))

    import cluster.ec

    stream.routeToShards(shardMap, dataset6, protocolActor)

    Thread sleep 2000 // time to accumulate 199 below
    coordinatorActor ! GetIngestionStats(dataset6.ref)
    expectMsg(IngestionActor.IngestionStatus(199))
  }
}