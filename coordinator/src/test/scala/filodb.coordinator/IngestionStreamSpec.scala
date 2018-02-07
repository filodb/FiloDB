package filodb.coordinator

import scala.concurrent.duration._

import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterEach, Ignore}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._

object IngestionStreamSpec extends ActorSpecConfig

// This is really an end to end ingestion test, it's what a client talking to a FiloDB node would do.
// Most of the tests use the automated DatasetSetup where the coordinators set up the IngestionStream, but
// some set them up manually by invoking the factories directly.
@Ignore
class IngestionStreamSpec extends ActorTest(IngestionStreamSpec.getNewSystem) with StrictLogging
  with ScalaFutures with BeforeAndAfterEach {

  import akka.testkit._

  import client.IngestionCommands._
  import NodeClusterActor._
  import sources.CsvStreamFactory
  import GdeltTestData._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  val config = ConfigFactory.parseString("""filodb.memstore.groups-per-shard = 4""".stripMargin)
                            .withFallback(ConfigFactory.load("application_test.conf"))
                            .getConfig("filodb")

  private val within = 5.seconds.dilated
  private val cluster = FilodbCluster(system)
  cluster.join()

  private val coordinatorActor = cluster.coordinatorActor
  private val clusterActor = cluster.clusterSingleton(ClusterRole.Server, None)
  private val memStore = cluster.memStore
  private val metaStore = cluster.metaStore
  private val dataset33 = dataset3.copy(name = "gdelt2")

  private var shardMap: ShardMapper = ShardMapper.default

  override def beforeAll(): Unit = {
    metaStore.initialize().futureValue
    metaStore.clearAllData().futureValue
  }

  override def beforeEach(): Unit = {
    memStore.reset()
    // Make sure checkpoints are reset
    metaStore.clearAllData().futureValue
    metaStore.newDataset(dataset6).futureValue shouldEqual Success
    metaStore.newDataset(dataset33).futureValue shouldEqual Success
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

  def setup(ref: DatasetRef, resource: String, rowsToRead: Int = 5, source: Option[IngestionSource]): Unit = {
    innerSetup(ref, resource, rowsToRead, source)
    logger.info("Wating for ingestion started")
    expectMsg(IngestionStarted(ref, 0, coordinatorActor))
  }

  def innerSetup(ref: DatasetRef, resource: String, rowsToRead: Int = 5, source: Option[IngestionSource]): Unit = {
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
    setup(dataset6.ref, "/GDELT-sample-test.csv", rowsToRead = 5, None)

    val invalidShard = -1
    coordinatorActor ! StartShardIngestion(dataset6.ref, invalidShard, None)
    // We don't know exact order of IngestionStopped vs IngestionError
    (0 to 1).foreach { n =>
      expectMsgPF(within) {
        case IngestionError(_, shard, reason) => shard shouldEqual invalidShard
        case IngestionStopped(dataset6.ref, 0) =>
      }
    }
  }

  // TODO: consider getting rid of this test, it's *almost* the same as the next one
  it("should start and stop cleanly") {
    import IngestionActor.IngestionStatus

    val batchSize = 100
    setup(dataset6.ref, "/GDELT-sample-test.csv", rowsToRead = batchSize, None)

    import akka.pattern.ask
    implicit val timeout: Timeout = cluster.settings.InitializationTimeout

    // Wait for all messages to be ingested
    expectMsg(IngestionStopped(dataset6.ref, 0))

    val func = (coordinatorActor ? GetIngestionStats(dataset6.ref)).mapTo[IngestionStatus]
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

  /**
   * OK, this is the recovery test, it is a bit complicated
   * - Set up checkpoints for each group
   * - Group 0: 5  Group 1: 10   Group 2: 15   Group 3: 20
   * - Should skip over records with offsets lower than the above
   * - Should start recovery at earliest checkpoint, that is offset 5.  Thus GLOBALEVENTID <= 5 will be skipped
   * - Should end recovery just past last checkpoint which is 20
   * - Only works if rowsToRead is 5 or less (otherwise might read too much past end of recovery)
   * - Should get recovery status updates with every group of 5 lines read in
   *   (because only ~16 rows and default 20 reporting intervals)
   * - Should convert to normal status starting at offset 21 (or soonest offset after chunk of lines after 20)
   * - Should ingest the rest of the lines OK and stop
   * - total lines ingested should be 95 (first 5 lines skipped) minus other skipped lines in beginning
   */
  it("should recover rows based on checkpoint data, then move to normal ingestion") {
    metaStore.writeCheckpoint(dataset33.ref, 0, 0, 5L).futureValue shouldEqual Success
    metaStore.writeCheckpoint(dataset33.ref, 0, 1, 10L).futureValue shouldEqual Success
    metaStore.writeCheckpoint(dataset33.ref, 0, 2, 15L).futureValue shouldEqual Success
    metaStore.writeCheckpoint(dataset33.ref, 0, 3, 20L).futureValue shouldEqual Success

    innerSetup(dataset33.ref, "/GDELT-sample-test.csv", rowsToRead = 5, None)

    expectMsg(RecoveryInProgress(dataset33.ref, 0, coordinatorActor, 0))

    // A few more recovery status updates, and then finally the real IngestionStarted
    expectMsg(RecoveryInProgress(dataset33.ref, 0, coordinatorActor, 28))
    expectMsg(RecoveryInProgress(dataset33.ref, 0, coordinatorActor, 64))
    expectMsg(RecoveryInProgress(dataset33.ref, 0, coordinatorActor, 100))
    expectMsg(IngestionStarted(dataset33.ref, 0, coordinatorActor))

    expectMsg(IngestionStopped(dataset33.ref, 0))

    // Check the number of rows
    coordinatorActor ! GetIngestionStats(dataset33.ref)
    expectMsg(IngestionActor.IngestionStatus(84))
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
    val stream = (new CsvStreamFactory).create(config, dataset6, 0, None)
    val protocolActor = system.actorOf(IngestProtocol.props(clusterActor, dataset6.ref))

    import cluster.ec

    stream.routeToShards(shardMap, dataset6, protocolActor)

    Thread sleep 2000 // time to accumulate 199 below
    coordinatorActor ! GetIngestionStats(dataset6.ref)
    expectMsg(IngestionActor.IngestionStatus(199))
  }
}