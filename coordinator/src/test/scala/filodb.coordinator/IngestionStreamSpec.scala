package filodb.coordinator

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._
import filodb.core.store.StoreConfig

object IngestionStreamSpec extends ActorSpecConfig

// This is really an end to end ingestion test, it's what a client talking to a FiloDB node would do.
// Most of the tests use the automated DatasetSetup where the coordinators set up the IngestionStream, but
// some set them up manually by invoking the factories directly.
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

  private val within = 20.seconds.dilated
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
    expectMsg(NodeProtocol.StateReset)
    clusterActor ! NodeProtocol.ResetState
    expectMsg(NodeProtocol.StateReset)
    Thread sleep 2000
  }

  def setup(ref: DatasetRef, resource: String, rowsToRead: Int = 5, source: Option[IngestionSource]): Unit = {
    val config = ConfigFactory.parseString(s"""header = true
                                           batch-size = $rowsToRead
                                           resource = $resource
                                           noflush = true
                                           store {
                                             flush-interval = 1 hour
                                             ingestion-buffer-mem-size = 50MB
                                             retry-delay = 500ms
                                           }
                                           """).withFallback(TestData.sourceConf)

    val storeConf = StoreConfig(config.getConfig("store"))
    val ingestionSource = source.getOrElse(IngestionSource(classOf[CsvStreamFactory].getName, config))
    val command = SetupDataset(ref, DatasetResourceSpec(1, 1), ingestionSource, storeConf)
    clusterActor ! command
    expectMsg(within, DatasetVerified)

    clusterActor ! SubscribeShardUpdates(command.ref)
    expectMsgPF(within) {
      case CurrentShardSnapshot(ds, mapper) =>
        shardMap = mapper
        shardMap.numShards shouldEqual command.resources.numShards
        mapper.statuses.head shouldEqual ShardStatusAssigned
    }
  }

  // It's pretty hard to get an IngestionStream to fail when reading the stream itself, as no real parsing
  // happens until the IngestionActor ingests.   When the failure occurs, the cluster state is updated
  // but then we need to query for it.
  it("should fail if cannot parse input record during coordinator ingestion") {
    setup(dataset33.ref, "/GDELT-sample-test-errors.csv", rowsToRead = 5, None)

    var latestStatus: ShardStatus = ShardStatusAssigned
    // sometimes we receive multiple status snapshots
    while (latestStatus != ShardStatusError) {
      expectMsgPF(within) {
        case CurrentShardSnapshot(dataset33.ref, mapper) =>
          latestStatus = mapper.statuses.head
          if (latestStatus != ShardStatusError)
            mapper.shardsForCoord(coordinatorActor) shouldEqual Seq(0)
      }
      info(s"Latest status = $latestStatus")
    }
  }

  // TODO: Simulate more failures.  Maybe simulate I/O failure or use a custom source
  // where we can inject failures?

  it("should not start ingestion, raise a IngestionError vs IngestionStopped, " +
    "if incorrect shard is sent for the creation of the stream") {
    setup(dataset6.ref, "/GDELT-sample-test.csv", rowsToRead = 5, None)

    val invalidShard = -1
    coordinatorActor ! StartShardIngestion(dataset6.ref, invalidShard, None)

    expectMsgPF(within) {
      case CurrentShardSnapshot(dataset6.ref, mapper) =>
        mapper.shardsForCoord(coordinatorActor) shouldEqual Seq(0)
        mapper.statuses.head shouldEqual ShardStatusStopped
    }

  }

  it("should ingest all rows directly into MemStore") {
    // Also need a way to probably unregister datasets from NodeClusterActor.
    // this functionality already is built into the shard actor: shardActor ! RemoveSubscription(ref)
    setup(dataset33.ref, "/GDELT-sample-test.csv", rowsToRead = 5, None)

    // Wait for all messages to be ingested
    expectMsgPF(within) {
      case CurrentShardSnapshot(dataset33.ref, mapper) =>
        mapper.shardsForCoord(coordinatorActor) shouldEqual Seq(0)
        mapper.statuses.head shouldEqual ShardStatusStopped
    }

    coordinatorActor ! GetIngestionStats(dataset33.ref)
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
    metaStore.writeCheckpoint(dataset33.ref, 0, 0, 1L).futureValue shouldEqual Success
    metaStore.writeCheckpoint(dataset33.ref, 0, 1, 2L).futureValue shouldEqual Success
    metaStore.writeCheckpoint(dataset33.ref, 0, 2, 3L).futureValue shouldEqual Success
    metaStore.writeCheckpoint(dataset33.ref, 0, 3, 4L).futureValue shouldEqual Success

    setup(dataset33.ref, "/GDELT-sample-test.csv", rowsToRead = 5, None)

    // expectMsg(RecoveryInProgress(dataset33.ref, 0, coordinatorActor, 0))

    // A few more recovery status updates, and then finally the real IngestionStarted
    // expectMsg(RecoveryInProgress(dataset33.ref, 0, coordinatorActor, 28))
    // expectMsg(RecoveryInProgress(dataset33.ref, 0, coordinatorActor, 64))
    // expectMsg(RecoveryInProgress(dataset33.ref, 0, coordinatorActor, 100))
    // expectMsg(IngestionStarted(dataset33.ref, 0, coordinatorActor))

    // expectMsg(IngestionStopped(dataset33.ref, 0))
    // Unfortunately since we do not get every message we cannot actually check the progression of recovery
    expectMsgPF(within) {
      case CurrentShardSnapshot(dataset33.ref, mapper) =>
        mapper.shardsForCoord(coordinatorActor) shouldEqual Seq(0)
        mapper.statuses.head shouldEqual ShardStatusStopped
    }

    // Check the number of rows
    coordinatorActor ! GetIngestionStats(dataset33.ref)
    expectMsg(IngestionActor.IngestionStatus(85))    // <-- must be rounded to 5, we ingest entire batches
  }
}