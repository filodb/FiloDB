package filodb.coordinator

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.pattern.gracefulStop
import akka.testkit.{EventFilter, TestProbe}
import com.opencsv.CSVReader
import com.typesafe.config.{Config, ConfigFactory}
import monix.execution.Scheduler
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.Try
import scalax.file.Path

import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset, RichProjection}
import filodb.core.reprojector.SegmentStateCache
import filodb.core.store._

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}


object IngestStreamSpec extends ActorSpecConfig

// This is really an end to end ingestion test, it's what a client talking to a FiloDB node would do.
// Most of the tests use the automated DatasetSetup where the coordinators set up the IngestStream, but
// some set them up manually by invoking the factories directly.
class IngestStreamSpec extends ActorTest(IngestStreamSpec.getNewSystem)
with CoordinatorSetupWithFactory with ScalaFutures {
  import akka.testkit._
  import DatasetCommands._
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

  coordinatorActor
  cluster.join(cluster.selfAddress)
  val clusterActor = singletonClusterActor("worker")

  val ref = projection6.datasetRef
  metaStore.newDataset(dataset6).futureValue should equal (Success)
  schema.foreach { col => metaStore.newColumn(col, ref).futureValue should equal (Success) }

  val dataset33 = dataset3.withName("gdelt2")
  metaStore.newDataset(dataset33).futureValue should equal (Success)
  val proj2 = RichProjection(dataset33, schema)
  val ref2 = proj2.datasetRef
  schema.foreach { col => metaStore.newColumn(col, ref2).futureValue should equal (Success) }

  val shardMap = new ShardMapper(1)
  shardMap.registerNode(Seq(0), coordinatorActor)

  before {
    memStore.reset()
    clusterActor ! NodeClusterActor.Reset
    coordinatorActor ! NodeCoordinatorActor.Reset
  }

  override def afterAll(): Unit = {
    super.afterAll()
    gracefulStop(clusterActor, 3.seconds.dilated, PoisonPill).futureValue
  }

  val sampleReader = new java.io.InputStreamReader(getClass.getResourceAsStream("/GDELT-sample-test.csv"))
  val headerCols = CsvStream.getHeaderColumns(sampleReader)

  def setup(dataset: Dataset, resource: String, rowsToRead: Int = 5): Unit = {
    val config = ConfigFactory.parseString(s"""header = true
                                           batch-size = $rowsToRead
                                           resource = $resource
                                           """)
    val msg = SetupDataset(dataset.projections.head.dataset,
                           headerCols,
                           DatasetResourceSpec(1, 1),
                           IngestionSource(classOf[CsvStreamFactory].getName, config))
    clusterActor ! msg
    expectMsg(DatasetVerified)
  }

  // It's pretty hard to get an IngestStream to fail when reading the stream itself, as no real parsing
  // happens until the MemStoreCoordActor ingests.   When the failure occurs, the cluster state is updated
  // but then we need to query for it.
  it("should fail if cannot parse input RowReader during coordinator ingestion") {
    setup(dataset33, "/GDELT-sample-test-errors.csv", rowsToRead = 5)

    // Note: this relies on the MemStoreCoordActor not disappearing after errors.
    // Also the thread sleep is needed as we have to wait for MemStoreCoordActor to be setup and ingesting
    // A more reliable way is to query the NodeClusterActor for status, or subscribe for status
    // updates (in which case no more thread sleeps!)
    Thread sleep 1400
    coordinatorActor ! GetIngestionStats(ref2, 0)
    val status = expectMsgPF(5.seconds.dilated) {
      // 5 rows read at a time, 53 before error, so 50 successfully ingested
      case MemStoreCoordActor.Status(50, Some(ex)) =>
        ex shouldBe a[NumberFormatException]
    }
  }

  // TODO: Not sure how to simulate this failure.  Maybe simulate I/O failure or use a custom source
  // where we can inject failures?
  ignore("should fail fast if somehow the source stream itself bombs") {
    setup(dataset6, "/GDELT-sample-test.csv", rowsToRead = 5)

    Thread sleep 2000
    coordinatorActor ! GetIngestionStats(ref2, 0)
    val status = expectMsgPF(5.seconds.dilated) {
      case MemStoreCoordActor.Status(53, Some(ex)) =>
        ex shouldBe a[NumberFormatException]
    }
  }

  it("should ingest all rows directly into MemStore") {
    // Also need a way to probably unregister datasets from NodeClusterActor.
    setup(dataset33, "/GDELT-sample-test.csv", rowsToRead = 10)

    Thread sleep 1400
    coordinatorActor ! GetIngestionStats(ref2, 0)
    expectMsg(MemStoreCoordActor.Status(99, None))
  }

  import Ingest._
  it("should ingest all rows using routeToShards and ProtocolActor") {
    // Empty ingestion source - we're going to pump in records ourselves
    val msg = SetupDataset(projection6.datasetRef,
                           headerCols,
                           DatasetResourceSpec(1, 1), noOpSource)
    clusterActor ! msg
    expectMsg(DatasetVerified)

    // TODO: replace with waiting for node ready message
    Thread sleep 1000

    val config = ConfigFactory.parseString(s"""header = true
                                           batch-size = 10
                                           resource = "/GDELT-sample-test-200.csv"
                                           """)
    val stream = (new CsvStreamFactory).create(config, projection6)
    val protocolActor = system.actorOf(IngestProtocol.props(clusterActor, projection6.datasetRef))
    stream.routeToShards(new ShardMapper(1), projection6, protocolActor)

    Thread sleep 1000
    coordinatorActor ! GetIngestionStats(projection6.datasetRef, 0)
    expectMsg(MemStoreCoordActor.Status(199, None))
  }
}