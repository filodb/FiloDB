package filodb.standalone

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps

import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.coordinator.{FilodbCluster, ShardStatus, ShardStatusNormal, ShardStatusRecovery}
import filodb.coordinator.client.{Client, LocalClient}
import filodb.coordinator.NodeClusterActor.{DatasetResourceSpec, IngestionSource}
import filodb.coordinator.QueryCommands.{MostRecentTime, QueryArgs}
import filodb.core.{DatasetRef, ErrorResponse, Success}
import filodb.core.metadata.Dataset
import filodb.core.query.ColumnFilter
import filodb.core.query.Filter.Equals
import filodb.timeseries.TestTimeseriesProducer

object IngestionAndRecoveryMultiNodeConfig extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  val globalConfig = ConfigFactory.parseFile(new java.io.File("conf/timeseries-filodb-server.conf"))

  commonConfig(debugConfig(on = false)
    .withFallback(overrides)
    .withFallback(globalConfig))

  def overrides: Config = ConfigFactory.parseString(
    """
    akka {
      actor.provider = cluster
      remote.netty.tcp {
        hostname = "127.0.0.1"
        port = 0
      }
    }
    """)
}

@Ignore
class IngestionAndRecoverySpecMultiJvmNode1 extends IngestionAndRecoverySpec

@Ignore
class IngestionAndRecoverySpecMultiJvmNode2 extends IngestionAndRecoverySpec

/**
  * This Multi-JVM Test validates ingestion, query execution, restart/recovery from kafka
  * and correctness of query execution after recovery.
  *
  * How to run:
  * 1. Start Kafka and Cassandra on default ports:
  * 2. Setup kafka topic:
  * kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic timeseries-dev
  * 3. Run test using:
  * standalone/multi-jvm:testOnly filodb.standalone.IngestionAndRecoverySpec
  */
@Ignore  // ignored since automated build cannot run Kafka based tests yet
abstract class IngestionAndRecoverySpec extends MultiNodeSpec(IngestionAndRecoveryMultiNodeConfig)
  with Suite with StrictLogging
  with ScalaFutures with FlatSpecLike
  with Matchers with BeforeAndAfterAll {

  override def initialParticipants: Int = roles.size

  import IngestionAndRecoveryMultiNodeConfig._

  // Ingestion Source section
  val source = ConfigFactory.parseFile(new java.io.File("conf/timeseries-dev-source.conf"))
  val dataset = DatasetRef(source.getString("dataset"))
  val numShards = source.getInt("numshards")
  val resourceSpec = DatasetResourceSpec(numShards, source.getInt("min-num-nodes"))
  val sourceconfig = source.getConfig("sourceconfig")
  val ingestionSource = source.as[Option[String]]("sourcefactory").map { factory =>
    IngestionSource(factory, sourceconfig)
  }.get
  val chunkDuration = sourceconfig.getDuration("chunk-duration")

  // Initializing a FilodbCluster to get a handle to the metastore and colstore so we can clear and validate data
  // It is not used beyond that. We never start this cluster.
  val cluster = FilodbCluster(system)
  val metaStore = cluster.metaStore
  val colStore = cluster.memStore.sink
  val numGroupsPerShard = cluster.settings.allConfig.getInt("filodb.memstore.groups-per-shard")

  // Used for first start of servers on each node
  lazy val server1A = new FiloServer()
  lazy val server2A = new FiloServer()
  lazy val client1 = Client.standaloneClient(system, "127.0.0.1", 2552)

  // Used during restart of servers on each node
  lazy val server1B = new FiloServer()
  lazy val server2B = new FiloServer()
  lazy val client2 = Client.standaloneClient(system, "127.0.0.1", 2552)

  // Test fields
  var query1Response: Double = 0
  val chunkDurationTimeout = FiniteDuration(chunkDuration.toMillis + 20000, TimeUnit.MILLISECONDS)

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()

  override def afterAll(): Unit = multiNodeSpecAfterAll()
  val producePatience = PatienceConfig(timeout = Span(10, Seconds), interval = Span(100, Millis))

  "IngestionAndRecoverySpec Multi-JVM Test" should "clear data on node 1" in {
    runOn(first) {
      metaStore.initialize().futureValue shouldBe Success
      metaStore.clearAllData().futureValue shouldBe Success
      colStore.initialize(dataset).futureValue shouldBe Success
      colStore.truncate(dataset).futureValue shouldBe Success
    }
    enterBarrier("existing-data-cleared")
  }

  it should "be able to create dataset on node 1" in {
    runOn(first) {
      val datasetObj = Dataset(dataset.dataset, Seq("tags:map"),
        Seq("timestamp:long", "value:double"), Seq("timestamp"))
      metaStore.newDataset(datasetObj).futureValue shouldBe Success
      colStore.initialize(dataset).futureValue shouldBe Success
      info("Dataset created")
    }
    enterBarrier("dataset-created")
  }

  it should "be able to bring up FiloServer on node 1" in {

    runOn(first) {
      server1A.start()
      awaitAssert(server1A.cluster.isInitialized shouldBe true, 5 seconds)
    }
    enterBarrier("first-node-started")
  }

  it should "be able to bring up FiloServer on node 2" in {
    runOn(second) {
      server1B.start()
      awaitAssert(server1B.cluster.isInitialized shouldBe true, 5 seconds)
    }
    enterBarrier("second-node-started")
  }

  it should "be able to set up dataset successfully on node 1" in {
    runOn(first) {
      setupDataset
      Thread.sleep(10000) // not an easy way to check if Kafka connections have been set up. It is this for now.
    }
    enterBarrier("dataset-set-up")
  }

  it should "be able to validate the cluster status as normal via CLI" in {
    runOn(first) {
      validateShardStatus(client1)(_ == ShardStatusNormal)
    }
  }

  it should "be able to ingest data into FiloDB via Kafka" in {
    within(chunkDurationTimeout) {
      runOn(first) {
        TestTimeseriesProducer.produceMetrics(1000, 100, 400).futureValue(producePatience)
        info("Waiting for chunk-duration to pass so checkpoints for all groups are created")
        Thread.sleep(chunkDuration.toMillis + 7000)
      }
      enterBarrier("data1-ingested")
    }
  }

  it should "validate that all checkpoints are created in metastore" in {
    runOn(first) {
      (0 until numShards).foreach { s =>
        val checkpoints = metaStore.readCheckpoints(dataset, s).futureValue
        (0 until numGroupsPerShard).foreach { g =>
          withClue(s"Checkpoint $g on shard $s: ") {
            checkpoints.contains(g) shouldBe true
            checkpoints(g) should be > 0L
          }
        }
      }
    }
    enterBarrier("checkpoints-validated")
  }

  it should "be able to ingest larger amounts of data into FiloDB via Kafka again" in {
    within(chunkDurationTimeout) {
      runOn(first) {
        TestTimeseriesProducer.produceMetrics(10000, 100, 200).futureValue(producePatience)
        info("Waiting for part of the chunk-duration so that there are unpersisted chunks")
        Thread.sleep(chunkDuration.toMillis / 3)
      }
      enterBarrier("data2-ingested")
    }
  }

  it should "answer query successfully" in {
    runOn(first) {
      query1Response = runQuery(client1)
    }
    enterBarrier("query1-answered")
  }

  it should "be able to shutdown nodes prior to chunks being written to persistent store" in {
    runOn(first) {
      server1A.shutdown()
      awaitCond(server1A.cluster.isTerminated == true, 3 seconds)
    }
    runOn(second) {
      server1B.shutdown()
      awaitCond(server1B.cluster.isTerminated == true, 3 seconds)
    }
    enterBarrier("both-nodes-shutdown")
  }

  it should "be able to restart node 1" in {
    runOn(first) {
      server2A.start()
      awaitAssert(server2A.cluster.isInitialized shouldBe true, 5 seconds)
    }
    enterBarrier("first-node-restarted")
  }

  it should "be able to restart node 2" in {
    runOn(second) {
      server2B.start()
      awaitAssert(server2B.cluster.isInitialized shouldBe true, 5 seconds)
      Thread.sleep(5000) // wait for coordinator and query actors to be initialized
    }
    enterBarrier("second-node-restarted")
  }

  it should "be able to validate the cluster status as recovering via CLI" in {
    runOn(first) {
      validateShardStatus(client2) {
        case ShardStatusRecovery(p) => // ok
        case _ => fail("All shards should be in shard recovery state")
      }
    }
  }

  it should "answer promQL query successfully with same value" in {
    runOn(first) {
      val query2Response = runQuery(client2)
      (query2Response - query1Response).abs should be < 0.0001
    }
    enterBarrier("query2-answered")
  }

  it should "be able to ingest some more data on node 1" in {
    runOn(first) {
      TestTimeseriesProducer.produceMetrics(1000, 100, 50).futureValue(producePatience)
    }
    enterBarrier("data3-ingested")
  }

  it should "be able to validate the cluster status as normal again via CLI" in {
    runOn(first) {
      validateShardStatus(client2)(_ == ShardStatusNormal)
    }
    enterBarrier("shard-normal-end-of-test")
  }

  def validateShardStatus(client: LocalClient)(statusValidator: ShardStatus => Unit): Unit = {
    client.getShardMapper(dataset) match {
      case Some(map) =>
        map.shardValues.size shouldBe 4  // numer of shards
        map.shardValues.foreach { case (_, status) =>
          statusValidator(status)
        }
      case _ =>
        fail(s"Unable to obtain status for dataset $dataset")
    }
  }

  def setupDataset(): Unit = {
    client1.setupDataset(dataset, resourceSpec, ingestionSource).foreach {
      case e: ErrorResponse => fail(s"Errors setting up dataset $dataset: $e")
    }
  }

  def runQuery(client: LocalClient): Double = {
    // This is the promQL equivalent: sum(heap_usage{partition="P0"}[1000m])
    val query = QueryArgs("sum", "value", List(), MostRecentTime(60000000), "simple", List())
    val filters = Vector(ColumnFilter("partition", Equals("P0")), ColumnFilter("__name__", Equals("heap_usage")))
    val response1 = client.partitionFilterAggregate(dataset, query, filters)
    val answer = response1.elements.head.asInstanceOf[Double]
    info(s"Query Response was: $answer")
    answer.asInstanceOf[Double]
  }

}
