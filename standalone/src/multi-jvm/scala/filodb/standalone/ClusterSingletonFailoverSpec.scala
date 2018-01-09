package filodb.standalone

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps

import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.coordinator.{CurrentShardSnapshot, ShardStatusActive}
import filodb.coordinator.client.{LocalClient}
import filodb.coordinator.NodeClusterActor.SubscribeShardUpdates
import filodb.core.Success
import filodb.core.metadata.Dataset
import filodb.timeseries.TestTimeseriesProducer

object ClusterSingletonFailoverMultiNodeConfig extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  // NOTE: none of these configs really matter, because FiloServer() creates its own ActorSystem
  // and thus resolves config using FiloDB own priorities.
  val tsConfig = ConfigFactory.parseFile(new java.io.File("conf/timeseries-filodb-server.conf"))
  val myConfig = overrides.withFallback(tsConfig)
                          .withFallback(ConfigFactory.load("application_test.conf"))
  commonConfig(myConfig)

  val ingestDuration = 15.seconds

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

class ClusterSingletonFailoverSpecMultiJvmNode1 extends ClusterSingletonFailoverSpec
class ClusterSingletonFailoverSpecMultiJvmNode2 extends ClusterSingletonFailoverSpec
class ClusterSingletonFailoverSpecMultiJvmNode3 extends ClusterSingletonFailoverSpec

/**
  * This Multi-JVM Test validates cluster singleton failover when the node running the singleton fails.
  * The next node should take over and recover existing shard state and subscribers.  The shard status
  * should still be the same as before (other than shard down) and ingestion should resume on the new node.
  * This simulates a typical upgrade scenario.
  *
  * How to run:
  * 1. Start Kafka and Cassandra on default ports:
  * 2. Setup kafka topic:
  * kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic timeseries-dev
  * 3. Run test using:
  * standalone/multi-jvm:testOnly filodb.standalone.ClusterSingletonFailoverSpec
  *
  * NOTE: this test will run as part of the standard test directive when MAYBE_MULTI_JVM is set in the environment
  */
abstract class ClusterSingletonFailoverSpec extends StandaloneMultiJvmSpec(ClusterSingletonFailoverMultiNodeConfig) {
  import akka.testkit._
  import ClusterSingletonFailoverMultiNodeConfig._

  // Used for first start of servers on each node
  lazy val server = new FiloServer()
  lazy val client1 = new LocalClient(server.cluster.coordinatorActor)

  val numGroupsPerShard = server.cluster.settings.allConfig.getInt("filodb.memstore.groups-per-shard")
  logger.info(s"Accessed metaStore and colStore.  Cluster should _not_ be starting up yet.")

  // Test fields
  var query1Response: Double = 0
  val chunkDurationTimeout = FiniteDuration(chunkDuration.toMillis + 20000, TimeUnit.MILLISECONDS)

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()

  override def afterAll(): Unit = multiNodeSpecAfterAll()

  "ClusterSingletonFailoverSpec Multi-JVM Test" should "clear data and init dataset on node 1" in {
    runOn(second) {
      // Get a handle to the metastore and colstore from the server so we can clear and validate data/
      // Note that merely accessing them does not start up any cluster, but we are using the server's to ensure
      // that configs are consistent.
      val metaStore = server.cluster.metaStore
      val colStore = server.cluster.memStore.sink
      metaStore.initialize().futureValue shouldBe Success
      metaStore.clearAllData().futureValue shouldBe Success
      colStore.initialize(dataset).futureValue shouldBe Success
      colStore.truncate(dataset).futureValue shouldBe Success

      val datasetObj = Dataset(dataset.dataset, Seq("tags:map"),
        Seq("timestamp:long", "value:double"), Seq("timestamp"))
      metaStore.newDataset(datasetObj).futureValue shouldBe Success
      colStore.initialize(dataset).futureValue shouldBe Success
      logger.info("Dataset created")
    }
    enterBarrier("existing-data-cleared-and-dataset-created")
  }

  it should "be able to bring up FiloServers in right sequence" in {
    // Have second node join cluster first.  The first node is where multi-jvm TestController runs, and it cannot
    // be shut down.  this way singleton runs on second node and we can shut it down to test it
    runOn(second) {
      server.start()
      awaitAssert(server.cluster.isInitialized shouldBe true, 5 seconds)
    }
    enterBarrier("second-node-started")

    runOn(first) {
      server.start()
      awaitAssert(server.cluster.isInitialized shouldBe true, 5 seconds)
    }
    enterBarrier("first-node-started")

    // Third node is the standby node.  No shards should be assigned to it
    runOn(third) {
      server.start()
      awaitAssert(server.cluster.isInitialized shouldBe true, 5 seconds)
      awaitCond(server.cluster.state.members.size == 3)
    }
    enterBarrier("third-node-started")
    info(s"All nodes initialized, address = ${server.cluster.selfAddress}")
  }

  it should "be able to set up dataset successfully on node 2" in {
    // NOTE: for some reason this does not work when run from a node other than the singleton node
    runOn(second) {
      setupDataset(client1)
      server.cluster.clusterActor.get ! SubscribeShardUpdates(dataset)
      expectMsgPF(6.seconds.dilated) {
        case CurrentShardSnapshot(ref, newMap) => info(s"Received initial ShardMap for $ref: $newMap")
      }
      waitAllShardsIngestionActive()
    }
    enterBarrier("dataset-set-up-ingestion-started")
  }

  it should "be able to validate the cluster status as normal via CLI" in {
    runOn(first) {
      validateShardStatus(client1){ _ == ShardStatusActive }
    }
  }

  it should "be able to ingest data into FiloDB via Kafka" in {
    within(ingestDuration + 5.seconds) {
      runOn(first) {
        val producePatience = PatienceConfig(timeout = Span(10, Seconds), interval = Span(100, Millis))
        TestTimeseriesProducer.produceMetrics(1000, 100, 400).futureValue(producePatience)
        info("Waiting for ingest-duration to pass")
        Thread.sleep(ingestDuration.toMillis)
      }
      enterBarrier("data1-ingested")
    }
  }

  it should "answer query successfully" in {
    runOn(first) {
      query1Response = runQuery(client1)
    }
    enterBarrier("query1-answered")
  }

  it should "be able to shutdown node 2 and failover to another node" in {
    runOn(second) {
      // NOTE: Ideally we would like to use the testConductor, but I cannot get this to work at all. :(
      // runOn(first) {
      //   testConductor.shutdown(second, abort=true).futureValue
      server.shutdown()
      awaitCond(server.cluster.isTerminated == true, 3 seconds)
    }
    enterBarrier("one-node-shutdown")

    // Now how to check that singleton has failed over to another node?
    // Maybe query the cluster actor for status again?
    within(chunkDurationTimeout) {
      runOn(first) {
        // NOTE: We don't need to mark node 2 as down since it was a graceful shutdown.
        // We need to wait maybe 20 seconds for the CLusterSingleton to restart on another node
        // TODO: remove this thread sleep if there's a way to detect this change from ClusterProxy or ClusterManager
        Thread sleep 20000

        // TODO: change this when the NodeClusterActor receives and handles ShardRemoved etc. events
        // because then, the shards that are shut down status will change, and maybe get reassigned
        validateShardStatus(client1){ _ == ShardStatusActive }
      }
      enterBarrier("check-new-singleton")
    }
  }

  ignore should "answer promQL query successfully with same value" in {
    runOn(first) {
      val query2Response = runQuery(client1)
      (query2Response - query1Response).abs should be < 0.0001
    }
    enterBarrier("query2-answered")
  }

  // TODO enable this test - future is timing out for some reason
  ignore should "be able to validate the cluster status as normal again via CLI" in {
    runOn(first) {
      validateShardStatus(client1){ _ == ShardStatusActive }
    }
    enterBarrier("shard-normal-end-of-test")
  }
}
