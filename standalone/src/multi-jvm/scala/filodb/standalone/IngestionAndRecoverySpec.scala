package filodb.standalone

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{FiniteDuration, _}

import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.coordinator._
import filodb.coordinator.NodeClusterActor.SubscribeShardUpdates
import filodb.coordinator.client.Client
import filodb.core.Success
import filodb.core.metadata.Dataset
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

class IngestionAndRecoverySpecMultiJvmNode1 extends IngestionAndRecoverySpec
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
  *
  * NOTE: this test will run as part of the standard test directive when MAYBE_MULTI_JVM is set in the environment
  */
abstract class IngestionAndRecoverySpec extends StandaloneMultiJvmSpec(IngestionAndRecoveryMultiNodeConfig) {
  import akka.testkit._
  import IngestionAndRecoveryMultiNodeConfig._

  // Used when servers are started first time
  lazy val server1 = new FiloServer(Some(watcher.ref))
  lazy val client1 = Client.standaloneClient(system, "127.0.0.1", 2552)

  // Used when servers are restarted after shutdown
  lazy val server2 = new FiloServer(Some(watcher.ref))
  lazy val client2 = Client.standaloneClient(system, "127.0.0.1", 2552)

  // Initializing a FilodbCluster to get a handle to the metastore and colstore so we can clear and validate data
  // It is not used beyond that. We never start this cluster.
  val cluster = FilodbCluster(system)
  val metaStore = cluster.metaStore
  val colStore = cluster.memStore.sink
  val numGroupsPerShard = cluster.settings.allConfig.getInt("filodb.memstore.groups-per-shard")

  // Test fields
  var query1Response: Double = 0
  val chunkDurationTimeout = FiniteDuration(chunkDuration.toMillis + 20000, TimeUnit.MILLISECONDS)
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
      awaitNodeUp(server1)
    }
    enterBarrier("first-node-started")
  }

  it should "be able to bring up FiloServer on node 2" in {
    runOn(second) {
      awaitNodeUp(server1)
    }
    enterBarrier("second-node-started")
  }

  it should "be able to set up dataset successfully on node 1" in {
    runOn(first) {
      setupDataset(client1)
      server1.cluster.clusterActor.get ! SubscribeShardUpdates(dataset)
      expectMsgPF(6.seconds.dilated) {
        case CurrentShardSnapshot(ref, newMap) => info(s"Got initial ShardMap for $ref: $newMap")
      }
      waitAllShardsIngestionActive()
    }
    enterBarrier("dataset-set-up")
  }

  it should "be able to validate the cluster status as normal via CLI" in {
    runOn(first) {
      validateShardStatus(client1)(_ == ShardStatusActive)
    }
  }

  it should "be able to ingest data into FiloDB via Kafka" in {
    within(chunkDurationTimeout) {
      runOn(first) {
        TestTimeseriesProducer.produceMetrics(source, 1000, 100, 400).futureValue(producePatience)
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
        TestTimeseriesProducer.produceMetrics(source, 10000, 100, 200).futureValue(producePatience)
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
      awaitNodeDown(server1)
    }
    runOn(second) {
      awaitNodeDown(server1)
    }
    enterBarrier("both-nodes-shutdown")
  }

  it should "be able to restart node 1" in {
    runOn(first) {
      awaitNodeUp(server2)
    }
    enterBarrier("first-node-restarted")
  }

  it should "be able to restart node 2" in {
    runOn(second) {
      awaitNodeUp(server2)
      Thread.sleep(5000) // wait for coordinator and query actors to be initialized
    }
    enterBarrier("second-node-restarted")
  }

  it should "be able to validate the cluster status as recovering via CLI" in {
    runOn(first) {
      validateShardStatus(client2) {
        case ShardStatusRecovery(p) => true   // ok
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
      TestTimeseriesProducer.produceMetrics(source, 1000, 100, 50).futureValue(producePatience)
    }
    enterBarrier("data3-ingested")
  }

  it should "be able to validate the cluster status as normal again via CLI" in {
    runOn(first) {
      validateShardStatus(client2)(_ == ShardStatusActive)
    }
    enterBarrier("shard-normal-end-of-test")
  }
}
