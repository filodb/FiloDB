package filodb.standalone

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{FiniteDuration, _}

import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.coordinator._
import filodb.coordinator.client.Client
import filodb.core.{GlobalConfig, Success}
import filodb.timeseries.TestTimeseriesProducer

object IngestionAndRecoveryMultiNodeConfig extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  // To be consistent, the config file is actually passed in the MultiJvmNode*.opt files, and resolved automatically
  // using GlobalConfig.  This means config resolution is identical between us and standalone FiloServer.
  commonConfig(debugConfig(on = false)
    .withFallback(overrides)
    .withFallback(GlobalConfig.systemConfig))

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
  import IngestionAndRecoveryMultiNodeConfig._

  // Used when servers are started first time
  lazy val server1 = new FiloServer(Some(watcher.ref))
  lazy val client1 = Client.standaloneClient(system, false, "127.0.0.1", 2552)

  // Used when servers are restarted after shutdown
  lazy val server2 = new FiloServer(Some(watcher.ref))
  lazy val client2 = Client.standaloneClient(system, false, "127.0.0.1", 2552)

  // Initializing a FilodbCluster to get a handle to the metastore and colstore so we can clear and validate data
  // It is not used beyond that. We never start this cluster.
  val cluster = FilodbCluster(system)
  val metaStore = cluster.metaStore
  val colStore = cluster.memStore.store

  // Test fields
  var query1Response: Double = 0
  val chunkDurationTimeout = FiniteDuration(chunkDuration.toMillis + 60000, TimeUnit.MILLISECONDS)
  implicit val producePatience = PatienceConfig(timeout = Span(10, Seconds), interval = Span(100, Millis))

  "IngestionAndRecoverySpec Multi-JVM Test" should "clear data on node 1" in {
    runOn(first) {
      metaStore.initialize().futureValue shouldBe Success
      metaStore.clearAllData().futureValue shouldBe Success
      colStore.initialize(dataset, numShards).futureValue shouldBe Success
      colStore.truncate(dataset, numShards).futureValue shouldBe Success
    }
    enterBarrier("existing-data-cleared")
  }

  it should "be able to create dataset on node 1" in {
    runOn(first) {
      colStore.initialize(dataset, numShards).futureValue shouldBe Success
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

  it should "be able to ingest data into FiloDB via Kafka" in {
    within(chunkDurationTimeout) {
      runOn(first) {
        Thread.sleep(15000) // needed since awaitNodeUp doesnt wait for shard consumption to begin
        TestTimeseriesProducer.produceMetrics(source, 1, 1000, 100, 400, 10).futureValue
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
        info(s"shard $s: checkpoints=$checkpoints")
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
  var queryTimestamp: Long = _

  it should "be able to ingest larger amounts of data into FiloDB via Kafka again" in {
    within(chunkDurationTimeout) {
      runOn(first) {
        // NOTE: 10000 samples / 100 time series = 100 samples per series
        // 100 * 10s = 1000seconds =~ 16 minutes
        queryTimestamp = System.currentTimeMillis() - 195.minutes.toMillis
        TestTimeseriesProducer.produceMetrics(source, 1, 10000, 100, 200, 10).futureValue
        info("Waiting for part of the chunk-duration so that there are unpersisted chunks")
        Thread.sleep(chunkDuration.toMillis / 3)
      }
      enterBarrier("data2-ingested")
    }
  }

  var rangeStart = 0L
  var rangeEnd = 0L
  var rangeResponse: Map[String, Array[Double]] = _

  it should "answer query successfully" in {
    runOn(first) {
      rangeStart = queryTimestamp - 4.minutes.toMillis
      rangeEnd   = rangeStart + 15.minutes.toMillis

      // Print the top values in each shard just for debugging
      topValuesInShards(client1, "_ws_", 0 to 3)
      topValuesInShards(client1, "_ns_", 0 to 3)
      topValuesInShards(client1, "dc", 0 to 3)

      query1Response = runCliQuery(client1, queryTimestamp)
      val httpQueryResponse = runHttpQuery(queryTimestamp)
      query1Response shouldEqual httpQueryResponse
      val remoteReadQueryResponse = runRemoteReadQuery(queryTimestamp)
      query1Response shouldEqual remoteReadQueryResponse

      // Do a range query to find missing values
      rangeResponse = runRangeQuery(client1, rangeStart, rangeEnd)
    }
    enterBarrier("query1-answered")
  }

  it should "be able to shutdown node1 prior to chunks being written to persistent store" in {
    runOn(first) {
      awaitNodeDown(server1)
    }
    enterBarrier("node1-shutdown")
  }

  it should "be able to shutdown node2 prior to chunks being written to persistent store" in {
    runOn(second) {
      awaitNodeDown(server1)
    }
    enterBarrier("node2-shutdown")
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
    }
    enterBarrier("second-node-restarted")
  }

  it should "be able to validate the cluster status as recovering via CLI" in {
    runOn(first) {
      validateShardStatus(client2) {
        case ShardStatusRecovery(p) => true   // ok
        // For now, due to changed ingestion message size, some shards might be active instead of recovery, that's OK
        case ShardStatusActive      => true
        case _ => fail("All shards should be in shard recovery state")
      }
    }
    enterBarrier("recovery-status-validated")
  }

  it should "answer promQL query successfully with same value" in {
    runOn(first) {
      // Debug chunk metadata
      printChunkMeta(client2)

      val query2Response = runCliQuery(client2, queryTimestamp)
      (query2Response - query1Response).abs should be < 0.0001

      val rangeResp2 = runRangeQuery(client2, rangeStart, rangeEnd)
      compareRangeResults(rangeResponse, rangeResp2)
    }
    enterBarrier("query2-answered")
  }

  it should "be able to ingest some more data on node 1" in {
    runOn(first) {
      TestTimeseriesProducer.produceMetrics(source, 1, 1000, 100, 50, 10).futureValue
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
