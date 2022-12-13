package filodb.standalone

import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.coordinator._
import filodb.coordinator.client.LocalClient
import filodb.core.{GlobalConfig, Success}
import filodb.timeseries.TestTimeseriesProducer

object ClusterSingletonFailoverMultiNodeConfig extends MultiNodeConfig {
  val first = role("first") // controller
  val second = role("second")
  val third = role("third")

  // To be consistent, the config file is actually passed in the MultiJvmNode*.opt files, and resolved automatically
  // using GlobalConfig.  This means config resolution is identical between us and standalone FiloServer.
  val myConfig = overrides.withFallback(GlobalConfig.systemConfig)
  commonConfig(myConfig)

  val ingestDuration = 70.seconds   // Needs to be long enough to allow Lucene to flush index

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
abstract class ClusterSingletonFailoverSpec extends StandaloneMultiJvmSpec(ClusterSingletonFailoverMultiNodeConfig)
  with ScalaFutures {

  import ClusterSingletonFailoverMultiNodeConfig._

  // Used for first start of servers on each node, stopped in test
  lazy val server = new FiloServer(watcher.ref)
  lazy val client1 = new LocalClient(server.cluster.coordinatorActor)

  logger.info(s"Accessed metaStore and colStore.  Cluster should _not_ be starting up yet.")

  // Test fields
  var query1Response: Double = 0
  val chunkDurationTimeout = chunkDuration + 20000.millis

  override def afterAll(): Unit = {
    awaitNodeDown(server)
    super.afterAll()
  }

  "ClusterSingletonFailoverSpec Multi-JVM Test" should "clear data and init dataset on node 1" in {
    runOn(second) {
      // Get a handle to the metastore and colstore from the server so we can clear and validate data/
      // Note that merely accessing them does not start up any cluster, but we are using the server's to ensure
      // that configs are consistent.
      val metaStore = server.cluster.metaStore
      val colStore = server.cluster.memStore.store

      implicit val patienceConfig = PatienceConfig(timeout = Span(10, Seconds), interval = Span(100, Millis))
      metaStore.initialize().futureValue shouldBe Success
      metaStore.clearAllData().futureValue shouldBe Success
      colStore.initialize(dataset, numShards).futureValue shouldBe Success
      colStore.truncate(dataset, numShards).futureValue shouldBe Success

      val datasetObj = TestTimeseriesProducer.dataset
      colStore.initialize(dataset, 4).futureValue shouldBe Success
      logger.info("Dataset created")
    }
    enterBarrier("existing-data-cleared-and-dataset-created")
  }

  it should "be able to bring up FiloServers in right sequence" in {
    // The controller node (first) is where multi-jvm TestController runs.
    // It cannot be shut down, and in this test, is not assigned shards due to
    // the shard assignment strategy of youngest first.
    // The cluster singleton (second) always hands over to the next oldest (first) when oldest goes down.
    // The next oldest (first) then becomes oldest.
    // Nodes up order: second (oldest member), first (second oldest member), third
    // Shards assigned order: third (youngest), first (second-youngest)

    // oldest node hosts the initial cluster singleton
    runOn(second) {
      awaitNodeUp(server)
      watcher.expectMsgPF(20.seconds) {
        case NodeProtocol.PreStart(identity, address) =>
          address shouldEqual server.cluster.selfAddress
          Some(identity) shouldEqual server.cluster.clusterActor
          info(s" => PreStart on address=$address")
      }
    }
    enterBarrier("second-node-started")

    // second-oldest node hosts the handover cluster singleton when oldest becomes unreachable
    // first is a shards-unassigned standby node and the controller
    runOn(first) {
      awaitNodeUp(server)
      watcher.expectNoMessage(20.seconds)
    }
    enterBarrier("first-node-started")

    // shards assigned to second-youngest node, order = second
    runOn(third) {
      awaitNodeUp(server)
      watcher.expectNoMessage(20.seconds)
    }
    enterBarrier("third-node-started")

    awaitCond(server.cluster.state.members.size == roles.size, longDuration)
    info(s"All nodes initialized, address = ${server.cluster.selfAddress}")

    runOn(roles: _*) {
      info(s"  ${myself.name}: ${server.cluster.selfAddress}")
    }
    enterBarrier(s"${roles.size}-roles-initialized")
  }

  it should "be able to validate the cluster status as normal via CLI" in {
    runOn(first, third) {
      validateShardStatus(client1, Some(server.cluster.coordinatorActor)){ _ == ShardStatusActive }
    }
    enterBarrier("cluster-status-normal-on-cli")
  }

        // NOTE: 10000 samples / 100 time series = 100 samples per series
        // 100 * 10s = 1000seconds =~ 16 minutes
  val queryTimestamp = System.currentTimeMillis() - 195.minutes.toMillis

  it should "be able to ingest data into FiloDB via Kafka" in {
    within(chunkDurationTimeout) {
      runOn(third) {
        TestTimeseriesProducer.produceMetrics(source, 1, 10000, 100, 200, 10)
        info(s"Waiting for ingest-duration ($ingestDuration) to pass")
        Thread.sleep(chunkDuration.toMillis + 7000)
      }
      enterBarrier("data1-ingested")
    }
  }

  it should "answer query successfully" in {
    runOn(first, third) { // TODO check second=UnknownDataset
      query1Response = runCliQuery(client1, queryTimestamp)
    }
    enterBarrier("query1-answered")
  }

  it should "have the expected initial shard assignments for up nodes by assignment strategy" in {

    within(chunkDurationTimeout * 30) {
      runOn(third) {
        validateShardAssignments(client1, 2, Seq(0, 1), server.cluster.coordinatorActor)
      }
      enterBarrier("shards-validated-on-singleton-restart-on-third")

      runOn(first) {
        validateShardAssignments(client1, 2, Seq(2, 3), server.cluster.coordinatorActor)
      }
      enterBarrier("shards-validated-on-singleton-restart-on-first")
    }
  }

  it should "failover the ClusterSingleton and recover state on first downed" in {
    // start order: second, first, third
    // stop order: second (no shards but we can't stop first?)
    // singleton handover: second to first

    within(removedDuration) {
      runOn(second) {
        // TODO use the testConductor for this
        // runOn(first) {
        //   testConductor.shutdown(second, abort=true).futureValue
        info(s" Downing $second ${server.cluster.selfAddress}")
        awaitNodeDown(server)
        watcher.expectMsgPF(longDuration) {
          case e @ NodeProtocol.PostStop(_, address) =>
            address shouldEqual server.cluster.selfAddress
        }
      }
      enterBarrier("downed-second")
    }

    within(removedDuration * 5) {
      runOn(first) {
        watcher.expectMsgPF(removedDuration) {
          case e @ NodeProtocol.PreStart(_, address) =>
            address shouldEqual server.cluster.selfAddress
        }
      }
      enterBarrier("cluster-singleton-restarted-on-first")
    }

    runOn(first, third) {
      awaitCond(server.cluster.state.members.size == roles.size - 1, removedDuration)
    }
    enterBarrier("cluster-member-size-2")
  }

  it should "down first, un-assign shards (2,3), update mappers and verify against CLI" in {

    runOn(first) {
      awaitNodeDown(server)
    }
    enterBarrier("shard-assigned-node-down")

    within(removedDuration * 5) {
      runOn(third) {
        awaitCond(server.cluster.state.members.size == 1, longDuration)
      }
      enterBarrier("third-received-member-removed")
    }

    within(removedDuration * 10) {
      runOn(third) {

        client1.getShardMapper(dataset, false, longDuration) forall { mapper =>
          mapper.shardValues.count { case (ref, status) =>
            ref == ActorRef.noSender && status == ShardStatusDown } == 2 &&
          // proves the fix for recovery, with a shard-assigned node that was the singleton node, downed:
          // previously this was still assigned to the stale ref
          // and the stale coord was in the mapper, where now it is not
          mapper.unassignedShards == Seq(2, 3) &&
          mapper.assignedShards  == Seq(0, 1) &&
          mapper.shardsForCoord(server.cluster.coordinatorActor) == Seq(0, 1)
          mapper.unassignedShards.forall(s => mapper.coordForShard(s) == ActorRef.noSender) &&
          mapper.allNodes.headOption.forall(_ == server.cluster.coordinatorActor) &&
          mapper.allNodes.size == 1
        } shouldEqual true
      }
      enterBarrier("on-recovery-shards-unassigned-node-removed")
    }
  }

  ignore should "answer promQL query successfully with same value" in {
    runOn(third) {
      val query2Response = runCliQuery(client1, queryTimestamp)
      (query2Response - query1Response).abs should be < 0.0001
    }
    enterBarrier("query2-answered")
  }

  // no way yet to have a node to reassign shards(2,3) to - constraint in test frameworks
  // and node assignment strategy. 10-20 different ways tried
  ignore should "be able to validate the cluster status as normal again via CLI" in {
    runOn(second) {
      validateShardStatus(client1, Some(server.cluster.coordinatorActor)) { _ == ShardStatusActive }
    }
    enterBarrier("shard-normal-end-of-test")
  }
}
