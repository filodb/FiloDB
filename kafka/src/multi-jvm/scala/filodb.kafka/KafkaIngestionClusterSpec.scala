package filodb.kafka

import java.lang.{Long => JLong}

import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.coordinator.{NodeClusterActor, QueryCommands}
import filodb.core._
import filodb.core.memstore.{IngestRecord, IngestRouting}
import filodb.core.metadata.Dataset

import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord

object KafkaIngestionClusterSpecConfig extends MultiNodeConfig {
  // register the named roles (nodes) of the test
  val first = role("first")
  val second = role("second")

  val NumPartitions = 4
  val NumRecords = 10000

  var convertedRecords = 0

  val sourceConfig = ConfigFactory.parseString(
    s"""
       |filo-topic-name="multijvm-test-topic"
       |filo-record-converter="${classOf[CSVRecordConverter].getName}"
       |partitioner.class = "${classOf[LongKeyPartitionStrategy].getName}"
       |value.serializer=org.apache.kafka.common.serialization.StringSerializer
       |value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
       |auto.offset.reset=latest
        """.stripMargin)

  // this configuration will be used for all nodes
  val globalConfig = ConfigFactory.parseString("""filodb.memtable.write.interval = 500 ms
                                                 |akka.logger-startup-timeout = 10s
                                                 |filodb.memtable.filo.chunksize = 70
                                                 |filodb.memtable.max-rows-per-table = 70""".stripMargin)
                       .withFallback(ConfigFactory.load("application_test.conf"))
  commonConfig(globalConfig)
}

/**
  * A multi-JVM Kafka ingestion test
  *
  * 1. Start Zookeeper
  * 2. Start Kafka (tested with Kafka 0.10.2.1 and 0.11)
  * 3. Create a new topic
  *   ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic multijvm-test-topic
  *    Make sure the configured settings for number of partitions and topic name below match what you created.
  * 4. Run test either from Intellij or SBT:
  *    > kafka/multi-jvm:testOnly filodb.kafka.KafkaIngestionClusterSpec
  */
abstract class KafkaIngestionClusterSpec extends ClusterSpec(KafkaIngestionClusterSpecConfig) {

  import KafkaIngestionClusterSpecConfig._
  import NodeClusterActor._
  import QueryCommands._

  override def initialParticipants = roles.size

  override def beforeAll() = multiNodeSpecBeforeAll()

  override def afterAll() = multiNodeSpecAfterAll()

  val config = globalConfig.getConfig("filodb")
  val settings = new KafkaSettings(sourceConfig)

  val address1 = node(first).address
  val address2 = node(second).address

  // implicit val timeout: Timeout = 10.seconds
  implicit val io = Scheduler.io("filodb-kafka-tests")

  private val metaStore = cluster.metaStore
  metaStore.newDataset(dataset1).futureValue should equal (Success)

  it("should start actors, join cluster, setup ingestion, and wait for all nodes to enter barrier") {
    // Start NodeCoordinator on all nodes so the ClusterActor will register them
    cluster.coordinatorActor
    cluster join address1
    awaitCond(cluster.isJoined)
    enterBarrier("both-nodes-joined-cluster")

    val clusterActor = cluster.clusterSingletonProxy("worker", withManager = true)
    enterBarrier("cluster-actor-started")

    runOn(first) {
      val source = IngestionSource(classOf[KafkaIngestionStreamFactory].getName, sourceConfig)
      val msg = SetupDataset(datasetRef,
                             DatasetResourceSpec(NumPartitions, 2),
                             source)
      clusterActor ! msg
      // It takes a _really_ long time for the cluster actor singleton to start.
      expectMsg(30.seconds.dilated, DatasetVerified)
      Thread sleep 15000 // wait for kafka consumers to start
    }
    enterBarrier("dataset-setup-done")
  }

  /**
   * Only one node is going to send to Kafka, but we will get counts from both nodes and all shards
   */
  it("should send data to Kafka, and do distributed querying") {
    runOn(first) {
      val producer = PartitionedProducerSink.create[JLong, String](settings, io)
      var produced = 0

      // TODO: switch to a Partitioner that actually uses ShardMapper logic to produce shard #
      // Here, we just cheat a little bit.  Take hash of series name -> shard
      val stream = linearMultiSeries().take(NumRecords)
      val sinkT = Observable.fromIterable(stream)
        .map { values => produced += 1
          new ProducerRecord[JLong, String](settings.IngestionTopic,
            JLong.valueOf(values.last.hashCode % NumPartitions),
            values.mkString(",")) }
        .bufferIntrospective(1024)
        .consumeWith(producer)

      Await.result(sinkT.runAsync, 30.seconds)
      logger.info(s" ==> total records produced:  $produced")

      // TODO: find a way to wait for ingestion to be done via subscribing to node events
      Thread sleep 10000
    }

    enterBarrier("ingestion-done")

    val ingestedRows = (0 until NumPartitions).map(s => cluster.memStore.numRowsIngested(datasetRef, s))
    logger.info(s" ==> memStore ingested rows by shard: $ingestedRows")
    logger.info(s" ==> total number of converted records: $convertedRecords")

    // Both nodes can execute a distributed query to all shards and should get back the same answer
    // Count all the records in every partition in every shard
    // counting a non-partition column... can't count a partition column yet
    val query = AggregateQuery(datasetRef, QueryArgs("count", "min"), FilteredPartitionQuery(Nil))
    cluster.coordinatorActor ! query
    val answer = expectMsgClass(classOf[AggregateResponse[Int]])
    answer.elementClass should equal (classOf[Int])
    answer.elements should equal (Array(NumRecords))
  }
}

final class CSVRecordConverter(dataset: Dataset) extends RecordConverter {
  val routing = IngestRouting(dataset, Seq("timestamp", "min", "avg", "max", "p90", "series"))

  override def convert(event: AnyRef, partition: Int, offset: Long): Seq[IngestRecord] = {
    KafkaIngestionClusterSpecConfig.convertedRecords += 1
    Seq(IngestRecord(routing, ArrayStringRowReader(event.asInstanceOf[String].split(',')), offset))
  }

}

class KafkaIngestionClusterSpecMultiJvmNode1 extends KafkaIngestionClusterSpec
class KafkaIngestionClusterSpecMultiJvmNode2 extends KafkaIngestionClusterSpec