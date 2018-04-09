package filodb.kafka

import java.lang.{Long => JLong}

import scala.collection.mutable.{HashMap => MutableHashMap, Map => MutableMap}
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import monix.kafka.KafkaConsumerObservable
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition

import filodb.coordinator.{IngestionStream, IngestionStreamFactory}
import filodb.coordinator.NodeClusterActor.IngestionSource
import filodb.coordinator.client.IngestionCommands.DatasetSetup
import filodb.core.memstore.{IngestRecord, IngestRouting, TimeSeriesMemStore}
import filodb.core.metadata.Dataset
import filodb.core.store.{InMemoryMetaStore, NullColumnStore}
import filodb.memory.format.ArrayStringRowReader

object KafkaIngestionStreamSuite {

  val topic = "integration-suite-topic16"

  ConfigFactory.invalidateCaches()

  private val commonConfig = ConfigFactory.parseString(
    s"""
       |sourceconfig {
       |  filo-topic-name = "$topic"
       |  filo-log-config = true
       |  bootstrap.servers = "localhost:9092"
       |}
        """.stripMargin)

  val streamConfig = ConfigFactory.parseString(
    s"""
       |sourceconfig {
       |  filo-record-converter = "${classOf[PartitionRecordConverter].getName}"
       |  value.deserializer = "org.example.CustomDeserializer"
       |}
        """.stripMargin).withFallback(commonConfig)

  val producerConfig = ConfigFactory.parseString(
    s"""
       |sourceconfig {
       |  key.serializer = "org.apache.kafka.common.serialization.LongSerializer"
       |  value.serializer = "org.example.CustomSerializer"
       |  partitioner.class = "${classOf[LongKeyPartitionStrategy].getName}"
       |}
        """.stripMargin).withFallback(commonConfig)
}

/** 1. Start Zookeeper
  * 2. Start Kafka (tested with Kafka 0.10.2.1 and 0.11)
  * 3. Create a new topic
  * ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 \
  * --partitions 2 --topic ${KafkaIngestionStreamSuite.topic}
  * Make sure the configured settings for number of partitions and topic name below match what you created.
  * 4. Run test either from Intellij or SBT:
  * > kafka/it:testOnly filodb.kafka.KafkaIngestionStreamSuite
  */
class KafkaIngestionStreamSuite extends KafkaSpec with StrictLogging {

  import KafkaIngestionStreamSuite._

  private val count = 1000
  private val runs = 2
  private val numPartitions = 2

  private var offsetsByRunAndPartition: MutableMap[(Int, Int), Long] = MutableHashMap.empty
  private var total: Seq[Long] = Seq.empty

  private val dataset = Dataset("metrics", Seq("series:string"), Seq("timestamp:long", "value:int"))
  private val src = IngestionSource(classOf[KafkaIngestionStreamFactory].getName)
  private val ds = DatasetSetup(dataset.asCompactString, src)
  private val ctor = Class.forName(ds.source.streamFactoryClass).getConstructors.head

  implicit val timeout: Timeout = 10.seconds
  implicit val io = Scheduler.io("filodb-kafka-tests")

  private val sinkConfig = new SinkConfig(producerConfig)
  private val producer = PartitionedProducerObservable.create[JLong, String](sinkConfig, io)

  private val globalConfig = ConfigFactory.load("application_test.conf")

  private val memStore = new TimeSeriesMemStore(
    globalConfig.getConfig("filodb"), new NullColumnStore, new InMemoryMetaStore())

  override def beforeAll(): Unit = memStore.setup(dataset, 0)

  override def beforeEach(): Unit = {
    memStore.reset()
    offsetsByRunAndPartition = MutableHashMap.empty
    total = Seq.empty
  }

  override def afterAll(): Unit = memStore.shutdown()

  "KafkaIngestionStreamFactory" must {
    s"consume messages from $numPartitions partitions, and not commit offsets using the Kafka offset API" in {

      val streamFactory = ctor.newInstance().asInstanceOf[IngestionStreamFactory]
      streamFactory.isInstanceOf[KafkaIngestionStreamFactory] shouldEqual true
      validateIngestion(streamFactory)
    }

    s"consume messages from $numPartitions partitions, and commit offsets using the Kafka offset API" in {

      validateIngestion(new IngestionStreamFactory {
        override def create(config: Config, dataset: Dataset, shard: Int, offset: Option[Long]): IngestionStream =
          new CommittingKafkaIngestionStream(config, dataset, shard, offset)
      })
    }
  }

  private def validateIngestion(streamFactory: IngestionStreamFactory): Unit = {

    (0 until numPartitions).foreach(memStore.setup(dataset, _))

    for {
      run       <- 0 until runs
      partition <- 0 until numPartitions
    } {
      val from = if (run > 0) offsetsByRunAndPartition.get((run - 1, partition)).map(_ + 1) else None
      val offsets = tasks(streamFactory, partition, from)
      offsets.map(_.size).size shouldEqual count
      val currentOffsets = offsets.flatMap(_.headOption)
      total ++= currentOffsets

      val latestOffset = currentOffsets.max
      offsetsByRunAndPartition((run, partition)) = latestOffset

      logger.info(
        s"""
           |Task [run=$run, partition=$partition]:
           |      Offset range:     ${currentOffsets.min} - $latestOffset
           |      All last offsets: $offsetsByRunAndPartition
           |      Total ingested:   ${total.size}
           """.stripMargin)
    }

    total.size shouldEqual (count * numPartitions) * runs

    // We pass in the 'from' offset on the second run in order to validate these:
    val run0_partition0_offset = offsetsByRunAndPartition((0, 0))
    val run0_partition1_offset = offsetsByRunAndPartition((0, 1))
    val run1_partition0_offset = offsetsByRunAndPartition((1, 0))
    val run1_partition1_offset = offsetsByRunAndPartition((1, 1))

    run1_partition0_offset shouldEqual run0_partition0_offset + count
    run1_partition1_offset shouldEqual run0_partition1_offset + count

    logger.info(
      s"""
         |Published: ${(count * numPartitions) * runs}
         |Ingested:  ${total.size}
         |
         |Run=0, partition=0 offset=$run0_partition0_offset
         |Run=1, partition=0 offset=$run1_partition0_offset
         |
         |Run=0, partition=1 offset=$run0_partition1_offset
         |Run=1, partition=1 offset=$run1_partition1_offset
         """.stripMargin)
  }

  // this is currently a 1:1 Observable stream
  // The producer task creates `count` ProducerRecords, each range divided equally between the topic's partitions
  private def tasks(streamFactory: IngestionStreamFactory, partition: Int, from: Option[Long]): Seq[Seq[Long]] = {

    var values: Seq[Seq[Long]] = Seq.empty

    val sourceT = {
      val stream = streamFactory
        .create(streamConfig, dataset, partition, from)
        .get
        .map { m => values :+= m.map(_.offset); m }
        .take(count)

      Task.fromFuture(memStore.ingestStream(dataset.ref, partition, stream) { err => throw err })
    }

    val sinkT = Observable.range(0, count)
      .map(msg => new ProducerRecord[JLong, String](
        sinkConfig.IngestionTopic, JLong.valueOf(partition), msg.toString))
      .bufferIntrospective(1024)
      .consumeWith(producer)

    Thread.sleep(1000) // TODO for some reason this is needed for this specific test
    val task = Task.zip2(Task.fork(sourceT), Task.fork(sinkT)).runAsync

    Await.result(task, 60.seconds)

    values
  }
}

final class CommittingKafkaIngestionStream(config: Config,
                                           dataset: Dataset,
                                           shard: Int,
                                           offset: Option[Long])
  extends KafkaIngestionStream(config, dataset, shard, offset) {

  override protected val consumer = {
    val sourceConfig = new SourceConfig(config, shard)
    val tp = new TopicPartition(sourceConfig.IngestionTopic, shard)
    val consumer = PartitionedConsumerObservable.createConsumer(sourceConfig, tp, offset)
    val cfg = PartitionedConsumerObservable.consumerConfig(sourceConfig)
    require(!cfg.enableAutoCommit, "'enable.auto.commit' must be false.")
    KafkaConsumerObservable[JLong, Any](cfg, consumer)
  }
}

final class PartitionRecordConverter(dataset: Dataset) extends RecordConverter with StrictLogging {

  private val routing = IngestRouting(dataset, Seq("series", "timestamp", "value"))

  override def convert(event: AnyRef, partition: Int, offset: Long): Seq[IngestRecord] = {
    val reader = ArrayStringRowReader(Array(partition.toString, event.asInstanceOf[String], partition.toString))
    Seq(IngestRecord(routing, reader, offset))
  }
}
