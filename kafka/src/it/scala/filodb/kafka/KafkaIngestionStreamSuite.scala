package filodb.kafka

import java.lang.{Long => JLong}

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord

import filodb.coordinator.IngestionStreamFactory
import filodb.coordinator.client.IngestionCommands.DatasetSetup
import filodb.coordinator.NodeClusterActor.IngestionSource
import filodb.core.memstore.{IngestRecord, IngestRouting, TimeSeriesMemStore}
import filodb.core.metadata.Dataset
import filodb.core.store.{InMemoryMetaStore, NullColumnStore}
import filodb.memory.format.ArrayStringRowReader

/** 1. Start Zookeeper
  * 2. Start Kafka (tested with Kafka 0.10.2.1 and 0.11)
  * 3. Create a new topic
  *   ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 \
  *              --partitions 2 --topic integration-test-topic
  *    Make sure the configured settings for number of partitions and topic name below match what you created.
  * 4. Run test either from Intellij or SBT:
  *    > kafka/it:testOnly filodb.kafka.KafkaIngestionStreamSuite
  */
class KafkaIngestionStreamSuite extends KafkaSpec with StrictLogging {

  private val count = 1000
  private val numPartitions = 2
  private val dataset = Dataset("metrics", Seq("series:string"), Seq("timestamp:long", "value:int"))
  private val src = IngestionSource(classOf[KafkaIngestionStreamFactory].getName)
  private val ds = DatasetSetup(dataset.asCompactString, src)
  private val ctor = Class.forName(ds.source.streamFactoryClass).getConstructors.head

  implicit val timeout: Timeout = 10.seconds
  implicit val io = Scheduler.io("filodb-kafka-tests")

  ConfigFactory.invalidateCaches()
  val globalConfig = ConfigFactory.load("application_test.conf")

  private val memStore = new TimeSeriesMemStore(
    globalConfig.getConfig("filodb"), new NullColumnStore, new InMemoryMetaStore())

  override def beforeAll(): Unit = memStore.setup(dataset, 0)

  override def beforeEach(): Unit = memStore.reset()

  override def afterAll(): Unit = memStore.shutdown()

  "KafkaIngestionStreamFactory" must {
    s"consume messages from $numPartitions partitions" in {
      val sourceConfig = ConfigFactory.parseString(
        s"""
           |include file("./src/test/resources/sourceconfig.conf")
           |sourceconfig {
           |  filo-topic-name = "integration-test-topic"
           |  filo-record-converter = "${classOf[PartitionRecordConverter].getName}"
           |  filo-log-consumer-config = true
           |  bootstrap.servers = "localhost:9092"
           |  value.serializer = "org.example.CustomSerializer"
           |  value.deserializer = "org.example.CustomDeserializer"
           |  partitioner.class = "${classOf[LongKeyPartitionStrategy].getName}"
           |}
        """.stripMargin)
      val settings = new KafkaSettings(sourceConfig)
      val producer = PartitionedProducerSink.create[JLong, String](settings, io)

      val tasks = for (partition <- 0 until numPartitions) yield {

        memStore.setup(dataset, partition)

        // start consumers before producers so that all messages are consumed properly.
        // The consumer task creates one ingestion stream per topic-partition (consumer.assign(topic,partition)
        // this is currently a 1:1 Observable stream
        val sourceT = {
          val streamFactory = ctor.newInstance().asInstanceOf[IngestionStreamFactory]
          streamFactory.isInstanceOf[KafkaIngestionStreamFactory] shouldEqual true
          val stream = streamFactory
            .create(sourceConfig, dataset, partition, None)
            .get
            .take(count)

          Task.fromFuture(memStore.ingestStream(dataset.ref, partition, stream) { err => throw err })
        }
        Thread.sleep(1000) // so that the consumers start fully before the producers begin

        // now start producers
        // The producer task creates `count` ProducerRecords, each range divided equally between the topic's partitions
        val sinkT = Observable.range(0, count)
          .map(msg => new ProducerRecord[JLong, String](settings.IngestionTopic,
                                                        JLong.valueOf(partition),
                                                        msg.toString))
          .bufferIntrospective(1024)
          .consumeWith(producer)

        Task.zip2(Task.fork(sourceT), Task.fork(sinkT)).runAsync
      }

      tasks foreach { task => Await.result(task, 60.seconds) }
    }
  }
}


final class PartitionRecordConverter(dataset: Dataset) extends RecordConverter {
  val routing = IngestRouting(dataset, Seq("series", "timestamp", "value"))
  override def convert(event: AnyRef, partition: Int, offset: Long): Seq[IngestRecord] =
    Seq(IngestRecord(routing, ArrayStringRowReader(Array(partition.toString,
                                                         event.asInstanceOf[String],
                                                         partition.toString)), offset))

}
