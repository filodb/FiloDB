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
import filodb.coordinator.NodeClusterActor.IngestionSource
import filodb.coordinator.client.IngestionCommands.DatasetSetup
import filodb.core.memstore.{IngestRecord, IngestRouting, TimeSeriesMemStore}
import filodb.core.metadata.Dataset
import filodb.core.store.{InMemoryMetaStore, NullColumnStore}
import filodb.memory.format.ArrayStringRowReader

/** 1. Start Zookeeper
  * 2. Start Kafka (tested with Kafka 0.10.2.1 and 0.11)
  * 3. Create a new topic
  *   ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 \
  *              --partitions 2 --topic integration-test-topic2
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
      val config = ConfigFactory.parseString(
        s"""
           |include file("./src/test/resources/sourceconfig.conf")
           |sourceconfig {
           |  filo-topic-name = "integration-test-topic"
           |  # consumer:
           |  filo-record-converter = "${classOf[PartitionRecordConverter].getName}"
           |  filo-log-config = true
           |  value.deserializer = "org.example.CustomDeserializer"
           |  # producer:
           |  value.serializer = "org.example.CustomSerializer"
           |  partitioner.class = "${classOf[LongKeyPartitionStrategy].getName}"
           |}
        """.stripMargin)
      val sinkConfig = new SinkConfig(config)
      val producer = PartitionedProducerSink.create[JLong, String](sinkConfig, io)

      var read = 0

      val tasks = for (partition <- 0 until numPartitions) yield {

        memStore.setup(dataset, partition)

        // start consumers before producers so that all messages are consumed properly.
        // The consumer task creates one ingestion stream per topic-partition (consumer.assign(topic,partition)
        // this is currently a 1:1 Observable stream
        val sourceT = {
          val streamFactory = ctor.newInstance().asInstanceOf[IngestionStreamFactory]
          streamFactory.isInstanceOf[KafkaIngestionStreamFactory] shouldEqual true
          val stream = streamFactory
            .create(config, dataset, partition, None)
            .get
            .map { m => read +=1; m }
            .take(count)

          Task.fromFuture(memStore.ingestStream(dataset.ref, partition, stream) { err => throw err })
        }

        // The producer task creates `count` ProducerRecords, each range divided equally between the topic's partitions
        val sinkT = Observable.range(0, count)
          .map(msg => new ProducerRecord[JLong, String](sinkConfig.IngestionTopic,
                                                        JLong.valueOf(partition),
                                                        msg.toString))
          .bufferIntrospective(1024)
          .consumeWith(producer)

        Task.zip2(Task.fork(sourceT), Task.fork(sinkT)).runAsync
      }

      tasks foreach { task => Await.result(task, 60.seconds) }

      read should be > (count)
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
