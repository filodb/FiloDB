package filodb.kafka

import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import monix.kafka.{KafkaProducer, _}
import monix.kafka.config.{Acks, AutoOffsetReset}
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.JavaConverters._

/** Run against kafka:
  * ./bin/kafka-topics.sh --create --zookeeper localhost:2181   --replication-factor 1 --partitions 2   --topic filodb-kafka-tests4
  */
class SourceSinkSuite extends AbstractSpec {

  private val topic = "filodb-kafka-tests5"
  private val partitions = 1

  private val settings = new KafkaSettings(ConfigFactory.parseString(
    s"""
       |filodb.kafka.config.file="./src/test/resources/full-test.properties"
       |filodb.kafka.topics.ingestion=$topic
       |filodb.kafka.partitions=$partitions
       |filodb.kafka.record-converter="filodb.kafka.StringRecordConverter"
        """.stripMargin))

  private val tps = (0 until settings.NumPartitions).map(p => new TopicPartition(topic, p)).toList

  lazy val io = Scheduler.io("filodb-kafka-tests")

  "FiloDBKafka" must {
    "have the expected configuration" in {
      settings.IngestionTopic must be(topic)
      settings.NumPartitions must be(partitions)
      tps.size must be(partitions)
      settings.RecordConverterClass must be(classOf[StringRecordConverter].getName)
      settings.BootstrapServers must be("localhost:9092")
    }
    "have the expected consumer/producer configuration" in {
      val producerCfg = KafkaProducerConfig(settings.producerConfig)
      val consumerCfg = KafkaConsumerConfig(settings.consumerConfig)

      producerCfg.bootstrapServers must be (List("localhost:9092"))
      consumerCfg.bootstrapServers must be (List("localhost:9092"))
      producerCfg.acks must be (Acks.NonZero(1))
      producerCfg.clientId.contains("filodb") must be (true)

      consumerCfg.autoOffsetReset must be (AutoOffsetReset.Earliest)
      consumerCfg.groupId must be ("") // why does monix not use null if unset? bad.
    }
    "publish one message" in {
      val producerCfg = KafkaProducerConfig(settings.producerConfig)
      val consumerCfg = KafkaConsumerConfig(settings.consumerConfig)
      consumerCfg.autoOffsetReset must be (AutoOffsetReset.Earliest)
      consumerCfg.groupId must be ("")

      val producer = KafkaProducer[String, String](producerCfg, io)
      val consumerTask = PartitionedConsumerObservable.createConsumer[String, String](
        consumerCfg, tps).executeOn(io)

      val consumer = Await.result(consumerTask.runAsync, 60.seconds)

      try {
        val send = producer.send(topic, "my-message")
        Await.result(send.runAsync, 30.seconds)

        consumer.assign(tps.asJava)
        val records = consumer.poll(10.seconds.toMillis).asScala.map(_.value()).toList
        assert(records == List("my-message"))
      }
      finally {
        Await.result(producer.close().runAsync, Duration.Inf)
        consumer.close()
      }
    }

    "listen for one message" in {
      val producerCfg = KafkaProducerConfig(settings.producerConfig)
      val consumerCfg = KafkaConsumerConfig(settings.consumerConfig)
      consumerCfg.autoOffsetReset must be (AutoOffsetReset.Earliest)
      consumerCfg.groupId must be ("")

      val producer = KafkaProducer[String, String](producerCfg, io)
      val consumer = PartitionedConsumerObservable[String, String](consumerCfg, tps).executeOn(io)

      try {
        val send = producer.send(topic, "test-message")
        Await.result(send.runAsync, 30.seconds)

        val first = consumer.take(1).map(_.value()).firstL
        val result = Await.result(first.runAsync, 30.seconds)
        assert(result == "test-message")
      }
      finally {
        Await.result(producer.close().runAsync, Duration.Inf)
      }
    }

    "full producer/consumer test" in {
      val count = 10000

      val producerCfg = KafkaProducerConfig(settings.producerConfig)
      val consumerCfg = KafkaConsumerConfig(settings.consumerConfig)
      consumerCfg.autoOffsetReset must be (AutoOffsetReset.Earliest)
      consumerCfg.groupId must be ("")

      val producer = KafkaProducerSink[String, String](producerCfg, io)
      val consumer = PartitionedConsumerObservable[String, String](consumerCfg, tps).executeOn(io).take(count)

      val pushT = Observable.range(0, count)
        .map(msg => new ProducerRecord(topic, "obs", msg.toString))
        .bufferIntrospective(1024)
        .consumeWith(producer)

      val listT = consumer
        .map(record => (record.offset, record.value))
        .toListL

      val (result, _) = Await.result(Task.zip2(Task.fork(listT), Task.fork(pushT)).runAsync, 60.seconds)
      assert(result.map(_._2.toInt).sum == (0 until count).sum)
    }
  }
}
