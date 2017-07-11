package filodb.kafka

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, SourceConfig}
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}

class MergeableConsumerConfigSpec extends AbstractSpec {
  "MergeableConsumerConfig" must {
    "consumer test" in {
      val topic = "test"
      val partitions = 1

      val settings = new KafkaSettings(ConfigFactory.parseString(
        s"""
           |filodb.kafka.config.file="./src/test/resources/full-test.properties"
           |filodb.kafka.topics.ingestion=$topic
           |filodb.kafka.partitions=$partitions
           |filodb.kafka.record-converter="filodb.kafka.StringRecordConverter"
        """.stripMargin))

      val config = new SourceConfig(settings.BootstrapServers, settings.clientId, settings.nativeKafkaConfig)
      val values = config.kafkaConfig

      values(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) must be (settings.BootstrapServers)
      values(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) must be (classOf[LongDeserializer].getName)
      values(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) must be (classOf[StringDeserializer].getName)

      val props = values.asProps
      props.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) must be (settings.BootstrapServers)
      props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) must be (classOf[LongDeserializer].getName)
      props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) must be (classOf[StringDeserializer].getName)
    }
  }
}
