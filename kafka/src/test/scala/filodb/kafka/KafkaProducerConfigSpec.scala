package filodb.kafka

import com.typesafe.config.ConfigFactory
import monix.kafka.{KafkaConsumerConfig, KafkaProducerConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

class KafkaProducerConfigSpec extends ConfigSpec {
  "KafkaProducerConfig" must {
    "have the expected configurations after user configuration and KafkaSettings are passed in" in {
      val settings = new KafkaSettings(ConfigFactory.parseString(
        s"""
           |filodb.kafka.topics.ingestion="fu"
           |filodb.kafka.partitions=128
           |filodb.kafka.record-converter="filodb.kafka.StringRecordConverter"
        """.stripMargin))

      settings.sinkConfig.kafkaConfig(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG) must be (classOf[StringSerializer].getName)

      val config = KafkaProducerConfig(settings.sinkConfig.asConfig)
      config.bufferMemoryInBytes must be (settings.sinkConfig.kafkaConfig(ProducerConfig.BUFFER_MEMORY_CONFIG))
    }
  }
}

class KafkaConsumerConfigSpec extends ConfigSpec {
  "KafkaConsumerConfig" must {
    "have the expected configurations after user configuration and KafkaSettings are passed in" in {
      val settings = new KafkaSettings(ConfigFactory.parseString(
        s"""
           |filodb.kafka.topics.ingestion="fu"
           |filodb.kafka.partitions=128
           |filodb.kafka.record-converter="filodb.kafka.StringRecordConverter"
        """.stripMargin))

      settings.sourceConfig.kafkaConfig(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) must be (classOf[StringDeserializer].getName)

      val config = KafkaConsumerConfig(settings.sourceConfig.asConfig)
      config.enableAutoCommit must be (settings.sourceConfig.kafkaConfig(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
    }
  }
}