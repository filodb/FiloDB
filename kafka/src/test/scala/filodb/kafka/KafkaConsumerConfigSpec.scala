package filodb.kafka

import monix.kafka.KafkaConsumerConfig
import monix.kafka.config.AutoOffsetReset
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.example.CustomDeserializer

class KafkaConsumerConfigSpec extends KafkaSpec {

  "KafkaConsumerConfig" must {
    "have the expected configurations after user configuration and KafkaSettings are passed in" in {
      val settings = new KafkaSettings(source)

      settings.sourceConfig(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) shouldEqual classOf[CustomDeserializer].getName

      val consumerCfg = KafkaConsumerConfig(settings.sourceConfig.asConfig)
      consumerCfg.enableAutoCommit shouldEqual settings.sourceConfig(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
      consumerCfg.autoOffsetReset should be(AutoOffsetReset.Latest)
      consumerCfg.bootstrapServers should be(List("localhost:9092"))
      consumerCfg.groupId.contains("org.example.cluster1.filodb.consumer1") shouldEqual true
    }
  }
}
