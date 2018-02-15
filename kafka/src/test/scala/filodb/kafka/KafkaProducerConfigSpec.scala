package filodb.kafka

import monix.kafka.KafkaProducerConfig
import monix.kafka.config.Acks
import org.apache.kafka.clients.producer.ProducerConfig
import org.example.CustomSerializer

class KafkaProducerConfigSpec extends KafkaSpec {

  "KafkaProducerConfig" must {
    "have the expected configurations after user configuration and KafkaSettings are passed in" in {
      val settings = new KafkaSettings(source)

      settings.sinkConfig(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG) shouldEqual classOf[CustomSerializer].getName

      val producerCfg = KafkaProducerConfig(settings.sinkConfig.asConfig)
      producerCfg.acks shouldEqual Acks.NonZero(1)
      producerCfg.bootstrapServers shouldEqual List("localhost:9092")
    }
  }
}