package filodb.kafka

import monix.kafka.KafkaProducerConfig
import monix.kafka.config.Acks
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.LongSerializer

class KafkaProducerConfigSpec extends KafkaSpec {
  import ProducerConfig._

  "SinkConfig" must {
    "have the expected KafkaProducerConfig" in {
      val sink = new SinkConfig(testConfig)
      sink.IngestionTopic shouldEqual "raw_events"
      sink.LogConfig shouldEqual false

      val values = sink.config
      values("my.custom.client.namespace") shouldEqual "custom.value"
      values(BOOTSTRAP_SERVERS_CONFIG) shouldEqual "localhost:9092"
      values(KEY_SERIALIZER_CLASS_CONFIG) shouldEqual classOf[LongSerializer].getName

      val producerCfg = KafkaProducerConfig(sink.config.asConfig)
      producerCfg.acks shouldEqual Acks.NonZero(1)
      producerCfg.bootstrapServers shouldEqual List("localhost:9092")
    }
  }
}