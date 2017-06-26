package filodb.kafka

import scala.concurrent.duration._

import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetResetStrategy}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes.LongSerde
import org.apache.kafka.common.serialization.{LongDeserializer, LongSerializer}
import org.apache.kafka.streams.StreamsConfig
import org.example._

class SettingsSpec extends AbstractSpec {
  "SourceSettings" must {
    "have a default ConsumerConfig" in {
      val settings = new SourceSettings()

      val config = settings.consumerConfig
      config.contains(ConsumerConfig.GROUP_ID_CONFIG) must be (true)
      config(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) must be(settings.BootstrapServers)
      config(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) must be(classOf[LongDeserializer].getName)
      config(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) must be(classOf[CustomDeserializer].getName)
      config(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) must be(OffsetResetStrategy.EARLIEST.toString.toLowerCase)
      config(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) must be("false")
    }
    "have a default ProducerConfig" in {
      val settings = new SinkSettings()
      import settings._

      StatusLogInterval must be (5000.millis)
      EnableFailureChannel must be (false)
      StatusLogInterval must be (5000.millis)
      PublishTimeout must be (5000.millis)

      val config = settings.producerConfig
      config(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) must be(settings.BootstrapServers)
      config(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG) must be(classOf[LongSerializer].getName)
      config(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG) must be(classOf[CustomSerializer].getName)
    }
    "have a default StreamsConfig" in {
      val settings = new SourceSettings()

      val config = settings.streamsConfig
      config(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG) must be(settings.BootstrapServers)
      config(StreamsConfig.KEY_SERDE_CLASS_CONFIG) must be(classOf[LongSerde])
      config(StreamsConfig.VALUE_SERDE_CLASS_CONFIG) must be(classOf[CustomSerde].getName)
      config.get(StreamsConfig.APPLICATION_ID_CONFIG).nonEmpty must be(true)
    }
  }
}

