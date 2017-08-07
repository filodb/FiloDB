package filodb.kafka

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{ProducerConfig, SinkConfig}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

class MergeableProducerConfigSpec extends ConfigSpec {
  "MergeableProducerConfig" must {
    "producer test" in {
      val topic = "test"
      val partitions = 1

      val settings = new KafkaSettings(ConfigFactory.parseString(
        s"""
           |include file("$FullTestPropsPath")
           |filo-topic-name=$topic
           |filo-record-converter="filodb.kafka.StringRecordConverter"
        """.stripMargin))

      val config = new SinkConfig(settings.BootstrapServers, settings.clientId, settings.kafkaConfig)
      val values = config.kafkaConfig

      values(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) should be ("localhost:9092")
      values(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG) should be (classOf[LongSerializer].getName)
      values(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG) should be (classOf[StringSerializer].getName)

      val props = values.asProps
      props.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) should be (settings.BootstrapServers)
      props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG) should be (classOf[LongSerializer].getName)
      props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG) should be (classOf[StringSerializer].getName)
    }
  }
}