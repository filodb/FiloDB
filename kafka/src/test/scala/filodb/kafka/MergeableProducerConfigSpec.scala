package filodb.kafka

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{ProducerConfig, SinkConfig}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

class MergeableProducerConfigSpec extends AbstractSpec {
  "MergeableProducerConfig" must {
    "producer test" in {
      val topic = "test"
      val partitions = 1

      val settings = new KafkaSettings(ConfigFactory.parseString(
        s"""
           |filodb.kafka.config.file="./src/test/resources/full-test.properties"
           |filodb.kafka.topics.ingestion=$topic
           |filodb.kafka.partitions=$partitions
           |filodb.kafka.record-converter="filodb.kafka.StringRecordConverter"
        """.stripMargin))

      val config = new SinkConfig(settings.BootstrapServers, settings.clientId, settings.nativeKafkaConfig)
      val values = config.kafkaConfig

      values(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) must be ("localhost:9092")
      values(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG) must be (classOf[LongSerializer].getName)
      values(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG) must be (classOf[StringSerializer].getName)

      val props = values.asProps
      props.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) must be (settings.BootstrapServers)
      props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG) must be (classOf[LongSerializer].getName)
      props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG) must be (classOf[StringSerializer].getName)
    }
  }
}