package filodb.kafka

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.example.{CustomDeserializer, CustomRecordConverter}

import filodb.coordinator.GlobalConfig
import filodb.core.AbstractSpec

class KafkaSettingsSpec extends AbstractSpec {
  val defaultConfigKeys = GlobalConfig.systemConfig.getConfig("filodb.kafka")
                                      .flattenToMap
                                      .keys
                                      .filterNot(_.startsWith("filo-"))

  "KafkaSettings" must {
    "parse Typesafe config correctly with defaults" in {
      val settings = new KafkaSettings(ConfigFactory.parseString(
        """
          |filo-record-converter="org.example.CustomRecordConverter"
          |filo-topic-name=raw_events
          |value.deserializer=org.example.CustomDeserializer
        """.stripMargin))

      import settings._
      settings.kafkaConfig.keys should be(Set("value.deserializer") ++ defaultConfigKeys)
      settings.sourceConfig.kafkaConfig(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) should be (classOf[CustomDeserializer].getName)
      BootstrapServers should be("localhost:9092")
      RecordConverterClass should be (classOf[CustomRecordConverter].getName)
      IngestionTopic should be("raw_events")
    }

    "parse a properties file correctly with defaults" in {
      val settings = new KafkaSettings(ConfigFactory.parseFile(
                                       new java.io.File("./src/test/resources/settings-full.properties")))
      import settings._
      settings.kafkaConfig.keys should be(Set("value.deserializer") ++ defaultConfigKeys)
      settings.sourceConfig.kafkaConfig(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) should be (classOf[CustomDeserializer].getName)
      BootstrapServers should be("localhost:9092")
      RecordConverterClass should be (classOf[CustomRecordConverter].getName)
      IngestionTopic should be("raw_events")
    }

    "parse Typesafe config that points to a properties file" in {
      val settings = new KafkaSettings(ConfigFactory.parseString(
        """
          |include file("./src/test/resources/settings-partial.properties")
          |filo-record-converter="org.example.CustomRecordConverter"
          |filo-topic-name=raw_events
        """.stripMargin))

      import settings._
      settings.kafkaConfig.keys should be(Set("value.deserializer") ++ defaultConfigKeys)
      settings.sourceConfig.kafkaConfig(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) should be (classOf[CustomDeserializer].getName)
      BootstrapServers should be("localhost:9092")
      RecordConverterClass should be (classOf[CustomRecordConverter].getName)
      IngestionTopic should be("raw_events")
    }

    "get correct global Kafka module defaults" in {
      KafkaSettings.FailureTopic should be("failure")
      KafkaSettings.StatusTimeout should be (3000.millis)
      KafkaSettings.ConnectedTimeout should be (8000.millis)
      KafkaSettings.GracefulStopTimeout should be (10.seconds)
    }
  }
}
