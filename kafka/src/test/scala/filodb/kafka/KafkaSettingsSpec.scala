package filodb.kafka

import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.example.{CustomDeserializer, CustomRecordConverter}

class KafkaSettingsSpec extends AbstractSpec {

  override def beforeAll(): Unit = {
    if (sys.props.get("idea.launcher.bin.path").nonEmpty) {
      System.setProperty("filodb.kafka.config.file", "./kafka/src/test/resources/kafka.client.properties")
    }
  }

  override def afterAll(): Unit = System.clearProperty("filodb.kafka.config.file")

  "KafkaSettings" must {
    "have the correct default settings" in {
      val settings = new KafkaSettings()
      import settings._
      settings.nativeKafkaConfig.nonEmpty must be (true)
      settings.sourceConfig.kafkaConfig(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) must be (classOf[CustomDeserializer].getName)
      BootstrapServers must be("localhost:9092")
      RecordConverterClass must be (classOf[StringRecordConverter].getName)
      IngestionTopic must be("filo_db")
      FailureTopic must be("failure")
      StatusTimeout must be (3000.millis)
      ConnectedTimeout must be (8000.millis)
      GracefulStopTimeout must be (10.seconds)
    }
    "have the correct override settings" in {
      val settings = new KafkaSettings(ConfigFactory.parseString(
        """
          |filodb.kafka.record-converter="org.example.CustomRecordConverter"
          |filodb.kafka.topics.ingestion="sstest"
        """.stripMargin))
      import settings._
      settings.nativeKafkaConfig.nonEmpty must be (true)
      BootstrapServers must be("localhost:9092")
      RecordConverterClass must be (classOf[CustomRecordConverter].getName)
      IngestionTopic must be("sstest")
    }
  }
}
