package filodb.kafka

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.example.CustomRecordConverter

class KafkaSettingsSpec extends AbstractSpec {

  def clear(): Unit = {
    System.clearProperty("filodb.kafka.config.file")
    System.clearProperty("filodb.kafka.topics.ingestion")
  }
  override def afterAll(): Unit = clear()
  override def afterEach(): Unit = clear()

  "KafkaSettings" must {
    "have the correct default settings" in {
      System.setProperty("filodb.kafka.config.file", "./src/test/resources/kafka.client.properties")
      val settings = new KafkaSettings()
      import settings._
      settings.nativeKafkaConfig.nonEmpty must be (true)
      BootstrapServers must be("localhost:9092")
      RecordConverterClass must be (classOf[StringRecordConverter].getName)
      IngestionTopic must be("filo_db")
      FailureTopic must be("failure")
      StatusTimeout must be (3000.millis)
      ConnectedTimeout must be (8000.millis)
      GracefulStopTimeout must be (10.seconds)
    }
    "have the correct override settings" in {
      System.setProperty("filodb.kafka.config.file", "./src/test/resources/kafka.client.properties")
      System.setProperty("filodb.kafka.topics.ingestion", "sstest")

      val settings = new KafkaSettings(ConfigFactory.parseString(
        """filodb.kafka.record-converter="org.example.CustomRecordConverter""""))
      import settings._
      settings.nativeKafkaConfig.nonEmpty must be (true)
      BootstrapServers must be("localhost:9092")
      RecordConverterClass must be (classOf[CustomRecordConverter].getName)
      IngestionTopic must be("sstest")
    }
  }
}
