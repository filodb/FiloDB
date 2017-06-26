package filodb.kafka

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.example.SimpleRecordConverter

class KafkaSettingsSpec extends AbstractSpec {
  "KafkaSettings" must {
    "have the correct default settings" in {
      val settings = new KafkaSettings()
      import settings._
      BootstrapServers must be("localhost:9092")
      RecordConverterClass must be ("org.example.SimpleRecordConverter")
      IngestionTopic must be("sstest")
      FailureTopic must be("failure")
      StatusTimeout must be (3000.millis)
      ConnectedTimeout must be (8000.millis)
      GracefulStopTimeout must be (10.seconds)
    }
    "have overwritten values" in {
      val settings = new KafkaSettings(ConfigFactory.parseString(
        """
          |filodb.kafka.topics.ingestion="fu"
          |filodb.kafka.record-converter="org.example.SimpleRecordConverter"
        """.stripMargin))
      import settings._
      RecordConverterClass must be (classOf[SimpleRecordConverter].getName)
      IngestionTopic must be("fu")
    }
  }
}
