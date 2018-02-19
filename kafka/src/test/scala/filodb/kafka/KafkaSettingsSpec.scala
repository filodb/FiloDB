package filodb.kafka

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import filodb.core.AbstractSpec

trait KafkaSpec extends AbstractSpec {
  // if run by intellij, ./kafka/src
  val testConfig =  ConfigFactory.parseFile(new java.io.File("./src/test/resources/sourceconfig.conf"))

}

class KafkaSettingsSpec extends KafkaSpec {
  "KafkaSettings" must {
    "get correct global Kafka module defaults" in {
      KafkaSettings.FailureTopic shouldEqual "failure"
      KafkaSettings.StatusTimeout shouldEqual 3000.millis
      KafkaSettings.ConnectedTimeout shouldEqual 8000.millis
      KafkaSettings.GracefulStopTimeout shouldEqual 10.seconds
    }
  }
}
