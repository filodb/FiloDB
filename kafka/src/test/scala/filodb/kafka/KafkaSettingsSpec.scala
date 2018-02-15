package filodb.kafka

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.LongDeserializer
import org.example._

import filodb.coordinator.GlobalConfig
import filodb.core.AbstractSpec

trait KafkaSpec extends AbstractSpec {
  // if run by intellij, ./kafka/src
  val source =  ConfigFactory.parseFile(new java.io.File("./src/test/resources/sourceconfig.conf"))

}

class KafkaSettingsSpec extends KafkaSpec {

  private val defaultConfigKeys =
    GlobalConfig.systemConfig.getConfig("filodb.kafka")
      .flattenToMap
      .keys.toSeq.filterNot(_.startsWith("filo-"))

  "KafkaSettings" must {
    "fail if no required configs by user are not provided" in {
      intercept[IllegalArgumentException](new KafkaSettings(ConfigFactory.empty))
    }

    "fail if record converter from user is not provided" in {
      intercept[IllegalArgumentException] {
        new KafkaSettings(ConfigFactory.parseString(
          """sourceconfig.filo-record-converter = "org.example.CustomRecordConverter""""))
      }
    }

    "fail if topic from user is not provided" in {
      intercept[IllegalArgumentException] {
        new KafkaSettings(ConfigFactory.parseString(
          """sourceconfig.filo-topic-name = "test""""))
      }
    }

    "have the expected defaults" in {
      val settings = new KafkaSettings(ConfigFactory.parseString(
        """
          |sourceconfig.bootstrap.servers = "localhost:9092"
          |sourceconfig.filo-topic-name = "test"
          |sourceconfig.filo-record-converter = "org.example.CustomRecordConverter"
          |sourceconfig.value.deserializer = "org.example.CustomDeserializer"
          |sourceconfig.client.id = "org.example.env.client1"
          |sourceconfig.sasl.mechanism = "PLAIN"
          |sourceconfig.my.custom.client.namespace = 1
          """.stripMargin
      ))
      import settings._

      val expected = defaultConfigKeys ++ Seq(
        "bootstrap.servers", "value.deserializer", "sasl.mechanism", "client.id", "my.custom.client.namespace")

      kafkaConfig.size shouldEqual expected.size
      kafkaConfig("my.custom.client.namespace") shouldEqual 1
      kafkaConfig(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) shouldEqual classOf[CustomDeserializer].getName
      kafkaConfig(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) shouldEqual false
      kafkaConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) shouldEqual "latest"
      kafkaConfig(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) shouldEqual "localhost:9092"
      kafkaConfig(CommonClientConfigs.CLIENT_ID_CONFIG) shouldEqual "org.example.env.client1"
      RecordConverterClass shouldEqual classOf[CustomRecordConverter].getName
      IngestionTopic shouldEqual "test"
    }

    "have expected consumer configurations" in {
      val settings = new KafkaSettings(source)
      val values = settings.sourceConfig

      values.get("my.custom.client.namespace") shouldEqual Some("custom.value")
      values(ConsumerConfig.GROUP_ID_CONFIG) shouldEqual "org.example.cluster1.filodb.consumer1"
      values(CommonClientConfigs.CLIENT_ID_CONFIG) shouldEqual "org.example.cluster1.filodb.client1"
      values(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) shouldEqual "localhost:9092"
      values(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) shouldEqual classOf[LongDeserializer].getName
      values(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) shouldEqual classOf[CustomDeserializer].getName

      val props = values.asProps
      props.get("my.custom.client.namespace") shouldEqual "custom.value"
      props.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) shouldEqual "localhost:9092"
      props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) shouldEqual classOf[LongDeserializer].getName
      props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) shouldEqual classOf[CustomDeserializer].getName
    }

    "have the expected producer configurations" in {
      val settings = new KafkaSettings(source.withFallback(
        ConfigFactory.parseString(s"""sourceconfig.key.serializer = "org.example.CustomSerializer"""")))
      val values = settings.sinkConfig

      values("my.custom.client.namespace") shouldEqual "custom.value"
      values(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) shouldEqual "localhost:9092"
      values(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG) shouldEqual classOf[CustomSerializer].getName
      values(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG) shouldEqual classOf[CustomSerializer].getName

      val props = values.asProps
      props.get("my.custom.client.namespace") shouldEqual "custom.value"
      props.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) shouldEqual "localhost:9092"
      props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG) shouldEqual classOf[CustomSerializer].getName
      props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG) shouldEqual classOf[CustomSerializer].getName
    }

    "get correct global Kafka module defaults" in {
      KafkaSettings.FailureTopic shouldEqual "failure"
      KafkaSettings.StatusTimeout should be (3000.millis)
      KafkaSettings.ConnectedTimeout should be (8000.millis)
      KafkaSettings.GracefulStopTimeout should be (10.seconds)
    }
  }
}
