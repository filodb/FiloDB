package filodb.kafka

import com.typesafe.config.ConfigFactory
import monix.kafka.KafkaConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.LongDeserializer
import org.example.{CustomDeserializer, CustomRecordConverter}

class KafkaConsumerConfigSpec extends KafkaSpec {
  import ConsumerConfig._

  "SourceConfig" must {

    "fail if no required configs by user are not provided" in {
      intercept[IllegalArgumentException](new SourceConfig(ConfigFactory.empty, 0))
    }

    "fail if record converter from user is not provided" in {
      intercept[IllegalArgumentException] {
        new SourceConfig(ConfigFactory.parseString(
          """sourceconfig.filo-record-converter = "org.example.CustomRecordConverter""""), 0)
      }
    }

    "fail if topic from user is not provided" in {
      intercept[IllegalArgumentException] {
        new SourceConfig(ConfigFactory.parseString(
          """sourceconfig.filo-topic-name = "test""""), 0)
      }
    }

    "have the expected default values" in {
      val source = new SourceConfig(ConfigFactory.parseString(
        s"""
           |filo-record-converter = "some.custom.RecordConverter"
           |filo-topic-name = "test"
         """.stripMargin), 0)
      source.AutoOffsetReset shouldEqual "latest"
      source.KeyDeserializer shouldEqual classOf[LongDeserializer].getName
      source.asConfig.getString(AUTO_OFFSET_RESET_CONFIG) shouldEqual "latest"
      source.asConfig.getBoolean(ENABLE_AUTO_COMMIT_CONFIG) shouldEqual false
      source.asConfig.getString(KEY_DESERIALIZER_CLASS_CONFIG) shouldEqual classOf[LongDeserializer].getName
      source.asProps.getProperty(AUTO_OFFSET_RESET_CONFIG) shouldEqual "latest"
      source.asProps.getProperty(ENABLE_AUTO_COMMIT_CONFIG) shouldEqual "false"
      source.asProps.getProperty(KEY_DESERIALIZER_CLASS_CONFIG) shouldEqual classOf[LongDeserializer].getName
    }

    "have the expected Config" in {
      val source = new SourceConfig(testConfig, 0)
      source.IngestionTopic shouldEqual "raw_events"
      source.RecordConverterClass shouldEqual classOf[CustomRecordConverter].getName
      source.AutoOffsetReset shouldEqual "latest"
      source.LogConfig shouldEqual false
      source.KeyDeserializer shouldEqual classOf[LongDeserializer].getName

      val sourceConfig = source.asConfig
      sourceConfig.getString(KEY_DESERIALIZER_CLASS_CONFIG) shouldEqual classOf[LongDeserializer].getName
      sourceConfig.getString(VALUE_DESERIALIZER_CLASS_CONFIG) shouldEqual classOf[CustomDeserializer].getName
      sourceConfig.getString(VALUE_DESERIALIZER_CLASS_CONFIG) shouldEqual classOf[CustomDeserializer].getName
      sourceConfig.getString(BOOTSTRAP_SERVERS_CONFIG) shouldEqual "localhost:9092"
      sourceConfig.getString("my.custom.client.namespace") shouldEqual "custom.value"
    }

    "have the expected Properties" in {
      val source = new SourceConfig(testConfig, 0)
      val props = source.asProps
      props.getProperty("my.custom.client.namespace") shouldEqual "custom.value"
      props.getProperty(AUTO_OFFSET_RESET_CONFIG) shouldEqual "latest"
      props.getProperty(ENABLE_AUTO_COMMIT_CONFIG) shouldEqual "false"
      props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) shouldEqual "localhost:9092"
      props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) shouldEqual classOf[LongDeserializer].getName
      props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) shouldEqual classOf[CustomDeserializer].getName
    }

    "have the expected KafkaConsumerConfig" in {
      val source = new SourceConfig(testConfig, 0)
      val consumerCfg = KafkaConsumerConfig(source.config.asConfig)
      consumerCfg.autoOffsetReset.toString.toLowerCase shouldEqual "latest"
      consumerCfg.enableAutoCommit shouldEqual false
      consumerCfg.bootstrapServers should be(List("localhost:9092"))
      consumerCfg.enableAutoCommit shouldBe false

      // from monix-kafka KafkaConsumerObservable v0.14
      // monix-kafka version 0.14 KafkaConsumerObservable commits offsets even though enable.auto.commit=false
      val shouldCommitBefore = !consumerCfg.enableAutoCommit && consumerCfg.observableCommitOrder.isBefore
      val shouldCommitAfter = !consumerCfg.enableAutoCommit && consumerCfg.observableCommitOrder.isAfter
      shouldCommitBefore shouldBe false

      // TODO the patch:
      // this is the issue with KafkaConsumerObservable v0.14 and its use of the kafka commit api
      intercept[org.scalatest.exceptions.TestFailedException] {
        shouldCommitAfter shouldBe false
      }
    }
  }
}
