package filodb.kafka

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SslConfigs

import filodb.coordinator.GlobalConfig

/**
  * Global settings for FiloDB Kafka overall, not tied to individual sources
  */
object KafkaSettings {

  val moduleConfig = GlobalConfig.systemConfig.getConfig("filodb.kafka-module")

  val EnableFailureChannel = moduleConfig.getBoolean("failures.channel-enabled")
  val FailureTopic: String = moduleConfig.getString("failures.topic")

  val ConnectedTimeout = moduleConfig.as[FiniteDuration]("tasks.lifecycle.connect-timeout")
  val StatusTimeout = moduleConfig.as[FiniteDuration]("tasks.status-timeout")
  lazy val StatusLogInterval = moduleConfig.as[FiniteDuration]("tasks.status.log-interval")
  lazy val PublishTimeout = moduleConfig.as[FiniteDuration]("tasks.publish-timeout")

  val GracefulStopTimeout = Duration(moduleConfig.getDuration(
    "tasks.lifecycle.shutdown-timeout", SECONDS), SECONDS)

}

/** Kafka config class wrapping the user-supplied source configuration
  * when they start a Kafka stream using the filo-cli.
  * NOTE: This is NOT the global FiloDB config loaded at startup.  A different source config
  * can be used to start different Kafka streams while the FiloDB nodes/cluster is up.
  *
  * An example of a config to pass into KafkaSettings is in src/main/resources/example-source.conf
  * Due to the design, this config may include or even be parsed from a properties file!  However
  * this property file cannot have a namespace.
  *
  * Required User Configurations:
  *
  * `filo-kafka-servers` the kafka cluster hosts to use. If none provided,
  * defaults to localhost:9092
  *
  * `record-converter` the converter used to convert the event to a filodb row source.
  * A custom event type converter or one of the primitive
  * type filodb.kafka.*Converter
  */
final class KafkaSettings(conf: Config) extends StrictLogging {

  val config = {
    val resolved = conf.resolve
    val sourceconfig = if (resolved.hasPath("sourceconfig")) resolved.as[Config]("sourceconfig") else resolved
    sourceconfig.withFallback(GlobalConfig.systemConfig.as[Config]("filodb.kafka")).resolve
  }

  require(config.hasPath("filo-topic-name"),
    "'filo-topic-name' must not be empty. Configure an ingestion topic.")

  require(config.hasPath("filo-record-converter"),
    "'record-converter' must not be empty. Configure a custom converter.")

  val IngestionTopic = config.as[String]("filo-topic-name")

  val RecordConverterClass = config.as[String]("filo-record-converter")

  /** Optionally log consumer configuration on load. Defaults to false.
    * {{{
    *   sourceconfig {
    *     log-consumer-config = true
    *   }
    * }}}
    */
  val LogConsumerConfig = config.as[Option[Boolean]]("filo-log-consumer-config").getOrElse(false)

  /** Contains configurations including common for the two client types: producer, consumer,
    * if configured. E.g. bootstrap.servers, client.id - Kafka does not namespace these.
    */
  val kafkaConfig: Map[String, AnyRef] =
    config
      .flattenToMap
      .filterNot { case (k, _) => k.startsWith("filo-") || k.startsWith("akka.") }

  /** Because kafka does not namespace client configurations, e.g. consumer.*, producer.*. */
  private val customClientConfig: Map[String, AnyRef]  = {
    // if there are custom client configurations, retain them
    val filter = ConsumerConfig.configNames.asScala ++ ProducerConfig.configNames.asScala
    kafkaConfig.filterKeys(k => !filter.contains(k))
  }

  /** Bridges the monix config gap with native kafka client properties. */
  def sourceConfig: Map[String, AnyRef] = {
    import ConsumerConfig._

    require(kafkaConfig.get(VALUE_DESERIALIZER_CLASS_CONFIG).isDefined,
      "'value.deserializer' must be configured.")
    require(kafkaConfig.get(AUTO_OFFSET_RESET_CONFIG).isDefined,
      "'auto.offset.reset' must be configured.")

    mergedConfig(ConsumerConfig.configNames.asScala.toSet)
  }

  /** Bridges the monix config gap with native kafka client properties. */
  def sinkConfig: Map[String, AnyRef] = {
    import ProducerConfig._

    require(kafkaConfig.get(VALUE_SERIALIZER_CLASS_CONFIG).isDefined,
      s"'$VALUE_SERIALIZER_CLASS_CONFIG' must be defined.")

    mergedConfig(ProducerConfig.configNames.asScala.toSet)
  }

  /** The merged configuration into Kafka `ConsumerConfig` or `ProducerConfig`
    * key-value pairs with all Kafka defaults for settings not provided by the user.
    */
  private def mergedConfig(filterFor: Set[String]): Map[String, AnyRef]  = {
    // avoid: WARN  o.a.k.c.c.ConsumerConfig The configuration 'producer.acks' was supplied but isn't a known config
    val kafkaClientConfig = kafkaConfig collect { case (k, v) if filterFor contains k => valueTyped((k, v)) }

    customClientConfig ++ kafkaClientConfig
  }


  // workaround for monix/kafka List types that should accept comma-separated strings
  private val listTypes = Seq(
    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
    SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG)

  private def valueTyped(kv: (String, Any)): (String, AnyRef) =
    kv match {
      case (k, v) if listTypes contains k =>
        k -> Try(v.asInstanceOf[java.util.Collection[String]].asScala.mkString(","))
          .getOrElse(v.asInstanceOf[String])
      case (k, v) =>
        k -> v.asInstanceOf[AnyRef]
    }
}

