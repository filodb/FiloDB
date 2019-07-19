package filodb.kafka

import java.util.{Properties => JProperties}

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
import org.apache.kafka.common.serialization.LongDeserializer

import filodb.core.GlobalConfig

class SourceConfig(conf: Config, shard: Int)
  extends KafkaSettings(conf, ConsumerConfig.configNames.asScala.toSet) {

  import ConsumerConfig._

  val KeyDeserializer = resolved
    .as[Option[String]](KEY_DESERIALIZER_CLASS_CONFIG)
    .getOrElse(classOf[LongDeserializer].getName)

  val AutoOffsetReset = resolved.as[Option[String]](AUTO_OFFSET_RESET_CONFIG).getOrElse("latest")

  val EnableAutoCommit = resolved.as[Option[Boolean]](ENABLE_AUTO_COMMIT_CONFIG).getOrElse(false)

  override def config: Map[String, AnyRef] = {
    val filterNot = Set(KEY_DESERIALIZER_CLASS_CONFIG, AUTO_OFFSET_RESET_CONFIG, ENABLE_AUTO_COMMIT_CONFIG,
                        VALUE_DESERIALIZER_CLASS_CONFIG)
    val defaults = Map(
      KEY_DESERIALIZER_CLASS_CONFIG -> KeyDeserializer,
      AUTO_OFFSET_RESET_CONFIG -> AutoOffsetReset,
      ENABLE_AUTO_COMMIT_CONFIG -> EnableAutoCommit.toString,
      // The value deserializer MUST be the RecordContainer one
      VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[RecordContainerDeserializer].getName)

    defaults ++ super.config.filterNot { case (k, _) => filterNot contains k }
  }
}

class SinkConfig(conf: Config) extends KafkaSettings(conf, ProducerConfig.configNames.asScala.toSet)

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
  * `filo-topic-name` the kafka topic to consume from.
  *
  * `bootstrap.servers` the kafka cluster hosts to use. If none provided,
  * defaults to localhost:9092
  *
  * `record-converter` the converter used to convert the event to a filodb row source.
  * A custom event type converter or one of the primitive
  * type filodb.kafka.*Converter
  */
class KafkaSettings(conf: Config, keys: Set[String]) extends StrictLogging {

  protected val resolved = {
    val resolvedConfig = conf.resolve
    if (resolvedConfig.hasPath("sourceconfig")) resolvedConfig.as[Config]("sourceconfig")
    else resolvedConfig
  }

  val IngestionTopic = {
    require(resolved.hasPath("filo-topic-name"), "'filo-topic-name' must not be empty. Configure an ingestion topic.")
    resolved.as[String]("filo-topic-name")
  }

  /** Optionally log consumer configuration on load. Defaults to false.
    * Helpful for debugging configuration on load issues.
    *
    * {{{
    *   sourceconfig.log-config = true
    * }}}
    */
  val LogConfig = resolved.as[Option[Boolean]]("filo-log-config").getOrElse(false)

  /** Contains configurations including common for the two client types: producer, consumer,
    * if configured. E.g. bootstrap.servers, client.id - Kafka does not namespace these.
    */
  val kafkaConfig: Map[String, AnyRef] =
    resolved
      .flattenToMap
      .filterNot { case (k, _) => k.startsWith("filo-") || k.startsWith("akka.") }

  /** Because kafka does not namespace client configurations, e.g. consumer.*, producer.*. */
  private val customClientConfig: Map[String, AnyRef]  = {
    // if there are custom client configurations, retain them
    val filter = ConsumerConfig.configNames.asScala ++ ProducerConfig.configNames.asScala
    kafkaConfig.filterKeys(k => !filter.contains(k))
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

  /** Bridges the monix config gap with native kafka client properties.
    * The merged configuration into Kafka `ConsumerConfig` or `ProducerConfig`
    * key-value pairs with all Kafka defaults for settings not provided by the user.
    */
  def config: Map[String, AnyRef] = {
    val kafkaClientConfig = kafkaConfig collect { case (k, v) if keys contains k => valueTyped((k, v)) }
    customClientConfig ++ kafkaClientConfig
  }

  /** Used by Monix-Kafka. */
  def asConfig: Config = config.asConfig

  /** Used by Kafka Consumer. */
  def asProps: JProperties = config.asProps

}

/** Global settings for FiloDB Kafka overall, not tied to individual sources. */
object KafkaSettings {

  val moduleConfig = GlobalConfig.systemConfig.as[Config]("filodb.kafka").resolve
  val EnableFailureChannel = moduleConfig.as[Boolean]("failures.channel-enabled")
  val FailureTopic: String = moduleConfig.as[String]("failures.topic")
  val ConnectedTimeout = moduleConfig.as[FiniteDuration]("tasks.lifecycle.connect-timeout")
  val StatusTimeout = moduleConfig.as[FiniteDuration]("tasks.status-timeout")
  val GracefulStopTimeout = moduleConfig.as[FiniteDuration]("tasks.lifecycle.shutdown-timeout")

}

