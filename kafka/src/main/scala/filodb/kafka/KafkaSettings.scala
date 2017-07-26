package filodb.kafka

import java.net.InetAddress

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.{Random, Try}
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.SourceConfig
import org.apache.kafka.clients.producer.SinkConfig
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
class KafkaSettings(conf: Config) {
  val config = conf.resolve().withFallback(GlobalConfig.systemConfig.getConfig("filodb.kafka"))

  /** Override with `akka.cluster.Cluster.selfAddress` if on the classpath. */
  val selfAddress = InetAddress.getLocalHost.getHostAddress

  def clientId: String = s"filodb.kafka.$selfAddress-${System.nanoTime}"

  def kafkaConfig: Map[String, AnyRef] = config.flattenToMap
                                               .filterNot { case (k, _) => k.startsWith("filo-") }

  /** Returns a comma-separated String of host:port entries required by the Kafka client.
    *
    * Set from either a comma-separated string from `bootstrap.servers` config key
    * or use `filo-kafka-servers` in your .conf file using HOCON list syntax:
    * {{{
    *      filo-kafka-servers = [
    *       "dockerKafkaBroker1:9092",
    *       "dockerKafkaBroker2:9093" ]
    * }}}
    */
  val BootstrapServers: String = kafkaConfig
      .get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG).map(_.toString)
      .getOrElse(config.as[Seq[String]]("filo-kafka-servers").distinct.mkString(","))

  val IngestionTopic = config.getString("filo-topic-name")

  require(config.hasPath("filo-record-converter"),
    "'filodb.kafka.record-converter' must not be empty. Configure a custom converter.")

  val RecordConverterClass = config.getString("filo-record-converter")

  /** Bridges the monix config gap with native kafka user properties. */
  def sourceConfig: SourceConfig = new SourceConfig(BootstrapServers, clientId, kafkaConfig)

  /** Bridges the monix config gap with native kafka user properties. */
  def sinkConfig: SinkConfig = new SinkConfig(BootstrapServers, clientId, kafkaConfig)

}

/** A class that merges the user-provided kafka config with generated config such as client ID
  *
  * @param provided the configs to use
  */
abstract class MergeableConfig(bootstrapServers: String,
                               clientId: String,
                               provided: Map[String, AnyRef],
                               namespace: String) {

  /** The merged configuration into Kafka `ConsumerConfig` or `ProducerConfig`
    * key-value pairs with all Kafka defaults for settings not provided by the user.
    */
  def kafkaConfig: Map[String, AnyRef]

  private val random = new Random()

  // workaround for monix/kafka List types that should accept comma-separated strings
  private val listTypes = Seq(
    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
    SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG)

  // no namespacing in kafka, used by both producers and consumers
  protected def commonConfig: Map[String, AnyRef] = Map(
    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
    CommonClientConfigs.CLIENT_ID_CONFIG -> s"$clientId-$namespace-${random.nextInt()}")

  final def asConfig: Config =
    ConfigFactory.parseMap(kafkaConfig.map {
      case (k, v: Class[_]) => k -> v.getName // resolves `Config` Class type issue
      case (k, v: java.util.Collection[_]) => k -> v.asScala.map(_.toString).mkString(",")
      case (k, v) => Try(k -> v).getOrElse(k -> v.toString)
    }.asJava)

  protected final def valueTyped(kv: (String, Any)): (String, AnyRef) =
    kv match {
      case (k, v) if listTypes contains k =>
        k -> v.asInstanceOf[java.util.Collection[String]].asScala.mkString(",")
      case (k, v) =>
        k -> v.asInstanceOf[AnyRef]
    }
}