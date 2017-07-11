package filodb.kafka

import java.net.InetAddress

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.SourceConfig
import org.apache.kafka.clients.producer.SinkConfig
import org.apache.kafka.common.config.SslConfigs

/** Creates an immutable `com.typesafe.config.Config`.
  *
  * Required User Configurations:
  *   `filodb.kafka.config.file` can be either provided by the user in their Typesafe
  *   custom.conf file or passed in as -Dfilodb.kafka.config.file=/path/to/custom.properties.
  *
  *   `filodb.kafka.bootstrap.servers` the kafka cluster hosts to use. If none provided,
  *   defaults to localhost:9092
  *
  *   `filodb.kafka.record-converter` the converter used to convert the event to a filodb row source.
  *   A custom event type converter or one of the primitive
  *   type filodb.kafka.*Converter
  */
class KafkaSettings(conf: Config) {

  def this() = this(ConfigFactory.load())

  ConfigFactory.invalidateCaches()

  val config = conf.withFallback(ConfigFactory.load("filodb-defaults.conf"))

  protected val kafka = config.getConfig("filodb.kafka")

  /** Override with `akka.cluster.Cluster.selfAddress` if on the classpath. */
  val selfAddress = InetAddress.getLocalHost.getHostAddress

  def addressId: String = s"$selfAddress-${System.nanoTime}"

  def clientId: String = s"filodb.kafka-$addressId"

  /** `filodb.kafka.config.file` can be either provided by the user in their Typesafe
    * custom.conf file or passed in as -Dfilodb.kafka.config.file=/path/to/custom.properties.
    *
    * For the native way Kafka properties are loaded by users, and retaining full keys
    * which Typesafe config would break up to the last key which does not work for how
    * Kafka loads its configuration. It requires `the.full.key=value`.
    */
    // scalastyle:off
  private[kafka] val nativeKafkaConfig: Map[String, AnyRef] =
    sys.props.getOrElse("filodb.kafka.config.file", kafka.getString("config.file")) match {
      case path if path.nonEmpty =>
        val file = new java.io.File(path.replace("./", ""))
        require(file.exists, s"'filodb.kafka.config.file=${file.getAbsolutePath}' not found.")
        for {
          (k, v) <- file.asMap
          if k.startsWith("filodb.kafka")
        } yield k.replace("filodb.kafka.", "") -> v
      case _ =>
        throw new IllegalArgumentException(
          "'filodb.kafka.config.file=/path/your.properties' must be set to load your kafka client configuration.")
    }
  // scalastyle:on

  /** Set from either a comma-separated string from the user's kafka properties file
    * or configured in their custom.conf file as a list of host:port entries like this
    * {{{
    *   bootstrap.servers = [
    *     "dockerKafkaBroker1:9092",
    *     "dockerKafkaBroker2:9093"
    *   ]
    * }}}
    *
    * Returns a comma-separated String of host:port entries required by the Kafka client. */
  val BootstrapServers: String =
    nativeKafkaConfig.get("bootstrap.servers").map(_.toString)
      .getOrElse(kafka.getStringList("bootstrap.servers").asScala.toList.distinct.mkString(","))

  val NumPartitions = kafka.getInt("partitions")

  val IngestionTopic = kafka.getString("topics.ingestion")

  val FailureTopic: String = kafka.getString("topics.failure")

  require(kafka.hasPath("record-converter"),
    "'filodb.kafka.record-converter' must not be empty. Configure a custom converter.")

  val RecordConverterClass = kafka.getString("record-converter")

  val EnableFailureChannel = kafka.getBoolean("failures.channel-enabled")

  val ConnectedTimeout = Duration(kafka.getDuration(
    "tasks.lifecycle.connect-timeout", MILLISECONDS), MILLISECONDS)

  val GracefulStopTimeout = Duration(kafka.getDuration(
    "tasks.lifecycle.shutdown-timeout", SECONDS), SECONDS)

  val StatusTimeout = Duration(kafka.getDuration(
    "tasks.status-timeout", MILLISECONDS), MILLISECONDS)

  lazy val StatusLogInterval = Duration(kafka.getDuration(
    "tasks.status.log-interval", MILLISECONDS), MILLISECONDS)

  lazy val PublishTimeout = Duration(kafka.getDuration(
    "tasks.publish-timeout", MILLISECONDS), MILLISECONDS)

  /** Bridges the monix config gap with native kafka user properties. */
  def consumerConfig: Config = new SourceConfig(BootstrapServers, clientId, nativeKafkaConfig).asConfig

  /** Bridges the monix config gap with native kafka user properties. */
  def producerConfig: Config = new SinkConfig(BootstrapServers, clientId, nativeKafkaConfig).asConfig
}

/** A class that merges the user-provided native kafka properties file
  * loaded and merged with any required by filodb and or defaults.
  *
  * @param provided the configs to use
  */
abstract class MergeableConfig(provided: Map[String, AnyRef]) {

  // workaround for monix/kafka List types that should accept comma-separated strings
  private val listTypes = Seq(
    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
    SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG)

  /** The merged configuration into Kafka `ConsumerConfig` or `ProducerConfig`
    * key-value pairs with all Kafka defaults for settings not provided by the user.
    */
  def kafkaConfig: Map[String, AnyRef]

  def asConfig: Config = {
    val c = kafkaConfig.map {
      case (k, v: Class[_]) => k -> v.getName // resolves `Config` Class type issue
      case (k, v: java.util.Collection[_]) => k -> v.asScala.map(_.toString).mkString(",")
      case (k, v) => Try(k -> v).getOrElse(k -> v.toString)
    }
    ConfigFactory.parseMap(c.asJava)
  }

  protected def filter(namespace: String): Map[String, AnyRef] =
    provided collect {
      case (k,v) if k.startsWith(namespace) =>
        k.replace(namespace + ".", "") -> v
      case (k,v) if k.startsWith("kafka.") =>
        k.replace("kafka.", "") -> v
    }

  protected def valueTyped(kv: (String, Any)): (String, AnyRef) =
    kv match {
    case (k, v) if listTypes contains k =>
      k -> v.asInstanceOf[java.util.Collection[String]].asScala.mkString(",")
    case (k, v) =>
      k -> v.asInstanceOf[AnyRef]
  }
}