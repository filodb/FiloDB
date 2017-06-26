package filodb.kafka

import java.net.InetAddress

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import com.typesafe.config.{Config, ConfigFactory}

import filodb.coordinator.Instance

/** Creates an immutable `com.typesafe.config.Config`.
  *
  * Override default settings in your deploy reference.conf file per environment.
  * Extendable class, see the sample:
  */
class KafkaSettings(conf: Config) extends Instance {

  def this() = this(ConfigFactory.load)

  ConfigFactory.invalidateCaches()

  val config = conf.withFallback(ConfigFactory.load())

  protected val kafka = config.getConfig("filodb.kafka")

  /** Override with `akka.cluster.Cluster.selfAddress` if on the classpath. */
  val selfAddress = InetAddress.getLocalHost.getHostAddress

  def createSelfId: String = selfAddress + "-" + System.currentTimeMillis

  /** Set from either a comma-separated string from -Dfilodb.kafka.bootstrap.servers
    * or configured list of host:port entries like this
    * {{{
    *   bootstrap.servers = [
    *     "dockerKafkaBroker1:9092",
    *     "dockerKafkaBroker2:9093"
    *   ]
    * }}}
    *
    * Returns a comma-separated String of host:port entries required by the Kafka client. */
  val BootstrapServers: String =
    sys.props.getOrElse("bootstrap.servers",
      kafka.getStringList("bootstrap.servers")
        .asScala.toList.distinct.mkString(","))

  val NumPartitions = kafka.getInt("partitions")

  val IngestionTopic = kafka.getString("topics.ingestion")

  val FailureTopic: String = kafka.getString("topics.failure")

  require(kafka.hasPath("record-converter"),
    "'filodb.kafka.record-converter' must not be empty. Configure a custom converter.")

  val RecordConverterClass = kafka.getString("record-converter")

  val ConnectedTimeout = Duration(kafka.getDuration(
    "tasks.lifecycle.connect-timeout", MILLISECONDS), MILLISECONDS)

  val GracefulStopTimeout = Duration(kafka.getDuration(
    "tasks.lifecycle.shutdown-timeout", SECONDS), SECONDS)

  val StatusTimeout = Duration(kafka.getDuration(
    "tasks.status-timeout", MILLISECONDS), MILLISECONDS)

  protected val security: Map[String, AnyRef] = {
    Map.empty
    /* wip kafka.getConfig("security").toMap ++
      kafka.getConfig("sasl").toMap ++
      kafka.getConfig("ssl").toMap*/
  }
}

/** For the native way Kafka properties are loaded by users,
  * and retaining full keys wich Typesafe config would break up to
  * the last key which does not work for Kafka settings.
  */
object Loaded {

  private[kafka] lazy val kafka: Map[String, AnyRef] =
    sys.props.get("filodb.kafka.clients.config")
      .map { path => new java.io.File(path.replace("./", "")).asMap }
      .getOrElse(throw new IllegalArgumentException(
        "'filodb.kafka.clients.config' must be set: path/to/your-kafka.properties"))

  def consumer: Map[String, AnyRef] = filter("consumer")
  def producer: Map[String, AnyRef] = filter("producer")

  private def filter(client: String): Map[String, AnyRef] =
    kafka.collect { case (k,v) if k.startsWith(client) =>
      k.replace(client + ".", "") -> v
    }
}
