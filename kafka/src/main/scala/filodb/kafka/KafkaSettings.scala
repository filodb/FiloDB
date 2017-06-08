package filodb.kafka

import java.util.{Properties => JProperties}

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArraySerializer, IntegerSerializer}

import scala.collection.immutable

object KafkaSettings {

  private val JaasConfKey = "java.security.auth.login.config"

  def apply(system: ActorSystem): KafkaSettings =
    new KafkaSettings(system.settings.config)

  def apply(conf: Config): KafkaSettings =
    new KafkaSettings(conf)

}

/** Creates an immutable `com.typesafe.config.Config`.
  *
  * Override default settings in your deploy reference.conf file per environment.
  * Extendable class, see the sample:
  */
class KafkaSettings(conf: Config) extends NodeSettings(conf) {

  import scala.collection.JavaConverters._

  def this() = this(ConfigFactory.load())

  ConfigFactory.invalidateCaches()

  protected val filo = conf.withFallback(ConfigFactory.load()).getConfig("filodb")

  protected val kafka = filo.getConfig("kafka")

  protected val secureConfig: Map[String, AnyRef] = Map.empty[String, AnyRef]

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
    sys.props.getOrElse("kafka.bootstrap.servers",
      filo.getStringList("kafka.bootstrap.servers")
        .asScala.toList.distinct.mkString(","))

  /** The FiloDB event stream ingestion topic. */
  val IngestionTopic = kafka.getString("topics.ingestion")

  private val patterns = kafka.getObject("topics.patterns")

  /** Returns a single topic of configured, otherwise an empty String. */
  val WireTapTopic: String = patterns.get("wire-tap").unwrapped.toString

  /** Returns a single topic of configured, otherwise an empty String. */
  val FailureTopic: String = patterns.get("failure").unwrapped.toString

  /*val ProducerPartitionerClass: Class[_] =
    createClass(filo.getString("kafka.producer.partitioner"),
      classOf[ShardPartitioner].getCanonicalName)
*/
  val ProducerKeySerializerClass: Class[_] =
    createClass(filo.getString("kafka.producer.key.serializer"),
      classOf[IntegerSerializer].getCanonicalName)

  val ProducerValueSerializerClass: Class[_] =
    createClass(filo.getString("kafka.producer.value.serializer"),
      classOf[ByteArraySerializer].getCanonicalName)

  def producerConfig(userConfig: immutable.Map[String, AnyRef] = Map.empty): JProperties = {
    (userConfig ++ secureConfig ++ Map(
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG ->
        userConfig.getOrElse(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers),
      ProducerConfig.ACKS_CONFIG ->
        userConfig.getOrElse(ProducerConfig.ACKS_CONFIG, "1"), // ack=1 from partition leader only, test:"all" slowest, safest
      ProducerConfig.CLIENT_ID_CONFIG ->
        userConfig.getOrElse(ProducerConfig.CLIENT_ID_CONFIG, selfAddressId),
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG ->
        ProducerKeySerializerClass,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG ->
        userConfig.getOrElse(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProducerValueSerializerClass))
      ).asProps
  }
}
