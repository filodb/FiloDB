package filodb.kafka

import java.util.{Properties => JProperties}

import scala.util.Try

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}

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
class KafkaSettings(conf: Config) {
  import scala.collection.JavaConverters._

  def this() = this(ConfigFactory.load())

  ConfigFactory.invalidateCaches()

  protected val filo = conf.withFallback(ConfigFactory.load()).getConfig("filodb")

  protected val kafka = filo.getConfig("kafka")

  /** Root conf file checks for environment variable, then this
    * checks user app -Dargs or java system properties. If neither is found,
    * returns an empty string.
    */
  val AuthLoginConfig: String = Try(kafka.getString(KafkaSettings.JaasConfKey)).getOrElse("")
  val a = System.setProperty(KafkaSettings.JaasConfKey, AuthLoginConfig)

  /** Enabled by default. Can disable for unit tests if needed. */
  val SecurityEnabled: Boolean = kafka.getBoolean("client.security.enabled")

  val SecurityProtocol: String = kafka.getString("client.security.protocol")

  val SaslMechanism: String = kafka.getString("client.sasl.mechanism")

  val SslEnabledProtocols: String = kafka.getString("client.ssl.enabled.protocols")

  val SslTruststoreType: String = kafka.getString("client.ssl.truststore.type")

  val SslTruststoreLocation: String = kafka.getString("client.ssl.truststore.location")

  val SslTruststorePassword: String = kafka.getString("client.ssl.truststore.password")

  /** Returns a map of the three main secure client configs if `SecurityEnabled` is either
    * `on` or `true`. Otherwise returns an empty map.
    */
  protected val secureConfig: Map[String, AnyRef] =
    if (SecurityEnabled) Map(
      "security.protocol"       -> SecurityProtocol,
      "sasl.mechanism"          -> SaslMechanism,
      "ssl.enabled.protocols"   -> SslEnabledProtocols,
      "ssl.truststore.type"     -> SslTruststoreType,
      "ssl.truststore.location" -> SslTruststoreLocation,
      "ssl.truststore.password" -> SslTruststorePassword,
      KafkaSettings.JaasConfKey -> AuthLoginConfig)
    else Map.empty[String, AnyRef]

  import filodb.implicits._

  /** Returns immutable map of configurations as a java.util.Properties. */
  val securityConfig: JProperties = secureConfig.asProps

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

  private[filodb] def createClass(fqcn: String, defaultFqcn: String): Class[_] = {
    val name = Option(fqcn).getOrElse(defaultFqcn)
    Try(Class.forName(name)).getOrElse(Class.forName(defaultFqcn))
  }
}
