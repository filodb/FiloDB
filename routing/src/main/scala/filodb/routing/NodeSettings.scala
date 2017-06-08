package filodb.routing

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import com.typesafe.config.{Config, ConfigFactory}

object NodeSettings {

  def apply(conf: Config): NodeSettings = new NodeSettings(conf)

}

final class NodeSettings(conf: Config) {

  ConfigFactory.invalidateCaches()

  protected val config = conf.withFallback(ConfigFactory.load())
  protected val filodb = config.getConfig("filodb")
  protected val tasks = filodb.getConfig("tasks")
  protected val routing = filodb.getConfig("routing")

  val ConnectedTimeout = Duration(tasks.getDuration(
    "tasks.connect-timeout", MILLISECONDS), MILLISECONDS)

  val PublishInterval = Duration(tasks.getDuration(
    "tasks.publish-interval", MILLISECONDS), MILLISECONDS)

  val TaskLogInterval = Duration(tasks.getDuration(
    "tasks.log-interval", MILLISECONDS), MILLISECONDS)

  val PublishTimeout = Duration(tasks.getDuration(
    "tasks.publish-timeout", MILLISECONDS), MILLISECONDS)

  val StatusTimeout = Duration(tasks.getDuration(
    "tasks.publish-timeout", MILLISECONDS), MILLISECONDS)

  val GracefulStopTimeout = Duration(tasks.getDuration(
    "tasks.shutdown-timeout", SECONDS), SECONDS)

  val RouteTimeout = Duration(tasks.getDuration(
    "tasks.route-timeout", MILLISECONDS), MILLISECONDS)

  /** Returns a list of event topics to use if configured, else an empty list. */
  val IngestionTopic = routing.getString("topics.ingestion")

  private val patterns = routing.getObject("topics.patterns")

  /** Returns a single topic of configured, otherwise an empty String. */
  val WireTapTopic: String = patterns.get("wire-tap").unwrapped.toString

  /** Returns a single topic of configured, otherwise an empty String. */
  val FailureTopic: String = patterns.get("failure").unwrapped.toString

}
