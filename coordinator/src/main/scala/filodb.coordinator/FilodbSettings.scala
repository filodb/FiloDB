package filodb.coordinator

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

/** Settings for the FiloCluster Akka Extension which gets
  * config from `GlobalConfig`. Uses Ficus.
  */
final class FilodbSettings(val conf: Config) {

  def this() = this(ConfigFactory.empty)

  ConfigFactory.invalidateCaches()

  val allConfig: Config = conf.withFallback(GlobalConfig.systemConfig).resolve()

  /** The filodb configuration specifically. */
  val config: Config = allConfig.as[Config]("filodb")

  lazy val metrics: Config = config.as[Option[Config]]("metrics-logger").getOrElse(ConfigFactory.empty)

  lazy val SeedNodes: immutable.Seq[String] =
    sys.props.get("filodb.seed-nodes")
      .map(_.trim.split(",").toList)
      .getOrElse(config.as[Seq[String]]("seed-nodes").toList)

  val StorageStrategy: StoreStrategy = config.as[String]("store") match {
    case "in-memory" => StoreStrategy.InMemory
    case fqcn        => StoreStrategy.Configured(fqcn)
  }

  val DefaultTaskTimeout = config.as[FiniteDuration]("tasks.timeouts.default")

  val GracefulStopTimeout = config.as[FiniteDuration]("tasks.timeouts.graceful-stop")

  val InitializationTimeout = config.as[FiniteDuration]("tasks.timeouts.initialization")

  val PoolName = "filodb.core"

  val QueueLength = config.as[Int]("core-futures-queue-length")

  val PoolSize = config.as[Int]("core-futures-pool-size")

  val MaxPoolSize = config.as[Int]("core-futures-max-pool-size")

  lazy val DatasetDefinitions = config.as[Option[Map[String, Config]]]("dataset-definitions")
                                      .getOrElse(Map.empty[String, Config])

}
