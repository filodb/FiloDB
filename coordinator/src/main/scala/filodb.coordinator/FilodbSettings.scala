package filodb.coordinator

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

import akka.actor.{ActorPath, Address, RootActorPath}
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

  val StorageStrategy = StoreStrategy.Configured(config.as[String]("store-factory"))

  val DefaultTaskTimeout = config.as[FiniteDuration]("tasks.timeouts.default")

  val GracefulStopTimeout = config.as[FiniteDuration]("tasks.timeouts.graceful-stop")

  val InitializationTimeout = config.as[FiniteDuration]("tasks.timeouts.initialization")

  val IOPoolName = "filodb.io"

  lazy val DatasetDefinitions = config.as[Option[Map[String, Config]]]("dataset-definitions")
                                      .getOrElse(Map.empty[String, Config])

  /** The timeout to use to resolve an actor ref for new nodes. */
  val ResolveActorTimeout = config.as[FiniteDuration]("tasks.timeouts.resolve-actor")

}

/** Consistent naming: allows other actors to accurately filter
  * by actor.path.name and the creators of the actors to use the
  * name others look up.
  *
  * Actors have an internal dataset of all their children. No need
  * to duplicate them as a collection to query and track and incur
  * additional resources. Simply leverage the existing with a
  * naming convention.
  *
  * See `filodb.coordinator.NamingAwareBaseActor`
  */
object ActorName {

  val NodeGuardianName = "node"
  val CoordinatorName = "coordinator"
  val TraceLoggerName = "trace-logger"

  /* The actor name of the child singleton actor */
  val ClusterSingletonManagerName = "nodecluster"
  val ClusterSingletonName = "singleton"
  val ClusterSingletonProxyName = "singletonProxy"
  val ShardName = "shard-coordinator"

  /** MemstoreCoord Worker name prefix. Naming pattern is prefix-datasetRef.dataset */
  val Ingestion = "ingestion"
  /** Query Worker name prefix. Naming pattern is prefix-datasetRef.dataset */
  val Query = "query"

  /* Actor Paths */
  def nodeCoordinatorPath(addr: Address): ActorPath =
    RootActorPath(addr) / "user" / NodeGuardianName / CoordinatorName

}
