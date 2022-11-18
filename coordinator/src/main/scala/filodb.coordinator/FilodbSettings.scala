package filodb.coordinator

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

import akka.actor.{ActorPath, Address, RootActorPath}
import com.typesafe.config.{Config, ConfigFactory}
import monix.execution.atomic.AtomicAny
import net.ceedubs.ficus.Ficus._
import org.scalactic._

import filodb.core.GlobalConfig
import filodb.core.metadata.{Dataset, Schemas}

/** Settings for the FiloCluster Akka Extension which gets
  * config from `GlobalConfig`. Uses Ficus.
  */
final class FilodbSettings(val conf: Config) {
  def this() = this(ConfigFactory.empty)

  ConfigFactory.invalidateCaches()

  lazy val allConfig: Config = conf.withFallback(GlobalConfig.systemConfig).resolve()

  /** The filodb configuration specifically. */
  lazy val config: Config = allConfig.as[Config]("filodb")

  lazy val SeedNodes: immutable.Seq[String] =
    sys.props.get("filodb.seed-nodes")
      .map(_.trim.split(",").toList)
      .getOrElse(config.as[Seq[String]]("seed-nodes").toList)

  lazy val StorageStrategy = StoreStrategy.Configured(config.as[String]("store-factory"))

  lazy val DefaultTaskTimeout = config.as[FiniteDuration]("tasks.timeouts.default")

  lazy val GracefulStopTimeout = config.as[FiniteDuration]("tasks.timeouts.graceful-stop")

  lazy val InitializationTimeout = config.as[FiniteDuration]("tasks.timeouts.initialization")

  lazy val ShardMapPublishFrequency = config.as[FiniteDuration]("tasks.shardmap-publish-frequency")

  lazy val DatasetDefinitions = config.as[Option[Map[String, Config]]]("dataset-definitions")
                                      .getOrElse(Map.empty[String, Config])

  /** The timeout to use to resolve an actor ref for new nodes. */
  lazy val ResolveActorTimeout = config.as[FiniteDuration]("tasks.timeouts.resolve-actor")

  lazy val datasetConfPaths = config.as[Seq[String]]("dataset-configs")

  lazy val numNodes = config.getInt("cluster-discovery.num-nodes")
  lazy val k8sHostFormat = config.as[Option[String]]("cluster-discovery.k8s-stateful-sets-hostname-format")

  // used for development mode only
  lazy val hostList = config.as[Option[Seq[String]]]("cluster-discovery.host-list")
  lazy val localhostOrdinal = config.as[Option[Int]]("cluster-discovery.localhost-ordinal")


  /**
   * Returns IngestionConfig/dataset configuration from parsing dataset-configs file paths.
   * If those are empty, then parse the "streams" config key for inline configs.
   */
  lazy val streamConfigs: Seq[Config] =
    if (datasetConfPaths.nonEmpty) {
      datasetConfPaths.map { d => ConfigFactory.parseFile(new java.io.File(d)) }
    } else {
      config.as[Seq[Config]]("inline-dataset-configs")
    }

  lazy val schemas = Schemas.fromConfig(config) match {
    case Good(sch) => sch
    case Bad(errs)  => throw new RuntimeException("Errors parsing schemas:\n" +
                         errs.map { case (ds, err) => s"Schema $ds\t$err" }.mkString("\n"))
  }

  // Creates a Dataset from a stream config. NOTE: this is a temporary thing to keep old code using Dataset
  // compatible and minimize changes.  The Dataset name is taken from the dataset/stream config, but the schema
  // underneath points to one of the schemas above and schema may have a different name.  The approach below
  // allows one schema (with one schema name) to be shared amongst datasets using different names.
  def datasetFromStream(streamConf: Config): Dataset =
    Dataset(streamConf.getString("dataset"),
            schemas.schemas(streamConf.getString("schema")))
}

object FilodbSettings {
  // Initialization of a global FilodbSettings, important for SchemaSerializer and tests
  val global = AtomicAny[Option[FilodbSettings]](None)

  def initialize(overrides: Config): FilodbSettings = {
    global.compareAndSet(None, Some(new FilodbSettings(overrides)))
    global().get
  }

  def globalOrDefault: FilodbSettings =
    global().getOrElse {
      val settings = new FilodbSettings()
      global := Some(settings)
      settings
    }

  def reset(): Unit = global := None
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
  def nodeCoordinatorPath(addr: Address, v2ClusterEnabled: Boolean): ActorPath = {
    if (v2ClusterEnabled) RootActorPath(addr) / "user" / CoordinatorName
    else RootActorPath(addr) / "user" / NodeGuardianName / CoordinatorName
  }

}
