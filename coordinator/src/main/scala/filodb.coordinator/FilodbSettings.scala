package filodb.coordinator

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

import akka.actor.{ActorPath, Address, RootActorPath}
import com.typesafe.config.{Config, ConfigFactory}
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

  val allConfig: Config = conf.withFallback(GlobalConfig.systemConfig).resolve()

  /** The filodb configuration specifically. */
  val config: Config = allConfig.as[Config]("filodb")

  lazy val SeedNodes: immutable.Seq[String] =
    sys.props.get("filodb.seed-nodes")
      .map(_.trim.split(",").toList)
      .getOrElse(config.as[Seq[String]]("seed-nodes").toList)

  val StorageStrategy = StoreStrategy.Configured(config.as[String]("store-factory"))

  val DefaultTaskTimeout = config.as[FiniteDuration]("tasks.timeouts.default")

  val GracefulStopTimeout = config.as[FiniteDuration]("tasks.timeouts.graceful-stop")

  val InitializationTimeout = config.as[FiniteDuration]("tasks.timeouts.initialization")

  val ShardMapPublishFrequency = config.as[FiniteDuration]("tasks.shardmap-publish-frequency")

  lazy val DatasetDefinitions = config.as[Option[Map[String, Config]]]("dataset-definitions")
                                      .getOrElse(Map.empty[String, Config])

  /** The timeout to use to resolve an actor ref for new nodes. */
  val ResolveActorTimeout = config.as[FiniteDuration]("tasks.timeouts.resolve-actor")

  val datasetConfPaths = config.as[Seq[String]]("dataset-configs")

  /**
   * Returns IngestionConfig/dataset configuration from parsing dataset-configs file paths.
   * If those are empty, then parse the "streams" config key for inline configs.
   */
  val streamConfigs: Seq[Config] =
    if (datasetConfPaths.nonEmpty) {
      datasetConfPaths.map { d => ConfigFactory.parseFile(new java.io.File(d)) }
    } else {
      config.as[Seq[Config]]("inline-dataset-configs")
    }

  val schemas = Schemas.fromConfig(config) match {
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
  def nodeCoordinatorPath(addr: Address): ActorPath =
    RootActorPath(addr) / "user" / NodeGuardianName / CoordinatorName

}
