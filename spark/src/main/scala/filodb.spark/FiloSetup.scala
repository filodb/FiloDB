package filodb.spark

import akka.actor.{ActorSystem, ActorRef, AddressFromURIString}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.SparkContext

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.metastore.CassandraMetaStore
import filodb.coordinator.CoordinatorSetup
import filodb.coordinator.client.ClusterClient
import filodb.core.store.{InMemoryMetaStore, InMemoryColumnStore}

/**
 * FiloSetup handles the Spark side setup of both executors and the driver app, including one-time
 * initialization of the coordinator on executors.  Note that executor and driver setup are different.
 * Drivers initialize first, joins the cluster and adds the driver address to the config.
 * Executors initialize by joining the cluster, starting the coordinator.
 * Drivers do not need a NodeCoordinator.
 *
 * The nice thing about this design is that FiloExecutor.init() is called as needed in packages and/or
 * FiloRelation by each executor/partition, and thus even for contexts where the executors are added
 * dynamically, more nodes can be added to the cluster.
 */
trait FiloSetup extends CoordinatorSetup {
  // The global config of filodb with cassandra, columnstore, etc. sections
  def config: Config = _config.get
  var _config: Option[Config] = None
  var role: String = "executor"

  lazy val system = ActorSystem("filo-spark", configWithRole(role))
  lazy val columnStore = config.getString("store") match {
    case "cassandra" => new CassandraColumnStore(config, readEc)
    case "in-memory" => new InMemoryColumnStore(readEc)
  }
  lazy val metaStore = config.getString("store") match {
    case "cassandra" => new CassandraMetaStore(config.getConfig("cassandra"))
    case "in-memory" => new InMemoryMetaStore
  }

  def configWithRole(role: String): Config =
    ConfigFactory.parseString(s"akka.cluster.roles=[$role]").withFallback(ConfigFactory.load())
}

object FiloDriver extends FiloSetup with StrictLogging {
  import collection.JavaConverters._

  lazy val client = new ClusterClient(system, Some("executor"))

  // The init method called from a SparkContext is going to be from the driver/app.
  def init(context: SparkContext): Unit =
    _config.getOrElse {
      logger.info("Initializing FiloDriver clustering/coordination...")
      role = "driver"
      // Add in self cluster address, and join cluster ourselves
      val filoConfig = configFromSpark(context)
      _config = Some(filoConfig)
      val selfAddr = cluster.selfAddress
      cluster.join(selfAddr)
      Thread sleep 1000   // Wait a little bit for cluster joining to take effect
      val finalConfig = ConfigFactory.parseString(s"""spark-driver-addr = "$selfAddr"""")
                                     .withFallback(filoConfig)
      _config = Some(finalConfig)
      // TODO: remove coordinator startup, this should not be needed by driver, but unfortunately still is
      coordinatorActor
    }

  def initAndGetConfig(context: SparkContext): Config = {
    init(context)
    config
  }

  def configFromSpark(context: SparkContext): Config = {
    val conf = context.getConf
    val filoOverrides = conf.getAll.collect { case (k, v) if k.startsWith("spark.filodb") =>
                                                k.replace("spark.filodb.", "filodb.") -> v
                                            }
    ConfigFactory.parseMap(filoOverrides.toMap.asJava)
                 .withFallback(ConfigFactory.load)
                 .getConfig("filodb")
  }
}

object FiloExecutor extends FiloSetup with StrictLogging {
  def isSingleJvm: Boolean = FiloDriver._config.isDefined

  // If FiloDriver has also been initialized, then we are running in Spark local mode.  Don't use
  // our own coordinator, use the driver's.
  def coordinator: ActorRef = if (isSingleJvm) FiloDriver.coordinatorActor else this.coordinatorActor

  /**
   * Initializes the config if it is not set, and start things for an executor.
   * @param filoConfig The config within the filodb.** level.
   * @param role the Akka Cluster role, either "executor" or "driver"
   */
  def init(filoConfig: Config): Unit = if (isSingleJvm) {
    _config = FiloDriver._config
    logger.info("Skipping FiloExecutor initialization, driver and executor detected in same JVM")
  } else {
    // Normal cluster, driver and executor are separate JVMs, start up normally
    _config.getOrElse {
      this.role = "executor"
      _config = Some(filoConfig)
      coordinatorActor       // force coordinator to start
      // get address from config and join cluster.  note: it's ok to join cluster multiple times
      val addr = AddressFromURIString.parse(filoConfig.getString("spark-driver-addr"))
      logger.info(s"Initializing FiloExecutor clustering by joining driver at $addr...")
      cluster.join(addr)
      Thread sleep 1000
    }
  }
}