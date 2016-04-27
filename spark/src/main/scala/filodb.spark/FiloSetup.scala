package filodb.spark

import akka.actor.{ActorSystem, ActorRef, AddressFromURIString}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.SparkContext

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.metastore.CassandraMetaStore
import filodb.coordinator.{CoordinatorSetup, NodeClusterActor}
import filodb.coordinator.client.ClusterClient
import filodb.core.store.{InMemoryMetaStore, InMemoryColumnStore}

/**
 * FiloSetup handles the Spark side setup of both executors and the driver app, including one-time
 * initialization of the coordinator on executors.  Note that executor and driver setup are different.
 * Drivers initialize first, joins the cluster and adds the driver address to the config.
 * Executors initialize by joining the cluster, starting the coordinator.
 *
 * The nice thing about this design is that FiloExecutor.init() is called as needed in packages and/or
 * FiloRelation by each executor/partition, and thus even for contexts where the executors are added
 * dynamically, more nodes can be added to the cluster.
 *
 * Also, using a custom cluster router (NodeClusterActor), we can have a 2-node cluster system even within
 * the same JVM.  The benefit is that local mode works exactly the same as distributed cluster mode, making
 * the code simpler.
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
    case "in-memory" => SingleJvmInMemoryStore.metaStore
  }

  def configWithRole(role: String): Config =
    ConfigFactory.parseString(s"""akka.cluster.roles=[$role]""")
                 .withFallback(systemConfig)
}

// TODO: make the InMemoryMetaStore either distributed (using clustering to forward and distribute updates)
// or, perhaps modify NodeCoordinator to not need metastore.
object SingleJvmInMemoryStore {
  import scala.concurrent.ExecutionContext.Implicits.global
  lazy val metaStore = new InMemoryMetaStore
}

object FiloDriver extends FiloSetup with StrictLogging {
  import collection.JavaConverters._

  lazy val clusterActor = system.actorOf(NodeClusterActor.props(cluster), "cluster-actor")
  lazy val client = new ClusterClient(clusterActor, "executor", "driver")

  // The init method called from a SparkContext is going to be from the driver/app.
  def init(context: SparkContext): Unit = synchronized {
    _config.getOrElse {
      logger.info("Initializing FiloDriver clustering/coordination...")
      role = "driver"
      val filoConfig = configFromSpark(context)
      _config = Some(filoConfig)
      coordinatorActor

      // Add in self cluster address, and join cluster ourselves
      val selfAddr = cluster.selfAddress
      cluster.join(selfAddr)
      clusterActor
      Thread sleep 1000   // Wait a little bit for cluster joining to take effect
      val finalConfig = ConfigFactory.parseString(s"""spark-driver-addr = "$selfAddr"""")
                                     .withFallback(filoConfig)
      _config = Some(finalConfig)
    }
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
                 .withFallback(systemConfig)
                 .getConfig("filodb")
  }
}

object FiloExecutor extends FiloSetup with StrictLogging {
  /**
   * Initializes the config if it is not set, and start things for an executor.
   * @param filoConfig The config within the filodb.** level.
   * @param role the Akka Cluster role, either "executor" or "driver"
   */
  def init(filoConfig: Config): Unit = synchronized {
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