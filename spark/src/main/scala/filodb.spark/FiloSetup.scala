package filodb.spark

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.{ActorSystem, AddressFromURIString}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.filodb.MetaStoreSync

import filodb.coordinator.client.ClusterClient
import filodb.coordinator.{NodeClusterActor, _}

/**
 * FilodbSparkCluster handles the Spark side setup of both executors and the driver app, including one-time
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
private[filodb] trait FilodbSparkCluster extends FilodbClusterNode {

  // value actorSystem in class SparkEnv is deprecated: and actor system is no longer supported as of 1.4.0
  // When you have DSE set up on mutlinodes(each node is assigned it's own ip address) in one physical machine,
  // it's hard to identify Memtable WAL files created for each node. So to address this issue require to pass
  // Hostname to the akka configuration
  lazy val settings = {
    val port = systemConfig.as[Option[Int]](s"filodb.spark.$role.port").getOrElse(0)
    val host = MetaStoreSync.sparkHost
    val ourConf = ConfigFactory.parseString(s"""
      akka.cluster.roles=["$roleName"]
      akka.remote.netty.tcp.hostname=$host
      akka.remote.netty.tcp.port=$port""")

    new FilodbSettings(ourConf)
  }

  override lazy val system = ActorSystem(systemName, settings.allConfig)

  override lazy val cluster = FilodbCluster(system)

  var _config: Option[Config] = None

  // The global config of filodb with cassandra, columnstore, etc. sections
  def config: Config = _config.get

  lazy val memStore = cluster.memStore

  lazy val clusterActor = cluster.clusterSingleton(roleName, withManager = true)

}

object FiloDriver extends FilodbSparkCluster {
  import collection.JavaConverters._

  override val role = ClusterRole.Driver

  // NOTE: It's important that we use the executor's cluster actor so that the right singleton is reached
  lazy val client = new ClusterClient(FiloExecutor.clusterActor, "executor", "driver")

  // The init method called from a SparkContext is going to be from the driver/app.
  // It also initializes all executors.
  private[filodb] def init(context: SparkContext): Unit = synchronized {
    if (_config.isEmpty) {
      import cluster.settings._

      logger.info("Initializing FiloDriver clustering/coordination...")
      val filoConfig = configFromSpark(context)
      _config = Some(filoConfig)

      cluster.kamonInit(role)
      coordinatorActor // create it
      cluster.join(cluster.selfAddress)

      val finalConfig = ConfigFactory
        .parseString(s"""spark-driver-addr = "${cluster.selfAddress}"""")
        .withFallback(filoConfig)

      implicit val timeout = Timeout(InitializationTimeout.minus(1.second))
      Await.result(metaStore.initialize(), InitializationTimeout)

      FiloExecutor.initAllExecutors(finalConfig, context)
      // Because the clusterActor can only be instantiated on an executor/FiloDB node, this works by
      // waiting for the clusterActor to respond, thus guaranteeing cluster working correctly
      Await.result(FiloExecutor.clusterActor ? NodeClusterActor.GetRefs("executor"), InitializationTimeout)
      cluster._isInitialized.set(true)
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
                 .withFallback(settings.config) // the filodb config
  }

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = shutdown()
  })
}

object FiloExecutor extends FilodbSparkCluster {

  override val role = ClusterRole.Executor

  def initAllExecutors(filoConfig: Config, context: SparkContext, numPartitions: Int = 1000): Unit = {
      logger.info(s"Initializing executors using $numPartitions partitions...")
      context.parallelize(0 to numPartitions, numPartitions).mapPartitions { it =>
        init(filoConfig)
        it
      }.count()
      logger.info("Finished initializing executors")
  }

  /**
   * Initializes the config if it is not set, and start things for an executor.
   *
   * @param filoConfig The config within the filodb.** level.
   */
  def init(filoConfig: Config): Unit = synchronized {
    if (_config.isEmpty) {
      _config = Some(filoConfig)

      val addr = AddressFromURIString.parse(filoConfig.getString("spark-driver-addr"))
      logger.info(s"Initializing FiloExecutor clustering by joining driver at $addr...")
      cluster.kamonInit(role)
      coordinatorActor // create it
      cluster.join(addr)
      clusterActor
      cluster._isInitialized.set(true)
    }
  }

  def init(confStr: String): Unit = if (_config.isEmpty) {
    init(ConfigFactory.parseString(confStr))
  }
}