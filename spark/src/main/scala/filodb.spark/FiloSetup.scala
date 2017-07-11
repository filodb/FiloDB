package filodb.spark

import akka.actor.{ActorSystem, AddressFromURIString}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._
import org.apache.spark.SparkContext
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}

import filodb.coordinator.client.ClusterClient
import filodb.coordinator.IngestionCommands.DCAReady
import filodb.coordinator.NodeCoordinatorActor.ReloadDCA
import filodb.coordinator.{CoordinatorSetupWithFactory, NodeClusterActor}
import org.apache.spark.sql.hive.filodb.MetaStoreSync

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
trait FiloSetup extends CoordinatorSetupWithFactory {
  // The global config of filodb with cassandra, columnstore, etc. sections
  def config: Config = _config.get
  var _config: Option[Config] = None
  var role: String = "executor"

  lazy val system = {
    val port = systemConfig.as[Option[Int]](s"filodb.spark.$role.port").getOrElse(0)
    // value actorSystem in class SparkEnv is deprecated: and actor system is no longer supported as of 1.4.0
    // When you have DSE set up on mutlinodes(each node is assigned it's own ip address) in one physical machine,
    // it's hard to identify Memtable WAL files created for each node. So to address this issue require to pass
    // Hostname to the akka configuration
    val host = MetaStoreSync.sparkHost
    ActorSystem("filo-spark", configAkka(role, host, port))
  }

  def configAkka(role: String, host: String, akkaPort: Int): Config =
    ConfigFactory.parseString(s"""akka.cluster.roles=[$role]
                                  |akka.remote.netty.tcp.hostname=$host
                                  |akka.remote.netty.tcp.port=$akkaPort""".stripMargin)
              .withFallback(systemConfig)

  override def shutdown(): Unit = {
    _config.foreach(c => super.shutdown())
  }
}

object FiloDriver extends FiloSetup with StrictLogging {
  import collection.JavaConverters._

  lazy val clusterActor = getClusterActor("executor")
  lazy val client = new ClusterClient(clusterActor, "executor", "driver")

  // The init method called from a SparkContext is going to be from the driver/app.
  // It also initializes all executors.
  def init(context: SparkContext): Unit = synchronized {
    if (_config.isEmpty) {
      logger.info("Initializing FiloDriver clustering/coordination...")
      role = "driver"
      val filoConfig = configFromSpark(context)
      _config = Some(filoConfig)
      kamonInit()
      coordinatorActor

      // Add in self cluster address, and join cluster ourselves
      val selfAddr = cluster.selfAddress
      cluster.join(selfAddr)

      val finalConfig = ConfigFactory.parseString(s"""spark-driver-addr = "$selfAddr"""")
                                     .withFallback(filoConfig)
      implicit val timeout = Timeout(59.seconds)
      Await.result(metaStore.initialize(), 60.seconds)
      FiloExecutor.initAllExecutors(finalConfig, context)

      // Because the clusterActor can only be instantiated on an executor/FiloDB node, this works by
      // waiting for the clusterActor to respond, thus guaranteeing cluster working correctly
      Await.result(clusterActor ? NodeClusterActor.GetRefs("executor"), 60.seconds)

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

  lazy val clusterActor = singletonClusterActor("executor")

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
   * @param filoConfig The config within the filodb.** level.
   * @param role the Akka Cluster role, either "executor" or "driver"
   */
  def init(filoConfig: Config): Unit = synchronized {
    _config.getOrElse {
      this.role = "executor"
      _config = Some(filoConfig)
      kamonInit()
      coordinatorActor       // force coordinator to start

      // start ingesting data once reload DCA process is complete
      if (startReloadDCA()) {
        // get address from config and join cluster.  note: it's ok to join cluster multiple times
        val addr = AddressFromURIString.parse(filoConfig.getString("spark-driver-addr"))
        logger.info(s"Initializing FiloExecutor clustering by joining driver at $addr...")
        cluster.join(addr)
        clusterActor
      } else {
        // Stop ingestion if there is an exception occurs during reload DCA
        shutdown()
        FiloDriver.shutdown()
        sys.exit(2)
      }
    }
  }

  def startReloadDCA(): Boolean = {
    implicit val timeout = Timeout(FiniteDuration(10, SECONDS))
    val resp = coordinatorActor ? ReloadDCA
    // start reloading WAL files if reload-wal-enabled is set to true
    if (config.getBoolean("write-ahead-log.reload-wal-enabled")) {
      try {
        Await.result(resp, timeout.duration) match {
          case DCAReady =>
            logger.info("Reload of dataset coordinator actors is completed")
            return true
          case _ =>
            logger.info("Reload is not complete, and received a wrong acknowledgement")
            return false
        }
      } catch {
        case e: Exception =>
          logger.error(s"Exception occurred while reloading dataset coordinator actors:${e.printStackTrace()}")
          return false
      }
    }
    return true
  }

  def init(confStr: String): Unit = if (_config.isEmpty) {
    init(ConfigFactory.parseString(confStr))
  }
}