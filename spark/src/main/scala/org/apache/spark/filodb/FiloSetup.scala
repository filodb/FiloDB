package org.apache.spark.filodb

import _root_.filodb.cassandra.columnstore.CassandraColumnStore
import _root_.filodb.cassandra.metastore.CassandraMetaStore
import _root_.filodb.coordinator.IngestionCommands.DCAReady
import _root_.filodb.coordinator.NodeCoordinatorActor.ReloadDCA
import _root_.filodb.coordinator.client.ClusterClient
import _root_.filodb.coordinator.{CoordinatorSetup, NodeClusterActor}
import _root_.filodb.core.store.{InMemoryColumnStore, InMemoryMetaStore}
import akka.actor.{ActorSystem, AddressFromURIString}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.slf4j.StrictLogging
import net.ceedubs.ficus.Ficus._
import org.apache.spark.{SparkContext, SparkEnv}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}
import scala.concurrent.ExecutionContext.Implicits.global

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

  lazy val system = {
    val port = systemConfig.as[Option[Int]](s"filodb.spark.$role.port").getOrElse(0)
    // value actorSystem in class SparkEnv is deprecated: and actor system is no longer supported as of 1.4.0
    // When you have DSE set up on mutlinodes(each node is assigned it's own ip address) in one physical machine,
    // it's hard to identify Memtable WAL files created for each node. So to address this issue require to pass
    // Hostname to the akka configuration
    val host = SparkEnv.get.rpcEnv.address.host
    ActorSystem("filo-spark", configAkka(role, host, port))
  }

  lazy val columnStore = config.getString("store") match {
    case "cassandra" => new CassandraColumnStore(config, readEc)
    case "in-memory" => new InMemoryColumnStore(readEc)
  }
  lazy val metaStore = config.getString("store") match {
    case "cassandra" => new CassandraMetaStore(config.getConfig("cassandra"))
    case "in-memory" => SingleJvmInMemoryStore.metaStore
  }
  def configAkka(role: String, host: String, akkaPort: Int): Config =
    ConfigFactory.parseString(s"""akka.cluster.roles=[$role]
                                 |akka.remote.netty.tcp.hostname=$host
                                 |akka.remote.netty.tcp.port=$akkaPort""".stripMargin)
                 .withFallback(systemConfig)
}

// TODO: make the InMemoryMetaStore either distributed (using clustering to forward and distribute updates)
// or, perhaps modify NodeCoordinator to not need metastore.
object SingleJvmInMemoryStore {
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
      kamonInit()

      coordinatorActor

      // Add in self cluster address, and join cluster ourselves
      val selfAddr = cluster.selfAddress
      cluster.join(selfAddr)
      clusterActor
      // TODO(velvia): Get rid of thread sleep by using NodeClusterActor to time it
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
    *
    * @param filoConfig The config within the filodb.** level.
   * @param role the Akka Cluster role, either "executor" or "driver"
   */
  def init(filoConfig: Config): Unit = synchronized {
    var startIngestion = true
    _config.getOrElse {
      this.role = "executor"
      _config = Some(filoConfig)
        kamonInit()

      coordinatorActor       // force coordinator to start

      // start reloading WAL files
      implicit val timeout = Timeout(FiniteDuration(10, SECONDS))
      val resp = coordinatorActor ? ReloadDCA
      try {
        Await.result(resp, timeout.duration) match {
          case DCAReady =>
            logger.debug("Reload of dataset coordinator actors is completed")
          case _ =>
            logger.debug("Reload is not complete")
        }
      } catch {
        case e: Throwable =>
          e.printStackTrace()
          logger.error(s"Exception occurred while reloading dataset cordinator actors:${e.getMessage}")
          startIngestion = false
      }

      if(startIngestion) {
        // get address from config and join cluster.  note: it's ok to join cluster multiple times
        val addr = AddressFromURIString.parse(filoConfig.getString("spark-driver-addr"))
        logger.info(s"Initializing FiloExecutor clustering by joining driver at $addr...")
        cluster.join(addr)
        Thread sleep 1000
      }else{
        shutdown()
        FiloDriver.shutdown()
        sys.exit(2)
      }
    }
  }
}