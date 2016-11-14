package filodb.spark

import akka.actor.{ActorSystem, ActorRef, AddressFromURIString}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.slf4j.StrictLogging
import net.ceedubs.ficus.Ficus._
import org.apache.spark.SparkContext
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.metastore.CassandraMetaStore
import filodb.coordinator.{CoordinatorSetup, NodeClusterActor}
import filodb.coordinator.client.ClusterClient
import filodb.core.store.{ColumnStore, ColumnStoreScanner, InMemoryMetaStore, InMemoryColumnStore, MetaStore}

/**
 * A StoreFactory creates instances of ColumnStore and MetaStore one time.  The columnStore and metaStore
 * methods should return that created instance every time, not create a new instance.  The implementation
 * should be a class that is passed a single parameter, the config used to create the stores.
 */
trait StoreFactory {
  def columnStore: ColumnStore with ColumnStoreScanner
  def metaStore: MetaStore
}

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
    ActorSystem("filo-spark", configAkka(role, port))
  }

  class CassandraStoreFactory(config: Config) extends StoreFactory {
    val columnStore = new CassandraColumnStore(config, readEc)
    val metaStore = new CassandraMetaStore(config.getConfig("cassandra"))
  }

  class InMemoryStoreFactory(config: Config) extends StoreFactory {
    val columnStore = new InMemoryColumnStore(readEc)
    val metaStore = SingleJvmInMemoryStore.metaStore
  }

  lazy val factory = config.getString("store") match {
    case "cassandra" => new CassandraStoreFactory(config)
    case "in-memory" => new InMemoryStoreFactory(config)
    case className: String =>
      val ctor = Class.forName(className).getConstructors.head
      ctor.newInstance(config).asInstanceOf[StoreFactory]
  }

  def columnStore: ColumnStore with ColumnStoreScanner = factory.columnStore
  def metaStore: MetaStore = factory.metaStore

  def configAkka(role: String, akkaPort: Int): Config =
    ConfigFactory.parseString(s"""akka.cluster.roles=[$role]
                                 |akka.remote.netty.tcp.port=$akkaPort""".stripMargin)
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

  lazy val clusterActor = getClusterActor("executor")
  lazy val client = new ClusterClient(clusterActor, "executor", "driver")

  // The init method called from a SparkContext is going to be from the driver/app.
  // It also initializes all executors.
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

      val finalConfig = ConfigFactory.parseString(s"""spark-driver-addr = "$selfAddr"""")
                                     .withFallback(filoConfig)
      FiloExecutor.initAllExecutors(finalConfig, context)

      // Because the clusterActor can only be instantiated on an executor/FiloDB node, this works by
      // waiting for the clusterActor to respond, thus guaranteeing cluster working correctly
      implicit val timeout = Timeout(59.seconds)
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
      // get address from config and join cluster.  note: it's ok to join cluster multiple times
      val addr = AddressFromURIString.parse(filoConfig.getString("spark-driver-addr"))
      logger.info(s"Initializing FiloExecutor clustering by joining driver at $addr...")
      cluster.join(addr)
      clusterActor
    }
  }
}