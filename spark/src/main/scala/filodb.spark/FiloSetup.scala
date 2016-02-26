package filodb.spark

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.metastore.CassandraMetaStore
import filodb.coordinator.CoordinatorSetup
import filodb.core.store.{InMemoryMetaStore, InMemoryColumnStore}

object FiloSetup extends CoordinatorSetup {
  import collection.JavaConverters._

  // The global config of filodb with cassandra, columnstore, etc. sections
  def config: Config = _config.get
  var _config: Option[Config] = None
  lazy val system = ActorSystem("filo-spark")
  lazy val columnStore = config.getString("store") match {
    case "cassandra" => new CassandraColumnStore(config, readEc)
    case "in-memory" => new InMemoryColumnStore(readEc)
  }
  lazy val metaStore = config.getString("store") match {
    case "cassandra" => new CassandraMetaStore(config.getConfig("cassandra"))
    case "in-memory" => new InMemoryMetaStore
  }

  /**
   * Initializes the config if it is not set, and start things.
   * @param filoConfig The config within the filodb.** level.
   */
  def init(filoConfig: Config): Unit = _config.getOrElse {
    _config = Some(filoConfig)
    coordinatorActor       // Force NodeCoordinatorActor to start
  }

  def init(context: SparkContext): Unit = _config.getOrElse(init(configFromSpark(context)))

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