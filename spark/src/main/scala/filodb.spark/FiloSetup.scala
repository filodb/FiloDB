package filodb.spark

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.metastore.CassandraMetaStore
import filodb.coordinator.DefaultCoordinatorSetup

object FiloSetup extends DefaultCoordinatorSetup {
  import collection.JavaConverters._

  // The global config of filodb with cassandra, columnstore, etc. sections
  var config: Config = _
  lazy val system = ActorSystem("filo-spark")
  lazy val columnStore = new CassandraColumnStore(config)
  lazy val metaStore = new CassandraMetaStore(config.getConfig("cassandra"))

  def init(filoConfig: Config): Unit = {
    config = filoConfig
    coordinatorActor    // Force coordinatorActor to start
  }

  def init(context: SparkContext): Unit = init(configFromSpark(context))

  def configFromSpark(context: SparkContext): Config = {
    val conf = context.getConf
    val filoOverrides = conf.getAll.collect { case (k, v) if k.startsWith("filodb") =>
                                                k.replace("filodb.", "") -> v
                                            }
    ConfigFactory.parseMap(filoOverrides.toMap.asJava)
                 .withFallback(ConfigFactory.load)
  }
}