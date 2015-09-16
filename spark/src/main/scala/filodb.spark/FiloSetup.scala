package filodb.spark

import akka.actor.ActorSystem
import com.typesafe.config.Config

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.metastore.CassandraMetaStore
import filodb.coordinator.DefaultCoordinatorSetup

object FiloSetup extends DefaultCoordinatorSetup {
  var config: Config = _
  lazy val system = ActorSystem("filo-spark")
  lazy val columnStore = new CassandraColumnStore(config)
  lazy val metaStore = new CassandraMetaStore(config)

  def init(filoConfig: Config): Unit = {
    config = filoConfig
    coordinatorActor    // Force coordinatorActor to start
  }

  // TODO: get config from SparkConfig and options
}