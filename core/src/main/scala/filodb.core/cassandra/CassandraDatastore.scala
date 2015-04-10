package filodb.core.cassandra

import com.typesafe.config.Config

import filodb.core.datastore.Datastore

class CassandraDatastore(config: Config) extends Datastore {
  val dataApi = DataTable
  val partitionApi = PartitionTable
}