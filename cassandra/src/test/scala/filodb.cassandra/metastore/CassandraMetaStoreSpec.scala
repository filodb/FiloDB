package filodb.cassandra.metastore

import scala.concurrent.Future

import filodb.core._
import filodb.core.metadata.{Column, Dataset}
import filodb.core.store.MetaStoreSpec
import filodb.cassandra.AllTablesTest

import org.scalatest.{FunSpec, BeforeAndAfter}

class CassandraMetaStoreSpec extends MetaStoreSpec with AllTablesTest