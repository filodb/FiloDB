package filodb.cassandra.columnstore

import com.websudos.phantom.testkit._
import scala.concurrent.Future

import filodb.core._
import filodb.core.metadata.{Column, Projection, RichProjection}
import filodb.core.store.ColumnStoreSpec
import filodb.core.Types

class CassandraColumnStoreSpec extends CassandraFlatSpec with ColumnStoreSpec {
  import scala.concurrent.ExecutionContext.Implicits.global
  import com.websudos.phantom.dsl._
  import filodb.core.store._
  import NamesTestData._

  val colStore = new CassandraColumnStore(config, global)
  implicit val keySpace = KeySpace(config.getString("cassandra.keyspace"))

  "getScanSplits" should "return splits from Cassandra" in {
    // Single split, token_start should equal token_end
    val singleSplit = colStore.getScanSplits(dataset.name)
    singleSplit should have length (1)
    singleSplit.head("token_start") should equal (singleSplit.head("token_end"))
    singleSplit.head("replicas").split(",").size should equal (1)

    // Multiple splits.  Each split token start/end should not equal each other.
    val multiSplit = colStore.getScanSplits(dataset.name, Map("splits_per_node" -> "2"))
    multiSplit should have length (2)
    multiSplit.foreach { split =>
      split("token_start") should not equal (split("token_end"))
      split("replicas").split(",").size should equal (1)
    }
  }
}