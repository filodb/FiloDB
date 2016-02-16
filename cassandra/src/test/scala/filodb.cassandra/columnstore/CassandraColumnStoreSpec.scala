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
    val singleSplits = colStore.getScanSplits(dataset.name).asInstanceOf[Seq[CassandraTokenRangeSplit]]
    singleSplits should have length (1)
    singleSplits.head.tokenRange.getStart should equal (singleSplits.head.tokenRange.getEnd)
    singleSplits.head.replicas.size should equal (1)

    // Multiple splits.  Each split token start/end should not equal each other.
    val multiSplit = colStore.getScanSplits(dataset.name, 2).asInstanceOf[Seq[CassandraTokenRangeSplit]]
    multiSplit should have length (2)
    multiSplit.foreach { split =>
      split.tokenRange.getStart should not equal (split.tokenRange.getEnd)
      split.replicas.size should equal (1)
    }
  }
}