package filodb.cassandra.columnstore

import scala.concurrent.Future

import filodb.core._
import filodb.core.metadata.{Column, Projection, RichProjection}
import filodb.core.store.ColumnStoreSpec
import filodb.core.Types

class CassandraColumnStoreSpec extends ColumnStoreSpec {
  import scala.concurrent.ExecutionContext.Implicits.global
  import filodb.core.store._
  import NamesTestData._

  val colStore = new CassandraColumnStore(config, global)

  "getScanSplits" should "return splits from Cassandra" in {
    // Single split, token_start should equal token_end
    val singleSplits = colStore.getScanSplits(datasetRef).asInstanceOf[Seq[CassandraTokenRangeSplit]]
    singleSplits should have length (1)
    val split = singleSplits.head
    split.tokens should have length (1)
    split.tokens.head._1 should equal (split.tokens.head._2)
    split.replicas.size should equal (1)

    // Multiple splits.  Each split token start/end should not equal each other.
    val multiSplit = colStore.getScanSplits(datasetRef, 2).asInstanceOf[Seq[CassandraTokenRangeSplit]]
    multiSplit should have length (2)
    multiSplit.foreach { split =>
      split.tokens.head._1 should not equal (split.tokens.head._2)
      split.replicas.size should equal (1)
    }
  }
}