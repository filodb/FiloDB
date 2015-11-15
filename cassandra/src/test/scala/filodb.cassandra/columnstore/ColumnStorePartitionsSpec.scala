package filodb.cassandra.columnstore

import com.typesafe.config.ConfigFactory
import filodb.core.columnstore.{PartitionSegmentColStoreTests, SegmentSpec}

import org.scalatest.BeforeAndAfterAll

class ColumnStorePartitionsSpec extends PartitionSegmentColStoreTests with BeforeAndAfterAll {
  import SegmentSpec._
  import scala.concurrent.ExecutionContext.Implicits.global

  val config = ConfigFactory.load("application_test.conf")
  val columnStore = new CassandraColumnStore(config)

  override def beforeAll() {
    super.beforeAll()
    columnStore.initializeProjection(dataset.projections.head).futureValue
  }
}