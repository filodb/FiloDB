package filodb.cassandra.metastore

import filodb.cassandra.CassandraTest
import filodb.core.Setup
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfter, Matchers}

import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import scala.language.postfixOps


class CassandraMetaStoreSpec extends CassandraTest
with BeforeAndAfter with Matchers with ScalaFutures {

  import Setup._

  var metaStore: CassandraMetaStore = null
  implicit val executionContext = ExecutionContext.global

  override def beforeAll() {
    super.beforeAll()
    metaStore = new CassandraMetaStore(keySpace, session)
    Await.result(metaStore.initialize, 10 seconds)
  }

  before {
    Await.result(metaStore.clearAll, 10 seconds)
  }

  override def afterAll(): Unit = {
    Await.result(metaStore.clearAll, 10 seconds)
  }

  describe("Meta store") {
    it("should allow reading and writing dataset and projections") {
      val added = Await.result(
        metaStore.addProjection(dataset.projectionInfoSeq.head), 10 seconds)
      added should be(true)
      val projection = Await.result(
        metaStore.getProjection("dataset", 0), 10 seconds)
      projection should be(dataset.projectionInfoSeq.head)
    }
  }
}
