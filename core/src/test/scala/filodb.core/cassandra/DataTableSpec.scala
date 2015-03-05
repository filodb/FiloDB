package filodb.core.cassandra

import com.websudos.phantom.testing.CassandraFlatSpec
import java.nio.ByteBuffer
import org.scalatest.BeforeAndAfter
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.core.metadata.{Shard, Partition}
import filodb.core.messages._

class DataTableSpec extends CassandraFlatSpec with BeforeAndAfter {
  val keySpace = "test"

  // First create the partitions table
  override def beforeAll() {
    super.beforeAll()
    // Note: This is a CREATE TABLE IF NOT EXISTS
    Await.result(DataTable.create.future(), 3 seconds)
  }

  before {
    Await.result(DataTable.truncate.future(), 3 seconds)
  }

  import scala.concurrent.ExecutionContext.Implicits.global
  import com.websudos.phantom.Implicits._

  // Just some dummy data, doesn't actually correspond to anything
  val bb = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4))
  val shard = Shard(Partition("dummy", "0", chunkSize = 100), 0, 0L)
  val columnBytes = Map("a" -> bb, "b" -> bb)

  "DataTable" should "return ChunkMisaligned if trying to write a chunk not on boundary" in {
    whenReady(DataTable.insertOneChunk(shard, 5L, 7L, columnBytes)) { response =>
      response should equal (Shard.ChunkMisaligned)
    }
  }

  it should "write chunks successfully and return Ack()" in {
    whenReady(DataTable.insertOneChunk(shard, 0L, 49L, columnBytes)) { response =>
      response should equal (Shard.Ack(49L))
    }

    val f = DataTable.select(_.columnName, _.rowId).fetch()
    whenReady(f) { response =>
      response should equal (Seq(("a", 0L), ("b", 0L)))
    }
  }
}