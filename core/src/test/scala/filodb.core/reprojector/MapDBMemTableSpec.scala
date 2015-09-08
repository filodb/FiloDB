package filodb.core.reprojector

import com.typesafe.config.ConfigFactory

import filodb.core.KeyRange
import filodb.core.metadata.Dataset
import filodb.core.columnstore.{TupleRowReader, SegmentSpec}

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}

class MapDBMemTableSpec extends FunSpec with Matchers with BeforeAndAfter {
  import SegmentSpec._

  val keyRange = KeyRange("dataset", Dataset.DefaultPartitionKey, 0L, 10000L)
  var mTable: MemTable = _

  before {
    mTable = new MapDBMemTable(ConfigFactory.load)
  }

  after {
    mTable.close()
  }

  describe("insertRows and readRows") {
    it("should insert out of order rows and read them back in order") {
      val resp = mTable.ingestRows[Long](dataset, schema, names.map(TupleRowReader), 10000L)
      resp should equal (MemTable.Ingested)

      mTable.datasets should equal (Set(dataset.name))

      val outRows = mTable.readRows(keyRange)
      outRows.toSeq.map(_.getString(0)) should equal (firstNames)
    }

    it("should replace rows and read them back in order") {
      val resp = mTable.ingestRows[Long](dataset, schema, names.take(4).map(TupleRowReader), 10000L)
      resp should equal (MemTable.Ingested)
      val resp2 = mTable.ingestRows[Long](dataset, schema, names.take(2).map(TupleRowReader), 10001L)
      resp2 should equal (MemTable.Ingested)

      val outRows = mTable.readRows(keyRange)
      outRows.toSeq.map(_.getString(0)) should equal (Seq("Khalil", "Rodney", "Ndamukong", "Jerry"))
    }

    it("should correctly report stale partitions") (pending)
  }

  describe("concurrency") {
    it("should support concurrent inserts") (pending)
  }
}