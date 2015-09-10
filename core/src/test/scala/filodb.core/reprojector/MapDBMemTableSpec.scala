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
      val setupResp = mTable.setupIngestion(dataset, schema, 0)
      setupResp should equal (MemTable.SetupDone)

      val resp = mTable.ingestRows("dataset", 0, names.map(TupleRowReader))
      resp should equal (MemTable.Ingested)

      mTable.datasets should equal (Set(dataset.name))
      mTable.numRows("dataset", 0, MemTable.Active) should equal (Some(6L))

      val outRows = mTable.readRows(keyRange, 0, MemTable.Active)
      outRows.toSeq.map(_.getString(0)) should equal (firstNames)
    }

    it("should replace rows and read them back in order") {
      val setupResp = mTable.setupIngestion(dataset, schema, 0)
      setupResp should equal (MemTable.SetupDone)

      val resp = mTable.ingestRows("dataset", 0, names.take(4).map(TupleRowReader))
      resp should equal (MemTable.Ingested)
      val resp2 = mTable.ingestRows("dataset", 0, names.take(2).map(TupleRowReader))
      resp2 should equal (MemTable.Ingested)

      mTable.numRows("dataset", 0, MemTable.Active) should equal (Some(4L))

      val outRows = mTable.readRows(keyRange, 0, MemTable.Active)
      outRows.toSeq.map(_.getString(0)) should equal (Seq("Khalil", "Rodney", "Ndamukong", "Jerry"))
    }

    it("should get NoSuchDatasetVersion if do not setup first") {
      val resp = mTable.ingestRows("a_dataset", 0, names.map(TupleRowReader))
      resp should equal (MemTable.NoSuchDatasetVersion)
    }

    it("should correctly report stale partitions") (pending)
  }

  describe("concurrency") {
    it("should support concurrent inserts") (pending)
  }
}