package filodb.core.reprojector

import com.typesafe.config.ConfigFactory

import filodb.core.KeyRange
import filodb.core.metadata.{Column, Dataset}
import filodb.core.columnstore.{TupleRowReader, SegmentSpec}

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}

class MapDBMemTableSpec extends FunSpec with Matchers with BeforeAndAfter {
  import SegmentSpec._
  import MemTable._

  val keyRange = KeyRange("dataset", Dataset.DefaultPartitionKey, 0L, 10000L)
  var mTable: MemTable = _

  before {
    mTable = new MapDBMemTable(ConfigFactory.load)
  }

  after {
    mTable.close()
  }

  val schemaWithPartCol = schema ++ Seq(
    Column("league", "dataset", 0, Column.ColumnType.StringColumn)
  )

  val namesWithPartCol = (0 until 50).flatMap { partNum =>
    names.map { t => (t._1, t._2, t._3, Some(partNum.toString)) }
  }

  describe("insertRows, readRows, flip") {
    it("should insert out of order rows and read them back in order") {
      val setupResp = mTable.setupIngestion(dataset, schema, 0)
      setupResp should equal (SetupDone)

      val resp = mTable.ingestRows("dataset", 0, names.map(TupleRowReader))
      resp should equal (Ingested)

      mTable.datasets should equal (Set(dataset.name))
      mTable.numRows("dataset", 0, Active) should equal (Some(6L))

      val outRows = mTable.readRows(keyRange, 0, Active)
      outRows.toSeq.map(_.getString(0)) should equal (firstNames)
    }

    it("should replace rows and read them back in order") {
      val setupResp = mTable.setupIngestion(dataset, schema, 0)
      setupResp should equal (SetupDone)

      val resp = mTable.ingestRows("dataset", 0, names.take(4).map(TupleRowReader))
      resp should equal (Ingested)
      val resp2 = mTable.ingestRows("dataset", 0, names.take(2).map(TupleRowReader))
      resp2 should equal (Ingested)

      mTable.numRows("dataset", 0, Active) should equal (Some(4L))

      val outRows = mTable.readRows(keyRange, 0, Active)
      outRows.toSeq.map(_.getString(0)) should equal (Seq("Khalil", "Rodney", "Ndamukong", "Jerry"))
    }

    it("should get NoSuchDatasetVersion if do not setup first") {
      val resp = mTable.ingestRows("a_dataset", 0, names.map(TupleRowReader))
      resp should equal (NoSuchDatasetVersion)
    }

    it("should ingest into multiple partitions using partition column") {
      val setupResp = mTable.setupIngestion(dataset.copy(partitionColumn = "league"),
                                            schemaWithPartCol, 0)
      setupResp should equal (SetupDone)

      val resp = mTable.ingestRows("dataset", 0, namesWithPartCol.map(TupleRowReader))
      resp should equal (Ingested)

      mTable.numRows("dataset", 0, Active) should equal (Some(50L * names.length))

      val outRows = mTable.readRows(keyRange.copy(partition = "5"), 0, Active)
      outRows.toSeq.map(_.getString(0)) should equal (firstNames)
    }

    it("should be able to flip, insert into Active again, read from both") {
      val setupResp = mTable.setupIngestion(dataset, schema, 0)
      setupResp should equal (SetupDone)

      val resp = mTable.ingestRows("dataset", 0, names.map(TupleRowReader))
      resp should equal (Ingested)

      mTable.flipBuffers("dataset", 0) should equal (Flipped)

      mTable.ingestRows("dataset", 0, names.take(3).map(TupleRowReader)) should equal (Ingested)

      mTable.numRows("dataset", 0, Active) should equal (Some(3L))
      mTable.flushingDatasets should equal (Seq((("dataset", 0), 6L)))

      val outRows = mTable.readRows(keyRange, 0, Active)
      outRows.toSeq.map(_.getString(0)) should equal (firstNames take 3)

      // Now, if we attempt to flip again, it should error out because Locked is not empty
      mTable.flipBuffers("dataset", 0) should equal (LockedNotEmpty)
    }
  }

  describe("numRows") {
    it("should get no rows out of Locked partition") {
      val setupResp = mTable.setupIngestion(dataset, schema, 0)
      setupResp should equal (SetupDone)

      mTable.numRows("dataset", 0, Locked) should equal (Some(0L))
    }

    it("should get None out of unknown dataset or version") {
      val setupResp = mTable.setupIngestion(dataset, schema, 0)
      setupResp should equal (SetupDone)

      val resp = mTable.ingestRows("dataset", 0, names.map(TupleRowReader))
      resp should equal (Ingested)

      mTable.numRows("dataset", 1, Active) should equal (None)
      mTable.numRows("not_dataset", 0, Active) should equal (None)
    }
  }

  describe("setupIngestion errors") {
    it("should get BadSchema if cannot find sort column") {
      mTable.setupIngestion(Dataset("a", "boo"), schema, 0) should equal (BadSchema)
    }

    it("should get BadSchema if sort column is not supported type") {
      mTable.setupIngestion(Dataset("a", "first"), schema, 0) should equal (BadSchema)
    }

    it("should get BadSchema if partition column unknown or not String type") {
      mTable.setupIngestion(dataset.copy(partitionColumn = "XXX"), schema, 0) should equal (BadSchema)
      mTable.setupIngestion(dataset.copy(partitionColumn = "age"), schema, 0) should equal (BadSchema)
    }

    it("should get AlreadySetup if try to set up twice for same dataset/version") {
      mTable.setupIngestion(dataset, schema, 0) should equal (SetupDone)
      mTable.setupIngestion(dataset, schemaWithPartCol, 0) should equal (AlreadySetup)
    }
  }
}