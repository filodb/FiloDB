package filodb.core.reprojector

import com.typesafe.config.ConfigFactory
import org.velvia.filo.TupleRowReader

import filodb.core._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import filodb.core.store.SegmentInfo

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}

class FiloMemTableSpec extends FunSpec with Matchers with BeforeAndAfter {
  import NamesTestData._

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val segInfo = SegmentInfo(Dataset.DefaultPartitionKey, 0)

  val namesWithPartCol = (0 until 50).flatMap { partNum =>
    names.map { t => (t._1, t._2, t._3, t._4, Some(partNum.toString)) }
  }

  val projWithPartCol = RichProjection(largeDataset, schemaWithPartCol)

  val namesWithNullPartCol =
    util.Random.shuffle(namesWithPartCol ++ namesWithPartCol.take(3)
               .map { t => (t._1, t._2, t._3, t._4, None) })

  // Turn this into a common spec for all memTables
  describe("insertRows, readRows with forced flush") {
    it("should insert out of order rows and read them back in order") {
      val mTable = new FiloMemTable(projection, config)
      mTable.numRows should be (0)

      mTable.ingestRows(names.map(TupleRowReader))
      mTable.numRows should be (names.length)

      val outRows = mTable.readRows(segInfo.basedOn(mTable.projection))
      outRows.toSeq.map(_.getString(0)) should equal (firstNames)
    }

    it("should replace rows and read them back in order") {
      val mTable = new FiloMemTable(projection, config)
      mTable.ingestRows(names.take(4).map(TupleRowReader))
      mTable.ingestRows(names.take(2).map(TupleRowReader))

      val outRows = mTable.readRows(segInfo.basedOn(mTable.projection))
      outRows.toSeq.map(_.getString(0)) should equal (Seq("Khalil", "Rodney", "Ndamukong", "Jerry"))
    }

    it("should insert/replace rows with multiple partition keys and read them back in order") {
      // Multiple partition keys: Actor2Code, Year
      val mTable = new FiloMemTable(GdeltTestData.projection1, config)
      mTable.ingestRows(GdeltTestData.readers.take(10))
      mTable.ingestRows(GdeltTestData.readers.take(2))

      val segInfo = SegmentInfo(Seq("AGR", 1979), "0").basedOn(mTable.projection)
      val outRows = mTable.readRows(segInfo)
      outRows.toSeq.map(_.getString(5)) should equal (Seq("FARMER", "FARMER"))
    }

    it("should insert/replace rows with multiple row keys and read them back in order") {
      // Multiple row keys: Actor2Code, GLOBALEVENTID
      val mTable = new FiloMemTable(GdeltTestData.projection2, config)
      mTable.ingestRows(GdeltTestData.readers.take(6))
      mTable.ingestRows(GdeltTestData.readers.take(2))

      val segInfo = SegmentInfo(197901, 0).basedOn(mTable.projection)
      val outRows = mTable.readRows(segInfo)
      outRows.toSeq.map(_.getString(5)) should equal (
                 Seq("AFRICA", "FARMER", "FARMER", "CHINA", "POLICE", "IMMIGRANT"))
    }

    it("should ingest into multiple partitions using partition column") {
      val memTable = new FiloMemTable(projWithPartCol, config)

      memTable.ingestRows(namesWithPartCol.map(TupleRowReader))

      memTable.numRows should equal (50 * names.length)

      val outRows = memTable.readRows(segInfo.copy(partition = "5").basedOn(memTable.projection))
      outRows.toSeq.map(_.getString(0)) should equal (firstNames)
    }

    it("should throw error if null partition col value") {
      val mTable = new FiloMemTable(projWithPartCol, config)

      intercept[NullKeyValue] {
        mTable.ingestRows(namesWithNullPartCol.map(TupleRowReader))
      }
    }

    it("should not throw error if :getOrElse computed column used with null partition col value") {
      val largeDatasetGetOrElse = largeDataset.copy(partitionColumns = Seq(":getOrElse league --"))
      val projWithPartCol2 = RichProjection(largeDatasetGetOrElse, schemaWithPartCol)
      val mTable = new FiloMemTable(projWithPartCol2, config)

      mTable.ingestRows(namesWithNullPartCol.map(TupleRowReader))
      mTable.numRows should equal (namesWithNullPartCol.length)
    }
  }
}