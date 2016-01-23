package filodb.core.reprojector

import com.typesafe.config.ConfigFactory
import org.velvia.filo.TupleRowReader

import filodb.core._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import filodb.core.store.SegmentSpec

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}

class FiloMemTableSpec extends FunSpec with Matchers with BeforeAndAfter {
  import NamesTestData._

  val keyRange = KeyRange(Dataset.DefaultPartitionKey, 0, 0)
  val config = ConfigFactory.load("application_test.conf")

  var resp: Int = 0

  before {
    resp = -1
  }

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

      mTable.ingestRows(names.map(TupleRowReader)) { resp = 2 }
      // Not enough rows to auto flush.   Should not have flushed and made callback.
      resp should equal (-1)
      mTable.numRows should be (0)

      mTable.forceCommit()
      resp should equal (2)
      mTable.numRows should be (names.length)

      val outRows = mTable.readRows(keyRange.basedOn(mTable.projection))
      outRows.toSeq.map(_.getString(0)) should equal (firstNames)
    }

    it("should replace rows and read them back in order") {
      val mTable = new FiloMemTable(projection, config)
      mTable.ingestRows(names.take(4).map(TupleRowReader)) { resp = 1 }
      mTable.forceCommit()
      resp should equal (1)
      mTable.ingestRows(names.take(2).map(TupleRowReader)) { resp = 3 }
      mTable.forceCommit()
      resp should equal (3)

      val outRows = mTable.readRows(keyRange.basedOn(mTable.projection))
      outRows.toSeq.map(_.getString(0)) should equal (Seq("Khalil", "Rodney", "Ndamukong", "Jerry"))
    }

    it("should insert/replace rows with multiple partition keys and read them back in order") {
      // Multiple partition keys: Actor2Code, Year
      val mTable = new FiloMemTable(GdeltTestData.projection1, config)
      mTable.ingestRows(GdeltTestData.readers.take(10)) { resp = 7 }
      mTable.forceCommit()
      resp should equal (7)
      mTable.ingestRows(GdeltTestData.readers.take(2)) { resp = 9 }
      mTable.forceCommit()
      resp should equal (9)

      val keyRange = KeyRange(Seq("AGR", 1979), "0", "0")
      val outRows = mTable.readRows(keyRange.basedOn(mTable.projection))
      outRows.toSeq.map(_.getString(5)) should equal (Seq("FARMER", "FARMER"))
    }

    it("should insert/replace rows with multiple row keys and read them back in order") {
      // Multiple row keys: Actor2Code, GLOBALEVENTID
      val mTable = new FiloMemTable(GdeltTestData.projection2, config)
      mTable.ingestRows(GdeltTestData.readers.take(6)) { resp = 8 }
      mTable.forceCommit()
      resp should equal (8)
      mTable.ingestRows(GdeltTestData.readers.take(2)) { resp = 10 }
      mTable.forceCommit()
      resp should equal (10)

      val keyRange = KeyRange(197901, "0", "0")
      val outRows = mTable.readRows(keyRange.basedOn(mTable.projection))
      outRows.toSeq.map(_.getString(5)) should equal (
                 Seq("AFRICA", "FARMER", "FARMER", "CHINA", "POLICE", "IMMIGRANT"))
    }

    it("should ingest into multiple partitions using partition column") {
      val memTable = new FiloMemTable(projWithPartCol, config)

      memTable.ingestRows(namesWithPartCol.map(TupleRowReader)) { resp = 66 }
      resp should equal (66)

      memTable.numRows should equal (50 * names.length)

      val outRows = memTable.readRows(keyRange.copy(partition = "5").basedOn(memTable.projection))
      outRows.toSeq.map(_.getString(0)) should equal (firstNames)
    }

    it("should throw error if null partition col value") {
      val mTable = new FiloMemTable(projWithPartCol, config)

      intercept[NullKeyValue] {
        mTable.ingestRows(namesWithNullPartCol.map(TupleRowReader)) { resp = 22 }
      }
    }

    it("should not throw error if :getOrElse computed column used with null partition col value") (pending)
  }

  describe("flushing") {
    it("should flush automatically after flushInterval elapsed even if # rows < chunkSize") {
      // Ensure flush happens much sooner
      val modConfig = ConfigFactory.parseString("memtable.flush.interval = 500 ms")
                                   .withFallback(config)
      val mTable = new FiloMemTable(projection, modConfig)

      mTable.ingestRows(names.map(TupleRowReader)) { resp = 2 }
      resp should equal (-1)
      // Second set of rows should not cause flush either
      mTable.ingestRows(names.map(TupleRowReader)) { resp = 3 }
      resp should equal (-1)
      Thread sleep 1200    // Well beyond flush interval
      resp should equal (3)

      val outRows = mTable.readRows(keyRange.basedOn(mTable.projection))
      outRows.toSeq.map(_.getString(0)) should equal (firstNames)
    }
  }
}