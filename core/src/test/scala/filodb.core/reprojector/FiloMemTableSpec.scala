package filodb.core.reprojector

import java.nio.file.Files

import com.typesafe.config.ConfigFactory
import org.velvia.filo.TupleRowReader
import filodb.core._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import filodb.core.store.SegmentInfo
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}

import scala.util.Try
import scalax.file.Path

class FiloMemTableSpec extends FunSpec with Matchers with BeforeAndAfter {
  import NamesTestData._

  var config = ConfigFactory.load("application_test.conf").getConfig("filodb")

  before {
    val tempDir = Files.createTempDirectory("wal")
    // /var/folders/tv/qrqnpyzj0qdfgw122hf1d7zr0000gn/T

    config = ConfigFactory.parseString(
      s"""filodb.write-ahead-log.memtable-wal-dir = ${tempDir}
          filodb.write-ahead-log.mapped-byte-buffer-size = 2048
          filodb.write-ahead-log.write-ahead-log-enabled = false
       """)
      .withFallback(ConfigFactory.load("application_test.conf"))
      .getConfig("filodb")
  }

  val segInfo = SegmentInfo(Dataset.DefaultPartitionKey, 0)
  val version = 0
  val actorAddress = "localhost"

  val namesWithPartCol = (0 until 50).flatMap { partNum =>
    names.map { t => (t._1, t._2, t._3, t._4, Some(partNum.toString)) }
  }

  val projWithPartCol = RichProjection(largeDataset, schemaWithPartCol)

  val namesWithNullPartCol =
    scala.util.Random.shuffle(namesWithPartCol ++ namesWithPartCol.take(3)
               .map { t => (t._1, t._2, t._3, t._4, None) })

  after {
    val walDir = config.getString("write-ahead-log.memtable-wal-dir")
    val path = Path.fromString (walDir)
    Try (path.deleteRecursively(continueOnFailure = false))
  }

  // Turn this into a common spec for all memTables
  describe("insertRows, readRows with forced flush") {
    it("should insert out of order rows and read them back in order") {
      val mTable =  new FiloMemTable(projection, config, actorAddress, version)
      mTable.numRows should be (0)

      mTable.ingestRows(names.map(TupleRowReader))
      mTable.numRows should be (names.length)

      val outRows = mTable.readRows(segInfo.basedOn(mTable.projection))
      outRows.toSeq.map(_.getString(0)) should equal (sortedFirstNames)
    }

    it("should replace rows and read them back in order") {
      val mTable =  new FiloMemTable(projection, config, actorAddress, version)
      mTable.ingestRows(names.take(4).map(TupleRowReader))
      mTable.ingestRows(altNames.take(2).map(TupleRowReader))

      val outRows = mTable.readRows(segInfo.basedOn(mTable.projection))
      outRows.toSeq.map(_.getString(0)) should equal (Seq("Stacy", "Rodney", "Bruce", "Jerry"))
    }

    it("should insert/replace rows with multiple partition keys and read them back in order") {
      // Multiple partition keys: Actor2Code, Year
      val mTable = new FiloMemTable(GdeltTestData.projection1, config, actorAddress, version)
      mTable.ingestRows(GdeltTestData.readers.take(10))
      mTable.ingestRows(GdeltTestData.readers.take(2))

      val segInfo = SegmentInfo(Seq("AGR", 1979), "0").basedOn(mTable.projection)
      val outRows = mTable.readRows(segInfo)
      outRows.toSeq.map(_.getString(5)) should equal (Seq("FARMER", "FARMER"))
    }

    it("should insert/replace rows with multiple row keys and read them back in order") {
      // Multiple row keys: Actor2Code, GLOBALEVENTID
      val mTable = new FiloMemTable(GdeltTestData.projection2, config, actorAddress, version)
      mTable.ingestRows(GdeltTestData.readers.take(6))
      mTable.ingestRows(GdeltTestData.altReaders.take(2))

      val segInfo = SegmentInfo(197901, 0).basedOn(mTable.projection)
      val outRows = mTable.readRows(segInfo)
      outRows.toSeq.map(_.getString(5)) should equal (
                 Seq("africa", "farm-yo", "FARMER", "CHINA", "POLICE", "IMMIGRANT"))
    }

    it("should ingest into multiple partitions using partition column") {
      val memTable = new FiloMemTable(projWithPartCol, config, actorAddress, version)

      memTable.ingestRows(namesWithPartCol.map(TupleRowReader))

      memTable.numRows should equal (50 * names.length)

      val outRows = memTable.readRows(segInfo.copy(partition = "5").basedOn(memTable.projection))
      outRows.toSeq.map(_.getString(0)) should equal (sortedFirstNames)
    }

    it("should keep ingesting rows with null partition col value") {
      val mTable = new FiloMemTable(projWithPartCol, config, actorAddress, version)

      mTable.ingestRows(namesWithNullPartCol.map(TupleRowReader))
      mTable.numRows should equal (50 * names.length + 3)
    }

    it("should not throw error if :getOrElse computed column used with null partition col value") {
      val largeDatasetGetOrElse = largeDataset.copy(partitionColumns = Seq(":getOrElse league --"))
      val projWithPartCol2 = RichProjection(largeDatasetGetOrElse, schemaWithPartCol)
      val mTable = new FiloMemTable(projWithPartCol2, config, actorAddress, version)

      mTable.ingestRows(namesWithNullPartCol.map(TupleRowReader))
      mTable.numRows should equal (namesWithNullPartCol.length)
    }
  }
}