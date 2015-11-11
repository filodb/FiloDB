package filodb.core.reprojector

import com.typesafe.config.ConfigFactory
import org.velvia.filo.TupleRowReader

import filodb.core.KeyRange
import filodb.core.metadata.{Column, Dataset, RichProjection}
import filodb.core.columnstore.SegmentSpec
import scala.concurrent.Future

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}
import org.scalatest.concurrent.ScalaFutures

class MapDBMemTableSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  import SegmentSpec._

  val keyRange = KeyRange("dataset", Dataset.DefaultPartitionKey, 0L, 10000L)
  val config = ConfigFactory.load("application_test.conf")
  import scala.concurrent.ExecutionContext.Implicits.global

  var resp: Int = 0

  before {
    resp = -1
  }

  val schemaWithPartCol = schema ++ Seq(
    Column("league", "dataset", 0, Column.ColumnType.StringColumn)
  )

  val namesWithPartCol = (0 until 50).flatMap { partNum =>
    names.map { t => (t._1, t._2, t._3, Some(partNum.toString)) }
  }

  val projWithPartCol = RichProjection[Long](dataset.copy(partitionColumn = "league"), schemaWithPartCol)

  val namesWithNullPartCol =
    util.Random.shuffle(namesWithPartCol ++ namesWithPartCol.take(3).map { t => (t._1, t._2, t._3, None) })

  // Must be more than the max-rows-per-table setting in application_test.conf
  val lotsOfNames = (0 until 400).flatMap { partNum =>
    names.map { t => (t._1, t._2, t._3, Some(partNum.toString)) }
  }

  describe("insertRows, readRows, flip") {
    it("should insert out of order rows and read them back in order") {
      val mTable = new MapDBMemTable(projection, config)
      mTable.numRows should be (0)

      mTable.ingestRows(names.map(TupleRowReader)) { resp = 2 }
      resp should equal (2)

      mTable.numRows should equal (6)
      mTable.partitions should equal (Set(Dataset.DefaultPartitionKey))

      val outRows = mTable.readRows(keyRange)
      outRows.toSeq.map(_.getString(0)) should equal (firstNames)

      val outKeys = mTable.allKeys.toSeq
      outKeys.map(_._1).toSet should equal (Set(Dataset.DefaultPartitionKey))
      outKeys.map(_._2) should equal (Seq(24L, 25L, 28L, 29L, 39L, 40L))
    }

    it("should replace rows and read them back in order") {
      val mTable = new MapDBMemTable(projection, config)
      mTable.ingestRows(names.take(4).map(TupleRowReader)) { resp = 1 }
      resp should equal (1)
      mTable.ingestRows(names.take(2).map(TupleRowReader)) { resp = 3 }
      resp should equal (3)

      mTable.numRows should equal (4)

      val outRows = mTable.readRows(keyRange)
      outRows.toSeq.map(_.getString(0)) should equal (Seq("Khalil", "Rodney", "Ndamukong", "Jerry"))
    }

    it("should read rows with endExclusive true/false and start = None correctly") {
      val mTable = new MapDBMemTable(projection, config)
      mTable.ingestRows(names.map(TupleRowReader)) { resp = 2 }
      resp should equal (2)

      val outRows = mTable.readRows(keyRange.copy(start = None))
      outRows.toSeq.map(_.getString(0)) should equal (firstNames)

      val keyRange2 = keyRange.copy(start = Some(25L), end = Some(29L))
      val outRows2 = mTable.readRows(keyRange2)
      outRows2.toSeq.map(_.getString(0)) should equal (Seq("Rodney", "Ndamukong"))

      val outRows3 = mTable.readRows(keyRange2.copy(endExclusive = false))
      outRows3.toSeq.map(_.getString(0)) should equal (Seq("Rodney", "Ndamukong", "Terrance"))
    }

    it("should ingest into multiple partitions using partition column") {
      val memTable = new MapDBMemTable(projWithPartCol, config)

      memTable.ingestRows(namesWithPartCol.map(TupleRowReader)) { resp = 66 }
      resp should equal (66)

      memTable.numRows should equal (50 * names.length)
      memTable.partitions should equal (Set((0 until 50).map(_.toString) :_*))

      val outRows = memTable.readRows(keyRange.copy(partition = "5"))
      outRows.toSeq.map(_.getString(0)) should equal (firstNames)
    }

    it("should throw error if null partition col value and no defaultPartitionKey") {
      val mTable = new MapDBMemTable(projWithPartCol, config)

      intercept[Dataset.NullPartitionValue] {
        mTable.ingestRows(namesWithNullPartCol.map(TupleRowReader)) { resp = 22 }
      }
    }

    it("should use defaultPartitionKey if one provided and null part col value") {
      val newOptions = dataset.options.copy(defaultPartitionKey = Some("foobar"))
      val datasetWithDefPartKey = dataset.copy(options = newOptions, partitionColumn = "league")
      val newProj = RichProjection[Long](datasetWithDefPartKey, schemaWithPartCol)
      val mTable = new MapDBMemTable(newProj, config)

      mTable.ingestRows(namesWithNullPartCol.map(TupleRowReader)) { resp = 99 }
      resp should equal (99)

      mTable.numRows should equal (namesWithNullPartCol.length)
      val outRows = mTable.readRows(keyRange.copy(partition = "foobar"))
      outRows.toSeq should have length (3)
    }
  }

  describe("removeRows") {
    it("should be able to delete rows") {
      val mTable = new MapDBMemTable(projection, config)
      mTable.ingestRows(names.map(TupleRowReader)) { resp = 17 }
      resp should equal (17)

      mTable.removeRows(keyRange)
      mTable.numRows should equal (0)
    }
  }
}