package filodb.core.memtable

import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.velvia.filo.TupleRowReader

class MapDBMemTableSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {

  import filodb.core.Setup._

  val config = ConfigFactory.load("application_test.conf")

  // Must be more than the max-rows-per-table setting in application_test.conf
  val lotsOfNames = (0 until 400).flatMap { partNum =>
    names.map { t => (t._1, t._2, t._3, Some(partNum.toString)) }
  }

  describe("insertRows, readRows, flip") {
    it("should insert out of order rows and read them back in order") {
      val mTable = new MapDBMemTable(projection, config)
      mTable.numRows should be(0)

      mTable.ingestRows(names.map(TupleRowReader))
      mTable.numRows should equal(6)

      val outRows = mTable.readRows("US", keyRange)
      outRows.toSeq.map(_.getString(2)) should equal(firstNames.take(3))
    }

    it("should replace rows and read them back in order or key") {
      val mTable = new MapDBMemTable(projection, config)
      mTable.ingestRows(names.take(4).map(TupleRowReader))
      mTable.ingestRows(names.take(2).map(TupleRowReader))
      mTable.numRows should equal(4)

      val outRows = mTable.readAllRows().map(_._3)
      outRows.toSeq.map(_.getString(2)) should equal(Seq("Jerry", "Khalil", "Ndamukong", "Rodney"))
    }

  }

  describe("removeRows") {
    it("should be able to delete rows") {
      val mTable = new MapDBMemTable(projection, config)
      mTable.ingestRows(names.map(TupleRowReader))

      mTable.removeRows("US", keyRange)
      mTable.numRows should equal(3)
      mTable.removeRows("UK", keyRange)
      mTable.numRows should equal(0)
    }
  }
}
