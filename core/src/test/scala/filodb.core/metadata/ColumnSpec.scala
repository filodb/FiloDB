package filodb.core.metadata

import com.typesafe.config.ConfigFactory
import filodb.core.query.ColumnInfo
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ColumnSpec extends AnyFunSpec with Matchers {
  import Column.ColumnType
  import Dataset._

  val firstColumn = DataColumn(0, "first", ColumnType.StringColumn)
  val cumulHistColumn = DataColumn(3, "hist", ColumnType.HistogramColumn,
                              ConfigFactory.parseString("detectDrops = true"))
  val ageColumn = DataColumn(2, "age", ColumnType.IntColumn)
  val histColumnOpts = DataColumn(3, "hist", ColumnType.HistogramColumn,
                                  ConfigFactory.parseString("counter = true"))
  val histColumn2 = DataColumn(4, "h2", ColumnType.HistogramColumn,
                               ConfigFactory.parseString("counter = true\nsize=20000"))
  val deltaCountColumn = DataColumn(5, "dc", ColumnType.HistogramColumn,
    ConfigFactory.parseString("{counter = false, delta = true}"))

  describe("Column validations") {
    it("should check that regular column names don't have : in front") {
      val res1 = Column.validateColumnName(":illegal")
      res1.isBad shouldEqual true
      res1.swap.get.head shouldBe a[BadColumnName]
    }

    it("should return correct value for hasCumulativeIncrement") {
      cumulHistColumn.isCumulativeTemporality shouldEqual true
      deltaCountColumn.isCumulativeTemporality shouldEqual false
      firstColumn.isCumulativeTemporality shouldEqual false
    }

    it("should check that column names cannot contain illegal chars") {
      def checkIsIllegal(name: String): Unit = {
        val res1 = Column.validateColumnName(name)
        res1.isBad shouldEqual true
        res1.swap.get.head shouldBe a[BadColumnName]
      }

      checkIsIllegal("ille gal")
      checkIsIllegal("(illegal)")
      checkIsIllegal("ille\u0001gal")
    }
  }

  describe("Column serialization") {
    it("should serialize and deserialize properly") {
      DataColumn.fromString(firstColumn.toString) should equal (firstColumn)
      DataColumn.fromString(ageColumn.toString) should equal (ageColumn)
      DataColumn.fromString(histColumnOpts.toString) should equal (histColumnOpts)
      DataColumn.fromString(histColumn2.toString) should equal (histColumn2)
      DataColumn.fromString(deltaCountColumn.toString) should equal (deltaCountColumn)
    }
    it("check delta param") {
      ColumnInfo(firstColumn).isCumulative should equal (true)
      ColumnInfo(ageColumn).isCumulative should equal (true)
      ColumnInfo(histColumnOpts).isCumulative should equal (true)
      ColumnInfo(histColumn2).isCumulative should equal (true)
      ColumnInfo(deltaCountColumn).isCumulative should equal (false)
    }
  }
}