package filodb.core.metadata

import com.typesafe.config.ConfigFactory

import org.scalatest.{FunSpec, Matchers}

class ColumnSpec extends FunSpec with Matchers {
  import Column.ColumnType
  import Dataset._

  val firstColumn = DataColumn(0, "first", ColumnType.StringColumn)
  val ageColumn = DataColumn(2, "age", ColumnType.IntColumn)
  val histColumnOpts = DataColumn(3, "hist", ColumnType.HistogramColumn,
                                  ConfigFactory.parseString("counter = true"))
  val histColumn2 = DataColumn(4, "h2", ColumnType.HistogramColumn,
                               ConfigFactory.parseString("counter = true\nsize=20000"))

  describe("Column validations") {
    it("should check that regular column names don't have : in front") {
      val res1 = Column.validateColumnName(":illegal")
      res1.isBad shouldEqual true
      res1.swap.get.head shouldBe a[BadColumnName]
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
    }
  }
}