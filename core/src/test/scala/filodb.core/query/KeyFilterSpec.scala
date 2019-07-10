package filodb.core.query

import org.scalatest.{FunSpec, Matchers}

import filodb.core._
import filodb.core.metadata.{Column, ComputedColumn}
import filodb.memory.format.RowReader
import filodb.memory.format.ZeroCopyUTF8String._

class KeyFilterSpec extends FunSpec with Matchers {
  import NamesTestData._
  import Filter._

  it("should parse values for regular KeyTypes") {
    KeyFilter.parseSingleValue(dataset.dataColumns(1), "abc") should equal ("abc".utf8)
    KeyFilter.parseSingleValue(dataset.dataColumns(1), "abc".utf8) should equal ("abc".utf8)
    KeyFilter.parseSingleValue(dataset.partitionColumns.head, -15) should equal (-15)

    KeyFilter.parseValues(dataset.dataColumns(1), Set("abc", "def")) should equal (Set("abc".utf8, "def".utf8))
  }

  it("should validate equalsFunc for string and other types") {
    val eqFunc1 = Equals(KeyFilter.parseSingleValue(dataset.dataColumns(1), "abc")).filterFunc
    eqFunc1("abc".utf8) should equal (true)
    eqFunc1("abc") should equal (false)
    eqFunc1(15) should equal (false)

    val eqFunc2 = Equals(KeyFilter.parseSingleValue(dataset.partitionColumns.head, 12)).filterFunc
    eqFunc2(12) should equal (true)
  }

  it("should validate inFunc for string and other types") {
    val inFunc1 = In(KeyFilter.parseValues(dataset.dataColumns(1), Set("abc", "def")).toSet).filterFunc
    inFunc1("abc".utf8) should equal (true)
    inFunc1("aaa".utf8) should equal (false)
    inFunc1(15) should equal (false)
  }

  // it("should produce a multi-column partition filtering func from ColumnFilters") {
  //   import GdeltTestData._

  //   val filters = Seq(ColumnFilter("Actor2Code", Equals("JPN".utf8)),
  //                     ColumnFilter("Year", In(Set(1979, 1980))))
  //   val partFunc = KeyFilter.makePartitionFilterFunc(dataset1, filters)

  //   partFunc(dataset1.partKey("JPN", 1979)) should equal (true)
  //   partFunc(dataset1.partKey("JPN", 1980)) should equal (true)

  //   partFunc(dataset1.partKey("CAN", 1979)) should equal (false)
  //   partFunc(dataset1.partKey("JPN", 2003)) should equal (false)

  //   // It should work for computed columns as well
  //   val partFunc2 = KeyFilter.makePartitionFilterFunc(dataset3, filters)

  //   partFunc2(dataset3.partKey("JPN", 1979)) should equal (true)
  //   partFunc2(dataset3.partKey("CAN", 1979)) should equal (false)
  // }

  val prefixCol = ComputedColumn(0, ":stringPrefix 3", dataset.name,
                                 Column.ColumnType.StringColumn,
                                 Seq(0),
                                 new RowReader.WrappedExtractor((s: String) => s.take(3)))

  it("should parse values for computed KeyTypes") {
    KeyFilter.parseSingleValue(prefixCol, "warship") should equal ("war")
    KeyFilter.parseValues(prefixCol, Seq("ab", "cdefg", "hij")) should equal (Seq("ab", "cde", "hij"))
  }
}