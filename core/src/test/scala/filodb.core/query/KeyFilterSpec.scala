package filodb.core.query

import org.velvia.filo.RowReader
import org.velvia.filo.ZeroCopyUTF8String._
import filodb.core._
import filodb.core.metadata.{Column, ComputedColumn}

import org.scalatest.{Matchers, FunSpec}

class KeyFilterSpec extends FunSpec with Matchers {
  import SingleKeyTypes._
  import NamesTestData._
  import Filter._

  it("should parse values for regular KeyTypes") {
    KeyFilter.parseSingleValue(schema(0), "abc") should equal ("abc".utf8)
    KeyFilter.parseSingleValue(schema(0), "abc".utf8) should equal ("abc".utf8)
    KeyFilter.parseSingleValue(schema(3), -15) should equal (-15)

    KeyFilter.parseValues(schema(0), Set("abc", "def")) should equal (Set("abc".utf8, "def".utf8))
  }

  it("should validate equalsFunc for string and other types") {
    val eqFunc1 = Equals(KeyFilter.parseSingleValue(schema(0), "abc")).filterFunc
    eqFunc1("abc".utf8) should equal (true)
    eqFunc1("abc") should equal (false)
    eqFunc1(15) should equal (false)

    val eqFunc2 = Equals(KeyFilter.parseSingleValue(schema(3), 12)).filterFunc
    eqFunc2(12) should equal (true)
  }

  it("should validate inFunc for string and other types") {
    val inFunc1 = In(KeyFilter.parseValues(schema(0), Set("abc", "def")).toSet).filterFunc
    inFunc1("abc".utf8) should equal (true)
    inFunc1("aaa".utf8) should equal (false)
    inFunc1(15) should equal (false)
  }

  it("should produce a multi-column partition filtering func from ColumnFilters") {
    import GdeltTestData._

    val filters = Seq(ColumnFilter("Actor2Code", Equals("JPN".utf8)),
                      ColumnFilter("Year", In(Set(1979, 1980))))
    val partFunc = KeyFilter.makePartitionFilterFunc(projection1, filters)

    partFunc(projection1.partKey("JPN", 1979)) should equal (true)
    partFunc(projection1.partKey("JPN", 1980)) should equal (true)

    partFunc(projection1.partKey("CAN", 1979)) should equal (false)
    partFunc(projection1.partKey("JPN", 2003)) should equal (false)

    // It should work for computed columns as well
    val partFunc2 = KeyFilter.makePartitionFilterFunc(projection3, filters)

    partFunc2(projection3.partKey("JPN", 1979)) should equal (true)
    partFunc2(projection3.partKey("CAN", 1979)) should equal (false)
  }

  val prefixCol = ComputedColumn(0, ":stringPrefix 3", datasetRef.dataset,
                                 Column.ColumnType.StringColumn,
                                 Seq(0),
                                 new RowReader.WrappedExtractor((s: String) => s.take(3)))

  it("should parse values for computed KeyTypes") {
    KeyFilter.parseSingleValue(prefixCol, "warship") should equal ("war")
    KeyFilter.parseValues(prefixCol, Seq("ab", "cdefg", "hij")) should equal (Seq("ab", "cde", "hij"))
  }
}