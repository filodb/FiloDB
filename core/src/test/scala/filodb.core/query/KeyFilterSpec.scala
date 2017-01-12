package filodb.core.query

import org.velvia.filo.RowReader
import org.velvia.filo.ZeroCopyUTF8String._
import filodb.core._
import filodb.core.metadata.{Column, ComputedColumn}

import org.scalatest.{Matchers, FunSpec}

class KeyFilterSpec extends FunSpec with Matchers {
  import SingleKeyTypes._
  import NamesTestData._

  it("should parse values for regular KeyTypes") {
    KeyFilter.parseSingleValue(schema(0), "abc") should equal ("abc".utf8)
    KeyFilter.parseSingleValue(schema(0), "abc".utf8) should equal ("abc".utf8)
    KeyFilter.parseSingleValue(schema(3), -15) should equal (-15)

    KeyFilter.parseValues(schema(0), Set("abc", "def")) should equal (Set("abc".utf8, "def".utf8))
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