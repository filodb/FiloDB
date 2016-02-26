package filodb.core.query

import filodb.core._
import filodb.core.metadata.ComputedKeyTypes
import org.velvia.filo.RowReader

import org.scalatest.{Matchers, FunSpec}

class KeyFilterSpec extends FunSpec with Matchers {
  import SingleKeyTypes._

  it("should parse values for regular KeyTypes") {
    KeyFilter.parseSingleValue(StringKeyType)("abc") should equal ("abc")
    KeyFilter.parseSingleValue(IntKeyType)(-15) should equal (-15)

    KeyFilter.parseValues(StringKeyType)(Set("abc", "def")) should equal (Set("abc", "def"))
  }

  val prefixKT = ComputedKeyTypes.getComputedType(StringKeyType) {
    (r: RowReader) => r.getString(0).take(3)
  }

  it("should parse values for computed KeyTypes") {
    KeyFilter.parseSingleValue(prefixKT)("warship") should equal ("war")
    KeyFilter.parseValues(prefixKT)(Seq("ab", "cdefg", "hij")) should equal (Seq("ab", "cde", "hij"))
  }
}