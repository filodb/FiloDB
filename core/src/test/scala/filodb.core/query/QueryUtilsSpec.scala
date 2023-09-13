package filodb.core.query

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class QueryUtilsSpec extends AnyFunSpec with Matchers {
  it("should correctly identify the set of unescaped special regex chars") {
    val tests = Seq(
      ("a.b?c+d*e|f{g}h[i]j(k)?l\"m\\", Set('.', '?', '+', '*', '|', '{', '}', '[', ']', '(', ')', '"', '\\')),
      ("\\a.b?c+d*e|\\f{g}h[i]j(k)?l\"\\m\\", Set('.', '?', '+', '*', '|', '{', '}', '[', ']', '(', ')', '"', '\\')),
      ("a\\.b\\?c\\+d\\*e\\|f\\{g\\}h\\[i\\]j\\(k\\)\\?l\\\"m\\\\", Set('\\')),
      ("foo|.*", Set('.', '*', '|')),
      ("foo\\|.*", Set('.', '*', '\\')),
      ("foo\\\\|.*", Set('.', '*', '|')),
      ("foo\\\\\\|.*", Set('.', '*', '\\'))
    )
    for ((string, expected) <- tests) {
      val res = QueryUtils.getUnescapedSpecialRegexChars(string)
      res shouldEqual expected
    }
  }

  it("should correctly split strings at unescaped pipes") {
    val tests = Seq(
      ("this|is|a|test", Seq("this", "is", "a", "test")),
      ("this|is|a||test", Seq("this", "is", "a", "", "test")),
      ("this\\|is|a|test", Seq("this\\|is", "a", "test")),
      ("this\\\\|is|a|test", Seq("this\\\\", "is", "a", "test")),
      ("||this\\|is|\\+a|test||", Seq("", "", "this\\|is", "\\+a", "test", "", "")),
    )
    for ((string, expected) <- tests) {
      val res = QueryUtils.splitAtUnescapedPipes(string)
      res shouldEqual expected
    }
  }
}
