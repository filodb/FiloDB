package filodb.core.query

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class QueryUtilsSpec extends AnyFunSpec with Matchers{
  it("should correctly identify regex chars") {
    val testsNoRegex = Seq(
      "",
      ",",
      "no regex-1234!@#%&_,/`~=<>';:"
    )
    // includes one test for each single regex char
    val testsRegex = QueryUtils.REGEX_CHARS.map(c => c.toString) ++ Seq(
      "\\",
      "\\\\",
      "foo\\.bar",  // escape chars don't affect the result (at least for now).
      "foo\\\\.bar",
      "foo|bar",
      "foo\\|bar",
      ".foo\\|bar"
    )
    for (test <- testsNoRegex) {
      QueryUtils.containsRegexChars(test) shouldEqual false
    }
    for (test <- testsRegex) {
      QueryUtils.containsRegexChars(test) shouldEqual true
    }
  }

  it ("should correctly identify non-pipe regex chars") {
    val testsPipeOnly = Seq(
      "",
      "|",
      "||||",
      "a|b|c|d|e",
      "foobar-1|2|34!@|#%&_,|/`~=<>|';:",
      // NOTE: some regex chars are missing from QueryUtils.REGEX_CHARS.
      // This is intentional to preserve existing behavior.
      "^foo|bar$"
    )
    // includes one test for each single non-pipe regex char
    val testsNonPipeOnly = QueryUtils.REGEX_CHARS.filter(c => c != '|').map(c => c.toString) ++ Seq(
      "\\",
      "\\\\",
      "foo\\|bar",  // escape chars don't affect the result (at least for now).
      "foo\\\\|bar",
      "foo.bar.baz|",
      "!@#$%^&*()_+{}[];':\""
    )
    for (test <- testsPipeOnly) {
      QueryUtils.containsPipeOnlyRegex(test) shouldEqual true
    }
    for (test <- testsNonPipeOnly) {
      QueryUtils.containsPipeOnlyRegex(test) shouldEqual false
    }
  }
}
