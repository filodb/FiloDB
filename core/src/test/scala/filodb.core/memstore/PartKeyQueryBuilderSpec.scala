package filodb.core.memstore

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class PartKeyQueryBuilderSpec extends AnyFunSpec with Matchers {
  it("should match the regex after anchors stripped") {
    for ((regex, regexNoAnchors) <- Map(
      """^.*$""" -> """.*""", // both anchor are stripped.
      """\$""" -> """\$""", // \$ is not removed.
      """\\\$""" -> """\\\$""", // \$ is not removed.
      """\\$""" -> """\\""", // $ is removed.
      """$""" -> """""", // $ is removed.
      """\^.*$""" -> """\^.*""", // do not remove \^.
      """^ ^.*$""" -> """ ^.*""", // only remove the first ^.
      """^.*\$""" -> """.*\$""",  // do not remove \$
      """^ $foo""" -> """ $foo""",  // the $ is not at the end, keep it.
      """.* $ \ $$""" -> """.* $ \ $""",  // only remove the last $
      """foo.*\\\ $""" -> """foo.*\\\ """, // remove $ for it at the end and not escaped.
      """foo.*\\\$""" -> """foo.*\\\$""", // keep \$.
      """foo.*\\$""" -> """foo.*\\""",  // remove $ for it at the end and not escaped.
      """foo.*$\\\\$""" -> """foo.*$\\\\""",  // keep the first $ since it not at the end.
    )) {
      PartKeyQueryBuilder.removeRegexAnchors(regex) shouldEqual regexNoAnchors
    }
  }

  // Helper to run removeRegexAnchors then validateNoUnescapedMiddleDollar, expecting rejection
  private def expectRejection(originalRegex: String): Unit = {
    val stripped = PartKeyQueryBuilder.removeRegexAnchors(originalRegex)
    val ex = intercept[IllegalArgumentException] {
      PartKeyQueryBuilder.validateNoUnescapedMiddleDollar(stripped, originalRegex)
    }
    ex.getMessage should include("unescaped")
    ex.getMessage should include(originalRegex)
  }

  // Helper to run removeRegexAnchors then validateNoUnescapedMiddleDollar, expecting acceptance
  private def expectAcceptance(originalRegex: String): Unit = {
    val stripped = PartKeyQueryBuilder.removeRegexAnchors(originalRegex)
    // Should not throw
    PartKeyQueryBuilder.validateNoUnescapedMiddleDollar(stripped, originalRegex)
  }

  // ==================== Rejection cases ====================

  it("should reject the original error case: unresolved Grafana template variable .*$.*") {
    expectRejection(""".*$.*""")
  }

  it("should reject $ anchor at the start of the pattern") {
    expectRejection("""$.*""")     // $ at very start, followed by .*
    expectRejection("""$foo""")    // $ at very start, followed by literal
    expectRejection("""$.+""")     // $ at very start, followed by .+
  }

  it("should reject $ anchor in the middle between literals") {
    expectRejection("""foo$bar""")
    expectRejection("""a$b""")
  }

  it("should reject $ anchor between wildcard patterns") {
    expectRejection(""".*$.+""")
    expectRejection(""".+$.*""")
    expectRejection(""".+$.+""")
  }

  it("should reject $ anchor preceded by even number of backslashes (unescaped)") {
    // 2 backslashes before $ -> \\\\ is literal \\, so $ is unescaped
    expectRejection("""foo\\$bar""")
    // 4 backslashes before $ -> \\\\\\\\ is literal \\\\, so $ is unescaped
    expectRejection("""foo\\\\$bar""")
  }

  it("should reject when leading ^ is stripped but $ remains in middle") {
    expectRejection("""^$.*""")    // ^ stripped -> $.*
    expectRejection("""^foo$bar""") // ^ stripped -> foo$bar
    expectRejection("""^.*$.*""")  // ^ stripped -> .*$.* (trailing $ not at end)
  }

  it("should reject when trailing $ is stripped but another unescaped $ remains") {
    expectRejection("""$foo$""")   // trailing $ stripped -> $foo
    expectRejection("""$$""")      // trailing $ stripped -> $
    expectRejection("""a$b$""")    // trailing $ stripped -> a$b
  }

  it("should reject multiple unescaped $ in the pattern") {
    expectRejection("""a$b$c""")       // two unescaped $ signs
    expectRejection("""$foo$bar$""")   // trailing $ stripped, two remain
    expectRejection(""".*$.*$.*""")    // trailing not stripped (ends with *), two $ remain
  }

  it("should reject mixed escaped and unescaped $ where at least one is unescaped") {
    // \$ (escaped) then $ (unescaped)
    expectRejection("""a\$b$c""")
    // $ (unescaped) then \$ (escaped)
    expectRejection("""a$b\$c""")
    // \$ (escaped), \\$ (unescaped: even backslashes)
    expectRejection("""a\$\\$b""")
  }

  it("should reject $ anchor adjacent to regex quantifiers") {
    expectRejection("""a+$b+""")
    expectRejection("""a?$b*""")
    expectRejection("""a{2}$b{3}""")
  }

  it("should reject $ anchor inside grouping constructs") {
    expectRejection("""(foo$bar)""")
    expectRejection("""(a|b)$c""")
    expectRejection("""[abc]$def""")
  }

  it("should reject $ anchor with trailing $ stripped leaving middle $ exposed") {
    // .*$\\\\$ -> trailing $ stripped (unescaped, preceded by even \\) -> .*$\\\\
    // The first $ at index 2 has 0 preceding backslashes -> unescaped
    expectRejection(""".*$\\\\$""")
  }

  it("should reject $ as the only character after anchor stripping") {
    // ^$$ -> strip ^ -> $$ -> strip trailing $ -> $ -> single unescaped $
    // Wait: removeRegexTailDollarSign on "$$" -> last char $, Pattern matches -> strip -> "$"
    // Then validate "$" -> unescaped $ at index 0
    expectRejection("""^$$""")
  }

  // ==================== Acceptance cases ====================

  it("should accept empty regex") {
    expectAcceptance("")
  }

  it("should accept regex with no $ at all") {
    expectAcceptance(""".*""")
    expectAcceptance("""foo.*""")
    expectAcceptance("""foo""")
    expectAcceptance(""".+""")
    expectAcceptance("""[a-z]+""")
    expectAcceptance("""(a|b|c)""")
    expectAcceptance("""foo.*bar""")
  }

  it("should accept when leading ^ and trailing $ are both stripped cleanly") {
    expectAcceptance("""^.*$""")       // -> .*
    expectAcceptance("""^foo$""")      // -> foo
    expectAcceptance("""^.+$""")       // -> .+
    expectAcceptance("""^[a-z]+$""")   // -> [a-z]+
    expectAcceptance("""^(a|b)$""")    // -> (a|b)
  }

  it("should accept trailing $ stripped leaving no $ in result") {
    expectAcceptance(""".*$""")        // -> .*
    expectAcceptance("""foo$""")       // -> foo
    expectAcceptance("""$""")          // -> (empty)
    expectAcceptance(""".+$""")        // -> .+
  }

  it("should accept leading ^ stripped leaving no $ in result") {
    expectAcceptance("""^.*""")        // -> .*
    expectAcceptance("""^foo""")       // -> foo
    expectAcceptance("""^.+""")        // -> .+
  }

  it("should accept escaped \\$ (literal dollar sign) with 1 preceding backslash") {
    expectAcceptance("""\$""")         // escaped $, trailing $ NOT stripped (odd backslashes)
    expectAcceptance("""foo\$bar""")   // literal $ in the middle
    expectAcceptance("""foo\$""")      // literal $ at the end
    expectAcceptance("""\$foo""")      // literal $ at the start
    expectAcceptance("""\$\$""")       // two escaped $ signs
  }

  it("should accept escaped \\$ with 3 preceding backslashes (odd count)") {
    // \\\$ = \\(literal backslash) + \$(literal dollar) -> both escaped
    expectAcceptance("""\\\$""")
    expectAcceptance("""foo\\\$bar""")
    expectAcceptance("""\\\$\\\$""")   // two instances of \\\$
  }

  it("should accept trailing unescaped $ preceded by even backslashes when stripped") {
    // \\$ -> trailing $ stripped (2 backslashes = even, so $ is unescaped anchor) -> \\
    expectAcceptance("""\\$""")
    // \\\\$ -> trailing $ stripped (4 backslashes = even) -> \\\\
    expectAcceptance("""\\\\$""")
    // foo\\$ -> trailing $ stripped -> foo\\
    expectAcceptance("""foo\\$""")
  }

  it("should accept all escaped $ with mixed backslash counts (all odd before $)") {
    // multiple \$ throughout, all with odd backslash counts
    expectAcceptance("""a\$b\$c""")    // two escaped $, both with 1 backslash (odd)
    expectAcceptance("""\$.\$""")      // escaped $ around a dot
    expectAcceptance(""".*\$.*""")     // escaped $ between wildcards
  }

  it("should accept regex with only backslashes and no $") {
    expectAcceptance("""\\""")         // single literal backslash
    expectAcceptance("""\\\\""")       // two literal backslashes
    expectAcceptance("""foo\\bar""")   // literal backslash in middle
  }

  it("should accept complex valid patterns with escaped $ in various positions") {
    expectAcceptance("""^price_\$_usd$""")           // ^stripped, $stripped -> price_\$_usd
    expectAcceptance("""cost\$[0-9]+""")              // escaped $ followed by character class
    expectAcceptance("""(item\$price|item\$cost)""")  // escaped $ inside alternation
    expectAcceptance("""\$\$\$""")                    // three escaped $ signs
  }

  it("should accept regex where $ appears only as escaped within character classes") {
    expectAcceptance("""[\$]""")       // escaped $ inside character class
    expectAcceptance("[\\$" + "a-z]+")   // escaped $ in char class with range
  }
}
