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
}
