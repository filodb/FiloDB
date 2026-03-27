package filodb.query

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.matchers.should.Matchers
import filodb.query.exec.ExecPlan


import scala.collection.mutable

trait PlanValidationSpec extends Matchers with StrictLogging {

  private def getIndent(line: String): Int = {
    line.prefixLength(c => c == '-')
  }

  private def removeNoise(planString: String): String = {
    planString
      .replaceAll("testProbe-.*]", "testActor]")
      .replaceAll("InProcessPlanDispatcher.*\\)", "InProcessPlanDispatcher")
  }

  /**
   * Normalizes filter ordering in plan strings to make comparisons
   * insensitive to HashMap iteration order differences across Scala versions.
   *
   * Handles two patterns:
   * 1. ColumnFilter lists: filters=List(ColumnFilter(b,...), ColumnFilter(a,...))
   *    -> filters=List(ColumnFilter(a,...), ColumnFilter(b,...))
   * 2. PromQL label matchers: metric{_ws_="demo",_ns_="App"}
   *    -> metric{_ns_="App",_ws_="demo"}
   */
  protected def normalizeFilterOrder(s: String): String = {
    val step1 = normalizeColumnFilterLists(s)
    normalizePromQLBraces(step1)
  }

  /**
   * Finds all `filters=List(...)` sections (handling nested parens) and sorts
   * the ColumnFilter entries alphabetically within each.
   */
  private def normalizeColumnFilterLists(s: String): String = {
    val marker = "filters=List("
    val result = new StringBuilder
    var pos = 0
    while (pos < s.length) {
      val idx = s.indexOf(marker, pos)
      if (idx < 0) {
        result.append(s.substring(pos))
        pos = s.length
      } else {
        result.append(s.substring(pos, idx))
        result.append(marker)
        val contentStart = idx + marker.length
        // Find matching closing paren using depth counting
        var depth = 1
        var i = contentStart
        while (i < s.length && depth > 0) {
          if (s.charAt(i) == '(') depth += 1
          else if (s.charAt(i) == ')') depth -= 1
          if (depth > 0) i += 1
        }
        // i now points to the matching ')'
        val inner = s.substring(contentStart, i)
        val sorted = splitTopLevel(inner, ',').map(_.trim).sorted.mkString(", ")
        result.append(sorted)
        result.append(')')
        pos = i + 1
      }
    }
    result.toString
  }

  /**
   * Sorts label matchers within PromQL curly braces: metric{b="2",a="1"} -> metric{a="1",b="2"}
   */
  private def normalizePromQLBraces(s: String): String = {
    val result = new StringBuilder
    var pos = 0
    while (pos < s.length) {
      val idx = s.indexOf('{', pos)
      if (idx < 0) {
        result.append(s.substring(pos))
        pos = s.length
      } else {
        result.append(s.substring(pos, idx + 1))
        val contentStart = idx + 1
        val closeIdx = findMatchingBrace(s, contentStart)
        if (closeIdx < 0) {
          result.append(s.substring(contentStart))
          pos = s.length
        } else {
          val inner = s.substring(contentStart, closeIdx)
          // Only sort if it looks like label matchers (contains = sign)
          if (inner.contains("=") && !inner.contains("ColumnFilter")) {
            val matchers = splitPromQLMatchers(inner)
            result.append(matchers.sorted.mkString(","))
          } else {
            result.append(inner)
          }
          result.append('}')
          pos = closeIdx + 1
        }
      }
    }
    result.toString
  }

  private def findMatchingBrace(s: String, startAfterOpen: Int): Int = {
    var depth = 1
    var i = startAfterOpen
    var inQuote = false
    var escaped = false
    while (i < s.length && depth > 0) {
      val c = s.charAt(i)
      if (escaped) {
        escaped = false
      } else if (c == '\\') {
        escaped = true
      } else if (c == '"') {
        inQuote = !inQuote
      } else if (!inQuote) {
        if (c == '{') depth += 1
        else if (c == '}') depth -= 1
      }
      if (depth > 0) i += 1
    }
    if (depth == 0) i else -1
  }

  /**
   * Splits a string at top-level occurrences of the delimiter,
   * respecting nested parentheses.
   */
  private def splitTopLevel(s: String, delim: Char): Seq[String] = {
    val result = new mutable.ArrayBuffer[String]()
    var depth = 0
    var start = 0
    var i = 0
    while (i < s.length) {
      s.charAt(i) match {
        case '(' => depth += 1; i += 1
        case ')' => depth -= 1; i += 1
        case c if c == delim && depth == 0 =>
          val token = s.substring(start, i).trim
          if (token.nonEmpty) result.append(token)
          i += 1
          start = i
        case _ => i += 1
      }
    }
    val last = s.substring(start).trim
    if (last.nonEmpty) result.append(last)
    result.toSeq
  }

  /**
   * Splits PromQL label matchers like '_ws_="demo",_ns_="App"'
   * into Seq('_ws_="demo"', '_ns_="App"')
   * handling quoted strings that may contain commas.
   */
  private def splitPromQLMatchers(s: String): Seq[String] = {
    val result = new mutable.ArrayBuffer[String]()
    var inQuote = false
    var escaped = false
    var start = 0
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      if (escaped) {
        escaped = false
      } else if (c == '\\') {
        escaped = true
      } else if (c == '"') {
        inQuote = !inQuote
      } else if (c == ',' && !inQuote) {
        result.append(s.substring(start, i))
        start = i + 1
      }
      i += 1
    }
    result.append(s.substring(start))
    result.toSeq
  }

  /**
   * Prints the same tree for all logically-identical plans.
   */
  private def sortTree(planString: String): String = {
    // Returns the index to resume reading from "lines" and a sorted string tree.
    def helper(lines: Seq[String], index: Int): (Int, String) = {
      // Recursively build a list of child strings, then sort and concatenate.
      val currIndent = getIndent(lines(index))
      var i = index + 1
      val children = new mutable.ArrayBuffer[String]()
      // iterate while lines remain and the next line's indent is greater than the current line's
      while (i < lines.size && getIndent(lines(i)) > currIndent) {
        val (nextIndex, child) = helper(lines, i)
        children.append(child)
        i = nextIndex
      }
      val result = if (children.nonEmpty) {
        s"${lines(index)}\n${children.sorted.mkString("\n")}"
      } else {
        lines(index)
      }
      (i, result)
    }
    helper(planString.split('\n'), 0)._2
  }

  /**
   * Given an ExecPlan validate the provided plan string matches the required plan
   *
   * @param plan LogicalPlan instance
   * @param expected expected plan as String
   */
  def validatePlan(plan: ExecPlan,
                   expected: String,
                   sort: Boolean = true): Unit = {
    val originalPlanString = plan.printTree()
    val (planString, expectedString) = {
      val denoisedPlan = normalizeFilterOrder(removeNoise(originalPlanString))
      val denoisedExpected = normalizeFilterOrder(removeNoise(expected))
      if (sort) {
        (sortTree(denoisedPlan), sortTree(denoisedExpected))
      } else {
        (denoisedPlan, denoisedExpected)
      }
    }
    if (planString != expectedString) {
      logger.error(
        s"""======== PLAN VALIDATION FAILED ========
           |~~~~~~~~~~~~~~~ EXPECTED ~~~~~~~~~~~~~~~
           |$expected
           |~~~~~~~~~~~~~~~~ ACTUAL ~~~~~~~~~~~~~~~~
           |$originalPlanString
           |""".stripMargin
      )
    }
    planString shouldEqual expectedString
  }

}
