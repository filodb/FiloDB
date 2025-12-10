package filodb.query

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.matchers.should.Matchers
import filodb.query.exec.ExecPlan


import scala.collection.mutable

trait PlanValidationSpec extends Matchers with StrictLogging {

  private def getIndent(line: String): Int = {
    line.segmentLength(c => c == '-')
  }

  /**
   * Normalizes filter lists in plan strings to have consistent ordering.
   * This handles the Scala 2.13 Map iteration order changes that affect
   * how ColumnFilters are printed in execution plans.
   */
  private def normalizeFilterLists(planString: String): String = {
    // Pattern to match filters=List(...) content - need to handle nested parentheses
    // ColumnFilter(name,Equals(value)) has nested parens
    val filterListPattern = """filters=List\(((?:ColumnFilter\([^)]+\([^)]*\)\)[, ]*)*)\)""".r
    filterListPattern.replaceAllIn(planString, m => {
      val filtersContent = m.group(1)
      if (filtersContent == null || filtersContent.trim.isEmpty) {
        "filters=List()"
      } else {
        // Match each ColumnFilter including its nested parentheses
        val filterPattern = """ColumnFilter\([^)]+\([^)]*\)\)""".r
        val filters = filterPattern.findAllIn(filtersContent).toList.sorted
        s"filters=List(${filters.mkString(", ")})"
      }
    })
  }

  /**
   * Normalizes label selectors in PromQL query strings within plan output.
   * Handles patterns like: test{_ns_="App-1",_ws_="demo",instance="Inst-1"}
   * Sorts the labels alphabetically to ensure consistent ordering.
   */
  private def normalizeLabelSelectors(planString: String): String = {
    // Pattern to match metric name followed by label selectors in curly braces
    // e.g., test{label1="val1",label2="val2"} or just {label1="val1"}
    val labelSelectorPattern = """(\w*)\{([^}]+)\}""".r
    labelSelectorPattern.replaceAllIn(planString, m => {
      val metricName = m.group(1)
      val labelsContent = m.group(2)
      if (labelsContent == null || labelsContent.trim.isEmpty) {
        s"$metricName{}"
      } else {
        // Split by comma, but be careful with values that might contain commas (though unlikely here)
        // Pattern for individual label: name="value" or name=~"value" or name!="value" etc.
        val labelPattern = """([a-zA-Z_][a-zA-Z0-9_]*)(=~|!=|!~|=)("[^"]*")""".r
        val labels = labelPattern.findAllIn(labelsContent).toList.sorted
        s"$metricName{${labels.mkString(",")}}"
      }
    })
  }

  private def removeNoise(planString: String): String = {
    val denoised = planString
      .replaceAll("testProbe-.*]", "testActor]")
      .replaceAll("InProcessPlanDispatcher.*\\)", "InProcessPlanDispatcher")
    normalizeLabelSelectors(normalizeFilterLists(denoised))
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
    helper(planString.split('\n').toIndexedSeq, 0)._2
  }

  /**
   * Given an ExecPlan validate the provided plan string matches the required plan
   *
   * @param plan LogicalPlan instance
   * @param expected expected plan as String
   */
  def validatePlan(plan: ExecPlan,
                   expected: String,
                   sort: Boolean = false): Unit = {
    val originalPlanString = plan.printTree()
    val (planString, expectedString) = {
      val denoisedPlan = removeNoise(originalPlanString)
      val denoisedExpected = removeNoise(expected)
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
