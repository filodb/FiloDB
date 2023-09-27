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
                   sort: Boolean = false): Unit = {
    val (planString, expectedString) = {
      val denoisedPlan = removeNoise(plan.printTree())
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
           |$expectedString
           |~~~~~~~~~~~~~~~~ ACTUAL ~~~~~~~~~~~~~~~~
           |$planString
           |""".stripMargin
      )
    }
    planString shouldEqual expectedString
  }

}
