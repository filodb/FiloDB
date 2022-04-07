package filodb.query

import org.scalatest.matchers.should.Matchers

import filodb.query.exec.ExecPlan

trait PlanValidationSpec extends Matchers {

  /**
   * Given an ExecPlan validate the provided plan string matches the required plan
   *
   * @param plan LogicalPlan instance
   * @param expected expected plan as String
   */
  def validatePlan(plan: ExecPlan, expected: String): Unit = {
    val planString = plan.printTree()
      .replaceAll("testProbe-.*]", "testActor]")
      .replaceAll("InProcessPlanDispatcher.*\\)", "InProcessPlanDispatcher")
    val expectedString = expected.replaceAll("testProbe-.*]", "testActor]")
      .replaceAll("InProcessPlanDispatcher.*\\)", "InProcessPlanDispatcher")
    planString shouldEqual expectedString
  }

}
