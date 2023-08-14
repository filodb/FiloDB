package filodb.coordinator.queryplanner


import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.coordinator.queryplanner.PlannerUtil._
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.{LogicalPlan, RawSeries}


class PlannerUtilSpec  extends AnyFunSpec with Matchers {

  val (start, end) =  (1691965619L, 1691965619L)


  it ("should rewrite the plan to make RawSeries support remote calls") {

    val q1 = "foo{}[10m]"
    val lp1 = rewritePlanWithRemoteRawExport(Parser.queryRangeToLogicalPlan(q1, TimeStepParams(start, 0, end)))
    assert(LogicalPlan.findLeafLogicalPlans(lp1) forall{
      case rs: RawSeries  => rs.supportsRemoteDataCall
      case _              => false
    })

    val q2 = "foo{} + bar{}"
    val lp2 = rewritePlanWithRemoteRawExport(Parser.queryRangeToLogicalPlan(q2, TimeStepParams(start, 0, end)))
    assert(LogicalPlan.findLeafLogicalPlans(lp2) forall {
      case rs: RawSeries => rs.supportsRemoteDataCall
      case _ => false
    })

    val q3 = "sum(rate(foo{}[5m])) + sum(rate(bar{}[5m]))"
    val lp3 = rewritePlanWithRemoteRawExport(Parser.queryRangeToLogicalPlan(q3, TimeStepParams(start, 0, end)))
    assert(LogicalPlan.findLeafLogicalPlans(lp3) forall {
      case rs: RawSeries => rs.supportsRemoteDataCall
      case _ => false
    })

    val q4 = "sum_over_time(foo{}[10m])"
    val lp4 = rewritePlanWithRemoteRawExport(Parser.queryRangeToLogicalPlan(q4, TimeStepParams(start, 0, end)))
    assert(LogicalPlan.findLeafLogicalPlans(lp4) forall {
      case rs: RawSeries => rs.supportsRemoteDataCall
      case _ => false
    })


    val q5 = "sum(rate(foo{}[5m]))[10m:1m]"
    val lp5 = rewritePlanWithRemoteRawExport(Parser.queryRangeToLogicalPlan(q5, TimeStepParams(start, 0, end)))
    assert(LogicalPlan.findLeafLogicalPlans(lp5) forall {
      case rs: RawSeries => rs.supportsRemoteDataCall
      case _ => false
    })



  }
}
