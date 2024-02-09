package filodb.coordinator.queryplanner

import filodb.coordinator.client.QueryCommands.FunctionalTargetSchemaProvider
import filodb.core.TargetSchemaChange
import filodb.core.query.ColumnFilter
import filodb.core.query.Filter.Equals
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.LogicalPlan
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class LogicalPlanUtilsSpec   extends AnyFunSpec with Matchers {
  it ("should correctly determine whether-or-not plan has same/unchanging target-schema columns") {
    val timeParamsSec = TimeStepParams(1000, 10, 10000)
    val timeParamsMs = TimeStepParams(
      1000 * timeParamsSec.start,
      1000 * timeParamsSec.step,
      1000 * timeParamsSec.end)
    val query = """foo{operand="lhs"} + bar{operand=~"rhs1|rhs2"}"""

    val getResult = (tschemaProviderFunc: Seq[ColumnFilter] => Seq[TargetSchemaChange]) => {
      val tschemaProvider = FunctionalTargetSchemaProvider(tschemaProviderFunc)
      val lp = Parser.queryRangeToLogicalPlan(query, timeParamsSec)
      LogicalPlanUtils.sameRawSeriesTargetSchemaColumns(lp, tschemaProvider, LogicalPlan.getRawSeriesFilters)
    }

    val unchangingSingle = (colFilters: Seq[ColumnFilter]) => {
      Seq(TargetSchemaChange(0, Seq("hello")))
    }
    getResult(unchangingSingle) shouldEqual Some(Seq("hello"))

    val unchangingMultiple = (colFilters: Seq[ColumnFilter]) => {
      Seq(TargetSchemaChange(0, Seq("hello", "goodbye")))
    }
    getResult(unchangingMultiple) shouldEqual Some(Seq("hello", "goodbye"))

    val changingSingle = (colFilters: Seq[ColumnFilter]) => {
      Seq(TargetSchemaChange(timeParamsMs.start + timeParamsMs.step, Seq("hello")))
    }
    getResult(changingSingle) shouldEqual None

    val oneChanges = (colFilters: Seq[ColumnFilter]) => {
      if (colFilters.contains(ColumnFilter("operand", Equals("rhs2")))) {
        Seq(TargetSchemaChange(timeParamsMs.start + timeParamsMs.step, Seq("hello")))
      } else {
        Seq(TargetSchemaChange(0, Seq("hello")))
      }
    }
    getResult(oneChanges) shouldEqual None

    val differentCols = (colFilters: Seq[ColumnFilter]) => {
      if (colFilters.contains(ColumnFilter("operand", Equals("rhs2")))) {
        Seq(TargetSchemaChange(0, Seq("hello")))
      } else {
        Seq(TargetSchemaChange(0, Seq("goodbye")))
      }
    }
    getResult(differentCols) shouldEqual None
  }
}
