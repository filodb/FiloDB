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

  it ("should correctly determine whether-or-not a plan preserves labels") {
    val queryTimestep = 100
    val queryStep = 10
    val labelsToPreserve = Seq("pLabel1", "pLabel2")
    val preservedQueries = Seq(
      """foo{labelA="hello"}""",
      """sum(foo{labelA="hello"}) without (labelB)""",
      """sum(foo{labelA="hello"}) by (pLabel1, pLabel2)""",
      """sum(foo{labelA="hello"}) by (pLabel1, pLabel2) + foo{labelA="hello"}""",
      """sum(foo{labelA="hello"}) by (pLabel1, pLabel2) + sum(foo{labelA="hello"}) by (pLabel1, pLabel2)""",
      """0.5 * sum(foo{labelA="hello"}) by (pLabel1, pLabel2)"""
    )
    val unpreservedQueries = Seq(
      """sum(foo{labelA="hello"}) without (pLabel1)""",
      """sum(foo{labelA="hello"}) by (pLabel1)""",
      """sum(foo{labelA="hello"}) by (pLabel1) + foo{labelA="hello"}""",
      """sum(foo{labelA="hello"}) by (pLabel1, pLabel2) + sum(foo{labelA="hello"}) by (pLabel1)""",
      """0.5 * sum(foo{labelA="hello"}) by (pLabel1)""",
      """sum(foo{labelA="hello"}) by (pLabel1, pLabel2) + scalar(foo{labelA="hello"})"""
    )
    for (query <- preservedQueries) {
      val plan = Parser.queryToLogicalPlan(query, queryTimestep, queryStep)
      LogicalPlanUtils.treePreservesLabels(plan, labelsToPreserve) shouldEqual true
    }
    for (query <- unpreservedQueries) {
      val plan = Parser.queryToLogicalPlan(query, queryTimestep, queryStep)
      LogicalPlanUtils.treePreservesLabels(plan, labelsToPreserve) shouldEqual false
    }
  }

  it("should correctly resolve pipe-concatenated shard-key filters") {
    val shardKeyLabels = Seq("label1", "label2")
    // Each sequence describes the expected values of label1 and label2 filters, respectively.
    val queryExpectedPairs = Seq(
      ("""foo{label1=~"A|B", label2=~"C|D"}""", Set(
        Seq("A", "C"), Seq("A", "D"), Seq("B", "C"), Seq("B", "D"))),
      ("""foo{label1=~"A", label2=~"C|D"}""", Set(
        Seq("A", "C"), Seq("A", "D"))),
      ("""foo{label1="A", label2=~"C|D"}""", Set(
        Seq("A", "C"), Seq("A", "D"))),
      ("""foo{label1="A", label2=~"C|D", label3="shouldIgnore"}""", Set(
        Seq("A", "C"), Seq("A", "D"))),
      ("""sum(foo{label1=~"A|B", label2=~"C|D"})""", Set(
        Seq("A", "C"), Seq("A", "D"), Seq("B", "C"), Seq("B", "D"))),
      ("""sum(foo{label1=~"A|B", label2=~"C|D"}) + sum(foo{label1=~"A|B", label2=~"C|D"})""", Set(
        Seq("A", "C"), Seq("A", "D"), Seq("B", "C"), Seq("B", "D")))
    )
    val shouldErrorQueries = Seq(
      """foo{label1=~"A|B", label2=~"C|D."}""",  // non-pipe regex chars
      """foo{label1=~"A|B", label2!~"C|D"}"""    // non-equals[regex] filter
    )
    for ((query, expected) <- queryExpectedPairs) {
      val plan = Parser.queryToLogicalPlan(query, 100, 10)
      val res = LogicalPlanUtils.resolvePipeConcatenatedShardKeyFilters(plan, shardKeyLabels)
      // make sure all filters are Equals
      res.foreach(_.foreach(_.filter.isInstanceOf[Equals] shouldEqual true))
      // make sure all values are as expected
      res.map{ group =>
        group.sortBy(_.column).map(_.filter.valuesStrings.head)
      }.toSet shouldEqual expected
    }

    for (query <- shouldErrorQueries) {
      val plan = Parser.queryToLogicalPlan(query, 100, 10)
      intercept[IllegalArgumentException] {
        LogicalPlanUtils.resolvePipeConcatenatedShardKeyFilters(plan, shardKeyLabels)
      }
    }
  }
}
