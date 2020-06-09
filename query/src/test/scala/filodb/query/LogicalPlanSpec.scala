package filodb.query

import filodb.core.query.{ColumnFilter, RangeParams}
import filodb.core.query.Filter.{Equals, In}
import filodb.query.BinaryOperator.DIV
import filodb.query.Cardinality.OneToOne
import filodb.query.RangeFunctionId.SumOverTime
import org.scalatest.{FunSpec, Matchers}

class LogicalPlanSpec extends FunSpec with Matchers {

//  it("should get labelValueOps from logicalPlan") {
//
//    val rawSeries = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName")),
//      ColumnFilter("instance", NotEquals("Inst-0"))), Seq("_name_", "instance"), Some(300000), None)
//    val periodicSeriesWithWindowing = PeriodicSeriesWithWindowing(rawSeries, 1000, 500, 5000, 100, SumOverTime)
//
//    val res = LogicalPlan.getLabelValueOperatorsFromLogicalPlan(periodicSeriesWithWindowing)
//    res.get.size.shouldEqual(1)
//    res.get(0).labelValueOperators.size.shouldEqual(2)
//    res.get(0).labelValueOperators(0).columnName.shouldEqual("_name_")
//    res.get(0).labelValueOperators(0).value.shouldEqual(Seq("MetricName"))
//    res.get(0).labelValueOperators(0).operator.shouldEqual("=")
//    res.get(0).labelValueOperators(1).columnName.shouldEqual("instance")
//    res.get(0).labelValueOperators(1).value.shouldEqual(Seq("Inst-0"))
//    res.get(0).labelValueOperators(1).operator.shouldEqual("!=")
//  }

  it("should get labelValueOps from logicalPlan with filter In") {

    val rawSeries = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("name", Equals("MetricName")),
      ColumnFilter("instance", In(Set("Inst-1", "Inst-0")))), Seq("_name_", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing = PeriodicSeriesWithWindowing(rawSeries, 1000, 500, 5000, 100, SumOverTime)

    val res = LogicalPlan.getLabelValueOperatorsFromLogicalPlan(periodicSeriesWithWindowing)
    res.get.labelValueOperators.size shouldEqual 2
    res.get.labelValueOperators(0).columnName.shouldEqual("name")
//    res.get(0).labelValueOperators(0).value.shouldEqual(Seq("MetricName"))
//    res.get(0).labelValueOperators(0).operator.shouldEqual("=")
//    res.get(0).labelValueOperators(1).columnName.shouldEqual("instance")
//    res.get(0).labelValueOperators(1).value.shouldEqual(Seq("Inst-0", "Inst-1"))
//    res.get(0).labelValueOperators(1).operator.shouldEqual("in")
  }

//  it("should get labelValueOps from BinaryJoin LogicalPlan") {
//
//    val rawSeriesLhs = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName1")),
//      ColumnFilter("instance", EqualsRegex("Inst-0"))), Seq("_name_", "instance"), Some(300000), None)
//    val lhs = PeriodicSeries(rawSeriesLhs, 1000, 500, 50000)
//
//    val rawSeriesRhs = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("job", Equals("MetricName2")),
//      ColumnFilter("instance", NotEqualsRegex("Inst-1"))), Seq("job", "instance"), Some(300000), None)
//    val rhs = PeriodicSeries(rawSeriesRhs, 1000, 500, 50000)
//
//    val binaryJoin = BinaryJoin(lhs, DIV, OneToOne, rhs)
//
//    val res = LogicalPlan.getLabelValueOperatorsFromLogicalPlan(binaryJoin)
//    res.get.size.shouldEqual(2)
//    res.get(0).labelValueOperators.size.shouldEqual(2)
//    res.get(0).labelValueOperators(0).columnName.shouldEqual("_name_")
//    res.get(0).labelValueOperators(0).value.shouldEqual(Seq("MetricName1"))
//    res.get(0).labelValueOperators(0).operator.shouldEqual("=")
//    res.get(0).labelValueOperators(1).columnName.shouldEqual("instance")
//    res.get(0).labelValueOperators(1).value.shouldEqual(Seq("Inst-0"))
//    res.get(0).labelValueOperators(1).operator.shouldEqual("=~")
//    res.get(1).labelValueOperators.size.shouldEqual(2)
//    res.get(1).labelValueOperators(0).columnName.shouldEqual("job")
//    res.get(1).labelValueOperators(0).value.shouldEqual(Seq("MetricName2"))
//    res.get(1).labelValueOperators(0).operator.shouldEqual("=")
//    res.get(1).labelValueOperators(1).columnName.shouldEqual("instance")
//    res.get(1).labelValueOperators(1).value.shouldEqual(Seq("Inst-1"))
//    res.get(1).labelValueOperators(1).operator.shouldEqual("!~")
//  }
//
  it("should get labelValueOps fail for scalar logicalPlan") {
    val periodicSeriesWithWindowing = ScalarTimeBasedPlan(ScalarFunctionId.Year, RangeParams(1000, 500, 5000))
    val res = LogicalPlan.getLabelValueOperatorsFromLogicalPlan(periodicSeriesWithWindowing)
    res.isEmpty should be (true)
    intercept[NoSuchElementException] { res.get }
  }

  it("should get MetricName fail for scalar logicalPlan") {
    val periodicSeriesWithWindowing = ScalarTimeBasedPlan(ScalarFunctionId.Year, RangeParams(1000, 500, 5000))
    val res = LogicalPlan.getLabelValueFromLogicalPlan(periodicSeriesWithWindowing, "_name_")
    res.isEmpty should be (true)
    intercept[NoSuchElementException] { res.get }
  }

  it("should get MetricName from logicalPlan") {

    val rawSeries = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName")),
      ColumnFilter("instance", Equals("Inst-0"))), Seq("_name_", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing = PeriodicSeriesWithWindowing(rawSeries, 1000, 500, 5000, 100, SumOverTime)

    val res = LogicalPlan.getLabelValueFromLogicalPlan(periodicSeriesWithWindowing, "_name_")
    res.get.shouldEqual(Seq("MetricName"))
  }

  it("should get LabelName from logicalPlan with filter In") {

    val rawSeries = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName")),
      ColumnFilter("instance", In(Set("Inst-0", "Inst-1")))), Seq("_name_", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing = PeriodicSeriesWithWindowing(rawSeries, 1000, 500, 5000, 100, SumOverTime)

    val res = LogicalPlan.getLabelValueFromLogicalPlan(periodicSeriesWithWindowing, "instance")
    res.get.shouldEqual(Seq("Inst-0", "Inst-1"))
  }

  it("should get MetricName from BinaryJoin LogicalPlan") {

    val rawSeriesLhs = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName1")),
      ColumnFilter("instance", Equals("Inst-0"))), Seq("_name_", "instance"), Some(300000), None)
    val lhs = PeriodicSeries(rawSeriesLhs, 1000, 500, 50000)

    val rawSeriesRhs = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("job", Equals("MetricName2")),
      ColumnFilter("instance", Equals("Inst-1"))), Seq("job", "instance"), Some(300000), None)
    val rhs = PeriodicSeries(rawSeriesRhs, 1000, 500, 50000)

    val binaryJoin = BinaryJoin(lhs, DIV, OneToOne, rhs)

    val res = LogicalPlan.getLabelValueFromLogicalPlan(binaryJoin, "_name_")
    res.get.shouldEqual(Seq("MetricName1"))
  }

  it("should return None if label value is not present in logicalPlan") {

    val rawSeries = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName")),
      ColumnFilter("instance", Equals("Inst-0"))), Seq("_name_", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing = PeriodicSeriesWithWindowing(rawSeries, 1000, 500, 5000, 100, SumOverTime)

    val res = LogicalPlan.getLabelValueFromLogicalPlan(periodicSeriesWithWindowing, "_name")
    res.isEmpty shouldEqual(true)
  }

  it("should concatenate results from lhs and rhs for BinaryJoin LogicalPlan") {

    val rawSeriesLhs = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName1")),
      ColumnFilter("instance", Equals("Inst-0"))), Seq("_name_", "instance"), Some(300000), None)
    val lhs = PeriodicSeries(rawSeriesLhs, 1000, 500, 50000)

    val rawSeriesRhs = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("job", Equals("MetricName2")),
      ColumnFilter("instance", Equals("Inst-1"))), Seq("job", "instance"), Some(300000), None)
    val rhs = PeriodicSeries(rawSeriesRhs, 1000, 500, 50000)

    val binaryJoin = BinaryJoin(lhs, DIV, OneToOne, rhs)

    val res = LogicalPlan.getLabelValueFromLogicalPlan(binaryJoin, "instance")
    res.get.shouldEqual(Seq("Inst-0", "Inst-1"))
  }

}
