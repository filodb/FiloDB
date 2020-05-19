package filodb.coordinator.queryplanner

import filodb.core.query.ColumnFilter
import filodb.core.query.Filter.{Equals, In}
import filodb.query.BinaryOperator.DIV
import filodb.query.Cardinality.OneToOne
import filodb.query.RangeFunctionId.SumOverTime
import filodb.query._
import org.scalatest.{FunSpec, Matchers}

class LogicalPlanUtilsSpec extends FunSpec with Matchers {

  it("should get labelValueOps from logicalPlan") {

    val rawSeries = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName")),
      ColumnFilter("instance", Equals("Inst-0"))), Seq("_name_", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing = PeriodicSeriesWithWindowing(rawSeries, 1000, 500, 5000, 100, SumOverTime)

    val res = LogicalPlanUtils.getLabelValueOperatorsFromLogicalPlan(periodicSeriesWithWindowing)
    println(res)
//    Some(List(LabelValueOperatorGroup(List(LabelValueOperator(_name_,Set(MetricName),<function1>), LabelValueOperator(instance,Set(Inst-0),<function1>)))))
//    res.get.shouldEqual(Seq("MetricName"))
  }

  it("should get labelValueOps from logicalPlan with filter In") {

    val rawSeries = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName")),
      ColumnFilter("instance", In(Set("Inst-0", "Inst-1")))), Seq("_name_", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing = PeriodicSeriesWithWindowing(rawSeries, 1000, 500, 5000, 100, SumOverTime)

    val res = LogicalPlanUtils.getLabelValueOperatorsFromLogicalPlan(periodicSeriesWithWindowing)
    //Some(List(LabelValueOperatorGroup(List(LabelValueOperator(_name_,Set(MetricName),<function1>), LabelValueOperator(instance,Set(Inst-0, Inst-1),<function1>)))))
    println(res)
//    res.get.shouldEqual(Seq("Inst-0", "Inst-1"))
  }

  it("should get labelValueOps from BinaryJoin LogicalPlan") {

    val rawSeriesLhs = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName1")),
      ColumnFilter("instance", Equals("Inst-0"))), Seq("_name_", "instance"), Some(300000), None)
    val lhs = PeriodicSeries(rawSeriesLhs, 1000, 500, 50000)

    val rawSeriesRhs = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("job", Equals("MetricName2")),
      ColumnFilter("instance", Equals("Inst-1"))), Seq("job", "instance"), Some(300000), None)
    val rhs = PeriodicSeries(rawSeriesRhs, 1000, 500, 50000)

    val binaryJoin = BinaryJoin(lhs, DIV, OneToOne, rhs)

    val res = LogicalPlanUtils.getLabelValueOperatorsFromLogicalPlan(binaryJoin)
//    Some(List(LabelValueOperatorGroup(List(LabelValueOperator(_name_,Set(MetricName1),<function1>), LabelValueOperator(instance,Set(Inst-0),<function1>))),
    //    LabelValueOperatorGroup(List(LabelValueOperator(job,Set(MetricName2),<function1>), LabelValueOperator(instance,Set(Inst-1),<function1>)))))
    println(res)
    //    res.get.shouldEqual(Seq("MetricName1"))
  }

  it("should get MetricName from logicalPlan") {

    val rawSeries = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName")),
      ColumnFilter("instance", Equals("Inst-0"))), Seq("_name_", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing = PeriodicSeriesWithWindowing(rawSeries, 1000, 500, 5000, 100, SumOverTime)

    val res = LogicalPlanUtils.getLabelValueFromLogicalPlan(periodicSeriesWithWindowing, "_name_")
    res.get.shouldEqual(Seq("MetricName"))
  }

  it("should get LabelName from logicalPlan with filter In") {

    val rawSeries = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName")),
      ColumnFilter("instance", In(Set("Inst-0", "Inst-1")))), Seq("_name_", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing = PeriodicSeriesWithWindowing(rawSeries, 1000, 500, 5000, 100, SumOverTime)

    val res = LogicalPlanUtils.getLabelValueFromLogicalPlan(periodicSeriesWithWindowing, "instance")
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

    val res = LogicalPlanUtils.getLabelValueFromLogicalPlan(binaryJoin, "_name_")
    res.get.shouldEqual(Seq("MetricName1"))
  }

  it("should return None if label value is not present in logicalPlan") {

    val rawSeries = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName")),
      ColumnFilter("instance", Equals("Inst-0"))), Seq("_name_", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing = PeriodicSeriesWithWindowing(rawSeries, 1000, 500, 5000, 100, SumOverTime)

    val res = LogicalPlanUtils.getLabelValueFromLogicalPlan(periodicSeriesWithWindowing, "_name")
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

    val res = LogicalPlanUtils.getLabelValueFromLogicalPlan(binaryJoin, "instance")
    res.get.shouldEqual(Seq("Inst-0", "Inst-1"))
  }

}
