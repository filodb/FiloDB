package filodb.query

import filodb.core.query.{ColumnFilter, RangeParams}
import filodb.core.query.Filter.{Equals, EqualsRegex, In, NotEquals, NotEqualsRegex}
import filodb.query.BinaryOperator.DIV
import filodb.query.Cardinality.OneToOne
import filodb.query.RangeFunctionId.SumOverTime
import org.scalatest.{FunSpec, Matchers}

class LogicalPlanSpec extends FunSpec with Matchers {

  it("should get columnFilterGroup from logicalPlan") {

    val rawSeries = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName")),
      ColumnFilter("instance", NotEquals("Inst-0"))), Seq("_name_", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing = PeriodicSeriesWithWindowing(rawSeries, 1000, 500, 5000, 100, SumOverTime)

    val res = LogicalPlan.getColumnFilterGroup(periodicSeriesWithWindowing)
    res.size.shouldEqual(1)
    res(0).size.shouldEqual(2)
    for (cfSet <- res(0)) {
      if (cfSet.column == "_name_") {
        cfSet.column.shouldEqual("_name_")
        cfSet.filter.operatorString.shouldEqual("=")
        cfSet.filter.valuesStrings shouldEqual(Set("MetricName"))
      } else if (cfSet.column == "instance") {
        cfSet.column.shouldEqual("instance")
        cfSet.filter.operatorString.shouldEqual("!=")
        cfSet.filter.valuesStrings shouldEqual(Set("Inst-0"))
      } else {
        fail("invalid entry in column filter sequence " + cfSet)
      }
    }
  }

  it("should get columnFilterGroup from logicalPlan with filter In") {

    val rawSeries = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName")),
      ColumnFilter("instance", In(Set("Inst-1", "Inst-0")))), Seq("_name_", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing = PeriodicSeriesWithWindowing(rawSeries, 1000, 500, 5000, 100, SumOverTime)

    val res = LogicalPlan.getColumnFilterGroup(periodicSeriesWithWindowing)
    res.size.shouldEqual(1)
    for (cfSet <- res(0)) {
      if (cfSet.column.equals("_name_")) {
        cfSet.column.shouldEqual("_name_")
        cfSet.filter.operatorString.shouldEqual("=")
        cfSet.filter.valuesStrings shouldEqual(Set("MetricName"))
      } else if (cfSet.column.equals("instance")) {
        cfSet.column.shouldEqual("instance")
        cfSet.filter.operatorString.shouldEqual("in")
        cfSet.filter.valuesStrings shouldEqual(Set("Inst-0", "Inst-1"))
      } else {
        fail("invalid entry in column filter sequence " + cfSet)
      }
    }
  }

  it("should get columnFilterGroup from BinaryJoin LogicalPlan") {

    val rawSeriesLhs = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName1")),
      ColumnFilter("instance", EqualsRegex("Inst-0"))), Seq("_name_", "instance"), Some(300000), None)
    val lhs = PeriodicSeries(rawSeriesLhs, 1000, 500, 50000)

    val rawSeriesRhs = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("job", Equals("MetricName2")),
      ColumnFilter("instance", NotEqualsRegex("Inst-1"))), Seq("job", "instance"), Some(300000), None)
    val rhs = PeriodicSeries(rawSeriesRhs, 1000, 500, 50000)

    val binaryJoin = BinaryJoin(lhs, DIV, OneToOne, rhs)

    val res = LogicalPlan.getColumnFilterGroup(binaryJoin)

    res.size.shouldEqual(2)
    res(0).size.shouldEqual(2)
    for (cfSet <- res(0)) {
      if (cfSet.column == "_name_") {
        cfSet.column.shouldEqual("_name_")
        cfSet.filter.operatorString.shouldEqual("=")
        cfSet.filter.valuesStrings shouldEqual(Set("MetricName1"))
      } else if (cfSet.column == "instance") {
        cfSet.column.shouldEqual("instance")
        cfSet.filter.operatorString.shouldEqual("=~")
        cfSet.filter.valuesStrings shouldEqual(Set("Inst-0"))
      } else {
        fail("invalid entry in column filter sequence " + cfSet)
      }
    }
    res(1).size.shouldEqual(2)
    for (cfSet <- res(1)) {
      if (cfSet.column == "job") {
        cfSet.column.shouldEqual("job")
        cfSet.filter.operatorString.shouldEqual("=")
        cfSet.filter.valuesStrings shouldEqual(Set("MetricName2"))
      } else if (cfSet.column == "instance") {
        cfSet.column.shouldEqual("instance")
        cfSet.filter.operatorString.shouldEqual("!~")
        cfSet.filter.valuesStrings shouldEqual(Set("Inst-1"))
      } else {
        fail("invalid entry in column filter sequence " + cfSet)
      }
    }
  }

  it("should get columnFilterGroup fail for scalar logicalPlan") {
    val periodicSeriesWithWindowing = ScalarTimeBasedPlan(ScalarFunctionId.Year, RangeParams(1000, 500, 5000))
    val res = LogicalPlan.getColumnFilterGroup(periodicSeriesWithWindowing)
    res.isEmpty should be (true)
  }

  it("should get MetricName fail for scalar logicalPlan") {
    val periodicSeriesWithWindowing = ScalarTimeBasedPlan(ScalarFunctionId.Year, RangeParams(1000, 500, 5000))
    val res = LogicalPlan.getColumnValues(periodicSeriesWithWindowing, "_name_")
    res.isEmpty should be (true)
  }

  it("should get MetricName from logicalPlan") {

    val rawSeries = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName")),
      ColumnFilter("instance", Equals("Inst-0"))), Seq("_name_", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing = PeriodicSeriesWithWindowing(rawSeries, 1000, 500, 5000, 100, SumOverTime)

    val res = LogicalPlan.getColumnValues(periodicSeriesWithWindowing, "_name_")
    res.shouldEqual(Set("MetricName"))
  }

  it("should get LabelName from logicalPlan with filter In") {

    val rawSeries = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName")),
      ColumnFilter("instance", In(Set("Inst-0", "Inst-1")))), Seq("_name_", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing = PeriodicSeriesWithWindowing(rawSeries, 1000, 500, 5000, 100, SumOverTime)

    val res = LogicalPlan.getColumnValues(periodicSeriesWithWindowing, "instance")
    res.shouldEqual(Set("Inst-0", "Inst-1"))
  }

  it("should get MetricName from BinaryJoin LogicalPlan") {

    val rawSeriesLhs = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName1")),
      ColumnFilter("instance", Equals("Inst-0"))), Seq("_name_", "instance"), Some(300000), None)
    val lhs = PeriodicSeries(rawSeriesLhs, 1000, 500, 50000)

    val rawSeriesRhs = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("job", Equals("MetricName2")),
      ColumnFilter("instance", Equals("Inst-1"))), Seq("job", "instance"), Some(300000), None)
    val rhs = PeriodicSeries(rawSeriesRhs, 1000, 500, 50000)

    val binaryJoin = BinaryJoin(lhs, DIV, OneToOne, rhs)

    val res = LogicalPlan.getColumnValues(binaryJoin, "_name_")
    res.shouldEqual(Set("MetricName1"))
  }

  it("should return None if label value is not present in logicalPlan") {

    val rawSeries = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName")),
      ColumnFilter("instance", Equals("Inst-0"))), Seq("_name_", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing = PeriodicSeriesWithWindowing(rawSeries, 1000, 500, 5000, 100, SumOverTime)

    val res = LogicalPlan.getColumnValues(periodicSeriesWithWindowing, "_name")
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

    val res = LogicalPlan.getColumnValues(binaryJoin, "instance")
    res.shouldEqual(Set("Inst-0", "Inst-1"))
  }

  it("should sort ColumnFilters when only one group is present") {

    val rawSeries = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("name", Equals("MetricName")),
      ColumnFilter("instance", NotEquals("Inst-0"))), Seq("name", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing = PeriodicSeriesWithWindowing(rawSeries, 1000, 500, 5000, 100, SumOverTime)

    val res = LogicalPlan.getColumnFilterGroup(periodicSeriesWithWindowing)
    res.size.shouldEqual(1)
    res(0).size.shouldEqual(2)
    for (cfSet <- res(0)) {
      if (cfSet.column.equals("name")) {
        cfSet.column.shouldEqual("name")
        cfSet.filter.operatorString.shouldEqual("=")
        cfSet.filter.valuesStrings shouldEqual(Set("MetricName"))
      } else if (cfSet.column.equals("instance")) {
        cfSet.column.shouldEqual("instance")
        cfSet.filter.operatorString.shouldEqual("!=")
        cfSet.filter.valuesStrings shouldEqual(Set("Inst-0"))
      } else {
        fail("invalid entry in column filter sequence " + cfSet)
      }
    }
  }

  it("should get label values from nested binary join and sort") {
    val rawSeriesLhs1 = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("app", Equals("Mosaic")),
      ColumnFilter("instance", EqualsRegex("Inst-1"))), Seq("name", "instance"), Some(300000), None)
    val lhs1 = PeriodicSeries(rawSeriesLhs1, 1000, 500, 50000)

    val rawSeriesLhs2 = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("app", Equals("Cassandra")),
      ColumnFilter("instance", EqualsRegex("Inst-0"))), Seq("name", "instance"), Some(300000), None)
    val lhs2 = PeriodicSeries(rawSeriesLhs2, 1000, 500, 50000)

    val binaryJoin1 = BinaryJoin(lhs1, DIV, OneToOne, lhs2)

    val rawSeriesRhs = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("app", Equals("Test")),
      ColumnFilter("instance", NotEqualsRegex("Inst-1"))), Seq("job", "instance"), Some(300000), None)
    val rhs = PeriodicSeries(rawSeriesRhs, 1000, 500, 50000)


    val binaryJoin2 = BinaryJoin(binaryJoin1, DIV, OneToOne, rhs)

    val res = LogicalPlan.getColumnFilterGroup(binaryJoin2)
    res.size shouldEqual(3)

    res(0).size.shouldEqual(2)
    for (cfSet <- res(0)) {
      if (cfSet.column == "app") {
        cfSet.column.shouldEqual("app")
        cfSet.filter.operatorString.shouldEqual("=")
        cfSet.filter.valuesStrings shouldEqual(Set("Mosaic"))
      } else if (cfSet.column == "instance") {
        cfSet.column.shouldEqual("instance")
        cfSet.filter.operatorString.shouldEqual("=~")
        cfSet.filter.valuesStrings shouldEqual(Set("Inst-1"))
      } else {
        fail("invalid entry in column filter sequence " + cfSet)
      }
    }
    res(1).size.shouldEqual(2)
    for (cfSet <- res(1)) {
      if (cfSet.column == "app") {
        cfSet.column.shouldEqual("app")
        cfSet.filter.operatorString.shouldEqual("=")
        cfSet.filter.valuesStrings shouldEqual(Set("Cassandra"))
      } else if (cfSet.column == "instance") {
        cfSet.column.shouldEqual("instance")
        cfSet.filter.operatorString.shouldEqual("=~")
        cfSet.filter.valuesStrings shouldEqual(Set("Inst-0"))
      } else {
        fail("invalid entry in column filter sequence " + cfSet)
      }
    }
    res(2).size.shouldEqual(2)
    for (cfSet <- res(2)) {
      if (cfSet.column == "app") {
        cfSet.column.shouldEqual("app")
        cfSet.filter.operatorString.shouldEqual("=")
        cfSet.filter.valuesStrings shouldEqual(Set("Test"))
      } else if (cfSet.column == "instance") {
        cfSet.column.shouldEqual("instance")
        cfSet.filter.operatorString.shouldEqual("!~")
        cfSet.filter.valuesStrings shouldEqual(Set("Inst-1"))
      } else {
        fail("invalid entry in column filter sequence " + cfSet)
      }
    }
  }

  it("should have equal hashcode for identical ColumnFilterGroup") {
    val rawSeries1 = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("name", Equals("MetricName")),
      ColumnFilter("instance", NotEquals("Inst-0"))), Seq("name", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing1 = PeriodicSeriesWithWindowing(rawSeries1, 1000, 500, 5000, 100, SumOverTime)
    val res1 = LogicalPlan.getColumnFilterGroup(periodicSeriesWithWindowing1)
    val rawSeries2 = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("instance", NotEquals("Inst-0")),
      ColumnFilter("name", Equals("MetricName"))), Seq("name", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing2 = PeriodicSeriesWithWindowing(rawSeries2, 1000, 500, 5000, 100, SumOverTime)
    val res2 = LogicalPlan.getColumnFilterGroup(periodicSeriesWithWindowing2)
    res1.size.shouldEqual(1)
    res1(0).size.shouldEqual(2)
    res2.size.shouldEqual(1)
    res2(0).size.shouldEqual(2)
    res1.hashCode() shouldEqual res2.hashCode()
  }
}
