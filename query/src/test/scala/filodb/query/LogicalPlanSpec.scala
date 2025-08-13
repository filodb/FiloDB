package filodb.query

import filodb.core.query.{ColumnFilter, RangeParams}
import filodb.core.query.Filter.{Equals, EqualsRegex, In, NotEquals, NotEqualsRegex}
import filodb.query.BinaryOperator.DIV
import filodb.query.Cardinality.OneToOne
import filodb.query.RangeFunctionId.SumOverTime
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class LogicalPlanSpec extends AnyFunSpec with Matchers {

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

  it("should update logicalPlan filter") {
    val currFilter = ColumnFilter("instance", EqualsRegex("Inst*"))
    val rawSeries = RawSeries(IntervalSelector(1000, 3000), Seq(ColumnFilter("_name_", Equals("MetricName")),
      currFilter, ColumnFilter("job", Equals("job1"))), Seq("_name_", "instance"), Some(300000), None)
    val periodicSeriesWithWindowing = PeriodicSeriesWithWindowing(rawSeries, 1000, 500, 5000, 100, SumOverTime)
    val updatedFilter = ColumnFilter("instance", Equals("Inst1"))
    val res = periodicSeriesWithWindowing.replaceFilters(Seq(updatedFilter))
    res.asInstanceOf[PeriodicSeriesWithWindowing].series.asInstanceOf[RawSeries].filters.
      contains(updatedFilter) shouldEqual(true)
    res.asInstanceOf[PeriodicSeriesWithWindowing].series.asInstanceOf[RawSeries].filters.
      contains(currFilter) shouldEqual(false)
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

  it ("should construct TsCardinalities only when args are valid") {
    // TODO: these tests (and TsCardinalities construction requirements)
    //   are designed for a ws/ns/metric shard key prefix. If/when a more
    //   general setup is needed, these tests/requirements need to be updated.
    assertThrows[IllegalArgumentException] {
      // need ws/ns in order to group by metric
      TsCardinalities(Seq(), 3)
    }
    assertThrows[IllegalArgumentException] {
      // need ws/ns in order to group by metric
      TsCardinalities(Seq("a"), 3)
    }
    assertThrows[IllegalArgumentException] {
      // insufficient group depth
      TsCardinalities(Seq("a", "b"), 1)
    }
    assertThrows[IllegalArgumentException] {
      // insufficient group depth
      TsCardinalities(Seq("a", "b", "c"), 2)
    }
    TsCardinalities(Seq(), 1)
    TsCardinalities(Seq(), 2)
    TsCardinalities(Seq("a"), 1)
    TsCardinalities(Seq("a"), 2)
    TsCardinalities(Seq("a", "b"), 2)
    TsCardinalities(Seq("a", "b"), 3)
    TsCardinalities(Seq("a", "b", "c"), 3)
  }

  it ("TsCardinalities queryParams should have expected values") {
    val datasets = Seq("longtime-prometheus",
      "recordingrules-prometheus_rules_longterm")
    val userDatasets = "\"raw\",\"recordingrules\""
    val plan = TsCardinalities(Seq("a","b","c"), 3, datasets, userDatasets)
    val queryParamsMap = plan.queryParams()

    queryParamsMap.get("numGroupByFields").get shouldEqual "3"
    queryParamsMap.get("datasets").get shouldEqual userDatasets
    queryParamsMap.get("verbose").get shouldEqual "true"
    queryParamsMap.get("match[]").get shouldEqual "{_ws_=\"a\",_ns_=\"b\",__name__=\"c\"}"
  }

  describe("columnFilters()") {

    // Helper to create a simple RawSeries for tests
    def makeRawSeries(filters: Seq[ColumnFilter]): RawSeries = {
      RawSeries(IntervalSelector(1000, 5000), filters, Seq("value"))
    }

    it("should extract filters from a simple RawSeries plan") {
      val filters = Seq(ColumnFilter("job", Equals("node")), ColumnFilter("instance", NotEquals("localhost")))
      val plan = makeRawSeries(filters)
      plan.planColumnFilters() should contain theSameElementsAs filters
    }

    it("should extract filters from a nested PeriodicSeriesWithWindowing plan") {
      val filters = Seq(ColumnFilter("__name__", Equals("http_requests_total")),
                        ColumnFilter("status", EqualsRegex("5..")))
      val raw = makeRawSeries(filters)
      val plan = PeriodicSeriesWithWindowing(raw, 1000, 100, 5000, 300, SumOverTime)

      plan.planColumnFilters() should contain theSameElementsAs filters
    }

    it("should combine filters from both sides of a BinaryJoin") {
      val lhsFilters = Seq(ColumnFilter("__name__", Equals("metric_a")), ColumnFilter("dc", Equals("us-east-1")))
      val rhsFilters = Seq(ColumnFilter("__name__", Equals("metric_b")), ColumnFilter("dc", Equals("us-east-1")))
      val lhs = PeriodicSeries(makeRawSeries(lhsFilters), 1000, 100, 5000)
      val rhs = PeriodicSeries(makeRawSeries(rhsFilters), 1000, 100, 5000)
      val plan = BinaryJoin(lhs, DIV, OneToOne, rhs)

      val expected = lhsFilters ++ rhsFilters
      plan.planColumnFilters() should contain theSameElementsAs expected
    }

    it("should combine filters from a deeply nested BinaryJoin") {
      val filters1 = Seq(ColumnFilter("app", Equals("app-1")))
      val filters2 = Seq(ColumnFilter("app", Equals("app-2")))
      val filters3 = Seq(ColumnFilter("app", Equals("app-3")))

      val p1 = PeriodicSeries(makeRawSeries(filters1), 1000, 100, 5000)
      val p2 = PeriodicSeries(makeRawSeries(filters2), 1000, 100, 5000)
      val p3 = PeriodicSeries(makeRawSeries(filters3), 1000, 100, 5000)

      val innerJoin = BinaryJoin(p1, DIV, OneToOne, p2)
      val outerJoin = BinaryJoin(innerJoin, DIV, OneToOne, p3)

      val expected = filters1 ++ filters2 ++ filters3
      outerJoin.planColumnFilters() should contain theSameElementsAs expected
    }

    it("should return an empty sequence for scalar plans") {
      val scalarPlan1 = ScalarTimeBasedPlan(ScalarFunctionId.Time, RangeParams(1000, 100, 5000))
      val scalarPlan2 = ScalarFixedDoublePlan(123.4, RangeParams(1000, 100, 5000))

      scalarPlan1.planColumnFilters() should be (empty)
      scalarPlan2.planColumnFilters() should be (empty)
    }

    it("should extract filters from a VectorPlan wrapping a scalar with filters") {
      val innerFilters = Seq(ColumnFilter("mode", Equals("idle")))
      val scalarWithVector = ScalarVaryingDoublePlan(
        PeriodicSeries(makeRawSeries(innerFilters), 1000, 100, 5000),
        ScalarFunctionId.Scalar
      )
      val plan = VectorPlan(scalarWithVector)

      plan.planColumnFilters() should contain theSameElementsAs innerFilters
    }

    it("should combine filters from child and the node itself for ApplyAbsentFunction") {
      val childFilters = Seq(ColumnFilter("__name__", Equals("some_metric")))
      val absentFilters = Seq(ColumnFilter("job", Equals("prometheus")), ColumnFilter("instance", Equals("localhost")))

      val childPlan = PeriodicSeries(makeRawSeries(childFilters), 1000, 100, 5000)
      val plan = ApplyAbsentFunction(childPlan, absentFilters, RangeParams(1000, 100, 5000))

      val expected = childFilters ++ absentFilters
      plan.planColumnFilters() should contain theSameElementsAs expected
    }

    it("should combine filters from child and the node itself for ApplyLimitFunction") {
      val childFilters = Seq(ColumnFilter("__name__", Equals("some_metric")))
      val limitFilters = Seq(ColumnFilter("job", Equals("prometheus")), ColumnFilter("instance", Equals("localhost")))

      val childPlan = PeriodicSeries(makeRawSeries(childFilters), 1000, 100, 5000)
      val plan = ApplyLimitFunction(childPlan, limitFilters, RangeParams(1000, 100, 5000), 10)

      val expected = childFilters ++ limitFilters
      plan.planColumnFilters() should contain theSameElementsAs expected
    }

    it("should extract filters from a MetadataQueryPlan like LabelValues") {
      val filters = Seq(ColumnFilter("namespace", Equals("my-ns")), ColumnFilter("pod", EqualsRegex("api-.*")))
      val plan = LabelValues(Seq("pod"), filters, 1000, 5000)

      plan.planColumnFilters() should contain theSameElementsAs filters
    }

    it("should generate and extract filters from a TsCardinalities plan") {
      val plan = TsCardinalities(Seq("my-ws", "my-ns"), 2)
      val expected = Seq(
        ColumnFilter("_ws_", Equals("my-ws")),
        ColumnFilter("_ns_", Equals("my-ns"))
      )
      plan.planColumnFilters() should contain theSameElementsAs expected
    }
  }



}
