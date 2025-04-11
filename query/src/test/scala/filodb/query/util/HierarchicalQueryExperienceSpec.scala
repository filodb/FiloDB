package filodb.query.util

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import kamon.Kamon
import kamon.testkit.InstrumentInspection.Syntax.counterInstrumentInspection
import filodb.core.query.ColumnFilter
import filodb.core.query.Filter.{Equals, EqualsRegex}

class HierarchicalQueryExperienceSpec extends AnyFunSpec with Matchers {

  it("getMetricColumnFilterTag should return expected column") {
    HierarchicalQueryExperience.getMetricColumnFilterTag(Seq("tag1", "__name__"), "_metric_") shouldEqual "__name__"
    HierarchicalQueryExperience.getMetricColumnFilterTag(Seq("tag1", "_metric_"), "_metric_") shouldEqual "_metric_"
    HierarchicalQueryExperience.getMetricColumnFilterTag(Seq("tag1", "tag2"), "_metric_") shouldEqual "_metric_"
    HierarchicalQueryExperience.getMetricColumnFilterTag(Seq("tag1", "tag2"), "__name__") shouldEqual "__name__"
  }

  it("getNextLevelAggregatedMetricName should return expected metric name") {

    val params = IncludeAggRule("agg_2", Set("job", "instance"), "2")

    // Case 1: Should not update if metric doesn't have the aggregated metric identifier
    HierarchicalQueryExperience.getNextLevelAggregatedMetricName(
      "metric1", ":::", params.metricSuffix) shouldEqual "metric1"

    // Case 2: Should update if metric has the aggregated metric identifier
    HierarchicalQueryExperience.getNextLevelAggregatedMetricName(
      "metric1:::agg", ":::", params.metricSuffix) shouldEqual "metric1:::agg_2"
  }

  it("getAggMetricNameForRawMetric should return expected metric name") {
    val params = IncludeAggRule("agg_2", Set("job", "instance"), "2")
    // Case 1: Should not update if metric doesn't have the aggregated metric identifier
    HierarchicalQueryExperience.getAggMetricNameForRawMetric(
      "metric1", ":::", params.metricSuffix) shouldEqual "metric1:::agg_2"
  }

  it("isParentPeriodicSeriesPlanAllowedForRawSeriesUpdateForHigherLevelAggregatedMetric return expected values") {
    HierarchicalQueryExperience.isParentPeriodicSeriesPlanAllowed(
      Seq("BinaryJoin", "Aggregate", "ScalarOperation")) shouldEqual true

    HierarchicalQueryExperience.isParentPeriodicSeriesPlanAllowed(
      Seq("BinaryJoin", "ScalarOperation")) shouldEqual false
  }

  it("isRangeFunctionAllowed should return expected values") {
    HierarchicalQueryExperience.isRangeFunctionAllowed("rate") shouldEqual true
    HierarchicalQueryExperience.isRangeFunctionAllowed("increase") shouldEqual true
    HierarchicalQueryExperience.isRangeFunctionAllowed("sum_over_time") shouldEqual false
    HierarchicalQueryExperience.isRangeFunctionAllowed("last") shouldEqual false
  }

  it("isAggregationOperatorAllowed should return expected values") {
    HierarchicalQueryExperience.isAggregationOperatorAllowed("sum") shouldEqual true
    HierarchicalQueryExperience.isAggregationOperatorAllowed("min") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("max") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("avg") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("count") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("topk") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("bottomk") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("stddev") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("stdvar") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("quantile") shouldEqual false
  }

  it("should check if higher level aggregation is applicable with IncludeTags") {
    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      IncludeAggRule("agg_2", Set("tag1", "tag2"), "2"), Seq("tag1", "tag2", "_ws_", "_ns_", "_metric_")) shouldEqual true

    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      IncludeAggRule("agg_2", Set("tag1", "tag2", "tag3"), "2"), Seq("tag1", "tag2", "_ws_", "_ns_", "__name__")) shouldEqual true

    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      IncludeAggRule("agg_2", Set("tag1", "tag2", "tag3"), "2"), Seq("tag3", "tag4", "_ws_", "_ns_", "__name__")) shouldEqual false
  }

  it("should check if higher level aggregation is applicable with ExcludeTags") {
    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      ExcludeAggRule("agg_2", Set("tag1", "tag2"), "2"),Seq("tag1", "tag2", "_ws_", "_ns_", "_metric_")) shouldEqual false

    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      ExcludeAggRule("agg_2", Set("tag1", "tag3"), "2"),Seq("tag1", "tag2", "_ws_", "_ns_", "_metric_")) shouldEqual false

    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      ExcludeAggRule("agg_2", Set("tag1", "tag2"), "2"),Seq("tag1", "tag2", "_ws_", "_ns_", "_metric_")) shouldEqual false

    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      ExcludeAggRule("agg_2", Set("tag3", "tag4"), "2"), Seq("tag1", "tag2", "_ws_", "_ns_", "_metric_")) shouldEqual true
  }

  it("getColumnsAfterFilteringOutDotStarRegexFilters should return as expected") {
    var filters = Seq(
      ColumnFilter("__name__", Equals("metric1")),
      ColumnFilter("tag1", Equals("value1")),
      ColumnFilter("tag2", EqualsRegex("value2.*")),
      ColumnFilter("tag3", Equals("value3")),
      ColumnFilter("tag4", EqualsRegex(".*")))

    // should ignore tag4
    HierarchicalQueryExperience.getColumnsAfterFilteringOutDotStarRegexFilters(filters) shouldEqual
      Seq("__name__", "tag1", "tag2", "tag3")

    filters = Seq(
      ColumnFilter("tag1", Equals("value1")),
      ColumnFilter("tag2", Equals("value2")),
      ColumnFilter("tag3", EqualsRegex(".*abc")))

    // should not ignore any tags
    HierarchicalQueryExperience.getColumnsAfterFilteringOutDotStarRegexFilters(filters) shouldEqual
      Seq("tag1", "tag2", "tag3")

    filters = Seq(
      ColumnFilter("tag1", EqualsRegex(".*")),
      ColumnFilter("tag2", EqualsRegex(".*")),
      ColumnFilter("tag3", EqualsRegex(".*")))

    // should ignore all tags
    HierarchicalQueryExperience.getColumnsAfterFilteringOutDotStarRegexFilters(filters) shouldEqual Seq()
  }

  it("checkAggregateQueryEligibleForHigherLevelAggregatedMetric should increment counter if metric updated") {
    val excludeRule = ExcludeAggRule("agg_2", Set("notAggTag1", "notAggTag2"), "2")
    val params = HierarchicalQueryExperienceParams(":::", Map("agg" -> Set(excludeRule)), Map("metric1" -> Set(excludeRule)))
    Kamon.init()
    var counter = Kamon.counter("hierarchical-query-plans-optimized")

    // CASE 1: Should update if metric have the aggregated metric identifier
    counter.withTag("metric_ws", "testws").withTag("metric_ns", "testns").value shouldEqual 0
    var updatedFilters = HierarchicalQueryExperience.upsertMetricColumnFilterIfHigherLevelAggregationApplicable(
      params, Seq(
        ColumnFilter("__name__", Equals("metric1:::agg")),
        ColumnFilter("_ws_", Equals("testws")),
        ColumnFilter("_ns_", Equals("testns")),
        ColumnFilter("aggTag", Equals("value"))), false)
    updatedFilters.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
      .shouldEqual("metric1:::agg_2")
    counter.withTag("metric_ws", "testws").withTag("metric_ns", "testns").withTag("metric_type", "agg").value shouldEqual 1

    // CASE 2: Should not update if metric doesn't have the aggregated metric identifier
    // reset the counter
    counter = Kamon.counter("hierarchical-query-plans-optimized")
    counter.withTag("metric_ws", "testws").withTag("metric_ns", "testns").value shouldEqual 0
    updatedFilters = HierarchicalQueryExperience.upsertMetricColumnFilterIfHigherLevelAggregationApplicable(
      params, Seq(
        ColumnFilter("__name__", Equals("metric1:::agg")),
        ColumnFilter("_ws_", Equals("testws")),
        ColumnFilter("_ns_", Equals("testns")),
        ColumnFilter("notAggTag1", Equals("value"))), false) // using exclude tag, so should not optimize
    updatedFilters.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
      .shouldEqual("metric1:::agg")
    // count should not increment
    counter.withTag("metric_ws", "testws").withTag("metric_ns", "testns").withTag("metric_type", "").value shouldEqual 0

    // CASE 3: Should update for raw metric optimization
    counter = Kamon.counter("hierarchical-query-plans-optimized")
    updatedFilters = HierarchicalQueryExperience.upsertMetricColumnFilterIfHigherLevelAggregationApplicable(
      params, Seq(
        ColumnFilter("__name__", Equals("metric1")),
        ColumnFilter("_ws_", Equals("testws")),
        ColumnFilter("_ns_", Equals("testns")),
        ColumnFilter("aggTag", Equals("value"))), true)
    updatedFilters.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
      .shouldEqual("metric1:::agg_2")
    counter.withTag("metric_ws", "testws").withTag("metric_ns", "testns").withTag("metric_type", "raw").value shouldEqual 1
    Kamon.stop()
  }
}
