package filodb.core.query

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class QueryContextSpec extends AnyFunSpec with Matchers {

  it("should produce correct log line") {
    val pp = PlannerParams(
      applicationId = "fdb",
      queryTimeoutMillis = 10,
      enforcedLimits = PerQueryLimits(
        groupByCardinality = 123,
        joinQueryCardinality = 124,
        execPlanResultBytes = 125,
        execPlanSamples = 126,
        timeSeriesSamplesScannedBytes = 127,
        timeSeriesScanned = 200),
      warnLimits = PerQueryLimits(
        groupByCardinality = 128,
        joinQueryCardinality = 129,
        execPlanResultBytes = 130,
        execPlanSamples = 131,
        timeSeriesSamplesScannedBytes = 132),
      queryOrigin = Option("rr"),
      queryOriginId = Option("rr_id"),
      timeSplitEnabled = true,
      minTimeRangeForSplitMs = 10,
      splitSizeMs = 10,
      skipAggregatePresent = true,
      processMultiPartition = true,
      allowPartialResults = true
    )
    val queryContext = QueryContext(
      PromQlQueryParams(promQl= "myQuery", 1, 2, 3),
      plannerParams = pp
    )
    val logLine = queryContext.getQueryLogLine("My log message")
    logLine should equal (
      s"My log message promQL = -=# myQuery #=- queryOrigin = Some(rr) queryPrincipal = None " +
        s"queryOriginId = Some(rr_id) queryId = ${queryContext.queryId}"
    )
  }
}