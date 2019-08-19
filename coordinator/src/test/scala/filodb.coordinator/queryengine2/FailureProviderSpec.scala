package filodb.coordinator.queryengine2

import monix.eval.Task
import monix.execution.Scheduler
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.duration.FiniteDuration

import filodb.core.DatasetRef
import filodb.core.query.{ColumnFilter, Filter}
import filodb.query._
import filodb.query.exec.{ExecPlan, PlanDispatcher}

class FailureProviderSpec extends FunSpec with Matchers {
  val f1 = Seq(ColumnFilter("__name__", Filter.Equals("http_request")),
    ColumnFilter("job", Filter.Equals("myService")),
    ColumnFilter("le", Filter.Equals("0.3")))

  val to = 150000
  val from = to - 50000

  val intervalSelector = IntervalSelector(from, to)

  val raw1 = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
  val windowed1 = PeriodicSeriesWithWindowing(raw1, from, 1000, to, 5000, RangeFunctionId.Rate)
  val summed1 = Aggregate(AggregationOperator.Sum, windowed1, Nil, Seq("job"))

  val f2 = Seq(ColumnFilter("__name__", Filter.Equals("http_request_duration_seconds_count")),
    ColumnFilter("job", Filter.Equals("myService")))
  val raw2 = RawSeries(rangeSelector = intervalSelector, filters = f2, columns = Seq("value"))
  val windowed2 = PeriodicSeriesWithWindowing(raw2, from + 1000, 1000, to, 5000, RangeFunctionId.Rate)
  val summed2 = Aggregate(AggregationOperator.Sum, windowed2, Nil, Seq("job"))

  val binaryJoinLogicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)
  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlan)
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Task[QueryResponse] = ???
  }

  val datasetRef = DatasetRef("dataset", Some("cassandra"))

  it("should check for PeriodicSeries plan") {
    QueryRoutingPlanner.isPeriodicSeriesPlan(summed1) shouldEqual (true)
    QueryRoutingPlanner.isPeriodicSeriesPlan(raw2) shouldEqual (false)
  }

  it("should extract time from logical plan") {
    QueryRoutingPlanner.hasSingleTimeRange(summed1) shouldEqual (true)
    QueryRoutingPlanner.hasSingleTimeRange(binaryJoinLogicalPlan) shouldEqual (false)

    val timeRange = QueryRoutingPlanner.getPeriodicSeriesTimeFromLogicalPlan(summed1)

    timeRange.startInMillis shouldEqual (100000)
    timeRange.endInMillis shouldEqual (150000)
  }

  it("should update time in logical plan") {

    val expectedRaw = RawSeries(rangeSelector = IntervalSelector(20000, 30000), filters = f1, columns = Seq("value"))
    val updatedTimeLogicalPlan = QueryRoutingPlanner.copyWithUpdatedTimeRange(summed1, TimeRange(20000, 30000), 0)

    QueryRoutingPlanner.getPeriodicSeriesTimeFromLogicalPlan(updatedTimeLogicalPlan).startInMillis shouldEqual (20000)
    QueryRoutingPlanner.getPeriodicSeriesTimeFromLogicalPlan(updatedTimeLogicalPlan).endInMillis shouldEqual (30000)

    updatedTimeLogicalPlan.isInstanceOf[Aggregate] shouldEqual (true)
    val aggregate = updatedTimeLogicalPlan.asInstanceOf[Aggregate]
    aggregate.vectors.isInstanceOf[PeriodicSeriesWithWindowing] shouldEqual (true)
    aggregate.asInstanceOf[Aggregate].vectors.asInstanceOf[PeriodicSeriesWithWindowing].rawSeries.toString shouldEqual
      (expectedRaw.toString)

  }

  it("should sort and remove smaller overlapping failures and generate local and remote routes correctly") {
    val failureTimeRanges = Seq(FailureTimeRange("local", datasetRef,
      TimeRange(1500, 5000), false), FailureTimeRange("remote", datasetRef,
      TimeRange(100, 200), true), FailureTimeRange("local", datasetRef,
      TimeRange(1000, 2000), false), FailureTimeRange("remote", datasetRef,
      TimeRange(100, 700), true))

    val expectedResult = Seq(LocalRoute(Some(TimeRange(50, 1499))),
      RemoteRoute(Some(TimeRange(1500, 3000))))
    val routes = QueryRoutingPlanner.plan(failureTimeRanges, TimeRange(50, 3000), 0 , 1)

    routes(0).equals(expectedResult(0)) shouldEqual true
    routes(1).equals(expectedResult(1)) shouldEqual true
    routes.sameElements(expectedResult) shouldEqual (true)
  }

  it("should split failures to remote followed by local") {
    val failureTimeRangeNonOverlapping = Seq(FailureTimeRange("local", datasetRef,
      TimeRange(100, 200), false), FailureTimeRange("remote", datasetRef,
      TimeRange(1000, 3000), true))

    val expectedResult = Seq(RemoteRoute(Some(TimeRange(50, 999))),
      LocalRoute(Some(TimeRange(1000, 5000))))
    val routes = QueryRoutingPlanner.plan(failureTimeRangeNonOverlapping, TimeRange(50, 5000), 0 , 1)

    routes(0).equals(expectedResult(0)) shouldEqual true
    routes(1).equals(expectedResult(1)) shouldEqual true
    routes.sameElements(expectedResult) shouldEqual (true)
  }

  it("should generate remote route when there is only one failure which is in local") {
    val failureTimeRangeNonOverlapping = Seq(FailureTimeRange("local", datasetRef,
      TimeRange(100, 200), false))

    val expectedResult = Seq(RemoteRoute(Some(TimeRange(50, 5000))))
    val routes = QueryRoutingPlanner.plan(failureTimeRangeNonOverlapping, TimeRange(50, 5000), 0, 1)

    routes.sameElements(expectedResult) shouldEqual (true)
  }

  it("should generate correct routes for local-remote-local failures ") {
    val failureTimeRangeNonOverlapping = Seq(FailureTimeRange("local", datasetRef,
      TimeRange(100, 200), false), FailureTimeRange("remote", datasetRef,
      TimeRange(1000, 3000), true), FailureTimeRange("local", datasetRef,
      TimeRange(4000, 4500), false))

    val expectedResult = Seq(RemoteRoute(Some(TimeRange(50, 999))),
      LocalRoute(Some(TimeRange(1000, 3999))), RemoteRoute(Some(TimeRange(4000, 5000))))
    val routes = QueryRoutingPlanner.plan(failureTimeRangeNonOverlapping, TimeRange(50, 5000), 0 , 1)

    routes(0).equals(expectedResult(0)) shouldEqual true
    routes(1).equals(expectedResult(1)) shouldEqual true
    routes.sameElements(expectedResult) shouldEqual (true)

  }

  it("should generate correct routes for remote-local-remote failures ") {
    val failureTimeRangeNonOverlapping = Seq(FailureTimeRange("remote", datasetRef,
      TimeRange(100, 200), true), FailureTimeRange("local", datasetRef,
      TimeRange(1000, 3000), false), FailureTimeRange("remote", datasetRef,
      TimeRange(4000, 4500), true))

    val expectedResult = Seq(LocalRoute(Some(TimeRange(50, 999))),
      RemoteRoute(Some(TimeRange(1000, 3999))), LocalRoute(Some(TimeRange(4000, 5000))))

    val routes = QueryRoutingPlanner.plan(failureTimeRangeNonOverlapping, TimeRange(50, 5000), 0 , 1)
    routes(0).equals(expectedResult(0)) shouldEqual true
    routes(1).equals(expectedResult(1)) shouldEqual true
    routes.sameElements(expectedResult) shouldEqual (true)
  }

  it("should generate correct routes for remote-local failures with lookback and step") {
    val failureTimeRangeNonOverlapping = Seq(FailureTimeRange("remote", datasetRef,
      TimeRange(55, 500), true), FailureTimeRange("local", datasetRef,
      TimeRange(1000, 3000), false))

    val expectedResult = Seq(LocalRoute(Some(TimeRange(100, 980))),
      RemoteRoute(Some(TimeRange(1000, 5000))))

    //Query time is 100 to 5000
    val routes = QueryRoutingPlanner.plan(failureTimeRangeNonOverlapping, TimeRange(100, 5000), 50 , 20)
    println("routes:" + routes)
    routes(0).equals(expectedResult(0)) shouldEqual true
    routes(1).equals(expectedResult(1)) shouldEqual true
    routes.sameElements(expectedResult) shouldEqual (true)
  }

  it("should update time in logical plan when lookBack is present") {
    val expectedRaw = RawSeries(rangeSelector = IntervalSelector(19900, 30000), filters = f1, columns = Seq("value"))
    val updatedTimeLogicalPlan = QueryRoutingPlanner.copyWithUpdatedTimeRange(summed1, TimeRange(20000, 30000), 100)

    QueryRoutingPlanner.getPeriodicSeriesTimeFromLogicalPlan(updatedTimeLogicalPlan).startInMillis shouldEqual (20000)
    QueryRoutingPlanner.getPeriodicSeriesTimeFromLogicalPlan(updatedTimeLogicalPlan).endInMillis shouldEqual (30000)

    updatedTimeLogicalPlan.isInstanceOf[Aggregate] shouldEqual (true)
    val aggregate = updatedTimeLogicalPlan.asInstanceOf[Aggregate]
    aggregate.vectors.isInstanceOf[PeriodicSeriesWithWindowing] shouldEqual (true)
    aggregate.asInstanceOf[Aggregate].vectors.asInstanceOf[PeriodicSeriesWithWindowing].rawSeries.toString shouldEqual
      (expectedRaw.toString)

  }

  it("should generate correct routes for local-remote failures with lookback and step") {
    val failureTimeRangeNonOverlapping = Seq(FailureTimeRange("local", datasetRef,
      TimeRange(55, 500), false), FailureTimeRange("remote", datasetRef,
      TimeRange(1000, 3000), true))

    val expectedResult = Seq(RemoteRoute(Some(TimeRange(100, 980))),
      LocalRoute(Some(TimeRange(1000, 5000))))

    //Query time is 100 to 5000
    val routes = QueryRoutingPlanner.plan(failureTimeRangeNonOverlapping, TimeRange(100, 5000), 50 , 20)
    routes(0).equals(expectedResult(0)) shouldEqual true
    routes(1).equals(expectedResult(1)) shouldEqual true
    routes.sameElements(expectedResult) shouldEqual (true)
  }

  it("should generate correct routes when failure start time and end time is same ") {
    val failureTimeRangeNonOverlapping = Seq(FailureTimeRange("remote", datasetRef,
      TimeRange(100, 100), true), FailureTimeRange("local", datasetRef,
      TimeRange(1000, 3000), false), FailureTimeRange("remote", datasetRef,
      TimeRange(4000, 4000), true))

    val expectedResult = Seq(LocalRoute(Some(TimeRange(50, 999))),
      RemoteRoute(Some(TimeRange(1000, 3999))), LocalRoute(Some(TimeRange(4000, 5000))))

    val routes = QueryRoutingPlanner.plan(failureTimeRangeNonOverlapping, TimeRange(50, 5000), 0 , 1)
    routes.sameElements(expectedResult) shouldEqual (true)
  }

}
