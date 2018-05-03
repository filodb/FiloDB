package filodb.coordinator.queryengine2

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import org.scalatest.{FunSpec, Matchers}

import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.QueryOptions
import filodb.core.MetricsTestData
import filodb.core.query.{ColumnFilter, Filter}
import filodb.query._
import filodb.query.exec._

class QueryEngineSpec extends FunSpec with Matchers {

  implicit val system = ActorSystem()
  val node0 = TestProbe().ref
  val node1 = TestProbe().ref
  val node2 = TestProbe().ref
  val node3 = TestProbe().ref

  val mapper = new ShardMapper(4)
  mapper.registerNode(Seq(0), node0)
  mapper.registerNode(Seq(1), node1)
  mapper.registerNode(Seq(2), node2)
  mapper.registerNode(Seq(3), node3)

  private def mapperRef = mapper

  val dataset = MetricsTestData.timeseriesDataset

  val engine = new QueryEngine(dataset, mapperRef)

  it ("should generate ExecPlan for LogicalPlan") {

    /*
    This is the PromQL

    sum(rate(http_request_duration_seconds_bucket{job="myService",le="0.3"}[5m])) by (job)
     /
    sum(rate(http_request_duration_seconds_count{job="myService"}[5m])) by (job)
    */

    val f1 = Seq(ColumnFilter("__name__", Filter.Equals("http_request_duration_seconds_bucket")),
                 ColumnFilter("job", Filter.Equals("myService")),
                 ColumnFilter("le", Filter.Equals("0.3")))

    val to = System.currentTimeMillis()
    val from = to - 50000

    val intervalSelector = IntervalSelector(Seq(from), Seq(to))

    val raw1 = RawSeries(rangeSelector = intervalSelector, filters= f1, columns = Seq("value"))
    val windowed1 = PeriodicSeriesWithWindowing(raw1, from, 1000, to, 5000, RangeFunctionId.Rate)
    val summed1 = Aggregate(AggregationOperator.Sum, windowed1, Nil, Seq("job"))

    val f2 = Seq(ColumnFilter("__name__", Filter.Equals("http_request_duration_seconds_count")),
      ColumnFilter("job", Filter.Equals("myService")))
    val raw2 = RawSeries(rangeSelector = intervalSelector, filters= f2, columns = Seq("value"))
    val windowed2 = PeriodicSeriesWithWindowing(raw2, from, 1000, to, 5000, RangeFunctionId.Rate)
    val summed2 = Aggregate(AggregationOperator.Sum, windowed2, Nil, Seq("job"))

    // final logical plan
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)

    // materialized exec plan
    val execPlan = engine.materialize(logicalPlan, QueryOptions())

    /*
    Following ExecPlan should be generated:

    BinaryJoinExec(binaryOp=DIV, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-4#-498736684])
    -ReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-4#-498736684])
    --AggregateCombiner(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ---PeriodicSamplesMapper(start=1524001640281, step=1000, end=1524001690281, window=Some(5000), function=Some(Rate), funcParams=Some(List()))
    ----SelectRawPartitionsExec(shard=2, rangeSelector=IntervalSelector(b[1524001640281],b[1524001690281]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_bucket)), ColumnFilter(job,Equals(myService)), ColumnFilter(le,Equals(0.3)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#1295960843])
    --AggregateCombiner(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ---PeriodicSamplesMapper(start=1524001640281, step=1000, end=1524001690281, window=Some(5000), function=Some(Rate), funcParams=Some(List()))
    ----SelectRawPartitionsExec(shard=3, rangeSelector=IntervalSelector(b[1524001640281],b[1524001690281]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_bucket)), ColumnFilter(job,Equals(myService)), ColumnFilter(le,Equals(0.3)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-4#-498736684])
    -ReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2028091896])
    --AggregateCombiner(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ---PeriodicSamplesMapper(start=1524001640281, step=1000, end=1524001690281, window=Some(5000), function=Some(Rate), funcParams=Some(List()))
    ----SelectRawPartitionsExec(shard=0, rangeSelector=IntervalSelector(b[1524001640281],b[1524001690281]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_count)), ColumnFilter(job,Equals(myService)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2028091896])
    --AggregateCombiner(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ---PeriodicSamplesMapper(start=1524001640281, step=1000, end=1524001690281, window=Some(5000), function=Some(Rate), funcParams=Some(List()))
    ----SelectRawPartitionsExec(shard=1, rangeSelector=IntervalSelector(b[1524001640281],b[1524001690281]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_count)), ColumnFilter(job,Equals(myService)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-2#125110136])
    */

    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual true
    execPlan.children.foreach { l1 =>
      l1.isInstanceOf[ReduceAggregateExec] shouldEqual true
      l1.children.foreach { l2 =>
        l2.isInstanceOf[SelectRawPartitionsExec] shouldEqual true
        l2.rangeVectorTransformers.size shouldEqual 2
        l2.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
        l2.rangeVectorTransformers(1).isInstanceOf[AggregateCombiner] shouldEqual true
      }
    }
  }
}
