package filodb.coordinator.queryplanner

import scala.concurrent.duration._
import scala.math.min

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.{FunctionalSpreadProvider, StaticSpreadProvider}
import filodb.core.{MetricsTestData, SpreadChange}
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.core.store.TimeRangeChunkScan
import filodb.prometheus.ast.{TimeStepParams, WindowConstants}
import filodb.prometheus.parse.Parser
import filodb.query._
import filodb.query.exec._

class SingleClusterPlannerSplitSpec extends AnyFunSpec with Matchers with ScalaFutures {

  implicit val system = ActorSystem()
  private val node = TestProbe().ref

  private val mapper = new ShardMapper(32)
  for { i <- 0 until 32 } mapper.registerNode(Seq(i), node)

  private def mapperRef = mapper

  private val dataset = MetricsTestData.timeseriesDataset
  private val dsRef = dataset.ref
  private val schemas = Schemas(dataset.schema)

  private val config = ConfigFactory.load("application_test.conf")
  private val queryConfig = new QueryConfig(config.getConfig("filodb.query"))

  private val splitThresholdMs = 20000
  private val splitSizeMs = 10000

  // Splitting timewindow is enabled with threshold at 20secs with splitsize as 10sec
  private val engine = new SingleClusterPlanner(dsRef, schemas, mapperRef, earliestRetainedTimestampFn = 0, queryConfig,
    timeSplitEnabled = true, minTimeRangeForSplitMs = splitThresholdMs, splitSizeMs = splitSizeMs)

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

  val intervalSelector = IntervalSelector(from, to)

  val raw1 = RawSeries(rangeSelector = intervalSelector, filters= f1, columns = Seq("value"))
  val windowed1 = PeriodicSeriesWithWindowing(raw1, from, 1000, to, 5000, RangeFunctionId.Rate)
  val summed1 = Aggregate(AggregationOperator.Sum, windowed1, Nil, Seq("job"))

  val f2 = Seq(ColumnFilter("__name__", Filter.Equals("http_request_duration_seconds_count")),
    ColumnFilter("job", Filter.Equals("myService")))
  val raw2 = RawSeries(rangeSelector = intervalSelector, filters= f2, columns = Seq("value"))
  val windowed2 = PeriodicSeriesWithWindowing(raw2, from, 1000, to, 5000, RangeFunctionId.Rate)
  val summed2 = Aggregate(AggregationOperator.Sum, windowed2, Nil, Seq("job"))
  val promQlQueryParams = PromQlQueryParams("sum(heap_usage)", 100, 1, 1000)

  it ("should generate SplitLocalPartitionDistConcatExec plan with LocalPartitionDistConcatExec child plans" +
        " for LogicalPlan") {
    // final logical plan
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)

    // materialized exec plan
    val parentExecPlan = engine.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))

    /*
    Since threshold
    BinaryJoinExec will be divided into 5 time windows and aggregated
    with LocalPartitionDistConcatExec using StitchRvsMapper transformation

    Following ExecPlan should be generated:

    T~StitchRvsMapper()
    -E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-111191042])
    1#
    --E~BinaryJoinExec(binaryOp=DIV, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-111191042])
    ---T~AggregatePresenter(aggrOp=Sum, aggrParams=List())
    ----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-111191042])
    -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ------T~PeriodicSamplesMapper(start=1597173425483, step=1000, end=1597173435483, window=Some(5000), functionId=Some(Rate), rawSource=true, offsetMs=None)
    -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=15, chunkMethod=TimeRangeChunkScan(1597173125483,1597173435483), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_bucket)), ColumnFilter(job,Equals(myService)), ColumnFilter(le,Equals(0.3))), colName=Some(value), schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-111191042])
    -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ------T~PeriodicSamplesMapper(start=1597173425483, step=1000, end=1597173435483, window=Some(5000), functionId=Some(Rate), rawSource=true, offsetMs=None)
    -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=31, chunkMethod=TimeRangeChunkScan(1597173125483,1597173435483), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_bucket)), ColumnFilter(job,Equals(myService)), ColumnFilter(le,Equals(0.3))), colName=Some(value), schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-111191042])
    ---T~AggregatePresenter(aggrOp=Sum, aggrParams=List())
    ----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-111191042])
    -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ------T~PeriodicSamplesMapper(start=1597173425483, step=1000, end=1597173435483, window=Some(5000), functionId=Some(Rate), rawSource=true, offsetMs=None)
    -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=15, chunkMethod=TimeRangeChunkScan(1597173125483,1597173435483), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_count)), ColumnFilter(job,Equals(myService))), colName=Some(value), schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-111191042])
    -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ------T~PeriodicSamplesMapper(start=1597173425483, step=1000, end=1597173435483, window=Some(5000), functionId=Some(Rate), rawSource=true, offsetMs=None)
    -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=31, chunkMethod=TimeRangeChunkScan(1597173125483,1597173435483), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_count)), ColumnFilter(job,Equals(myService))), colName=Some(value), schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-111191042])
    2#
    --E~BinaryJoinExec(binaryOp=DIV, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-111191042])
    ---T~AggregatePresenter(aggrOp=Sum, aggrParams=List())
    ----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-111191042])
    -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ------T~PeriodicSamplesMapper(start=1597173436483, step=1000, end=1597173446483, window=Some(5000), functionId=Some(Rate), rawSource=true, offsetMs=None)
    -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=15, chunkMethod=TimeRangeChunkScan(1597173136483,1597173446483), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_bucket)), ColumnFilter(job,Equals(myService)), ColumnFilter(le,Equals(0.3))), colName=Some(value), schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-111191042])
    -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ------T~PeriodicSamplesMapper(start=1597173436483, step=1000, end=1597173446483, window=Some(5000), functionId=Some(Rate), rawSource=true, offsetMs=None)
    -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=31, chunkMethod=TimeRangeChunkScan(1597173136483,1597173446483), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_bucket)), ColumnFilter(job,Equals(myService)), ColumnFilter(le,Equals(0.3))), colName=Some(value), schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-111191042])
    ---T~AggregatePresenter(aggrOp=Sum, aggrParams=List())
    ----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-111191042])
    -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ------T~PeriodicSamplesMapper(start=1597173436483, step=1000, end=1597173446483, window=Some(5000), functionId=Some(Rate), rawSource=true, offsetMs=None)
    -------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=15, chunkMethod=TimeRangeChunkScan(1597173136483,1597173446483), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_count)), ColumnFilter(job,Equals(myService))), colName=Some(value), schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-111191042])
    -----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ------T~PeriodicSamplesMapper(start=1597173436483, step=1000, end=1597173446483, window=Some(5000), functionId=Some(Rate), rawSource=true, offsetMs=None)
    -------E~MultiSchemaPartitionsExec.(dataset=timeseries, shard=31, chunkMethod=TimeRangeChunkScan(1597173136483,1597173446483), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_count)), ColumnFilter(job,Equals(myService))), colName=Some(value), schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-111191042])
    3#...
    4#...
    5#...
    */

    parentExecPlan.isInstanceOf[SplitLocalPartitionDistConcatExec] shouldEqual true
    parentExecPlan.children.size shouldEqual 5 // Split query
    parentExecPlan.children.foreach { execPlan =>
      execPlan.isInstanceOf[BinaryJoinExec] shouldEqual true
      execPlan.children.foreach { l1 =>
        // Now there should be single level of reduce because we have 2 shards
        l1.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
        l1.children.foreach { l2 =>
          l2.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
          l2.rangeVectorTransformers.size shouldEqual 2
          l2.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
          l2.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
        }
      }
    }

  }

  it ("should generate SplitLocalPartitionDistConcatExec wrapper plan and parallelize underlying" +
        " binary join aggregation") {
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)

    // materialized exec plan
    val parentExecPlan = engine.materialize(logicalPlan,
      QueryContext(promQlQueryParams, Some(StaticSpreadProvider(SpreadChange(0, 4))), 1000000))

    parentExecPlan.isInstanceOf[SplitLocalPartitionDistConcatExec] shouldEqual true
    parentExecPlan.children.foreach { execPlan =>
      execPlan.isInstanceOf[BinaryJoinExec] shouldEqual true

      // Now there should be multiple levels of reduce because we have 16 shards
      execPlan.children.foreach { l1 =>
        l1.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
        l1.children.foreach { l2 =>
          l2.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
          l2.children.foreach { l3 =>
            l3.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
            l3.rangeVectorTransformers.size shouldEqual 2
            l3.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
            l3.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
          }
        }
      }
    }
  }

  it("should generate SplitLocalPartitionDistConcatExec wrapper plan and" +
      " materialize underlying ExecPlans correctly for _bucket_ histogram queries") {
    val lp = Parser.queryRangeToLogicalPlan("""rate(foo{job="bar",_bucket_="2.5"}[5m])""",
      TimeStepParams(310, 10, 400))

    info(s"LogicalPlan is $lp")
    lp match {
      case p: PeriodicSeriesWithWindowing => p.series.isInstanceOf[ApplyInstantFunctionRaw] shouldEqual true
      case _ => throw new IllegalArgumentException(s"Unexpected LP $lp")
    }

    val parentExecPlan = engine.materialize(lp,
      QueryContext(promQlQueryParams, Some(StaticSpreadProvider(SpreadChange(0, 4))), 1000000))

    info(s"First inner child plan: ${parentExecPlan.children.head.children.head.printTree()}")
    parentExecPlan.isInstanceOf[SplitLocalPartitionDistConcatExec] shouldEqual true
    parentExecPlan.children.size shouldEqual 5 // 1 + (to-from)/splitThreshold
    parentExecPlan.children.foreach { execPlan =>
      execPlan.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
      execPlan.children.foreach { l1 =>
        l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
        l1.rangeVectorTransformers.size shouldEqual 2
        l1.rangeVectorTransformers(0).isInstanceOf[InstantVectorFunctionMapper] shouldEqual true
        l1.rangeVectorTransformers(1).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
        l1.rangeVectorTransformers(1).asInstanceOf[PeriodicSamplesMapper].rawSource shouldEqual false
      }
    }

  }

  it("should generate SplitLocalPartitionDistConcatExec wrapper plan with appropriate splits," +
        "should generate child ExecPlans with appropriate shards for windows when there is a change in spread") {
    var filodbSpreadMap = new collection.mutable.HashMap[collection.Map[String, String], Int]
    filodbSpreadMap.put(collection.Map(("job" -> "myService")), 2)

    val stepMs = 1000

    val spreadFunc = QueryContext.simpleMapSpreadFunc(Seq("job"), filodbSpreadMap, 1)

    // final logical plan
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)

    // materialized exec plan
    val parentExecPlan = engine.materialize(logicalPlan, QueryContext(promQlQueryParams, Some(FunctionalSpreadProvider(spreadFunc)), 1000000))
    parentExecPlan.printTree()

    parentExecPlan.isInstanceOf[SplitLocalPartitionDistConcatExec] shouldEqual true
    parentExecPlan.children.size shouldEqual 5 // 5 splits = ((from - to) / splitSize)

    // assert that all window splits have proper start/end times
    parentExecPlan.children.zipWithIndex.foreach { case (exec, i) =>
      val rangeFrom = from + (splitSizeMs * i) + (stepMs * i)
      val rangeTo = min(rangeFrom + splitSizeMs, to)
      exec.children.head.children.head.rangeVectorTransformers
        .head.asInstanceOf[PeriodicSamplesMapper].start shouldEqual rangeFrom
      exec.children.head.children.head.rangeVectorTransformers
        .head.asInstanceOf[PeriodicSamplesMapper].end shouldEqual rangeTo
    }

    // assert that all child plans are not altered
    parentExecPlan.children.foreach { execPlan =>
      execPlan.isInstanceOf[BinaryJoinExec] shouldEqual true
      execPlan.children should have length (2)
      execPlan.children.foreach { reduceAggPlan =>
        reduceAggPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
        reduceAggPlan.children should have length (4)   // spread=2 means 4 shards
      }
    }
  }

  it("should generate SplitExec wrapper and should stitch child execplan results" +
      "when spread changes during query window") {
    val lp = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""", TimeStepParams(310, 10, 400))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(350000, 2)) // spread change time is in ms
    }
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams,
                                      Some(FunctionalSpreadProvider(spread)), 1000000))
    execPlan.isInstanceOf[SplitLocalPartitionDistConcatExec] shouldEqual true
    execPlan.rangeVectorTransformers.head.isInstanceOf[StitchRvsMapper] shouldEqual true // Stitch split plans
    execPlan.children should have length (5)

    // Inner StitchPlan due to spread change
    execPlan.children.head.rangeVectorTransformers.isEmpty shouldEqual true
    execPlan.children(1).rangeVectorTransformers.isEmpty shouldEqual true
    execPlan.children(2).rangeVectorTransformers.head.isInstanceOf[StitchRvsMapper] shouldEqual true // Spread changes here
    execPlan.children(3).rangeVectorTransformers.head.isInstanceOf[StitchRvsMapper] shouldEqual true
    execPlan.children.last.rangeVectorTransformers.head.isInstanceOf[StitchRvsMapper] shouldEqual true
  }

  it("should generate SplitExec wrapper and should not stitch results when spread has not changed in query range") {
    val lp = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""", TimeStepParams(310, 10, 400))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(450000, 2)) // spread change time is in ms
    }
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams,
      Some(FunctionalSpreadProvider(spread)), 1000000))
    execPlan.isInstanceOf[SplitLocalPartitionDistConcatExec] shouldEqual true
    execPlan.rangeVectorTransformers.head.isInstanceOf[StitchRvsMapper] shouldEqual true // Stitch split plans
    execPlan.children should have length (5)

    // no StitchPlan since there are no spread changes in the window
    execPlan.children.head.rangeVectorTransformers.isEmpty shouldEqual true
    execPlan.children(1).rangeVectorTransformers.isEmpty shouldEqual true
    execPlan.children(2).rangeVectorTransformers.isEmpty shouldEqual true
    execPlan.children(3).rangeVectorTransformers.isEmpty shouldEqual true
    execPlan.children.last.rangeVectorTransformers.isEmpty shouldEqual true
  }

  it("should generate SplitExec wrapper with appropriate splits " +
      "and should stitch child results before binary join when spread changed in query range") {
    val lp = Parser.queryRangeToLogicalPlan("""count(foo{job="bar"} + baz{job="bar"})""",
      TimeStepParams(310, 10, 400))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(350000, 2))
    }
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, Some(FunctionalSpreadProvider(spread)),
                                      1000000))

    execPlan.isInstanceOf[SplitLocalPartitionDistConcatExec] shouldEqual true
    execPlan.rangeVectorTransformers.head.isInstanceOf[StitchRvsMapper] shouldEqual true // Stitch split plans
    execPlan.children should have length (5)

    val binaryJoinNode0 = execPlan.children.head.children.head
    val binaryJoinNode1 = execPlan.children(1).children.head
    val binaryJoinNode2 = execPlan.children(2).children.head // Spread changes here
    val binaryJoinNode3 = execPlan.children(3).children.head
    val binaryJoinNode4 = execPlan.children(4).children.head

    binaryJoinNode0.isInstanceOf[BinaryJoinExec] shouldEqual true
    binaryJoinNode0.children.foreach(_.isInstanceOf[StitchRvsExec] shouldEqual false)

    binaryJoinNode1.isInstanceOf[BinaryJoinExec] shouldEqual true
    binaryJoinNode1.children.foreach(_.isInstanceOf[StitchRvsExec] shouldEqual false)

    binaryJoinNode2.isInstanceOf[BinaryJoinExec] shouldEqual true
    binaryJoinNode2.children.foreach(_.isInstanceOf[StitchRvsExec] shouldEqual true) // Spread changes here

    binaryJoinNode3.isInstanceOf[BinaryJoinExec] shouldEqual true
    binaryJoinNode3.children.foreach(_.isInstanceOf[StitchRvsExec] shouldEqual true)

    binaryJoinNode4.isInstanceOf[BinaryJoinExec] shouldEqual true
    binaryJoinNode4.children.foreach(_.isInstanceOf[StitchRvsExec] shouldEqual true)
  }

  it("should generate SplitExec wrapper with appropriate splits and" +
    " should not stitch child results before binary join when spread has not changed in query range") {
    val lp = Parser.queryRangeToLogicalPlan("""count(foo{job="bar"} + baz{job="bar"})""",
      TimeStepParams(310, 10, 400))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(450000, 2))
    }
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, Some(FunctionalSpreadProvider(spread)),
      1000000))

    execPlan.isInstanceOf[SplitLocalPartitionDistConcatExec] shouldEqual true
    execPlan.rangeVectorTransformers.head.isInstanceOf[StitchRvsMapper] shouldEqual true // Stitch split plans
    execPlan.children should have length (5)

    execPlan.children.foreach { localPartPlan =>
      val binaryJoinNode = localPartPlan.children.head
      binaryJoinNode.isInstanceOf[BinaryJoinExec] shouldEqual true
      binaryJoinNode.children should have length(8) // lhs(4) + rhs(4)
      binaryJoinNode.children.foreach(_.isInstanceOf[StitchRvsExec] shouldEqual false)
    }
  }

  it ("should generate SplitExec wrapper with appropriate splits and" +
        " should generate child SetOperatorExec for LogicalPlan with Set operator") {
    // final logical plan
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.LAND, Cardinality.ManyToMany, summed2)

    // materialized exec plan
    val parentExecPlan = engine.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))

    parentExecPlan.isInstanceOf[SplitLocalPartitionDistConcatExec] shouldEqual true
      parentExecPlan.rangeVectorTransformers.head.isInstanceOf[StitchRvsMapper] shouldEqual true

    parentExecPlan.children.foreach { execPlan =>
      execPlan.isInstanceOf[SetOperatorExec] shouldEqual true
      execPlan.children.foreach { l1 =>
        // Now there should be single level of reduce because we have 2 shards
        l1.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
        l1.children.foreach { l2 =>
          l2.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
          l2.rangeVectorTransformers.size shouldEqual 2
          l2.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
          l2.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
        }
      }
    }

  }

  it("should generate SplitExec wrapper with appropriate splits " +
      " and generate child EmptyResultExec if range is outside retention period " +
      " should bound queries until retention period and drop instants outside retention period") {
    val nowSeconds = System.currentTimeMillis() / 1000
     val planner = new SingleClusterPlanner(dsRef, schemas, mapperRef,
       earliestRetainedTimestampFn = nowSeconds * 1000 - 3.days.toMillis, queryConfig,
       timeSplitEnabled = true, minTimeRangeForSplitMs = 1.day.toMillis, splitSizeMs = 1.day.toMillis)

    // Case 1: no offset or window
    val logicalPlan1 = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""",
      TimeStepParams(nowSeconds - 4.days.toSeconds, 1.minute.toSeconds, nowSeconds))

    val splitEp1 = planner.materialize(logicalPlan1, QueryContext()).asInstanceOf[SplitLocalPartitionDistConcatExec]
    splitEp1.children should have length(4)

    splitEp1.children.head.isInstanceOf[EmptyResultExec] shouldEqual true // outside retention period

    val ep11 = splitEp1.children(1)
    val psm11 = ep11.children.head.asInstanceOf[MultiSchemaPartitionsExec]
                .rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper]
    psm11.start shouldEqual (nowSeconds * 1000
                            - 3.days.toMillis // retention
                            + 1.minute.toMillis // step
                            + WindowConstants.staleDataLookbackMillis) // default window
    psm11.end shouldEqual psm11.start - WindowConstants.staleDataLookbackMillis + 1.day.toMillis

    val ep12 = splitEp1.children(2)
    val psm12 = ep12.children.head.asInstanceOf[MultiSchemaPartitionsExec]
                .rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper]
    psm12.start shouldEqual psm11.end + 1.minute.toMillis // step
    psm12.end shouldEqual psm12.start + 1.day.toMillis

    val ep13 = splitEp1.children(3)
    val psm13 = ep13.children.head.asInstanceOf[MultiSchemaPartitionsExec]
      .rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper]
    psm13.start shouldEqual psm12.end + 1.minute.toMillis // step
    psm13.end shouldEqual min(psm13.start + 1.day.toMillis, nowSeconds*1000)

    // Case 2: no offset, some window
    val logicalPlan2 = Parser.queryRangeToLogicalPlan("""rate(foo{job="bar"}[20m])""",
      TimeStepParams(nowSeconds - 4.days.toSeconds, 1.minute.toSeconds, nowSeconds))
    val splitEp2 = planner.materialize(logicalPlan2, QueryContext()).asInstanceOf[SplitLocalPartitionDistConcatExec]
    splitEp2.children should have length(4)

    splitEp2.children.head.isInstanceOf[EmptyResultExec] shouldEqual true
    val ep21 = splitEp2.children(1)
    val psm21 = ep21.children.head.asInstanceOf[MultiSchemaPartitionsExec]
      .rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper]
    psm21.start shouldEqual (nowSeconds * 1000
      - 3.days.toMillis // retention
      + 1.minute.toMillis // step
      + 20.minutes.toMillis) // window
    psm21.end shouldEqual psm21.start - 20.minutes.toMillis + 1.day.toMillis

    // Case 3: offset and some window
    val logicalPlan3 = Parser.queryRangeToLogicalPlan("""rate(foo{job="bar"}[20m] offset 15m)""",
      TimeStepParams(nowSeconds - 4.days.toSeconds, 1.minute.toSeconds, nowSeconds))

    val splitEp3 = planner.materialize(logicalPlan3, QueryContext()).asInstanceOf[SplitLocalPartitionDistConcatExec]
    splitEp3.children should have length(4)

    splitEp3.children.head.isInstanceOf[EmptyResultExec] shouldEqual true
    val ep31 = splitEp3.children(1)
    val psm31 = ep31.children.head.asInstanceOf[MultiSchemaPartitionsExec]
      .rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper]
    psm31.start shouldEqual (nowSeconds * 1000
      - 3.days.toMillis // retention
      + 1.minute.toMillis // step
      + 20.minutes.toMillis  // window
      + 15.minutes.toMillis) // offset
    psm31.end shouldEqual psm31.start - 20.minutes.toMillis - 15.minutes.toMillis + 1.day.toMillis

    // Case 4: outside retention
    val logicalPlan4 = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""",
      TimeStepParams(nowSeconds - 10.days.toSeconds, 1.minute.toSeconds, nowSeconds - 5.days.toSeconds))
    val ep4 = planner.materialize(logicalPlan4, QueryContext())
    ep4.children should have length (5)
    ep4.children.foreach { childPlan =>
      childPlan.isInstanceOf[EmptyResultExec] shouldEqual true
      import filodb.core.GlobalScheduler._
      val res = childPlan.dispatcher.dispatch(childPlan).runAsync.futureValue.asInstanceOf[QueryResult]
      res.result.isEmpty shouldEqual true
    }
  }

  it("should generate SplitExec wrapper with appropriate splits " +
      " and should not split underlying queries with step > splitSize and materialize instant queries " +
      "with lookback == retention correctly") {
    val nowSeconds = System.currentTimeMillis() / 1000
    val planner = new SingleClusterPlanner(dsRef, schemas, mapperRef,
      earliestRetainedTimestampFn = nowSeconds * 1000 - 3.days.toMillis, queryConfig,
      timeSplitEnabled = true, minTimeRangeForSplitMs = 1.day.toMillis, splitSizeMs = 1.day.toMillis)

    val logicalPlan = Parser.queryRangeToLogicalPlan("""sum(rate(foo{job="bar"}[3d]))""",
      TimeStepParams(nowSeconds, 1.minute.toSeconds, nowSeconds))

    val ep = planner.materialize(logicalPlan, QueryContext()).asInstanceOf[LocalPartitionReduceAggregateExec]
    val psm = ep.children.head.asInstanceOf[MultiSchemaPartitionsExec]
      .rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper]
    psm.start shouldEqual (nowSeconds * 1000)
    psm.end shouldEqual (nowSeconds * 1000)
  }

  it("should generate SplitExec wrapper with appropriate splits and should generate child execPlans with offset") {
    val t = TimeStepParams(700, 1000, 10000)
    val lp = Parser.queryRangeToLogicalPlan("http_requests_total{job = \"app\"} offset 5m", t)
    val periodicSeries = lp.asInstanceOf[PeriodicSeries]
    periodicSeries.startMs shouldEqual 700000
    periodicSeries.endMs shouldEqual 10000000
    periodicSeries.stepMs shouldEqual 1000000

    val engine2 = new SingleClusterPlanner(dsRef, schemas, mapperRef, earliestRetainedTimestampFn = 0, queryConfig,
      timeSplitEnabled = true, minTimeRangeForSplitMs = 5000000, splitSizeMs = 2000000)

    val parentExecPlan = engine2.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    parentExecPlan.children should have length(4)
    parentExecPlan.children.zipWithIndex.foreach { case(execPlan, i) =>
      val stepAndSplit = (2000 + 1000) * i
      val expChunkScanStart = (t.start - 300 - 300 + stepAndSplit).seconds.toMillis
      val expChunkScanEnd = min(expChunkScanStart + (300 + 2000).seconds.toMillis, periodicSeries.endMs - 300000)
      val expRvtStart = (t.start + stepAndSplit).seconds.toMillis
      val expRvtEnd = min((expRvtStart + 2000000), periodicSeries.endMs)
      val expRvtStartOffset = (t.start - 300 + stepAndSplit).seconds.toMillis
      val expRvtEndOffset = min(expRvtStartOffset + 2000000, periodicSeries.endMs - 300000)
      execPlan.children(0).isInstanceOf[MultiSchemaPartitionsExec] shouldEqual(true)
      val multiSchemaExec = execPlan.children(0).asInstanceOf[MultiSchemaPartitionsExec]
      multiSchemaExec.chunkMethod.asInstanceOf[TimeRangeChunkScan].startTime shouldEqual(expChunkScanStart) //(700 - 300 - 300)*1000
      multiSchemaExec.chunkMethod.asInstanceOf[TimeRangeChunkScan].endTime shouldEqual(expChunkScanEnd) //(700 + 2000 - 300)*1000

      multiSchemaExec.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual(true)
      val rvt = multiSchemaExec.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper]
      rvt.offsetMs.get shouldEqual 300000
      rvt.startWithOffset shouldEqual(expRvtStartOffset) // (700 - 300) * 1000
      rvt.endWithOffset shouldEqual (expRvtEndOffset) // (10000 - 300) * 1000
      rvt.start shouldEqual expRvtStart // start and end should be same as query TimeStepParams
      rvt.end shouldEqual expRvtEnd
      rvt.step shouldEqual 1000000
    }

  }

  it("should generate SplitExec wrapper with appropriate splits " +
      "and should generate child execPlans with offset with window") {
    val t = TimeStepParams(700, 1000, 10000)
    val lp = Parser.queryRangeToLogicalPlan("rate(http_requests_total{job = \"app\"}[5m] offset 5m)", t)
    val periodicSeries = lp.asInstanceOf[PeriodicSeriesWithWindowing]
    periodicSeries.startMs shouldEqual 700000
    periodicSeries.endMs shouldEqual 10000000
    periodicSeries.stepMs shouldEqual 1000000

    val engine2 = new SingleClusterPlanner(dsRef, schemas, mapperRef, earliestRetainedTimestampFn = 0, queryConfig,
      timeSplitEnabled = true, minTimeRangeForSplitMs = 5000000, splitSizeMs = 2000000)

    val parentExecPlan = engine2.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    parentExecPlan.children should have length(4)
    parentExecPlan.children.zipWithIndex.foreach { case(execPlan, i) =>
      val stepAndSplit = (2000 + 1000) * i
      val expChunkScanStart = (t.start - 300 - 300 + stepAndSplit).seconds.toMillis
      val expChunkScanEnd = min(expChunkScanStart + (300 + 2000).seconds.toMillis, periodicSeries.endMs - 300000)
      val expRvtStart = (t.start + stepAndSplit).seconds.toMillis
      val expRvtEnd = min((expRvtStart + 2000000), periodicSeries.endMs)
      val expRvtStartOffset = (t.start - 300 + stepAndSplit).seconds.toMillis
      val expRvtEndOffset = min(expRvtStartOffset + 2000000, periodicSeries.endMs - 300000)
      execPlan.children(0).isInstanceOf[MultiSchemaPartitionsExec] shouldEqual(true)
      val multiSchemaExec = execPlan.children(0).asInstanceOf[MultiSchemaPartitionsExec]
      multiSchemaExec.chunkMethod.asInstanceOf[TimeRangeChunkScan].startTime shouldEqual(expChunkScanStart) //(700 - 300 - 300)*1000
      multiSchemaExec.chunkMethod.asInstanceOf[TimeRangeChunkScan].endTime shouldEqual(expChunkScanEnd) //(700 + 2000 - 300)*1000

      multiSchemaExec.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual(true)
      val rvt = multiSchemaExec.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper]
      rvt.offsetMs.get shouldEqual 300000
      rvt.startWithOffset shouldEqual(expRvtStartOffset) // (700 - 300) * 1000
      rvt.endWithOffset shouldEqual (expRvtEndOffset) // (10000 - 300) * 1000
      rvt.start shouldEqual expRvtStart // start and end should be same as query TimeStepParams
      rvt.end shouldEqual expRvtEnd
      rvt.step shouldEqual 1000000
    }
  }

  it ("should generate SplitExec wrapper with appropriate splits" +
        " and should replace __name__ with _metric_ in by and without in underlying execplans") {
    val dataset = MetricsTestData.timeseriesDatasetWithMetric
    val dsRef = dataset.ref
    val schemas = Schemas(dataset.schema)

    val engine = new SingleClusterPlanner(dsRef, schemas, mapperRef, earliestRetainedTimestampFn = 0, queryConfig,
      timeSplitEnabled = true, minTimeRangeForSplitMs = 200000, splitSizeMs = 100000)

    val logicalPlan1 = Parser.queryRangeToLogicalPlan("""sum(foo{_ns_="bar", _ws_="test"}) by (__name__)""",
      TimeStepParams(1000, 20, 2000))

    val execPlan1 = engine.materialize(logicalPlan1, QueryContext(origQueryParams = promQlQueryParams))
    execPlan1.isInstanceOf[SplitLocalPartitionDistConcatExec] shouldEqual true

    execPlan1.children.foreach { childPlan =>
      childPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
      childPlan.children.foreach { l1 =>
        l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
        l1.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
        l1.rangeVectorTransformers(1).asInstanceOf[AggregateMapReduce].by shouldEqual List("_metric_")
      }
    }

    val logicalPlan2 = Parser.queryRangeToLogicalPlan(
      """sum(foo{_ns_="bar", _ws_="test"})
        |without (__name__, instance)""".stripMargin,
      TimeStepParams(1000, 20, 2000))

    // materialized exec plan
    val execPlan2 = engine.materialize(logicalPlan2, QueryContext(origQueryParams = promQlQueryParams))
    execPlan2.isInstanceOf[SplitLocalPartitionDistConcatExec] shouldEqual true

    execPlan2.children.foreach { childPlan =>
      childPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
      childPlan.children.foreach { l1 =>
        l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
        l1.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
        l1.rangeVectorTransformers(1).asInstanceOf[AggregateMapReduce].without shouldEqual List("_metric_", "instance")
      }
    }
  }

  it ("should generate SplitExec wrapper with appropriate splits" +
      " and should replace __name__ with _metric_ in ignoring and group_left/group_right in child execplans") {
    val dataset = MetricsTestData.timeseriesDatasetWithMetric
    val dsRef = dataset.ref
    val schemas = Schemas(dataset.schema)

    val engine = new SingleClusterPlanner(dsRef, schemas, mapperRef, earliestRetainedTimestampFn = 0, queryConfig,
                                        timeSplitEnabled = true, minTimeRangeForSplitMs = 200000, splitSizeMs = 100000)

    val logicalPlan1 = Parser.queryRangeToLogicalPlan(
      """sum(foo{_ns_="bar1", _ws_="test"}) + ignoring(__name__)
        | sum(foo{_ns_="bar2", _ws_="test"})""".stripMargin,
      TimeStepParams(1000, 20, 2000))
    val execPlan1 = engine.materialize(logicalPlan1, QueryContext(origQueryParams = promQlQueryParams))
    execPlan1.isInstanceOf[SplitLocalPartitionDistConcatExec] shouldEqual true
    execPlan1.children should have length(9)

    execPlan1.children.foreach { childPlan =>
      childPlan.isInstanceOf[BinaryJoinExec] shouldEqual true
      childPlan.asInstanceOf[BinaryJoinExec].ignoring shouldEqual Seq("_metric_")
    }

    val logicalPlan2 = Parser.queryRangeToLogicalPlan(
      """sum(foo{_ns_="bar1", _ws_="test"}) + group_left(__name__)
        | sum(foo{_ns_="bar2", _ws_="test"})""".stripMargin,
      TimeStepParams(1000, 20, 2000))
    val execPlan2 = engine.materialize(logicalPlan2, QueryContext(origQueryParams = promQlQueryParams))

    execPlan2.isInstanceOf[SplitLocalPartitionDistConcatExec] shouldEqual true
    execPlan2.children should have length(9)

    execPlan2.children.foreach { childPlan =>
      childPlan.isInstanceOf[BinaryJoinExec] shouldEqual true
      childPlan.asInstanceOf[BinaryJoinExec].include shouldEqual Seq("_metric_")
    }
  }

  it("should generate SplitExec wrapper with appropriate splits" +
      " and should generate execPlan for binary join with offset in underlying child plans") {
    val t = TimeStepParams(700, 1000, 10000)
    val lp = Parser.queryRangeToLogicalPlan("rate(http_requests_total{job = \"app\"}[5m] offset 5m) / " +
      "rate(http_requests_total{job = \"app\"}[5m])", t)

    val engine2 = new SingleClusterPlanner(dsRef, schemas, mapperRef, earliestRetainedTimestampFn = 0, queryConfig,
      timeSplitEnabled = true, minTimeRangeForSplitMs = 5000000, splitSizeMs = 2000000)

    val periodicSeriesPlan = lp.asInstanceOf[BinaryJoin]
    periodicSeriesPlan.startMs shouldEqual 700000
    periodicSeriesPlan.endMs shouldEqual 10000000
    periodicSeriesPlan.stepMs shouldEqual 1000000

    val parentExecPlan = engine2.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    parentExecPlan.isInstanceOf[SplitLocalPartitionDistConcatExec] shouldEqual true
    parentExecPlan.children should have length(4)

    parentExecPlan.children.zipWithIndex.foreach { case(execPlan, i) =>
      execPlan.isInstanceOf[BinaryJoinExec] shouldEqual(true)
      val binaryJoin = execPlan.asInstanceOf[BinaryJoinExec]
      val stepAndSplit = (2000 + 1000) * i
      val expChunkScanStartOffset = (t.start - 300 - 300 + stepAndSplit).seconds.toMillis
      val expChunkScanStart = (t.start - 300 + stepAndSplit).seconds.toMillis
      val expChunkScanEndOffset = min(expChunkScanStartOffset + (300 + 2000).seconds.toMillis, periodicSeriesPlan.endMs - 300000)
      val expChunkScanEnd = min(expChunkScanStart + (300 + 2000).seconds.toMillis, periodicSeriesPlan.endMs)
      val expRvtStart = (t.start + stepAndSplit).seconds.toMillis
      val expRvtEnd = min((expRvtStart + 2000000), periodicSeriesPlan.endMs)
      val expRvtStartOffset = (t.start - 300 + stepAndSplit).seconds.toMillis
      val expRvtEndOffset = min(expRvtStartOffset + 2000000, periodicSeriesPlan.endMs - 300000)
      binaryJoin.lhs(0).isInstanceOf[MultiSchemaPartitionsExec] shouldEqual(true)
      val multiSchemaExec1 = binaryJoin.lhs(0).asInstanceOf[MultiSchemaPartitionsExec]
      multiSchemaExec1.chunkMethod.asInstanceOf[TimeRangeChunkScan].startTime shouldEqual(expChunkScanStartOffset)
      multiSchemaExec1.chunkMethod.asInstanceOf[TimeRangeChunkScan].endTime shouldEqual(expChunkScanEndOffset)

      multiSchemaExec1.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual(true)
      val rvt1 = multiSchemaExec1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper]
      rvt1.offsetMs.get shouldEqual(300000)
      rvt1.startWithOffset shouldEqual(expRvtStartOffset) // (700 - 300) * 1000
      rvt1.endWithOffset shouldEqual (expRvtEndOffset) // (10000 - 300) * 1000
      rvt1.start shouldEqual expRvtStart
      rvt1.end shouldEqual expRvtEnd
      rvt1.step shouldEqual 1000000

      binaryJoin.rhs(0).isInstanceOf[MultiSchemaPartitionsExec] shouldEqual(true)
      val multiSchemaExec2 = binaryJoin.rhs(0).asInstanceOf[MultiSchemaPartitionsExec]
      multiSchemaExec2.chunkMethod.asInstanceOf[TimeRangeChunkScan].startTime shouldEqual(expChunkScanStart) // (700 - 300) * 1000
      multiSchemaExec2.chunkMethod.asInstanceOf[TimeRangeChunkScan].endTime shouldEqual(expChunkScanEnd)

      multiSchemaExec2.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual(true)
      val rvt2 = multiSchemaExec2.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper]
      // No offset in rhs
      rvt2.offsetMs.isEmpty shouldEqual true
      rvt2.startWithOffset shouldEqual(expRvtStart)
      rvt2.endWithOffset shouldEqual (expRvtEnd)
      rvt2.start shouldEqual expRvtStart
      rvt2.end shouldEqual expRvtEnd
      rvt2.step shouldEqual 1000000
    }

  }

}
