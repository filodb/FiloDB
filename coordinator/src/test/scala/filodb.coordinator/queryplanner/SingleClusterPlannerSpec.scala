package filodb.coordinator.queryplanner

import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.{FunctionalSpreadProvider, StaticSpreadProvider}
import filodb.core.{GlobalScheduler, MetricsTestData, SpreadChange}
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter, PromQlQueryParams, QueryContext}
import filodb.core.store.TimeRangeChunkScan
import filodb.prometheus.ast.{TimeStepParams, WindowConstants}
import filodb.prometheus.parse.Parser
import filodb.query._
import filodb.query.exec._

class SingleClusterPlannerSpec extends FunSpec with Matchers with ScalaFutures {

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

  private val engine = new SingleClusterPlanner(dsRef, schemas, mapperRef, earliestRetainedTimestampFn = 0, queryConfig)

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
  val promQlQueryParams = PromQlQueryParams(ConfigFactory.empty, "sum(heap_usage)", 100, 1, 1000, None)

  it ("should generate ExecPlan for LogicalPlan") {
    // final logical plan
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)

    // materialized exec plan
    val execPlan = engine.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))

    /*
    Following ExecPlan should be generated:

    BinaryJoinExec(binaryOp=DIV, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-4#-325843755])
    -AggregatePresenter(aggrOp=Sum, aggrParams=List())
    --ReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-4#-325843755])
    ---AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ----PeriodicSamplesMapper(start=1526094025509, step=1000, end=1526094075509, window=Some(5000), functionId=Some(Rate), funcParams=List())
    -----MultiSchemaPartitionsExec(shard=2, rowKeyRange=RowKeyInterval(b[1526094025509],b[1526094075509]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_bucket)), ColumnFilter(job,Equals(myService)), ColumnFilter(le,Equals(0.3)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#342951049])
    ---AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ----PeriodicSamplesMapper(start=1526094025509, step=1000, end=1526094075509, window=Some(5000), functionId=Some(Rate), funcParams=List())
    -----SelectRawPartitionsExec(shard=3, rowKeyRange=RowKeyInterval(b[1526094025509],b[1526094075509]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_bucket)), ColumnFilter(job,Equals(myService)), ColumnFilter(le,Equals(0.3)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-4#-325843755])
    -AggregatePresenter(aggrOp=Sum, aggrParams=List())
    --ReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-2#-1576910232])
    ---AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ----PeriodicSamplesMapper(start=1526094025509, step=1000, end=1526094075509, window=Some(5000), functionId=Some(Rate), funcParams=List())
    -----SelectRawPartitionsExec(shard=0, rowKeyRange=RowKeyInterval(b[1526094025509],b[1526094075509]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_count)), ColumnFilter(job,Equals(myService)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-238515561])
    ---AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ----PeriodicSamplesMapper(start=1526094025509, step=1000, end=1526094075509, window=Some(5000), functionId=Some(Rate), funcParams=List())
    -----SelectRawPartitionsExec(shard=1, rowKeyRange=RowKeyInterval(b[1526094025509],b[1526094075509]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_count)), ColumnFilter(job,Equals(myService)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-2#-1576910232])
    */

    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual true
    execPlan.children.foreach { l1 =>
      // Now there should be single level of reduce because we have 2 shards
      l1.isInstanceOf[ReduceAggregateExec] shouldEqual true
      l1.children.foreach { l2 =>
        l2.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
        l2.rangeVectorTransformers.size shouldEqual 2
        l2.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
        l2.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
      }
    }
  }

  it ("should parallelize aggregation") {
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)

    // materialized exec plan
    val execPlan = engine.materialize(logicalPlan,
      QueryContext(promQlQueryParams, Some(StaticSpreadProvider(SpreadChange(0, 4))), 1000000))
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual true

    // Now there should be multiple levels of reduce because we have 16 shards
    execPlan.children.foreach { l1 =>
      l1.isInstanceOf[ReduceAggregateExec] shouldEqual true
      l1.children.foreach { l2 =>
        l2.isInstanceOf[ReduceAggregateExec] shouldEqual true
        l2.children.foreach { l3 =>
          l3.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
          l3.rangeVectorTransformers.size shouldEqual 2
          l3.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
          l3.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
        }
      }
    }
  }

  it("should materialize ExecPlan correctly for _bucket_ histogram queries") {
    val lp = Parser.queryRangeToLogicalPlan("""rate(foo{job="bar",_bucket_="2.5"}[5m])""",
      TimeStepParams(20000, 100, 30000))

    info(s"LogicalPlan is $lp")
    lp match {
      case p: PeriodicSeriesWithWindowing => p.series.isInstanceOf[ApplyInstantFunctionRaw] shouldEqual true
      case _ => throw new IllegalArgumentException(s"Unexpected LP $lp")
    }

    val execPlan = engine.materialize(lp,
      QueryContext(promQlQueryParams, Some(StaticSpreadProvider(SpreadChange(0, 4))), 1000000))

    info(s"First child plan: ${execPlan.children.head.printTree()}")
    execPlan.isInstanceOf[DistConcatExec] shouldEqual true
    execPlan.children.foreach { l1 =>
      l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
      l1.rangeVectorTransformers.size shouldEqual 2
      l1.rangeVectorTransformers(0).isInstanceOf[InstantVectorFunctionMapper] shouldEqual true
      l1.rangeVectorTransformers(1).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
      l1.rangeVectorTransformers(1).asInstanceOf[PeriodicSamplesMapper].rawSource shouldEqual false
    }
  }

  import com.softwaremill.quicklens._

  it("should rename Prom __name__ filters if dataset has different metric column") {
    // Custom SingleClusterPlanner with different dataset with different metric name
    val datasetOpts = dataset.options.copy(metricColumn = "kpi", shardKeyColumns = Seq("kpi", "job"))
    val dataset2 = dataset.modify(_.schema.partition.options).setTo(datasetOpts)
    val engine2 = new SingleClusterPlanner(dataset2.ref, Schemas(dataset2.schema), mapperRef,
      0, queryConfig)

    // materialized exec plan
    val execPlan = engine2.materialize(raw2, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[DistConcatExec] shouldEqual true
    execPlan.children.foreach { l1 =>
      l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
      val rpExec = l1.asInstanceOf[MultiSchemaPartitionsExec]
      rpExec.filters.map(_.column).toSet shouldEqual Set("kpi", "job")
    }
  }

  it("should use spread function to change/override spread and generate ExecPlan with appropriate shards") {
    var filodbSpreadMap = new collection.mutable.HashMap[collection.Map[String, String], Int]
    filodbSpreadMap.put(collection.Map(("job" -> "myService")), 2)

    val spreadFunc = QueryContext.simpleMapSpreadFunc(Seq("job"), filodbSpreadMap, 1)

    // final logical plan
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)

    // materialized exec plan
    val execPlan = engine.materialize(logicalPlan, QueryContext(promQlQueryParams, Some(FunctionalSpreadProvider(spreadFunc)), 1000000))
    execPlan.printTree()

    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual true
    execPlan.children should have length (2)
    execPlan.children.foreach { reduceAggPlan =>
      reduceAggPlan.isInstanceOf[ReduceAggregateExec] shouldEqual true
      reduceAggPlan.children should have length (4)   // spread=2 means 4 shards
    }
  }

  it("should stitch results when spread changes during query range") {
    val lp = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""", TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(25000000, 2)) // spread change time is in ms
    }
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams,
                                      Some(FunctionalSpreadProvider(spread)), 1000000))
    execPlan.rangeVectorTransformers.head.isInstanceOf[StitchRvsMapper] shouldEqual true
  }

  it("should not stitch results when spread has not changed in query range") {
    val lp = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""", TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(35000000, 2))
    }
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams,
                                      Some(FunctionalSpreadProvider(spread)), 1000000))
    execPlan.rangeVectorTransformers.isEmpty shouldEqual true
  }

  it("should stitch results before binary join when spread changed in query range") {
    val lp = Parser.queryRangeToLogicalPlan("""count(foo{job="bar"} + baz{job="bar"})""",
      TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(25000000, 2))
    }
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, Some(FunctionalSpreadProvider(spread)),
                                      1000000))
    val binaryJoinNode = execPlan.children(0)
    binaryJoinNode.isInstanceOf[BinaryJoinExec] shouldEqual true
    binaryJoinNode.children.size shouldEqual 2
    binaryJoinNode.children.foreach(_.isInstanceOf[StitchRvsExec] shouldEqual true)
  }

  it("should not stitch results before binary join when spread has not changed in query range") {
    val lp = Parser.queryRangeToLogicalPlan("""count(foo{job="bar"} + baz{job="bar"})""",
      TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(35000000, 2))
    }
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, Some(FunctionalSpreadProvider(spread)),
                                      1000000))
    val binaryJoinNode = execPlan.children(0)
    binaryJoinNode.isInstanceOf[BinaryJoinExec] shouldEqual true
    binaryJoinNode.children.foreach(_.isInstanceOf[StitchRvsExec] should not equal true)
  }

  it ("should generate SetOperatorExec for LogicalPlan with Set operator") {
    // final logical plan
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.LAND, Cardinality.ManyToMany, summed2)

    // materialized exec plan
    val execPlan = engine.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[SetOperatorExec] shouldEqual true
    execPlan.children.foreach { l1 =>
      // Now there should be single level of reduce because we have 2 shards
      l1.isInstanceOf[ReduceAggregateExec] shouldEqual true
      l1.children.foreach { l2 =>
        l2.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
        l2.rangeVectorTransformers.size shouldEqual 2
        l2.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
        l2.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
      }
    }
  }

  it("should bound queries until retention period and drop instants outside retention period") {
    val nowSeconds = System.currentTimeMillis() / 1000
     val planner = new SingleClusterPlanner(dsRef, schemas, mapperRef,
       earliestRetainedTimestampFn = nowSeconds * 1000 - 3.days.toMillis, queryConfig)

    // Case 1: no offset or window
    val logicalPlan1 = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""",
      TimeStepParams(nowSeconds - 4.days.toSeconds, 1.minute.toSeconds, nowSeconds))

    val ep1 = planner.materialize(logicalPlan1, QueryContext()).asInstanceOf[DistConcatExec]
    val psm1 = ep1.children.head.asInstanceOf[MultiSchemaPartitionsExec]
                .rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper]
    psm1.start shouldEqual (nowSeconds * 1000
                            - 3.days.toMillis // retention
                            + 1.minute.toMillis // step
                            + WindowConstants.staleDataLookbackMillis) // default window

    // Case 2: no offset, some window
    val logicalPlan2 = Parser.queryRangeToLogicalPlan("""rate(foo{job="bar"}[20m])""",
      TimeStepParams(nowSeconds - 4.days.toSeconds, 1.minute.toSeconds, nowSeconds))

    val ep2 = planner.materialize(logicalPlan2, QueryContext()).asInstanceOf[DistConcatExec]
    val psm2 = ep2.children.head.asInstanceOf[MultiSchemaPartitionsExec]
      .rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper]
    psm2.start shouldEqual (nowSeconds * 1000
      - 3.days.toMillis // retention
      + 1.minute.toMillis // step
      + 20.minutes.toMillis) // window
    psm2.end shouldEqual nowSeconds * 1000

    // Case 3: offset and some window
    val logicalPlan3 = Parser.queryRangeToLogicalPlan("""rate(foo{job="bar"}[20m] offset 15m)""",
      TimeStepParams(nowSeconds - 4.days.toSeconds, 1.minute.toSeconds, nowSeconds))

    val ep3 = planner.materialize(logicalPlan3, QueryContext()).asInstanceOf[DistConcatExec]
    val psm3 = ep3.children.head.asInstanceOf[MultiSchemaPartitionsExec]
      .rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper]
    psm3.start shouldEqual (nowSeconds * 1000
      - 3.days.toMillis // retention
      + 1.minute.toMillis // step
      + 20.minutes.toMillis  // window
      + 15.minutes.toMillis) // offset

    // Case 4: outside retention
    val logicalPlan4 = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""",
      TimeStepParams(nowSeconds - 10.days.toSeconds, 1.minute.toSeconds, nowSeconds - 5.days.toSeconds))
    val ep4 = planner.materialize(logicalPlan4, QueryContext())
    ep4.isInstanceOf[EmptyResultExec] shouldEqual true
    import GlobalScheduler._
    val res = ep4.dispatcher.dispatch(ep4).runAsync.futureValue.asInstanceOf[QueryResult]
    res.result.isEmpty shouldEqual true
  }

  it("should generate execPlan with offset") {
    val t = TimeStepParams(700, 1000, 10000)
    val lp = Parser.queryRangeToLogicalPlan("http_requests_total{job = \"app\"} offset 5m", t)
    val periodicSeries = lp.asInstanceOf[PeriodicSeries]
    periodicSeries.startMs shouldEqual 700000
    periodicSeries.endMs shouldEqual 10000000
    periodicSeries.stepMs shouldEqual 1000000

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.children(0).isInstanceOf[MultiSchemaPartitionsExec] shouldEqual(true)
    val multiSchemaExec = execPlan.children(0).asInstanceOf[MultiSchemaPartitionsExec]
    multiSchemaExec.chunkMethod.asInstanceOf[TimeRangeChunkScan].startTime shouldEqual(100000) // (700 - 300 - 300) * 1000
    multiSchemaExec.chunkMethod.asInstanceOf[TimeRangeChunkScan].endTime shouldEqual(9700000) // (10000 - 300) * 1000

    multiSchemaExec.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual(true)
    val rvt = multiSchemaExec.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper]
    rvt.offsetMs.get shouldEqual 300000
    rvt.startWithOffset shouldEqual(400000) // (700 - 300) * 1000
    rvt.endWithOffset shouldEqual (9700000) // (10000 - 300) * 1000
    rvt.start shouldEqual 700000 // start and end should be same as query TimeStepParams
    rvt.end shouldEqual 10000000
    rvt.step shouldEqual 1000000
  }

  it("should generate execPlan with offset with window") {
    val t = TimeStepParams(700, 1000, 10000)
    val lp = Parser.queryRangeToLogicalPlan("rate(http_requests_total{job = \"app\"}[5m] offset 5m)", t)

    val periodicSeriesPlan = lp.asInstanceOf[PeriodicSeriesWithWindowing]
    periodicSeriesPlan.startMs shouldEqual 700000
    periodicSeriesPlan.endMs shouldEqual 10000000
    periodicSeriesPlan.stepMs shouldEqual 1000000

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.children(0).isInstanceOf[MultiSchemaPartitionsExec] shouldEqual(true)
    val multiSchemaExec = execPlan.children(0).asInstanceOf[MultiSchemaPartitionsExec]
    multiSchemaExec.chunkMethod.asInstanceOf[TimeRangeChunkScan].startTime shouldEqual(100000)
    multiSchemaExec.chunkMethod.asInstanceOf[TimeRangeChunkScan].endTime shouldEqual(9700000)

    multiSchemaExec.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual(true)
    val rvt = multiSchemaExec.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper]
    rvt.offsetMs.get shouldEqual(300000)
    rvt.startWithOffset shouldEqual(400000) // (700 - 300) * 1000
    rvt.endWithOffset shouldEqual (9700000) // (10000 - 300) * 1000
    rvt.start shouldEqual 700000
    rvt.end shouldEqual 10000000
    rvt.step shouldEqual 1000000
  }

  it ("should replace __name__ with _metric_ in by and without") {
    val dataset = MetricsTestData.timeseriesDatasetWithMetric
    val dsRef = dataset.ref
    val schemas = Schemas(dataset.schema)

    val engine = new SingleClusterPlanner(dsRef, schemas, mapperRef, earliestRetainedTimestampFn = 0, queryConfig)

    val logicalPlan1 = Parser.queryRangeToLogicalPlan("""sum(foo{_ns_="bar", _ws_="test"}) by (__name__)""",
      TimeStepParams(1000, 20, 2000))
    
    val execPlan1 = engine.materialize(logicalPlan1, QueryContext(origQueryParams = promQlQueryParams))

    execPlan1.isInstanceOf[ReduceAggregateExec] shouldEqual true
    execPlan1.children.foreach { l1 =>
      l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
      l1.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
      l1.rangeVectorTransformers(1).asInstanceOf[AggregateMapReduce].by shouldEqual List("_metric_")
    }

    val logicalPlan2 = Parser.queryRangeToLogicalPlan(
      """sum(foo{_ns_="bar", _ws_="test"})
        |without (__name__, instance)""".stripMargin,
      TimeStepParams(1000, 20, 2000))

    // materialized exec plan
    val execPlan2 = engine.materialize(logicalPlan2, QueryContext(origQueryParams = promQlQueryParams))

    execPlan2.isInstanceOf[ReduceAggregateExec] shouldEqual true
    execPlan2.children.foreach { l1 =>
      l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
      l1.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
      l1.rangeVectorTransformers(1).asInstanceOf[AggregateMapReduce].without shouldEqual List("_metric_", "instance")
    }
  }

  it ("should replace __name__ with _metric_ in ignoring and group_left/group_right") {
      val dataset = MetricsTestData.timeseriesDatasetWithMetric
      val dsRef = dataset.ref
      val schemas = Schemas(dataset.schema)

      val engine = new SingleClusterPlanner(dsRef, schemas, mapperRef, earliestRetainedTimestampFn = 0, queryConfig)

      val logicalPlan1 = Parser.queryRangeToLogicalPlan(
        """sum(foo{_ns_="bar1", _ws_="test"}) + ignoring(__name__)
          | sum(foo{_ns_="bar2", _ws_="test"})""".stripMargin,
        TimeStepParams(1000, 20, 2000))
      val execPlan2 = engine.materialize(logicalPlan1, QueryContext(origQueryParams = promQlQueryParams))

      execPlan2.isInstanceOf[BinaryJoinExec] shouldEqual true
      execPlan2.asInstanceOf[BinaryJoinExec].ignoring shouldEqual Seq("_metric_")

      val logicalPlan2 = Parser.queryRangeToLogicalPlan(
        """sum(foo{_ns_="bar1", _ws_="test"}) + group_left(__name__)
          | sum(foo{_ns_="bar2", _ws_="test"})""".stripMargin,
        TimeStepParams(1000, 20, 2000))
      val execPlan3 = engine.materialize(logicalPlan2, QueryContext(origQueryParams = promQlQueryParams))

      execPlan3.isInstanceOf[BinaryJoinExec] shouldEqual true
      execPlan3.asInstanceOf[BinaryJoinExec].include shouldEqual Seq("_metric_")
    }

  it("should generate execPlan for binary join with offset") {
    val t = TimeStepParams(700, 1000, 10000)
    val lp = Parser.queryRangeToLogicalPlan("rate(http_requests_total{job = \"app\"}[5m] offset 5m) / " +
      "rate(http_requests_total{job = \"app\"}[5m])", t)

    val periodicSeriesPlan = lp.asInstanceOf[BinaryJoin]
    periodicSeriesPlan.startMs shouldEqual 700000
    periodicSeriesPlan.endMs shouldEqual 10000000
    periodicSeriesPlan.stepMs shouldEqual 1000000

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual(true)
    val binaryJoin = execPlan.asInstanceOf[BinaryJoinExec]

    binaryJoin.lhs(0).isInstanceOf[MultiSchemaPartitionsExec] shouldEqual(true)
    val multiSchemaExec1 = binaryJoin.lhs(0).asInstanceOf[MultiSchemaPartitionsExec]
    multiSchemaExec1.chunkMethod.asInstanceOf[TimeRangeChunkScan].startTime shouldEqual(100000)
    multiSchemaExec1.chunkMethod.asInstanceOf[TimeRangeChunkScan].endTime shouldEqual(9700000)

    multiSchemaExec1.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual(true)
    val rvt1 = multiSchemaExec1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper]
    rvt1.offsetMs.get shouldEqual(300000)
    rvt1.startWithOffset shouldEqual(400000) // (700 - 300) * 1000
    rvt1.endWithOffset shouldEqual (9700000) // (10000 - 300) * 1000
    rvt1.start shouldEqual 700000
    rvt1.end shouldEqual 10000000
    rvt1.step shouldEqual 1000000

    binaryJoin.rhs(0).isInstanceOf[MultiSchemaPartitionsExec] shouldEqual(true)
    val multiSchemaExec2 = binaryJoin.rhs(0).asInstanceOf[MultiSchemaPartitionsExec]
    multiSchemaExec2.chunkMethod.asInstanceOf[TimeRangeChunkScan].startTime shouldEqual(400000) // (700 - 300) * 1000
    multiSchemaExec2.chunkMethod.asInstanceOf[TimeRangeChunkScan].endTime shouldEqual(10000000)

    multiSchemaExec2.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual(true)
    val rvt2 = multiSchemaExec2.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper]
    // No offset in rhs
    rvt2.offsetMs.isEmpty shouldEqual true
    rvt2.startWithOffset shouldEqual(700000)
    rvt2.endWithOffset shouldEqual (10000000)
    rvt2.start shouldEqual 700000
    rvt2.end shouldEqual 10000000
    rvt2.step shouldEqual 1000000
  }
}
