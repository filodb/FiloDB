package filodb.coordinator.queryengine2

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import monix.eval.Task
import monix.execution.Scheduler
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.duration.FiniteDuration
import com.typesafe.config.ConfigFactory
import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands._
import filodb.core.{DatasetRef, MetricsTestData, SpreadChange}
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.store.TimeRangeChunkScan
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query
import filodb.query._
import filodb.query.exec._

class QueryEngineSpec extends FunSpec with Matchers {

  implicit val system = ActorSystem()
  val node = TestProbe().ref

  val mapper = new ShardMapper(32)
  for { i <- 0 until 32 } mapper.registerNode(Seq(i), node)

  private def mapperRef = mapper

  val dataset = MetricsTestData.timeseriesDataset
  val dsRef = dataset.ref
  val schemas = Schemas(dataset.schema)

  val emptyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlan)(implicit sched: Scheduler,
                                          timeout: FiniteDuration): Task[query.QueryResponse] = ???
  }

  val engine = new QueryEngine(dsRef, schemas, mapperRef, EmptyFailureProvider)

  val queryEngineConfigString = "routing {\n  buddy {\n    http {\n      timeout = 10.seconds\n    }\n  }\n}"

  val queryEngineConfig = ConfigFactory.parseString(queryEngineConfigString)
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
  val promQlQueryParams = PromQlQueryParams("sum(heap_usage)", 100, 1, 1000, None)

  it ("should generate ExecPlan for LogicalPlan") {
    // final logical plan
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)

    // materialized exec plan
    val execPlan = engine.materialize(logicalPlan, QueryOptions(), promQlQueryParams)

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
      QueryOptions(Some(StaticSpreadProvider(SpreadChange(0, 4))), 1000000), promQlQueryParams)
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

  it("should materializeHaPlan ExecPlan correctly for _bucket_ histogram queries") {
    val lp = Parser.queryRangeToLogicalPlan("""rate(foo{job="bar",_bucket_="2.5"}[5m])""", TimeStepParams(20000, 100, 30000))

    info(s"LogicalPlan is $lp")
    lp match {
      case p: PeriodicSeriesWithWindowing => p.series.isInstanceOf[ApplyInstantFunctionRaw] shouldEqual true
      case _ => throw new IllegalArgumentException(s"Unexpected LP $lp")
    }

    val execPlan = engine.materialize(lp,
      QueryOptions(Some(StaticSpreadProvider(SpreadChange(0, 4))), 1000000), promQlQueryParams)

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
    // Custom QueryEngine with different dataset with different metric name
    val datasetOpts = dataset.options.copy(metricColumn = "kpi", shardKeyColumns = Seq("kpi", "job"))
    val dataset2 = dataset.modify(_.schema.partition.options).setTo(datasetOpts)
    val engine2 = new QueryEngine(dataset2.ref, Schemas(dataset2.schema), mapperRef, EmptyFailureProvider)

    // materialized exec plan
    val execPlan = engine2.materialize(raw2, QueryOptions(), promQlQueryParams)
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

    val spreadFunc = QueryOptions.simpleMapSpreadFunc(Seq("job"), filodbSpreadMap, 1)

    // final logical plan
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)

    // materialized exec plan
    val execPlan = engine.materialize(logicalPlan, QueryOptions(Some(FunctionalSpreadProvider(spreadFunc)), 1000000), promQlQueryParams)
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
    val execPlan = engine.materialize(lp, QueryOptions(Some(FunctionalSpreadProvider(spread)), 1000000),
      promQlQueryParams)
    execPlan.rangeVectorTransformers.head.isInstanceOf[StitchRvsMapper] shouldEqual true
  }

  it("should not stitch results when spread has not changed in query range") {
    val lp = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""", TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(35000000, 2))
    }
    val execPlan = engine.materialize(lp, QueryOptions(Some(FunctionalSpreadProvider(spread)), 1000000),
      promQlQueryParams)
    execPlan.rangeVectorTransformers.isEmpty shouldEqual true
  }

  it("should stitch results before binary join when spread changed in query range") {
    val lp = Parser.queryRangeToLogicalPlan("""count(foo{job="bar"} + baz{job="bar"})""",
      TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(25000000, 2))
    }
    val execPlan = engine.materialize(lp, QueryOptions(Some(FunctionalSpreadProvider(spread)), 1000000),
      promQlQueryParams)
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
    val execPlan = engine.materialize(lp, QueryOptions(Some(FunctionalSpreadProvider(spread)), 1000000),
      promQlQueryParams)
    val binaryJoinNode = execPlan.children(0)
    binaryJoinNode.isInstanceOf[BinaryJoinExec] shouldEqual true
    binaryJoinNode.children.foreach(_.isInstanceOf[StitchRvsExec] should not equal true)
  }

  it ("should generate SetOperatorExec for LogicalPlan with Set operator") {
    // final logical plan
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.LAND, Cardinality.ManyToMany, summed2)

    // materialized exec plan
    val execPlan = engine.materialize(logicalPlan, QueryOptions(), promQlQueryParams)

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

  it("should not generate PromQlExec plan when local overlapping failure is smaller") {
    val to = 10000
    val from = 100
    val intervalSelector = IntervalSelector(from, to)
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from, 100, to, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, Seq("job"))

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("remote", datasetRef,
          TimeRange(1500, 4000), true),
          FailureTimeRange("local", datasetRef, //Removed
          TimeRange(2000, 3000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(5000, 6000), true))
      }
    }

    val engine = new QueryEngine(dsRef, schemas, mapperRef, failureProvider)
    val execPlan = engine.materialize(summed, QueryOptions(), promQlQueryParams)

    execPlan.isInstanceOf[ReduceAggregateExec] shouldEqual (true)

    // Should ignore smaller local failure which is from 1500 - 4000 and generate local exec plan
    val reduceAggregateExec = execPlan.asInstanceOf[ReduceAggregateExec]

    reduceAggregateExec.children.length shouldEqual (2) //default spread is 1 so 2 shards

    reduceAggregateExec.children.foreach { l1 =>
      l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
      l1.rangeVectorTransformers.size shouldEqual 2
      l1.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].start shouldEqual (100)
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].end shouldEqual (10000)
      l1.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
    }
  }

  it("should generate only PromQlExec when failure is present only in local") {
    val to = 10000
    val from = 100
    val intervalSelector = IntervalSelector(from, to)
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from, 100, to, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, Seq("job"))

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(1000, 6000), false))
      }
    }

    val engine = new QueryEngine(dsRef, schemas, mapperRef, failureProvider, StaticSpreadProvider(), queryEngineConfig)
    val execPlan = engine.materialize(summed, QueryOptions(), promQlQueryParams)

    execPlan.isInstanceOf[PromQlExec] shouldEqual (true)
    execPlan.asInstanceOf[PromQlExec].params.start shouldEqual(from/1000)
    execPlan.asInstanceOf[PromQlExec].params.end shouldEqual(to/1000)
  }

  it("should generate RemotExecPlan with RawSeries time according to lookBack") {
    val to = 2000000
    val from = 1000000
    val intervalSelector = IntervalSelector(from - 50000 , to) // Lookback of 50000
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from, 100, to, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, Seq("job"))
    val promQlQueryParams = PromQlQueryParams("", from/1000, 1, to/1000, None)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(1060000, 1090000), true))
      }
    }

    val engine = new QueryEngine(dsRef, schemas, mapperRef, failureProvider, StaticSpreadProvider(), queryEngineConfig)
    val execPlan = engine.materialize(summed, QueryOptions(), promQlQueryParams)

    execPlan.isInstanceOf[StitchRvsExec] shouldEqual (true)

    val stitchRvsExec = execPlan.asInstanceOf[StitchRvsExec]
    stitchRvsExec.children.size shouldEqual (2)
    stitchRvsExec.children(0).isInstanceOf[ReduceAggregateExec] shouldEqual (true)
    stitchRvsExec.children(1).isInstanceOf[PromQlExec] shouldEqual (true)

    val child1 = stitchRvsExec.children(0).asInstanceOf[ReduceAggregateExec]
    val child2 = stitchRvsExec.children(1).asInstanceOf[PromQlExec]

    child1.children.length shouldEqual (2) //default spread is 1 so 2 shards

    child1.children.foreach { l1 =>
      l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
      l1.asInstanceOf[MultiSchemaPartitionsExec].chunkMethod.asInstanceOf[TimeRangeChunkScan].startTime shouldEqual
        1010000
      l1.asInstanceOf[MultiSchemaPartitionsExec].chunkMethod.asInstanceOf[TimeRangeChunkScan].endTime shouldEqual 2000000
      l1.rangeVectorTransformers.size shouldEqual 2
      l1.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].start shouldEqual (1060000)
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].end shouldEqual (2000000)
      l1.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
    }

    child2.params.start shouldEqual from/1000
    child2.params.end shouldEqual (1060000-1)/1000
    child2.params.processFailure shouldEqual(false)
  }

  it("should generate only PromQlExec when local failure starts before query time") {
    val to = 10000
    val from = 100
    val intervalSelector = IntervalSelector(from, to)
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from, 100, to, 10000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, Seq("job"))

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(50, 200), false))
      }
    }

    val engine = new QueryEngine(dsRef, schemas, mapperRef, failureProvider, StaticSpreadProvider(), queryEngineConfig)
    val execPlan = engine.materialize(summed, QueryOptions(), promQlQueryParams)

    execPlan.isInstanceOf[PromQlExec] shouldEqual (true)
    execPlan.asInstanceOf[PromQlExec].params.start shouldEqual(from/1000)
    execPlan.asInstanceOf[PromQlExec].params.end shouldEqual(to/1000)
  }

  it("should generate only PromQlExec when local failure timerange coincide with query time range") {
    val to = 10000
    val from = 100
    val intervalSelector = IntervalSelector(from, to)
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from, 100, to, 10000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, Seq("job"))

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(100, 10000), false))
      }
    }

    val engine = new QueryEngine(dsRef, schemas, mapperRef, failureProvider, StaticSpreadProvider(), queryEngineConfig)
    val execPlan = engine.materialize(summed, QueryOptions(), promQlQueryParams)

    execPlan.isInstanceOf[PromQlExec] shouldEqual (true)
    execPlan.asInstanceOf[PromQlExec].params.start shouldEqual(from/1000)
    execPlan.asInstanceOf[PromQlExec].params.end shouldEqual(to/1000)
  }

  it("should generate only PromQlExec when local failure starts before query end time and ends after query end time") {
    val to = 10000
    val from = 100
    val intervalSelector = IntervalSelector(from, to)
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from, 100, to, 10000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, Seq("job"))

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(5000, 20000), false))
      }
    }

    val engine = new QueryEngine(dsRef, schemas, mapperRef, failureProvider, StaticSpreadProvider(), queryEngineConfig)
    val execPlan = engine.materialize(summed, QueryOptions(), promQlQueryParams)

    execPlan.isInstanceOf[PromQlExec] shouldEqual (true)
    execPlan.asInstanceOf[PromQlExec].params.start shouldEqual(from/1000)
    execPlan.asInstanceOf[PromQlExec].params.end shouldEqual(to/1000)
  }

  it("should generate PromQlExecPlan and LocalPlan with RawSeries time according to lookBack and step") {
    val to = 2000
    val from = 900
    val lookBack = 300000
    val step = 60
    val intervalSelector = IntervalSelector(from * 1000 - lookBack , to * 1000) // Lookback of 300
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from * 1000, step * 1000, to * 1000, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, Seq("job"))
    val promQlQueryParams = PromQlQueryParams("dummy query", from, step, to, None)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(1060000, 1090000), true))
      }
    }
    //900K to 1020K and 1020+60 k to 2000K

    val engine = new QueryEngine(dsRef, schemas, mapperRef, failureProvider, StaticSpreadProvider(), queryEngineConfig)
    val execPlan = engine.materialize(summed, QueryOptions(), promQlQueryParams)

    execPlan.isInstanceOf[StitchRvsExec] shouldEqual (true)

    val stitchRvsExec = execPlan.asInstanceOf[StitchRvsExec]
    stitchRvsExec.children.size shouldEqual (2)
    stitchRvsExec.children(0).isInstanceOf[ReduceAggregateExec] shouldEqual (true)
    stitchRvsExec.children(1).isInstanceOf[PromQlExec] shouldEqual (true)

    val child1 = stitchRvsExec.children(0).asInstanceOf[ReduceAggregateExec]
    val child2 = stitchRvsExec.children(1).asInstanceOf[PromQlExec]

    child1.children.length shouldEqual (2) //default spread is 1 so 2 shards

    child1.children.foreach { l1 =>
      l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
      l1.asInstanceOf[MultiSchemaPartitionsExec].chunkMethod.asInstanceOf[TimeRangeChunkScan].startTime shouldEqual
        (1080000-lookBack)
      l1.asInstanceOf[MultiSchemaPartitionsExec].chunkMethod.asInstanceOf[TimeRangeChunkScan].endTime shouldEqual 2000000
      l1.rangeVectorTransformers.size shouldEqual 2
      l1.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].start shouldEqual (1080000)
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].end shouldEqual (2000000)
      l1.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
    }

    child2.params.start shouldEqual 900
    child2.params.end shouldEqual 1020
    child2.params.step shouldEqual 60
    child2.params.processFailure shouldEqual(false)
  }

  it("should generate only PromQlExecPlan when second remote ends after query end time") {
    val to = 2000
    val from = 900
    val lookBack = 300000
    val step = 60
    val intervalSelector = IntervalSelector(from * 1000 - lookBack , to * 1000) // Lookback of 300
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from * 1000, step * 1000, to * 1000, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, Seq("job"))
    val promQlQueryParams = PromQlQueryParams("dummy query", from, step, to, None)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(2000000, 2500000), true))
      }
    }

    val engine = new QueryEngine(dsRef, schemas, mapperRef, failureProvider, StaticSpreadProvider(), queryEngineConfig)
    val execPlan = engine.materialize(summed, QueryOptions(), promQlQueryParams)

    execPlan.isInstanceOf[PromQlExec] shouldEqual (true)

    val child = execPlan.asInstanceOf[PromQlExec]

    child.params.start shouldEqual 900
    child.params.end shouldEqual 1980
    child.params.step shouldEqual 60
    child.params.processFailure shouldEqual(false)
  }

  it("should not do routing for InstantQueries when there are local and remote failures") {
    val to = 900
    val from = 900
    val lookBack = 300000
    val step = 1000
    val intervalSelector = IntervalSelector(from * 1000 - lookBack , to * 1000) // Lookback of 300
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from * 1000, step * 1000, to * 1000, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, Seq("job"))
    val promQlQueryParams = PromQlQueryParams("dummy query", from, step, to, None)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(2000000, 2500000), true))
      }
    }

    val engine = new QueryEngine(dsRef, schemas, mapperRef, failureProvider, StaticSpreadProvider(), queryEngineConfig)
    val execPlan = engine.materialize(summed, QueryOptions(), promQlQueryParams)

    execPlan.isInstanceOf[ReduceAggregateExec] shouldEqual (true)

    val reduceAggregateExec = execPlan.asInstanceOf[ReduceAggregateExec]

    reduceAggregateExec.children.length shouldEqual (2) //default spread is 1 so 2 shards

    reduceAggregateExec.children.foreach { l1 =>
      l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
      l1.rangeVectorTransformers.size shouldEqual 2
      l1.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].start shouldEqual from *1000
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].end shouldEqual  to * 1000
      l1.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
    }
  }

  it("should generate PromQlExec for InstantQueries when all failures are local") {
    val to = 900
    val from = 900
    val lookBack = 300000
    val step = 1000
    val intervalSelector = IntervalSelector(from * 1000 - lookBack , to * 1000) // Lookback of 300
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from * 1000, step * 1000, to * 1000, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, Seq("job"))
    val promQlQueryParams = PromQlQueryParams("dummy query", from, step, to, None)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(2000000, 2500000), false))
      }
    }

    val engine = new QueryEngine(dsRef, schemas, mapperRef, failureProvider, StaticSpreadProvider(), queryEngineConfig)
    val execPlan = engine.materialize(summed, QueryOptions(), promQlQueryParams)

    execPlan.isInstanceOf[PromQlExec] shouldEqual (true)

    val child = execPlan.asInstanceOf[PromQlExec]
    child.params.start shouldEqual from
    child.params.end shouldEqual to
    child.params.step shouldEqual step
    child.params.processFailure shouldEqual(false)
  }
}
