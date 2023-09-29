package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.{FunctionalSpreadProvider, FunctionalTargetSchemaProvider, StaticSpreadProvider}
import filodb.core.metadata.Column.ColumnType
import filodb.core.{GlobalScheduler, MetricsTestData, SpreadChange, TargetSchemaChange}
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, _}
import filodb.core.query.Filter.Equals
import filodb.core.store.TimeRangeChunkScan
import filodb.prometheus.ast.{TimeStepParams, WindowConstants}
import filodb.prometheus.parse.Parser
import filodb.query._
import filodb.query.exec._
import filodb.query.exec.InternalRangeFunction.Last
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import filodb.core.query.Filter.NotEquals
import filodb.query.LogicalPlan.getRawSeriesFilters
import filodb.query.exec.aggregator.{CountRowAggregator, SumRowAggregator}
import org.scalatest.exceptions.TestFailedException


import scala.concurrent.duration._

class SingleClusterPlannerSpec extends AnyFunSpec with Matchers with ScalaFutures with PlanValidationSpec {

  implicit val system = ActorSystem()
  private val node = TestProbe().ref

  private val mapper = new ShardMapper(32)
  for { i <- 0 until 32 } mapper.registerNode(Seq(i), node)

  private def mapperRef = mapper

  private val dataset = MetricsTestData.timeseriesDataset
  private val dsRef = dataset.ref
  private val schemas = Schemas(dataset.schema)

  private def regexPipeShardKeyMatcher(filters: Seq[ColumnFilter]) = {
    val values = filters.map { filter =>
      filter.column -> filter.filter.valuesStrings.toList.head.toString.split('|')
    }
    val triplets = QueryUtils.combinations(values.map(_._2.toSeq))
    triplets.map(triplet => values.map(_._1).zip(triplet).map(p => ColumnFilter(p._1, Equals(p._2))))
  }

  private val config = ConfigFactory.load("application_test.conf")
  private val queryConfig = QueryConfig(config.getConfig("filodb.query"))

  private val engine = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = 0,
    queryConfig, "raw", shardKeyMatcher = regexPipeShardKeyMatcher)

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
  val summed1 = Aggregate(AggregationOperator.Sum, windowed1, Nil, AggregateClause.byOpt(Seq("job")))

  val f2 = Seq(ColumnFilter("__name__", Filter.Equals("http_request_duration_seconds_count")),
    ColumnFilter("job", Filter.Equals("myService")))
  val raw2 = RawSeries(rangeSelector = intervalSelector, filters= f2, columns = Seq("value"))
  val windowed2 = PeriodicSeriesWithWindowing(raw2, from, 1000, to, 5000, RangeFunctionId.Rate)
  val summed2 = Aggregate(AggregationOperator.Sum, windowed2, Nil, AggregateClause.byOpt(Seq("job")))
  val promQlQueryParams = PromQlQueryParams("sum(heap_usage)", 100, 1, 1000)


  val f3 = Seq(ColumnFilter("__name__", Filter.Equals("http_request_duration_total")),
    ColumnFilter("job", Filter.Equals("myService")),
    ColumnFilter("instance", Filter.Equals("akgH34")))
  val raw3 = RawSeries(rangeSelector = intervalSelector, filters= f3, columns = Seq("value"))
  val windowed3 = PeriodicSeriesWithWindowing(raw3, from, 1000, to, 5000, RangeFunctionId.Rate)
  val summed3 = Aggregate(AggregationOperator.Sum, windowed3, Nil, AggregateClause.byOpt(Seq("job")))

  it ("should generate ExecPlan for LogicalPlan") {
    // final logical plan
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)

    // materialized exec plan
    val execPlan = engine.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))

    /*
    Following ExecPlan should be generated:

    BinaryJoinExec(binaryOp=DIV, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-4#-325843755])
    -AggregatePresenter(aggrOp=Sum, aggrParams=List())
    --LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-4#-325843755])
    ---AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ----PeriodicSamplesMapper(start=1526094025509, step=1000, end=1526094075509, window=Some(5000), functionId=Some(Rate), funcParams=List())
    -----MultiSchemaPartitionsExec(shard=2, rowKeyRange=RowKeyInterval(b[1526094025509],b[1526094075509]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_bucket)), ColumnFilter(job,Equals(myService)), ColumnFilter(le,Equals(0.3)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#342951049])
    ---AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ----PeriodicSamplesMapper(start=1526094025509, step=1000, end=1526094075509, window=Some(5000), functionId=Some(Rate), funcParams=List())
    -----SelectRawPartitionsExec(shard=3, rowKeyRange=RowKeyInterval(b[1526094025509],b[1526094075509]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_bucket)), ColumnFilter(job,Equals(myService)), ColumnFilter(le,Equals(0.3)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-4#-325843755])
    -AggregatePresenter(aggrOp=Sum, aggrParams=List())
    --LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-2#-1576910232])
    ---AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ----PeriodicSamplesMapper(start=1526094025509, step=1000, end=1526094075509, window=Some(5000), functionId=Some(Rate), funcParams=List())
    -----SelectRawPartitionsExec(shard=0, rowKeyRange=RowKeyInterval(b[1526094025509],b[1526094075509]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_count)), ColumnFilter(job,Equals(myService)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-238515561])
    ---AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
    ----PeriodicSamplesMapper(start=1526094025509, step=1000, end=1526094075509, window=Some(5000), functionId=Some(Rate), funcParams=List())
    -----SelectRawPartitionsExec(shard=1, rowKeyRange=RowKeyInterval(b[1526094025509],b[1526094075509]), filters=List(ColumnFilter(__name__,Equals(http_request_duration_seconds_count)), ColumnFilter(job,Equals(myService)))) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-2#-1576910232])
    */

    println(execPlan.printTree())
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

  it ("should parallelize aggregation") {
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)

    // materialized exec plan
    val execPlan = engine.materialize(logicalPlan,
      QueryContext(promQlQueryParams, plannerParams = PlannerParams(spreadOverride = Some(StaticSpreadProvider(SpreadChange(0, 4))), queryTimeoutMillis =1000000)))
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

  it("should materialize ExecPlan correctly for _bucket_ histogram queries") {
    val lp = Parser.queryRangeToLogicalPlan("""rate(foo{job="bar",_bucket_="2.5"}[5m])""",
      TimeStepParams(20000, 100, 30000))

    info(s"LogicalPlan is $lp")
    lp match {
      case p: PeriodicSeriesWithWindowing => p.series.isInstanceOf[ApplyInstantFunctionRaw] shouldEqual true
      case _ => throw new IllegalArgumentException(s"Unexpected LP $lp")
    }

    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, plannerParams = PlannerParams(spreadOverride =
      Some(StaticSpreadProvider(SpreadChange(0, 4))), queryTimeoutMillis =1000000)))
    info(s"First child plan: ${execPlan.children.head.printTree()}")
    execPlan.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
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
    val engine2 = new SingleClusterPlanner(dataset2, Schemas(dataset2.schema), mapperRef,
      0, queryConfig, "raw")

    // materialized exec plan
    val execPlan = engine2.materialize(raw2, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
    execPlan.children.foreach { l1 =>
      l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
      val rpExec = l1.asInstanceOf[MultiSchemaPartitionsExec]
      rpExec.filters.map(_.column).toSet shouldEqual Set("kpi", "job")
    }
  }

  it("should use target-schema and generate ExecPlan with appropriate shards") {
    val targetSchema = Map(Map("job" -> "myService") -> Seq(TargetSchemaChange(schema = Seq("job", "instance"))))

    val filodbSpreadMap = new collection.mutable.HashMap[collection.Map[String, String], Int]
    filodbSpreadMap.put(collection.Map(("job" -> "myService")), 2)
    val spreadFunc = QueryContext.simpleMapSpreadFunc(Seq("job"), filodbSpreadMap, 1)
    val targetSchemaFunc = QueryContext.mapTargetSchemaFunc(Seq("job"), targetSchema, "client")

    // final logical plan
    // LHS has all the targetSchema label filters (1 shard), second one doesn't (spread - 4 shards)
    val logicalPlan = BinaryJoin(summed3, BinaryOperator.DIV, Cardinality.OneToOne, summed2)

    // materialized exec plan
    val execPlan = engine.materialize(logicalPlan, QueryContext(promQlQueryParams, plannerParams = PlannerParams
    (spreadOverride = Some(FunctionalSpreadProvider(spreadFunc)),
      targetSchemaProviderOverride = Some(FunctionalTargetSchemaProvider(targetSchemaFunc)), queryTimeoutMillis =1000000)))
    execPlan.printTree()

    // LHS column filters includes all target-schema labels, so query will be routed to single shard.
    val expectedShards = Array(1, 4) // target-schema vs default spread

    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual true
    execPlan.children should have length (2)
    execPlan.children.zipWithIndex.foreach { case(reduceAggPlan, i) =>
      reduceAggPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
      reduceAggPlan.children should have length expectedShards(i)
    }
  }

  it("should use spread function to change/override spread and generate ExecPlan with appropriate shards") {
    var filodbSpreadMap = new collection.mutable.HashMap[collection.Map[String, String], Int]
    filodbSpreadMap.put(collection.Map(("job" -> "myService")), 2)

    val spreadFunc = QueryContext.simpleMapSpreadFunc(Seq("job"), filodbSpreadMap, 1)

    // final logical plan
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)

    // materialized exec plan
    val execPlan = engine.materialize(logicalPlan, QueryContext(promQlQueryParams, plannerParams = PlannerParams
    (spreadOverride = Some(FunctionalSpreadProvider(spreadFunc)), queryTimeoutMillis =1000000)))
    execPlan.printTree()

    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual true
    execPlan.children should have length (2)
    execPlan.children.foreach { reduceAggPlan =>
      reduceAggPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
      reduceAggPlan.children should have length (4)   // spread=2 means 4 shards
    }
  }

  it("should generate correct plan for subqueries with one child node for subquery") {
    val lp = Parser.queryRangeToLogicalPlan("""min_over_time(sum(rate(foo{job="bar"}[5m]))[3m:1m])""",
      TimeStepParams(20900, 90, 21800))
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
    execPlan.children should have length (2)
    execPlan.rangeVectorTransformers should have length (2)
    execPlan.rangeVectorTransformers(1).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
    val topPsm = execPlan.rangeVectorTransformers(1).asInstanceOf[PeriodicSamplesMapper]
    topPsm.startMs shouldEqual 20900000
    topPsm.endMs shouldEqual 21800000
    topPsm.stepMs shouldEqual 90000
    topPsm.window shouldEqual Some(180000)
    topPsm.functionId shouldEqual Some(InternalRangeFunction.MinOverTime)
    execPlan.children(0).rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper]
    val middlePsm = execPlan.children(0).rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper]
    //Notice that the start  is not 20 720 000, because 20 720 000 is not divisible by 60
    //Instead it's 20 760 000, ie next divisible after 20 720 000
    middlePsm.startMs shouldEqual 20760000
    //Similarly the end is not 21 800 000, because 20 800 000 is not divisible by 60
    //Instead it's 21 780 000, ie next divisible to the left of 20 800 000
    middlePsm.endMs shouldEqual 21780000
    middlePsm.stepMs shouldEqual 60000
    middlePsm.window shouldEqual Some(300000)
    val partExec = execPlan.children(0).asInstanceOf[MultiSchemaPartitionsExec]
    // 20 460 000 = 21 780 000 - 300 000
    partExec.chunkMethod.startTime shouldEqual 20460000
    partExec.chunkMethod.endTime shouldEqual 21780000
  }

  it("should generate correct plan for subqueries with multiple child nodes for subqueries") {
    val lp = Parser.queryRangeToLogicalPlan("""min_over_time(rate(foo{job="bar"}[5m])[3m:1m])""",
      TimeStepParams(20900, 90, 21800))
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
    execPlan.children should have length (2)
    execPlan.children(1).isInstanceOf[MultiSchemaPartitionsExec]
    val partExec = execPlan.children(1).asInstanceOf[MultiSchemaPartitionsExec]
    partExec.rangeVectorTransformers should have length (2)
    val topPsm = partExec.rangeVectorTransformers(1).asInstanceOf[PeriodicSamplesMapper]
    topPsm.startMs shouldEqual 20900000
    topPsm.endMs shouldEqual 21800000
    topPsm.stepMs shouldEqual 90000
    topPsm.window shouldEqual Some(180000)
    topPsm.functionId shouldEqual Some(InternalRangeFunction.MinOverTime)
    partExec.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper]
    val middlePsm = partExec.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper]
    //Notice that the start  is not 20 720 000, because 20 720 000 is not divisible by 60
    //Instead it's 20 760 000, ie next divisible after 20 720 000
    middlePsm.startMs shouldEqual 20760000
    //Similarly the end is not 21 800 000, because 20 800 000 is not divisible by 60
    //Instead it's 21 780 000, ie next divisible to the left of 20 800 000
    middlePsm.endMs shouldEqual 21780000
    middlePsm.stepMs shouldEqual 60000
    middlePsm.window shouldEqual Some(300000)
    // 20 460 000 = 21 780 000 - 300 000
    partExec.chunkMethod.startTime shouldEqual 20460000
    partExec.chunkMethod.endTime shouldEqual 21780000
  }

  it("should generate correct plan for nested subqueries") {
    val lp = Parser.queryRangeToLogicalPlan("""avg_over_time(max_over_time(rate(foo{job="bar"}[5m])[5m:1m])[10m:2m])""",
      TimeStepParams(20900, 90, 21800))
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
    execPlan.children should have length (2)
    execPlan.children(1).isInstanceOf[MultiSchemaPartitionsExec]
    val partExec = execPlan.children(1).asInstanceOf[MultiSchemaPartitionsExec]
    partExec.rangeVectorTransformers should have length (3)
    val topPsm = partExec.rangeVectorTransformers(2).asInstanceOf[PeriodicSamplesMapper]
    topPsm.startMs shouldEqual 20900000
    topPsm.endMs shouldEqual 21800000
    topPsm.stepMs shouldEqual 90000
    topPsm.window shouldEqual Some(600000)
    topPsm.functionId shouldEqual Some(InternalRangeFunction.AvgOverTime)
    partExec.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper]
    val middlePsm = partExec.rangeVectorTransformers(1).asInstanceOf[PeriodicSamplesMapper]
    // 20 900 000 - 600 000 = 20 300 000
    // 20 300 000 / 120 000 =  20 280 000
    // 20 280 000 + 120 000 = 20 400 000
    middlePsm.startMs shouldEqual 20400000
    //Similarly the end is not 21 800 000, because 20 800 000 is not divisible by 120
    //Instead it's 21 720 000, ie next divisible to the left of 20 800 000
    middlePsm.endMs shouldEqual 21720000
    middlePsm.stepMs shouldEqual 120000
    middlePsm.window shouldEqual Some(300000)
    middlePsm.functionId shouldEqual Some(InternalRangeFunction.MaxOverTime)
    val bottomPsm = partExec.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper]
    // 20 400 000 - 300 000 = 20 100 000
    bottomPsm.startMs shouldEqual 20100000
    bottomPsm.endMs shouldEqual 21720000
    bottomPsm.stepMs shouldEqual 60000
    bottomPsm.window shouldEqual Some(300000)
    // 20 100 000 - 300 000 = 19 800 000
    partExec.chunkMethod.startTime shouldEqual 19800000
    partExec.chunkMethod.endTime shouldEqual 21720000
  }

  it("should generate correct plan for top level subqueries") {
    val lp = Parser.queryRangeToLogicalPlan("""foo{job="bar"}[10m:2m]""",
      TimeStepParams(20900, 0, 20900))
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
    execPlan.children should have length (2)
    execPlan.children(1).isInstanceOf[MultiSchemaPartitionsExec]
    val partExec = execPlan.children(1).asInstanceOf[MultiSchemaPartitionsExec]
    partExec.rangeVectorTransformers should have length (1)
    val topPsm = partExec.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper]
    // (20 900 000 - 600 000)/ 120 000 = 169
    // (169 + 1) * 120 000 = 20 400 000
    topPsm.startMs shouldEqual 20400000
    topPsm.endMs shouldEqual 20880000
    topPsm.stepMs shouldEqual 120000
    topPsm.window shouldEqual None
    topPsm.functionId shouldEqual None
    partExec.chunkMethod.startTime shouldEqual 20100000
    partExec.chunkMethod.endTime shouldEqual 20880000
  }

  // Target-Schema start

  it("should stitch results when target-schema changes during query range") {
    val lp = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""", TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 2))
    }
    def targetSchema(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange] = {
      Seq(TargetSchemaChange(0, Seq("job")), TargetSchemaChange(25000000L, Seq("job")))
    }
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, plannerParams = PlannerParams
    (spreadOverride = Some(FunctionalSpreadProvider(spread)),
      targetSchemaProviderOverride = Some(FunctionalTargetSchemaProvider(targetSchema)), queryTimeoutMillis = 1000000)))
    println(execPlan)
    execPlan.rangeVectorTransformers.head.isInstanceOf[StitchRvsMapper] shouldEqual true
  }

  it("should apply the target schema appropriate to the query range") {

    val tschemas = Seq(
      TargetSchemaChange(0L, Seq("foo")),      // + shardKeys (job)
      TargetSchemaChange(10000, Seq("bar")),   // + shardKeys (job)
      TargetSchemaChange(20000, Seq("foo")),   // + shardKeys (job)
      TargetSchemaChange(30000, Seq("bar")),   // + shardKeys (job)
    )

    // Oscillate between "foo" and "bar" as the schema
    val queryRanges = Seq(
      (1000L,  9000L),
      (11000L, 19000L),
      (21000L, 29000L),
      (31000L, 39000L),
    )

    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 5))
    }

    def targetSchema(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange] = {
      tschemas
    }

    val tspOverride = Some(FunctionalTargetSchemaProvider(targetSchema))
    val spreadOverride = Some(FunctionalSpreadProvider(spread))
    val qContext = QueryContext(
      plannerParams = PlannerParams(
        spreadOverride = spreadOverride,
        targetSchemaProviderOverride = tspOverride))

    queryRanges.flatMap { case (start, end) =>
      engine.shardsFromFilters(
        Seq(
          ColumnFilter("job", Equals("hello")),
          ColumnFilter("__name__", Equals("name")),
          ColumnFilter("foo", Equals("abcdefg")),
          ColumnFilter("bar", Equals("hijklmnop")),
        ), qContext, start, end, useTargetSchemaForShards = (filts) => true)
    } should contain theSameElementsAs Seq(13, 20, 13, 20)
  }

  it("should create a single plan and not stitch results when target-schema has not changed in query range") {
    val lp = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""", TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 2))
    }
    def targetSchema(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange] = {
      Seq(TargetSchemaChange(0, Seq("job")), TargetSchemaChange(35000000L, Seq("job1")))
    }
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, plannerParams = PlannerParams
    (spreadOverride = Some(FunctionalSpreadProvider(spread)),
      targetSchemaProviderOverride = Some(FunctionalTargetSchemaProvider(targetSchema)), queryTimeoutMillis = 1000000)))
    execPlan.children.size shouldEqual 0
    execPlan.rangeVectorTransformers.head.isInstanceOf[StitchRvsMapper] shouldEqual false
  }

  it("should stitch results when target-schema has not changed but spread changed in query range") {
    val lp = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""", TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(25000000, 2)) // spread change time is in ms
    }
    def targetSchema(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange] = {
      Seq(TargetSchemaChange(0, Seq("job")))
    }
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, plannerParams = PlannerParams
    (spreadOverride = Some(FunctionalSpreadProvider(spread)),
      targetSchemaProviderOverride = Some(FunctionalTargetSchemaProvider(targetSchema)), queryTimeoutMillis = 1000000)))
    execPlan.children.size shouldEqual 0
    execPlan.rangeVectorTransformers.last.isInstanceOf[StitchRvsMapper] shouldEqual true
  }

  it("should stitch results when target-schema has changed but spread did not change in query range") {
    val lp = Parser.queryRangeToLogicalPlan("""foo{job="bar", instance="inst1"}""", TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 2)) // Spread 4
    }
    def targetSchema(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange] = {
      Seq(TargetSchemaChange(0, Seq("job")), TargetSchemaChange(25000000, Seq("job", "instance")))
    }
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, plannerParams = PlannerParams
    (spreadOverride = Some(FunctionalSpreadProvider(spread)),
      targetSchemaProviderOverride = Some(FunctionalTargetSchemaProvider(targetSchema)), queryTimeoutMillis = 1000000)))
    execPlan.children.size shouldEqual 4 // target-schema does not apply when there are changes during a query-window
    execPlan.rangeVectorTransformers.last.isInstanceOf[StitchRvsMapper] shouldEqual true
  }

  it("should not stitch when all the target-schema labels are present in column filters in a binary join") {
    val lp = Parser.queryRangeToLogicalPlan("""count(foo{job="bar"} + baz{job="bar"})""",
      TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 2))
    }
    def targetSchema(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange] = {
      Seq(TargetSchemaChange(0, Seq("job")))
    }
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, plannerParams = PlannerParams
    (spreadOverride = Some(FunctionalSpreadProvider(spread)),
      targetSchemaProviderOverride = Some(FunctionalTargetSchemaProvider(targetSchema)), queryTimeoutMillis = 1000000)))
    val binaryJoinNode = execPlan.children(0)
    binaryJoinNode.isInstanceOf[BinaryJoinExec] shouldEqual true
    binaryJoinNode.children.size shouldEqual 2
    binaryJoinNode.children.foreach(_.isInstanceOf[StitchRvsExec] shouldEqual false)
  }

  it("should create single child plan for LHS where target-schema filters provided" +
    " and 4 (spread 2) children for RHS (no target-schema filters) of the binary join") {
    val lp = Parser.queryRangeToLogicalPlan("""count(foo{job="bar", instance="inst1"} + baz{job="bar"})""",
      TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 2))
    }
    def targetSchema(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange] = {
      Seq(TargetSchemaChange(0, Seq("instance")))
    }
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, plannerParams = PlannerParams
    (spreadOverride = Some(FunctionalSpreadProvider(spread)),
      targetSchemaProviderOverride = Some(FunctionalTargetSchemaProvider(targetSchema)), queryTimeoutMillis = 1000000)))
    val binaryJoinNode = execPlan.children(0)
    binaryJoinNode.isInstanceOf[BinaryJoinExec] shouldEqual true
    binaryJoinNode.asInstanceOf[BinaryJoinExec].lhs.size shouldEqual 1
    binaryJoinNode.asInstanceOf[BinaryJoinExec].rhs.size shouldEqual 4
  }

  // Ignoring the test until we pickup this radar - rdar://108803361 (Fix vector(0) optimzation corner cases)
  ignore("should optimize or vector(0) queries by using InstantVectorFunctionMapper instead of SetOperatorExec") {

    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 2))
    }

    val queries = Seq(
      """foo{job="bar", instance="inst1"} or vector(0)""",
      """sum(foo{job="bar", instance="inst1"}) or vector(0)""",
    )
    val expected = Seq(
      """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-17403230],raw)
        |-T~InstantVectorFunctionMapper(function=OrVectorDouble)
        |--FA1~StaticFuncArgs(0.0,RangeParams(20000,100,30000))
        |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(instance,Equals(inst1)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-17403230],raw)
        |-T~InstantVectorFunctionMapper(function=OrVectorDouble)
        |--FA1~StaticFuncArgs(0.0,RangeParams(20000,100,30000))
        |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=13, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(instance,Equals(inst1)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-17403230],raw)
        |-T~InstantVectorFunctionMapper(function=OrVectorDouble)
        |--FA1~StaticFuncArgs(0.0,RangeParams(20000,100,30000))
        |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(instance,Equals(inst1)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-17403230],raw)
        |-T~InstantVectorFunctionMapper(function=OrVectorDouble)
        |--FA1~StaticFuncArgs(0.0,RangeParams(20000,100,30000))
        |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=29, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(instance,Equals(inst1)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-17403230],raw)""".stripMargin,

      """T~InstantVectorFunctionMapper(function=OrVectorDouble)
        |-FA1~StaticFuncArgs(0.0,RangeParams(20000,100,30000))
        |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
        |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(instance,Equals(inst1)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=13, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(instance,Equals(inst1)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(instance,Equals(inst1)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)
        |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=29, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(instance,Equals(inst1)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testActor],raw)""".stripMargin
    )

    queries.zip(expected).map { case (q, e) =>
      val lp = Parser.queryRangeToLogicalPlan(q, TimeStepParams(20000, 100, 30000))
      val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, plannerParams = PlannerParams
        (spreadOverride = Some(FunctionalSpreadProvider(spread)), queryTimeoutMillis = 1000000)))
      validatePlan(execPlan, e)
    }
  }

  it ("should pushdown BinaryJoins between different shard keys when shards are identical") {
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 5))
    }
    def targetSchema(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange] = {
      Seq(TargetSchemaChange(0, Seq("job", "app")))
    }
    val queries = Seq(
      """foo{job="baz"} + on(job,app) bar{job="bat"}""",
      """foo{job="baz"} + on(app) bar{job="bat"}""",
    )
    queries.foreach{ query =>
      val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(20000, 100, 30000))
      val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, plannerParams = PlannerParams(
              spreadOverride = Some(FunctionalSpreadProvider(spread)),
              targetSchemaProviderOverride = Some(FunctionalTargetSchemaProvider(targetSchema)),
              queryTimeoutMillis = 1000000)))
      execPlan.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
      execPlan.children.forall(c => c.isInstanceOf[BinaryJoinExec])
    }
  }

  it ("should pushdown BinaryJoins/Aggregates when valid") {

    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1))
    }
    def targetSchema(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange] = {
      Seq(TargetSchemaChange(0, Seq("job", "app")))
    }

    // Note: all expected plan strings are generated by sorting children on the
    //   smallest shard ID from which data is pulled.
    val queryExpectedPairs = Seq(

      // ============== BEGIN BINARY JOIN TESTS ==================

      // Binary join; same shards, join key is ts superset. Should pushdown.
      ("""foo{job="baz"} + on(job,app) bar{job="baz"}""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-284958332],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-284958332],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@765d55d5)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@765d55d5)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-284958332],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@765d55d5)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@765d55d5)""".stripMargin),
      // Binary join; different shards, join key is ts superset. Should not pushdown.
      ("""foo{job="baz"} + on(job, app) bar{job="bat"}""",
        """E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1594457115],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1594457115],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1594457115],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bat)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1594457115],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bat)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1594457115],raw)""".stripMargin),
      // BinaryJoin with SetOp; same shards, join key is strict ts superset. Should pushdown.
      ("""foo{job="baz"} and on(job, app, inst) bar{job="baz"}""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#169424276],raw)
          |-E~SetOperatorExec(binaryOp=LAND, on=List(job, app, inst), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#169424276],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@54755dd9)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@54755dd9)
          |-E~SetOperatorExec(binaryOp=LAND, on=List(job, app, inst), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#169424276],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@54755dd9)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@54755dd9)""".stripMargin),
      // BinaryJoin with SetOp; different shards, join key is ts superset. Should not pushdown.
      ("""foo{job="baz"} and on (job, app) bar{job="bat"}""",
        """E~SetOperatorExec(binaryOp=LAND, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-917749872],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-917749872],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-917749872],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bat)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-917749872],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bat)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-917749872],raw)""".stripMargin),
      // Should pushdown nested joins.
      ("""(foo{job="baz"} - on(job, app) bar{job="baz"}) and on(job, app) bat{job="baz"}""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1262424879],raw)
          |-E~SetOperatorExec(binaryOp=LAND, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1262424879],raw)
          |--E~BinaryJoinExec(binaryOp=SUB, on=List(job, app), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@2ddb3ae8)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@2ddb3ae8)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@2ddb3ae8)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@2ddb3ae8)
          |-E~SetOperatorExec(binaryOp=LAND, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1262424879],raw)
          |--E~BinaryJoinExec(binaryOp=SUB, on=List(job, app), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@2ddb3ae8)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@2ddb3ae8)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@2ddb3ae8)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@2ddb3ae8)""".stripMargin),
      // Should only pushdown inner join since outer join key is not ts superset.
      ("""(foo{job="baz"} - on(job, app) bar{job="baz"}) and on(job) bat{job="baz"}""",
        """E~SetOperatorExec(binaryOp=LAND, on=List(job), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1264611289],raw)
          |-E~BinaryJoinExec(binaryOp=SUB, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1264611289],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@765d55d5)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@765d55d5)
          |-E~BinaryJoinExec(binaryOp=SUB, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1264611289],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@765d55d5)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@765d55d5)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1264611289],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1264611289],raw)""".stripMargin),
      // Should not pushdown; missing ts "on" labels.
      ("""foo{job="baz"} + on(job, label, inst) bar{job="baz"}""",
        """E~BinaryJoinExec(binaryOp=ADD, on=List(job, label, inst), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1632120907],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1632120907],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1632120907],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1632120907],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1632120907],raw)""".stripMargin),
      // Should not pushdown w/ ignoring clause.
      ("""foo{job="baz"} + ignoring(inst) baz{job="baz"}""",
        """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List(inst)) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1742163606],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1742163606],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1742163606],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1742163606],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1742163606],raw)""".stripMargin),
      // Should pushdown each mapper function.
      ("""sgn(foo{job="baz"}) + on(job, app) rate(bar{job="baz"}[1m])""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1164681354],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1164681354],raw)
          |--T~InstantVectorFunctionMapper(function=Sgn)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@765d55d5)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=Some(60000), functionId=Some(Rate), rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19940000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@765d55d5)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1164681354],raw)
          |--T~InstantVectorFunctionMapper(function=Sgn)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@765d55d5)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=Some(60000), functionId=Some(Rate), rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19940000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@765d55d5)""".stripMargin),
      // Should pushdown outer mapper function.
      ("""sgn(foo{job="baz"} + on(job, app) bar{job="baz"})""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1827417788],raw)
          |-T~InstantVectorFunctionMapper(function=Sgn)
          |--E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1827417788],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@765d55d5)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@765d55d5)
          |-T~InstantVectorFunctionMapper(function=Sgn)
          |--E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1827417788],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@765d55d5)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@765d55d5)""".stripMargin),
      // Should pushdown inner join.
      ("""sum(foo{job="baz"} + on(job, app) bat{job="baz"})""",
        """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-852082083],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |---E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-852082083],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@7c3e4b1a)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@7c3e4b1a)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |---E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-852082083],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@7c3e4b1a)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@7c3e4b1a)""".stripMargin),
      // Should pushdown most scalars (see below for the exceptions).
      ("""(foo{job="bak"} + 3 + time()) + on(job, app) (baz{job="bak"} + (4 + 5))""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1563151078],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1563151078],raw)
          |--T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
          |---FA1~TimeFuncArgs(RangeParams(20000,100,30000))
          |---T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
          |----FA1~StaticFuncArgs(3.0,RangeParams(20000,100,30000))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=7, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@9301672)
          |--T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
          |---FA1~
          |---E~ScalarBinaryOperationExec(params = RangeParams(20000,100,30000), operator = ADD, lhs = Left(4.0), rhs = Left(5.0)) on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@5a545b0f)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=7, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@9301672)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1563151078],raw)
          |--T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
          |---FA1~TimeFuncArgs(RangeParams(20000,100,30000))
          |---T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
          |----FA1~StaticFuncArgs(3.0,RangeParams(20000,100,30000))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=23, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@9301672)
          |--T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
          |---FA1~
          |---E~ScalarBinaryOperationExec(params = RangeParams(20000,100,30000), operator = ADD, lhs = Left(4.0), rhs = Left(5.0)) on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@5a545b0f)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=23, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@9301672)""".stripMargin),
      // Should not pushdown scalar(<vector>) or vector(<scalar>).
      ("""(foo{job="bak"} + on(job, app) scalar(bar{job="bak"})) + (foo{job="bak"} + on(job, app) vector(0))""",
        """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@6749fe50)
          |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
          |--FA1~
          |--T~ScalarFunctionMapper(function=Scalar, funcParams=List())
          |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2037240425],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=7, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2037240425],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=23, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2037240425],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=7, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2037240425],raw)
          |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
          |--FA1~
          |--T~ScalarFunctionMapper(function=Scalar, funcParams=List())
          |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2037240425],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=7, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2037240425],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=23, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2037240425],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=23, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2037240425],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@6749fe50)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=7, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2037240425],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=23, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2037240425],raw)
          |--T~VectorFunctionMapper(funcParams=List())
          |---E~ScalarFixedDoubleExec(params = RangeParams(20000,100,30000), value = 0.0) on InProcessPlanDispatcher(filodb.core.query.EmptyQueryConfig$@6749fe50)""".stripMargin),
      // Should not pushdown with regex when some target-schema cols are missing from `on` clause.
      ("""foo{job="baz",app=~"abc|123"} + on(job) bar{job="baz",app=~"abc|123"}""",
        """E~BinaryJoinExec(binaryOp=ADD, on=List(job), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1629736397],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(app,EqualsRegex(abc|123)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1629736397],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(app,EqualsRegex(abc|123)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1629736397],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(app,EqualsRegex(abc|123)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1629736397],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(app,EqualsRegex(abc|123)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1629736397],raw)""".stripMargin),
      // Should pushdown with regex when all target-schema cols are given in `on` clause.
      ("""foo{job="baz",app=~"abc|123"} + on(job,app) bar{job="baz",app=~"abc|123"}""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-840432554],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-840432554],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(app,EqualsRegex(abc|123)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(app,EqualsRegex(abc|123)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-840432554],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(app,EqualsRegex(abc|123)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(app,EqualsRegex(abc|123)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))""".stripMargin),
      // Should only pushdown bottom / inner-most join.
      ("""(
         |  (
         |    foo{job="baz"} + on(job, inst, tag) bar{job="baz"}
         |  ) + on(job, inst) (
         |    rate(foo{job="baz"}[1m]) + ignoring(inst) ceil(bar{job="baz"})
         |  )
         |) + on(job, inst, tag) (
         |  (
         |    floor(foo{job="baz"}) + on(job) bar{job="baz"}
         |  ) + on(job, inst) (
         |    sgn(rate(foo{job="baz"}[1m])) + on(job, app) sqrt(bar{job="baz"})
         |  )
         |)""".stripMargin,
        """E~BinaryJoinExec(binaryOp=ADD, on=List(job, inst, tag), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(job, inst), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |--E~BinaryJoinExec(binaryOp=ADD, on=List(job, inst, tag), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |--E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List(inst)) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=Some(60000), functionId=Some(Rate), rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19940000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=Some(60000), functionId=Some(Rate), rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19940000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |---T~InstantVectorFunctionMapper(function=Ceil)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |---T~InstantVectorFunctionMapper(function=Ceil)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(job, inst), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |--E~BinaryJoinExec(binaryOp=ADD, on=List(job), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |---T~InstantVectorFunctionMapper(function=Floor)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |---T~InstantVectorFunctionMapper(function=Floor)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |--E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |---T~InstantVectorFunctionMapper(function=Sgn)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=Some(60000), functionId=Some(Rate), rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19940000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |---T~InstantVectorFunctionMapper(function=Sqrt)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-114649842],raw)
          |---T~InstantVectorFunctionMapper(function=Sgn)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=Some(60000), functionId=Some(Rate), rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19940000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |---T~InstantVectorFunctionMapper(function=Sqrt)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),

      // ============== BEGIN AGGREGATE TESTS ==================

      // Should not pushdown; no by columns.
      ("""sum(foo{job="bar"})""",
        """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1878225981],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1878225981],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1878225981],raw)""".stripMargin),
      // Should not pushdown; insufficient by columns.
      ("""sum(foo{job="bar"}) by (job, inst)""",
        """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2008564919],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, inst))
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2008564919],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, inst))
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2008564919],raw)""".stripMargin),
      // Should not pushdown; without column specified.
      ("""sum(foo{job="bar"}) without (inst)""",
        """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-667284122],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(inst), by=List())
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-667284122],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(inst), by=List())
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-667284122],raw)""".stripMargin),
      // Should pushdown; TS columns specified.
      ("""sum(foo{job="bar"}) by (job, app)""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1322662450],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1322662450],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5740ff5e)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1322662450],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5740ff5e)""".stripMargin),
      // Should pushdown; TS column superset specified.
      ("""sum(foo{job="bar"}) by (job, app, inst)""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2103736689],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2103736689],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app, inst))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5740ff5e)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2103736689],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app, inst))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5740ff5e)""".stripMargin),
      // Should pushdown nested aggregates when all by columns are TS supersets.
      ("""sum(sum(foo{job="bar"}) by (job, app, inst)) by (job, app)""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1479596767],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1479596767],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@6848a051)
          |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app, inst))
          |-------T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@6848a051)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1479596767],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@6848a051)
          |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app, inst))
          |-------T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@6848a051)""".stripMargin),
      // Should pushdown only inner aggregate when outer by columns are not TS superset.
      ("""sum(sum(foo{job="bar"}) by (job, app, inst)) by (job, inst)""",
        """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1071476300],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, inst))
          |---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1071476300],raw)
          |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app, inst))
          |------T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5740ff5e)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, inst))
          |---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1071476300],raw)
          |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app, inst))
          |------T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5740ff5e)""".stripMargin),
      // Should not pushdown when inner by columns are not TS superset.
      ("""sum(sum(foo{job="bar"}) by (job)) by (job, app)""",
        """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#182240494],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#182240494],raw)
          |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
          |------T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#182240494],raw)
          |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
          |------T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#182240494],raw)""".stripMargin),
      // Should pushdown inner mapper.
      ("""sum(sgn(foo{job="bar"})) by (job, app)""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1210748418],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1210748418],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |----T~InstantVectorFunctionMapper(function=Sgn)
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1210748418],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |----T~InstantVectorFunctionMapper(function=Sgn)
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))""".stripMargin),
      // Should pushdown outer mapper.
      ("""sgn(sum(foo{job="bar"}) by (job, app))""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1059429645],raw)
          |-T~InstantVectorFunctionMapper(function=Sgn)
          |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1059429645],raw)
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
          |-T~InstantVectorFunctionMapper(function=Sgn)
          |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1059429645],raw)
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))""".stripMargin),
      // Should pushdown with regex when all target-schema cols are given in `by` clause.
      ("""sum(foo{job="bar",app=~"abc|def"}) by (job,app)""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1645725393],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1645725393],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(app,EqualsRegex(abc|def)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1645725393],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(app,EqualsRegex(abc|def)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))""".stripMargin),
      // Should not pushdown with regex when some target-schema cols are missing from `by` clause.
      ("""sum(foo{job="bar",app=~"abc|def"}) by (job)""",
        """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1376873951],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(app,EqualsRegex(abc|def)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1376873951],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(app,EqualsRegex(abc|def)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1376873951],raw)""".stripMargin),

      // ============== BEGIN COMPOUND TESTS ==================

      // Should not pushdown join when aggregate 'by' labels are not TS label supersets.
      ("""sum(foo{job="baz"}) by (job) + on(job, app) bat{job="baz"}""",
        """E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#564777118],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#564777118],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#564777118],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#564777118],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#564777118],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#564777118],raw)""".stripMargin),
      // Should only pushdown aggregate when join 'on' labels are not TS label supersets.
      ("""sum(foo{job="baz"}) by (job, app) + on(job) bat{job="baz"}""",
        """E~BinaryJoinExec(binaryOp=ADD, on=List(job), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1162894714],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1162894714],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1162894714],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1162894714],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1162894714],raw)""".stripMargin),
      // Should pushdown join and aggregate when by/on labels are TS label supersets.
      ("""sum(foo{job="baz"}) by (job, app) + on(job, app) bat{job="baz"}""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-747302117],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-747302117],raw)
          |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5e5aafc6)
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5e5aafc6)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5e5aafc6)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-747302117],raw)
          |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5e5aafc6)
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5e5aafc6)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5e5aafc6)""".stripMargin),
      // Should pushdown join and aggregates when by/on labels are TS label supersets.
      ("""sum(foo{job="baz"}) by (job, app) + on(job, app) sum(bat{job="baz"}) by (job, app)""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1974371112],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1974371112],raw)
          |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)
          |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1974371112],raw)
          |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)
          |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)""".stripMargin),
      // Should pushdown only one aggregate when other 'by' labels are not TS label supersets.
      ("""sum(foo{job="baz"}) by (job, app) + on(job, app) sum(bat{job="baz"}) by (job)""",
        """E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1736249280],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1736249280],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1736249280],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None))
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1736249280],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1736249280],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1736249280],raw)""".stripMargin),
      // Should pushdown join and aggregates when by/on labels are TS label supersets.
      ("""sum(foo{job="baz"} + on(job, app) bat{job="baz"}) by (job, app)""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1207946820],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1207946820],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |----E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1207946820],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |----E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)""".stripMargin),
      // Should not pushdown when inner join 'on' labels are not TS superset.
      ("""sum(foo{job="baz"} + on(job) bat{job="baz"}) by (job, app)""",
        """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1835060741],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job, app))
          |---E~BinaryJoinExec(binaryOp=ADD, on=List(job), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1835060741],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1835060741],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1835060741],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1835060741],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1835060741],raw)""".stripMargin),
      // Should only pushdown join when aggregate 'by' labels are not TS superset.
      ("""sum(foo{job="baz"} + on(job, app) bat{job="baz"}) by (job)""",
        """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2022779139],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
          |---E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2022779139],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
          |---E~BinaryJoinExec(binaryOp=ADD, on=List(job, app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#2022779139],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@53ed80d3)""".stripMargin)
    )
    queryExpectedPairs.foreach{ case (query, expected) =>
      val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(20000, 100, 30000))
      val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, plannerParams = PlannerParams
      (spreadOverride = Some(FunctionalSpreadProvider(spread)),
        targetSchemaProviderOverride = Some(FunctionalTargetSchemaProvider(targetSchema)), queryTimeoutMillis = 1000000)))
      try {
        validatePlan(execPlan, expected)
      } catch {
        case e: TestFailedException =>
          println(s"Plan validation failed for query: $query")
          println(execPlan.printTree())
          throw e
      }
    }
  }

  it ("should pushdown BinaryJoins/Aggregates when valid and clause shard key labels are implicit") {

    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1))
    }
    def targetSchema(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange] = {
      Seq(TargetSchemaChange(0, Seq("app")))
    }

    // Note: all expected plan strings are generated by sorting children on the
    //   smallest shard ID from which data is pulled.
    val queryExpectedPairs = Seq(

      // ============== BEGIN BINARY JOIN TESTS ==================

      // Binary join; same shards, join key is ts superset. Should pushdown.
      ("""foo{job="baz"} + on(app) bar{job="baz"}""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-932570002],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-932570002],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-932570002],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Binary join; different shards, join key is ts superset. Should not pushdown.
      ("""foo{job="baz"} + on(app) bar{job="bat"}""",
        """E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1973748247],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1973748247],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1973748247],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bat)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1973748247],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bat)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1973748247],raw)""".stripMargin),
      // BinaryJoin with SetOp; same shards, join key is strict ts superset. Should pushdown.
      ("""foo{job="baz"} and on(app, inst) bar{job="baz"}""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1851388859],raw)
          |-E~SetOperatorExec(binaryOp=LAND, on=List(app, inst), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1851388859],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-E~SetOperatorExec(binaryOp=LAND, on=List(app, inst), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1851388859],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // BinaryJoin with SetOp; different shards, join key is ts superset. Should not pushdown.
      ("""foo{job="baz"} and on (app) bar{job="bat"}""",
        """E~SetOperatorExec(binaryOp=LAND, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#467623007],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#467623007],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#467623007],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=6, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bat)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#467623007],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=22, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bat)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#467623007],raw)""".stripMargin),
      // Should pushdown nested joins.
      ("""(foo{job="baz"} - on(app) bar{job="baz"}) and on(app) bat{job="baz"}""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1223793877],raw)
          |-E~SetOperatorExec(binaryOp=LAND, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1223793877],raw)
          |--E~BinaryJoinExec(binaryOp=SUB, on=List(app), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-E~SetOperatorExec(binaryOp=LAND, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1223793877],raw)
          |--E~BinaryJoinExec(binaryOp=SUB, on=List(app), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Should only pushdown inner join since outer join key is not ts superset.
      ("""(foo{job="baz"} - on(app) bar{job="baz"}) and on() bat{job="baz"}""",
        """E~SetOperatorExec(binaryOp=LAND, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1687173675],raw)
          |-E~BinaryJoinExec(binaryOp=SUB, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1687173675],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-E~BinaryJoinExec(binaryOp=SUB, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1687173675],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1687173675],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1687173675],raw)""".stripMargin),
      // Should not pushdown; missing ts "on" labels.
      ("""foo{job="baz"} + on(label, inst) bar{job="baz"}""",
        """E~BinaryJoinExec(binaryOp=ADD, on=List(label, inst), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#837645626],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#837645626],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#837645626],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#837645626],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#837645626],raw)""".stripMargin),
      // Should pushdown each mapper function.
      ("""sgn(foo{job="baz"}) + on(app) rate(bar{job="baz"}[1m])""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~InstantVectorFunctionMapper(function=Sgn)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=Some(60000), functionId=Some(Rate), rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19940000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~InstantVectorFunctionMapper(function=Sgn)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=Some(60000), functionId=Some(Rate), rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19940000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Should pushdown outer mapper function.
      ("""sgn(foo{job="baz"} + on(app) bar{job="baz"})""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~InstantVectorFunctionMapper(function=Sgn)
          |--E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-T~InstantVectorFunctionMapper(function=Sgn)
          |--E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Should pushdown inner join.
      ("""sum(foo{job="baz"} + on(app) bat{job="baz"})""",
        """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |---E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |---E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Should pushdown most scalars (see below for the exceptions).
      ("""(foo{job="bak"} + 3 + time()) + on(app) (baz{job="bak"} + (4 + 5))""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
          |---FA1~TimeFuncArgs(RangeParams(20000,100,30000))
          |---T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
          |----FA1~StaticFuncArgs(3.0,RangeParams(20000,100,30000))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=7, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
          |---FA1~
          |---E~ScalarBinaryOperationExec(params = RangeParams(20000,100,30000), operator = ADD, lhs = Left(4.0), rhs = Left(5.0)) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=7, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
          |---FA1~TimeFuncArgs(RangeParams(20000,100,30000))
          |---T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
          |----FA1~StaticFuncArgs(3.0,RangeParams(20000,100,30000))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=23, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
          |---FA1~
          |---E~ScalarBinaryOperationExec(params = RangeParams(20000,100,30000), operator = ADD, lhs = Left(4.0), rhs = Left(5.0)) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=23, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(baz))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Should not pushdown scalar(<vector>) or vector(<scalar>).
      ("""(foo{job="bak"} + on(app) scalar(bar{job="bak"})) + (foo{job="bak"} + on(app) vector(0))""",
        """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
          |--FA1~
          |--T~ScalarFunctionMapper(function=Scalar, funcParams=List())
          |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=7, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=23, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=7, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
          |--FA1~
          |--T~ScalarFunctionMapper(function=Scalar, funcParams=List())
          |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=7, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=23, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=23, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=7, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=23, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bak)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~VectorFunctionMapper(funcParams=List())
          |---E~ScalarFixedDoubleExec(params = RangeParams(20000,100,30000), value = 0.0) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Should not pushdown with regex when some target-schema cols are missing from `on` clause.
      ("""foo{job="baz",app=~"abc|123"} + on() bar{job="baz",app=~"abc|123"}""",
        """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(app,EqualsRegex(abc|123)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(app,EqualsRegex(abc|123)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(app,EqualsRegex(abc|123)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(app,EqualsRegex(abc|123)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)""".stripMargin),
      // Should pushdown with regex when all target-schema cols are given in `on` clause.
      ("""foo{job="baz",app=~"abc|123"} + on(app) bar{job="baz",app=~"abc|123"}""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(app,EqualsRegex(abc|123)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(app,EqualsRegex(abc|123)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(app,EqualsRegex(abc|123)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(app,EqualsRegex(abc|123)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Should only pushdown bottom / inner-most join.
      ("""(
         |  (
         |    foo{job="baz"} + on(inst, tag) bar{job="baz"}
         |  ) + on(inst) (
         |    rate(foo{job="baz"}[1m]) + ignoring(inst) ceil(bar{job="baz"})
         |  )
         |) + on(inst, tag) (
         |  (
         |    floor(foo{job="baz"}) + on() bar{job="baz"}
         |  ) + on(inst) (
         |    sgn(rate(foo{job="baz"}[1m])) + on(app) sqrt(bar{job="baz"})
         |  )
         |)""".stripMargin,
        """E~BinaryJoinExec(binaryOp=ADD, on=List(inst, tag), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(inst), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--E~BinaryJoinExec(binaryOp=ADD, on=List(inst, tag), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List(inst)) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=Some(60000), functionId=Some(Rate), rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19940000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=Some(60000), functionId=Some(Rate), rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19940000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~InstantVectorFunctionMapper(function=Ceil)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~InstantVectorFunctionMapper(function=Ceil)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(inst), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~InstantVectorFunctionMapper(function=Floor)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~InstantVectorFunctionMapper(function=Floor)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~InstantVectorFunctionMapper(function=Sgn)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=Some(60000), functionId=Some(Rate), rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19940000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |---T~InstantVectorFunctionMapper(function=Sqrt)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~InstantVectorFunctionMapper(function=Sgn)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=Some(60000), functionId=Some(Rate), rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19940000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |---T~InstantVectorFunctionMapper(function=Sqrt)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),

      // ============== BEGIN AGGREGATE TESTS ==================

      // Should not pushdown; insufficient by columns.
      ("""sum(foo{job="bar"}) by (inst)""",
        """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(inst))
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(inst))
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)""".stripMargin),
      // Should pushdown; TS columns specified.
      ("""sum(foo{job="bar"}) by (app)""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Should pushdown; TS column superset specified.
      ("""sum(foo{job="bar"}) by (app, inst)""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app, inst))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app, inst))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Should pushdown nested aggregates when all by columns are TS supersets.
      ("""sum(sum(foo{job="bar"}) by (app, inst)) by (app)""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app, inst))
          |-------T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |----T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |------T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app, inst))
          |-------T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Should pushdown only inner aggregate when outer by columns are not TS superset.
      ("""sum(sum(foo{job="bar"}) by (app, inst)) by (inst)""",
        """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#397822311],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(inst))
          |---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#397822311],raw)
          |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app, inst))
          |------T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(inst))
          |---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#397822311],raw)
          |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app, inst))
          |------T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Should not pushdown when inner by columns are not TS superset.
      ("""sum(sum(foo{job="bar"}) by ()) by (app)""",
        """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |------T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |------T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)""".stripMargin),
      // Should pushdown inner mapper.
      ("""sum(sgn(foo{job="bar"})) by (app)""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |----T~InstantVectorFunctionMapper(function=Sgn)
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |----T~InstantVectorFunctionMapper(function=Sgn)
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Should pushdown outer mapper.
      ("""sgn(sum(foo{job="bar"}) by (app))""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~InstantVectorFunctionMapper(function=Sgn)
          |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-T~InstantVectorFunctionMapper(function=Sgn)
          |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Should pushdown with regex when all target-schema cols are given in `by` clause.
      ("""sum(foo{job="bar",app=~"abc|def"}) by (app)""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(app,EqualsRegex(abc|def)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(app,EqualsRegex(abc|def)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Should not pushdown with regex when some target-schema cols are missing from `by` clause.
      ("""sum(foo{job="bar",app=~"abc|def"}) by ()""",
        """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(app,EqualsRegex(abc|def)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |---T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(bar)), ColumnFilter(app,EqualsRegex(abc|def)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)""".stripMargin),

      // ============== BEGIN COMPOUND TESTS ==================

      // Should not pushdown join when aggregate 'by' labels are not TS label supersets.
      ("""sum(foo{job="baz"}) by () + on(app) bat{job="baz"}""",
        """E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)""".stripMargin),
      // Should only pushdown aggregate when join 'on' labels are not TS label supersets.
      ("""sum(foo{job="baz"}) by (app) + on() bat{job="baz"}""",
        """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)""".stripMargin),
      // Should pushdown join and aggregate when by/on labels are TS label supersets.
      ("""sum(foo{job="baz"}) by (app) + on(app) bat{job="baz"}""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Should pushdown join and aggregates when by/on labels are TS label supersets.
      ("""sum(foo{job="baz"}) by (app) + on(app) sum(bat{job="baz"}) by (app)""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Should pushdown only one aggregate when other 'by' labels are not TS label supersets.
      ("""sum(foo{job="baz"}) by (app) + on(app) sum(bat{job="baz"}) by ()""",
        """E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)""".stripMargin),
      // Should pushdown join and aggregates when by/on labels are TS label supersets.
      ("""sum(foo{job="baz"} + on(app) bat{job="baz"}) by (app)""",
        """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |----E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |----E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |-----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin),
      // Should not pushdown when inner join 'on' labels are not TS superset.
      ("""sum(foo{job="baz"} + on() bat{job="baz"}) by (app)""",
        """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(app))
          |---E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)""".stripMargin),
      // Should only pushdown join when aggregate 'by' labels are not TS superset.
      ("""sum(foo{job="baz"} + on(app) bat{job="baz"}) by ()""",
        """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(20000,100,30000))
          |-E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |---E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=1, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |--T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
          |---E~BinaryJoinExec(binaryOp=ADD, on=List(app), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1929473151],raw)
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(foo))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))
          |----T~PeriodicSamplesMapper(start=20000000, step=100000, end=30000000, window=None, functionId=None, rawSource=true, offsetMs=None)
          |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=17, chunkMethod=TimeRangeChunkScan(19700000,30000000), filters=List(ColumnFilter(job,Equals(baz)), ColumnFilter(__name__,Equals(bat))), colName=None, schema=None) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,None,true,false,true))""".stripMargin)
    )
    queryExpectedPairs.foreach{ case (query, expected) =>
      val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(20000, 100, 30000))
      val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, plannerParams = PlannerParams
      (spreadOverride = Some(FunctionalSpreadProvider(spread)),
        targetSchemaProviderOverride = Some(FunctionalTargetSchemaProvider(targetSchema)), queryTimeoutMillis = 1000000)))
      try {
        validatePlan(execPlan, expected)
      } catch {
        case e: TestFailedException =>
          println(s"Plan validation failed for query: $query")
          throw e
      }
    }
  }

  // end

  it("should stitch results when spread changes during query range") {
    val lp = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""", TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(25000000, 2)) // spread change time is in ms
    }
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, plannerParams = PlannerParams
    (spreadOverride = Some(FunctionalSpreadProvider(spread)), queryTimeoutMillis = 1000000)))
    execPlan.rangeVectorTransformers.head.isInstanceOf[StitchRvsMapper] shouldEqual true
  }

  it("should not stitch results when spread has not changed in query range") {
    val lp = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""", TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(35000000, 2))
    }
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, plannerParams = PlannerParams
    (spreadOverride = Some(FunctionalSpreadProvider(spread)), queryTimeoutMillis = 1000000)))
    execPlan.rangeVectorTransformers.isEmpty shouldEqual true
  }

  it("should stitch results before binary join when spread changed in query range") {
    val lp = Parser.queryRangeToLogicalPlan("""count(foo{job="bar"} + baz{job="bar"})""",
      TimeStepParams(20000, 100, 30000))
    def spread(filter: Seq[ColumnFilter]): Seq[SpreadChange] = {
      Seq(SpreadChange(0, 1), SpreadChange(25000000, 2))
    }
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, plannerParams = PlannerParams
    (spreadOverride = Some(FunctionalSpreadProvider(spread)), queryTimeoutMillis = 1000000)))
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
    val execPlan = engine.materialize(lp, QueryContext(promQlQueryParams, plannerParams = PlannerParams
    (spreadOverride = Some(FunctionalSpreadProvider(spread)), queryTimeoutMillis = 1000000)))
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
      l1.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
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
     val planner = new SingleClusterPlanner(dataset, schemas, mapperRef,
       earliestRetainedTimestampFn = nowSeconds * 1000 - 3.days.toMillis, queryConfig, "raw")

    // Case 1: no offset or window
    val logicalPlan1 = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""",
      TimeStepParams(nowSeconds - 4.days.toSeconds, 1.minute.toSeconds, nowSeconds))

    val ep1 = planner.materialize(logicalPlan1, QueryContext()).asInstanceOf[LocalPartitionDistConcatExec]
    val psm1 = ep1.children.head.asInstanceOf[MultiSchemaPartitionsExec]
                .rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper]
    psm1.startMs shouldEqual (nowSeconds * 1000
                            - 3.days.toMillis // retention
                            + 1.minute.toMillis // step
                            + WindowConstants.staleDataLookbackMillis) // default window

    // Case 2: no offset, some window
    val logicalPlan2 = Parser.queryRangeToLogicalPlan("""rate(foo{job="bar"}[20m])""",
      TimeStepParams(nowSeconds - 4.days.toSeconds, 1.minute.toSeconds, nowSeconds))

    val ep2 = planner.materialize(logicalPlan2, QueryContext()).asInstanceOf[LocalPartitionDistConcatExec]
    val psm2 = ep2.children.head.asInstanceOf[MultiSchemaPartitionsExec]
      .rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper]
    psm2.startMs shouldEqual (nowSeconds * 1000
      - 3.days.toMillis // retention
      + 1.minute.toMillis // step
      + 20.minutes.toMillis) // window
    psm2.endMs shouldEqual nowSeconds * 1000

    // Case 3: offset and some window
    val logicalPlan3 = Parser.queryRangeToLogicalPlan("""rate(foo{job="bar"}[20m] offset 15m)""",
      TimeStepParams(nowSeconds - 4.days.toSeconds, 1.minute.toSeconds, nowSeconds))

    val ep3 = planner.materialize(logicalPlan3, QueryContext()).asInstanceOf[LocalPartitionDistConcatExec]
    val psm3 = ep3.children.head.asInstanceOf[MultiSchemaPartitionsExec]
      .rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper]
    psm3.startMs shouldEqual (nowSeconds * 1000
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
    val res = ep4.dispatcher.dispatch(ExecPlanWithClientParams(ep4, ClientParams
    (ep4.queryContext.plannerParams.queryTimeoutMillis)), UnsupportedChunkSource()).runToFuture.futureValue.asInstanceOf[QueryResult]
    res.result.isEmpty shouldEqual true
  }

  it("should materialize instant queries with lookback == retention correctly") {
    val nowSeconds = System.currentTimeMillis() / 1000
    val planner = new SingleClusterPlanner(dataset, schemas, mapperRef,
      earliestRetainedTimestampFn = nowSeconds * 1000 - 3.days.toMillis, queryConfig, "raw")

    val logicalPlan = Parser.queryRangeToLogicalPlan("""sum(rate(foo{job="bar"}[3d]))""",
      TimeStepParams(nowSeconds, 1.minute.toSeconds, nowSeconds))

    val ep = planner.materialize(logicalPlan, QueryContext(origQueryParams = PromQlQueryParams
    ("""sum(rate(foo{job="bar"}[3d]))""",1000, 100, 1000))).asInstanceOf[LocalPartitionReduceAggregateExec]
    val psm = ep.children.head.asInstanceOf[MultiSchemaPartitionsExec]
      .rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper]
    psm.startMs shouldEqual (nowSeconds * 1000)
    psm.endMs shouldEqual (nowSeconds * 1000)
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
    rvt.startMs shouldEqual 700000 // start and end should be same as query TimeStepParams
    rvt.endMs shouldEqual 10000000
    rvt.stepMs shouldEqual 1000000
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
    rvt.startMs shouldEqual 700000
    rvt.endMs shouldEqual 10000000
    rvt.stepMs shouldEqual 1000000
  }

  it ("should replace __name__ with _metric_ in by and without") {
    val dataset = MetricsTestData.timeseriesDatasetWithMetric
    val dsRef = dataset.ref
    val schemas = Schemas(dataset.schema)

    val engine = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = 0, queryConfig,
      "raw")

    val logicalPlan1 = Parser.queryRangeToLogicalPlan("""sum(foo{_ns_="bar", _ws_="test"}) by (__name__)""",
      TimeStepParams(1000, 20, 2000))

    val execPlan1 = engine.materialize(logicalPlan1, QueryContext(origQueryParams = promQlQueryParams))

    execPlan1.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
    execPlan1.children.foreach { l1 =>
      l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
      l1.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
      l1.rangeVectorTransformers(1).asInstanceOf[AggregateMapReduce].clauseOpt shouldEqual AggregateClause.byOpt(Seq("_metric_"))
    }

    val logicalPlan2 = Parser.queryRangeToLogicalPlan(
      """sum(foo{_ns_="bar", _ws_="test"})
        |without (__name__, instance)""".stripMargin,
      TimeStepParams(1000, 20, 2000))

    // materialized exec plan
    val execPlan2 = engine.materialize(logicalPlan2, QueryContext(origQueryParams = promQlQueryParams))

    execPlan2.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
    execPlan2.children.foreach { l1 =>
      l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
      l1.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
      l1.rangeVectorTransformers(1).asInstanceOf[AggregateMapReduce].clauseOpt shouldEqual AggregateClause.withoutOpt(Seq("_metric_", "instance"))
    }
  }

  it ("should replace __name__ with _metric_ in ignoring and group_left/group_right") {
      val dataset = MetricsTestData.timeseriesDatasetWithMetric
      val dsRef = dataset.ref
      val schemas = Schemas(dataset.schema)

      val engine = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = 0, queryConfig,
        "raw")

      val logicalPlan1 = Parser.queryRangeToLogicalPlan(
        """sum(foo{_ns_="bar1", _ws_="test"}) + ignoring(__name__)
          | sum(foo{_ns_="bar2", _ws_="test"})""".stripMargin,
        TimeStepParams(1000, 20, 2000))
      val execPlan2 = engine.materialize(logicalPlan1, QueryContext(origQueryParams = promQlQueryParams))

      execPlan2.isInstanceOf[BinaryJoinExec] shouldEqual true
      execPlan2.asInstanceOf[BinaryJoinExec].ignoring shouldEqual Seq("_metric_")

      val logicalPlan2 = Parser.queryRangeToLogicalPlan(
        """sum(foo{_ns_="bar1", _ws_="test"}) + ignoring(__name__) group_left(__name__)
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
    rvt1.startMs shouldEqual 700000
    rvt1.endMs shouldEqual 10000000
    rvt1.stepMs shouldEqual 1000000

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
    rvt2.startMs shouldEqual 700000
    rvt2.endMs shouldEqual 10000000
    rvt2.stepMs shouldEqual 1000000
  }

  it("periodicSamplesMapper time should be same as aggregatePresenter time") {
    val now = System.currentTimeMillis()
    val rawRetentionTime = 10.minutes.toMillis
    val logicalPlan = Parser.queryRangeToLogicalPlan("""topk(2, foo{job = "app"})""",
      TimeStepParams(now/1000 - 20.minutes.toSeconds, 1.minute.toSeconds, now/1000))
    val engine = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = now -
      rawRetentionTime, queryConfig, "raw")
    val ep = engine.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))
    ep.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)
    val presenterTime = ep.asInstanceOf[LocalPartitionReduceAggregateExec].rangeVectorTransformers.head.asInstanceOf[AggregatePresenter].rangeParams
    val periodicSamplesMapper = ep.children.head.rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper]
    ep.asInstanceOf[LocalPartitionReduceAggregateExec].maxRecordContainerSize(queryConfig) shouldEqual 65536
    presenterTime.startSecs shouldEqual(periodicSamplesMapper.startMs/1000)
    presenterTime.endSecs shouldEqual(periodicSamplesMapper.endMs/1000)
  }

  it("should generate empty exec plan when end time is less than earliest raw retention time ") {
    val now = System.currentTimeMillis()
    val rawRetention = 10.minutes.toMillis
    val logicalPlan = Parser.queryRangeToLogicalPlan("""topk(2, foo{job = "app"})""",
      TimeStepParams(now/1000 - 20.minutes.toSeconds, 1.minute.toSeconds, now/1000 - 12.minutes.toSeconds))
    val engine = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = now - rawRetention,
      queryConfig, "raw")
    val ep = engine.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))
   ep.isInstanceOf[EmptyResultExec] shouldEqual(true)
  }

  it("should generate execPlan for absent over time") {
    val t = TimeStepParams(700, 1000, 10000)
    val lp = Parser.queryRangeToLogicalPlan("""absent_over_time(http_requests_total{job = "app"}[10m])""", t)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
    execPlan.rangeVectorTransformers.head.isInstanceOf[AbsentFunctionMapper] shouldEqual true
    execPlan.children(0).isInstanceOf[MultiSchemaPartitionsExec] shouldEqual(true)
    val multiSchemaExec = execPlan.children(0).asInstanceOf[MultiSchemaPartitionsExec]
    execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].maxRecordContainerSize(queryConfig) shouldEqual 4096

    multiSchemaExec.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual(true)
    val rvt = multiSchemaExec.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper]
    rvt.window.get shouldEqual(10*60*1000)
    rvt.functionId.get.toString shouldEqual(Last.toString)
  }

  it("should generate execPlan for sum on absent over time") {
    val t = TimeStepParams(700, 1000, 10000)
    val lp = Parser.queryRangeToLogicalPlan("""sum(absent_over_time(http_requests_total{job = "app"}[10m]))""", t)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true

    execPlan.children.head.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual(true)
    execPlan.children.head.rangeVectorTransformers.head.isInstanceOf[AbsentFunctionMapper] shouldEqual true

    val multiSchemaExec = execPlan.children.head.children.head
    multiSchemaExec.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual(true)
    val rvt = multiSchemaExec.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper]
    rvt.window.get shouldEqual(10*60*1000)
    rvt.functionId.get.toString shouldEqual(Last.toString)
  }

  it("should generate execPlan for absent function") {
    val t = TimeStepParams(700, 1000, 10000)
    val lp = Parser.queryRangeToLogicalPlan("""absent(http_requests_total{job = "app"})""", t)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
    execPlan.rangeVectorTransformers.head.isInstanceOf[AbsentFunctionMapper] shouldEqual true
    execPlan.children(0).isInstanceOf[MultiSchemaPartitionsExec] shouldEqual(true)
  }

  it("should convert histogram bucket query") {
    val t = TimeStepParams(700, 1000, 10000)
    val lp = Parser.queryRangeToLogicalPlan("""my_hist_bucket{job="prometheus",le="0.5"}""", t)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val multiSchemaPartitionsExec = execPlan.children.head.asInstanceOf[MultiSchemaPartitionsExec]
    // _bucket should be removed from name
    multiSchemaPartitionsExec.filters.filter(_.column == "__name__").head.filter.valuesStrings.
      head.equals("my_hist") shouldEqual true
    // le filter should be removed
    multiSchemaPartitionsExec.filters.filter(_.column == "le").isEmpty shouldEqual true
    multiSchemaPartitionsExec.rangeVectorTransformers(1).isInstanceOf[InstantVectorFunctionMapper].
      shouldEqual(true)
    multiSchemaPartitionsExec.rangeVectorTransformers(1).asInstanceOf[InstantVectorFunctionMapper].funcParams.head.
      isInstanceOf[StaticFuncArgs] shouldEqual(true)
  }

  it("should convert rate histogram bucket query") {
    val t = TimeStepParams(700, 1000, 10000)
    val lp = Parser.queryRangeToLogicalPlan("""rate(my_hist_bucket{job="prometheus",le="0.5"}[10m])""", t)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val multiSchemaPartitionsExec = execPlan.children.head.asInstanceOf[MultiSchemaPartitionsExec]
    // _bucket should be removed from name
    multiSchemaPartitionsExec.filters.filter(_.column == "__name__").head.filter.valuesStrings.
      head.equals("my_hist") shouldEqual true
  }

  it("should generate correct execPlan for instant vector functions") {
    // ensures:
    //   (1) the execPlan tree has a LocalPartitionDistConcatExec root, and
    //   (2) the tree has a max depth 1 where all children are MultiSchemaPartitionsExec nodes, and
    //   (3) the final RangeVectorTransformer at each child is an InstantVectorFunctionMapper, and
    //   (4) the InstantVectorFunctionMapper has the appropriate InstantFunctionId
    val queryIdPairs = Seq(
      ("""abs(metric{job="app"})""", InstantFunctionId.Abs),
      ("""ceil(metric{job="app"})""", InstantFunctionId.Ceil),
      ("""clamp_max(metric{job="app"}, 1)""", InstantFunctionId.ClampMax),
      ("""clamp_min(metric{job="app"}, 1)""", InstantFunctionId.ClampMin),
      ("""exp(metric{job="app"})""", InstantFunctionId.Exp),
      ("""floor(metric{job="app"})""", InstantFunctionId.Floor),
      ("""histogram_quantile(0.9, metric{job="app"})""", InstantFunctionId.HistogramQuantile),
      ("""histogram_max_quantile(0.9, metric{job="app"})""", InstantFunctionId.HistogramMaxQuantile),
      ("""histogram_bucket(0.1, metric{job="app"})""", InstantFunctionId.HistogramBucket),
      ("""ln(metric{job="app"})""", InstantFunctionId.Ln),
      ("""log10(metric{job="app"})""", InstantFunctionId.Log10),
      ("""log2(metric{job="app"})""", InstantFunctionId.Log2),
      ("""round(metric{job="app"})""", InstantFunctionId.Round),
      ("""sgn(metric{job="app"})""", InstantFunctionId.Sgn),
      ("""sqrt(metric{job="app"})""", InstantFunctionId.Sqrt),
      ("""days_in_month(metric{job="app"})""", InstantFunctionId.DaysInMonth),
      ("""day_of_month(metric{job="app"})""", InstantFunctionId.DayOfMonth),
      ("""day_of_week(metric{job="app"})""", InstantFunctionId.DayOfWeek),
      ("""hour(metric{job="app"})""", InstantFunctionId.Hour),
      ("""minute(metric{job="app"})""", InstantFunctionId.Minute),
      ("""month(metric{job="app"})""", InstantFunctionId.Month),
      ("""year(metric{job="app"})""", InstantFunctionId.Year)
    )
    for ((query, funcId) <- queryIdPairs) {
      val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
      execPlan.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
      for (child <- execPlan.children) {
        child.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
        child.children.size shouldEqual 0
        val finalTransformer = child.asInstanceOf[MultiSchemaPartitionsExec].rangeVectorTransformers.last
        finalTransformer.isInstanceOf[InstantVectorFunctionMapper] shouldEqual true
        finalTransformer.asInstanceOf[InstantVectorFunctionMapper].function shouldEqual funcId
      }
    }
  }

  it("should generate correct execPlan for simple aggregate queries") {
    // ensures:
    //   (1) the execPlan tree has a LocalPartitionReduceAggregateExec root, and
    //   (2) the tree has a max depth 1 where all children are MultiSchemaPartitionsExec nodes, and
    //   (3) the final RangeVectorTransformer at each child is an AggregateMapReduce, and
    //   (4) the AggregateMapReduce has the appropriate InstantFunctionId
    val queryIdPairs = Seq(
      ("""avg(metric{job="app"})""", AggregationOperator.Avg),
      ("""count(metric{job="app"})""", AggregationOperator.Count),
      ("""group(metric{job="app"})""", AggregationOperator.Group),
      ("""sum(metric{job="app"})""", AggregationOperator.Sum),
      ("""min(metric{job="app"})""", AggregationOperator.Min),
      ("""max(metric{job="app"})""", AggregationOperator.Max),
      ("""stddev(metric{job="app"})""", AggregationOperator.Stddev),
      ("""stdvar(metric{job="app"})""", AggregationOperator.Stdvar),
      ("""topk(1, metric{job="app"})""", AggregationOperator.TopK),
      ("""bottomk(1, metric{job="app"})""", AggregationOperator.BottomK),
      ("""count_values(1, metric{job="app"})""", AggregationOperator.CountValues),
      ("""quantile(0.9, metric{job="app"})""", AggregationOperator.Quantile)
    )
    for ((query, funcId) <- queryIdPairs) {
      val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
      execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
      execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].aggrOp shouldEqual funcId
      val op = execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].aggrOp
      if (op == AggregationOperator.TopK
        || op == AggregationOperator.BottomK
        || op == AggregationOperator.CountValues
        || op == AggregationOperator.Quantile) {
        execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].maxRecordContainerSize(queryConfig) shouldEqual 65536
      } else {
        execPlan.asInstanceOf[LocalPartitionReduceAggregateExec].maxRecordContainerSize(queryConfig) shouldEqual 4096
      }
      for (child <- execPlan.children) {
        child.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
        child.children.size shouldEqual 0
        val lastTransformer = child.asInstanceOf[MultiSchemaPartitionsExec].rangeVectorTransformers.last
        lastTransformer.isInstanceOf[AggregateMapReduce] shouldEqual true
        lastTransformer.asInstanceOf[AggregateMapReduce].aggrOp shouldEqual funcId
      }
    }
  }

  it("should materialize LabelCardinalityPlan") {
    val filters = Seq(
      ColumnFilter("job", Equals("job")),
      ColumnFilter("__name__", Equals("metric"))
    )
    val lp = LabelCardinality(filters, 0 * 1000, 1634920729000L)

    val queryContext = QueryContext(origQueryParams = promQlQueryParams)
    val execPlan = engine.materialize(lp, queryContext)

    val expected =
      """T~LabelCardinalityPresenter(LabelCardinalityPresenter)
        |-E~LabelCardinalityReduceExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#758856902],raw)
        |--E~LabelCardinalityExec(shard=3, filters=List(ColumnFilter(job,Equals(job)), ColumnFilter(__name__,Equals(metric))), limit=1000000, startMs=0, endMs=1634920729000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#758856902],raw)
        |--E~LabelCardinalityExec(shard=19, filters=List(ColumnFilter(job,Equals(job)), ColumnFilter(__name__,Equals(metric))), limit=1000000, startMs=0, endMs=1634920729000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#758856902],raw)"""
        .stripMargin
    validatePlan(execPlan, expected)
  }

  it ("should correctly materialize TsCardExec") {
    val shardKeyPrefix = Seq("foo", "bar")
    val numGroupByFields = 3

    val lp = TsCardinalities(shardKeyPrefix, numGroupByFields)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[TsCardReduceExec] shouldEqual true

    val reducer = execPlan.asInstanceOf[TsCardReduceExec]
    reducer.children.size shouldEqual mapper.numShards
    reducer.children.foreach{ child =>
      child.isInstanceOf[TsCardExec] shouldEqual true
      val leaf = child.asInstanceOf[TsCardExec]
      leaf.shardKeyPrefix shouldEqual shardKeyPrefix
      leaf.numGroupByFields shouldEqual numGroupByFields
    }
  }

  it("should recursively replace column filters") {
    // TODO: this test should exist in LogicalPlanSpec, but it's substantially easier to run
    //   more comprehensive tests with access to a Parser (filodb.prometheus depends on filodb.query,
    //   so Parser use would give a cyclical dependency).
    val newFilters = Seq(ColumnFilter("new1", Equals("new1val")),
                         ColumnFilter("new2", NotEquals("new2val")))
    val queries = Seq(
        """scalar(my_gauge{l1="foo",new1="bar"})  +  my_gauge{l1="baz",new2="bat"}""",
        """clamp_max(my_counter{new1="foo",new2="bar"},scalar(my_counter{l1="foo",new1="bar"}) )""",
        """absent(my_gauge{l1="foo",l2="bar"})""",
        """scalar(my_counter{l1="foo",new1="bar"}) < bool(my_counter{new1="foo",l2="bar"})""",
        """absent_over_time(my_counter{new1="foo",new2="bar"}[10m])""",
        """absent(my_gauge{new1="foo",l1="bar"})[20m:1m]""",
        """sum_over_time(absent(my_gauge{l2="foo",new2="bar"})[20m:1m])"""
    )
    for (query <- queries) {
      val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
      getRawSeriesFilters(lp).foreach{ filters =>
        // sanity check; if this fails, just change the test query
        filters.intersect(newFilters).size shouldEqual 0
      }
      getRawSeriesFilters(lp.replaceFilters(newFilters)).foreach{ filters =>
        filters.intersect(newFilters).size shouldEqual newFilters.size
      }
    }
  }

  it("should generate aggregation LogicalPlan/ExecPlan with correct by/without clause") {
    val queryClausePairs = Seq (
      ("""sum (test{job="app"})""", None),
      ("""sum without()(test{job="app"})""", AggregateClause.withoutOpt()),
      ("""sum by()(test{job="app"})""", AggregateClause.byOpt()),
      ("""sum without(foo, bar)(test{job="app"})""", AggregateClause.withoutOpt(Seq("foo", "bar"))),
      ("""sum by(foo, bar)(test{job="app"})""", AggregateClause.byOpt(Seq("foo", "bar")))
    )
    for ((query, clauseOpt) <- queryClausePairs) {
      val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
      lp.asInstanceOf[Aggregate].clauseOpt shouldEqual clauseOpt
      val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
      for (child <- execPlan.children) {
        child.asInstanceOf[MultiSchemaPartitionsExec]
             .rangeVectorTransformers
             .last.asInstanceOf[AggregateMapReduce]
             .clauseOpt shouldEqual clauseOpt
      }
    }
  }

  it("should propagate new filters to FunctionArgs") {
    // not intended to be all-encompassing; just specific to this test
    def getLeafFilterGroups(plan: LogicalPlan): Seq[Seq[ColumnFilter]] = {
      val res = plan match {
        case sqw: SubqueryWithWindowing =>
          sqw.functionArgs.flatMap(getLeafFilterGroups(_)) ++ getLeafFilterGroups(sqw.innerPeriodicSeries)
        case psw: PeriodicSeriesWithWindowing =>
          psw.functionArgs.flatMap(getLeafFilterGroups(_)) ++ getLeafFilterGroups(psw.series)
        case aif: ApplyInstantFunction =>
          aif.functionArgs.flatMap(getLeafFilterGroups(_)) ++ getLeafFilterGroups(aif.vectors)
        case air: ApplyInstantFunctionRaw =>
          air.functionArgs.flatMap(getLeafFilterGroups(_)) ++ getLeafFilterGroups(air.vectors)
        case svd: ScalarVaryingDoublePlan =>
          svd.functionArgs.flatMap(getLeafFilterGroups(_)) ++ getLeafFilterGroups(svd.vectors)
        case nlp: NonLeafLogicalPlan =>
          nlp.children.flatMap(getLeafFilterGroups(_))
        case rs: RawSeries =>
          Seq(rs.filters)
        case _ => throw new IllegalArgumentException(s"unhandled type: ${plan.getClass}")
      }
      res.filter(_.nonEmpty)
    }

    // the filters for all plans that use them
    val initialFilters = Seq(
      ColumnFilter("foo", Equals("bar")),
      ColumnFilter("lname1", Equals("lvalA"))  // should be replaced by updFilters
    )
    // the filters we'll pass to replaceFilters()
    val updFilters = Seq(
      ColumnFilter("lname1", Equals("lval1")),
      ColumnFilter("lname2", Equals("lval2"))
    )
    // the filters we expect to see post-replaceFilters() in any plan that has them
    val expFilters = Seq(
      ColumnFilter("foo", Equals("bar")),
      ColumnFilter("lname1", Equals("lval1")),
      ColumnFilter("lname2", Equals("lval2"))
    )

    // dummy plans
    val rawSeries = RawSeries(
      IntervalSelector(1, 100),
      initialFilters,
      Nil
    )
    val periodicSeries = PeriodicSeries(
      rawSeries,
      1, 10, 100
    )
    val funcArg = ScalarVaryingDoublePlan(periodicSeries, ScalarFunctionId.Scalar, Nil)

    val plans = Seq(
      SubqueryWithWindowing(
        periodicSeries,
        1, 10, 100,
        RangeFunctionId.QuantileOverTime,
        Seq(funcArg),
        100, 50, None
      ),
      PeriodicSeriesWithWindowing(
        rawSeries,
        1, 10, 100, 50,
        RangeFunctionId.QuantileOverTime,
        functionArgs = Seq(funcArg)
      ),
      ApplyInstantFunction(
        periodicSeries,
        InstantFunctionId.ClampMax,
        Seq(funcArg)
      ),
      ApplyInstantFunctionRaw(
        rawSeries,
        InstantFunctionId.ClampMax,
        Seq(funcArg)
      ),
      ScalarVaryingDoublePlan(
        periodicSeries,
        ScalarFunctionId.Scalar,
        Seq(funcArg)
      )
    )

    plans.foreach{ lp =>
      // sanity check-- make sure the initial setup is as-expected
      val filterGroups = getLeafFilterGroups(lp)
      filterGroups.size shouldEqual 2  // "base" plan and its child func arg
      filterGroups.foreach { group =>
        group.toSet shouldEqual initialFilters.toSet
      }

      // should contain the new filters after replaceFilters is called
      val updFilterGroups = getLeafFilterGroups(lp.replaceFilters(updFilters))
      updFilterGroups.size shouldEqual 2  // same reasoning as above
      updFilterGroups.foreach { group =>
        group.toSet shouldEqual expFilters.toSet
      }
    }
  }

  it ("should correctly reduce LocalPartitionDistConcatExec ResultSchemas to support pushdowns") {
    val originalResultSchema = ResultSchema(Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
                                                ColumnInfo("value", ColumnType.DoubleColumn)),
                                            numRowKeyColumns = 1,
                                            fixedVectorLen = Some(123))
    // Simulate QueryResults from two sides of a BinaryJoin: count(stuff) + sum(stuff).
    val countQres = {
      val redSchema = CountRowAggregator.double.reductionSchema(originalResultSchema)
      val presSchema = CountRowAggregator.double.presentationSchema(redSchema)
      QueryResult("foobar", presSchema, Nil)
    }
    val sumQres = {
      val redSchema = SumRowAggregator.reductionSchema(originalResultSchema)
      val presSchema = SumRowAggregator.presentationSchema(redSchema)
      QueryResult("foobar", presSchema, Nil)
    }
    // If this join were pushed down, a LocalPartitionDistConcatExec would join the shard-local BinaryJoinExecs
    val concat = {
      val dummyLp = Parser.queryRangeToLogicalPlan("""foo{job="bar"}""", TimeStepParams(20900, 0, 20900))
      val dummyEp = engine.materialize(dummyLp, QueryContext(origQueryParams = promQlQueryParams))
      LocalPartitionDistConcatExec(QueryContext(), InProcessPlanDispatcher(queryConfig), Seq(dummyEp))
    }
    // initialize a BinaryJoinExec just so we can use its reduceSchemas method
    val binaryJoin = {
      val bjLp = Parser.queryRangeToLogicalPlan("""foo{job="bar"} + baz{job="bat"}""", TimeStepParams(20900, 0, 20900))
      engine.materialize(bjLp, QueryContext(origQueryParams = promQlQueryParams)).asInstanceOf[BinaryJoinExec]
    }
    // If this join were pushed down, one of the shard-local joins might process the `count` QueryResult first...
    val qres1 = {
      var reduced = binaryJoin.reduceSchemas(ResultSchema.empty, countQres.resultSchema)
      reduced = binaryJoin.reduceSchemas(reduced, sumQres.resultSchema)
      QueryResult("foobar", reduced, Nil)
    }
    // ...and the other might process the `sum` QueryResult first.
    val qres2 = {
      var reduced = binaryJoin.reduceSchemas(ResultSchema.empty, sumQres.resultSchema)
      reduced = binaryJoin.reduceSchemas(reduced, countQres.resultSchema)
      QueryResult("foobar", reduced, Nil)
    }

    // When these schemas are reduced, a SchemaMismatch should not be thrown.
    var reduced = concat.reduceSchemas(ResultSchema.empty, qres1.resultSchema)
    reduced = concat.reduceSchemas(reduced, qres2.resultSchema)
  }
}
