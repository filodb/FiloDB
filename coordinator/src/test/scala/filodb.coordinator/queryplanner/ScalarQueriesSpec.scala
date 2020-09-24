package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

import filodb.coordinator.ShardMapper
import filodb.core.MetricsTestData
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter, PromQlQueryParams, QueryConfig, QueryContext, RangeParams}
import filodb.prometheus.parse.Parser
import filodb.query._
import filodb.query.ScalarFunctionId.Time
import filodb.query.exec._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ScalarQueriesSpec extends AnyFunSpec with Matchers {

  implicit val system = ActorSystem()
  val node = TestProbe().ref

  val mapper = new ShardMapper(32)
  for {i <- 0 until 32} mapper.registerNode(Seq(i), node)

  private def mapperRef = mapper

  val dataset = MetricsTestData.timeseriesDataset
  val dsRef = dataset.ref
  val schemas = Schemas(dataset.schema)

  val config = ConfigFactory.load("application_test.conf")
  val queryConfig = new QueryConfig(config.getConfig("filodb.query"))

  val engine = new SingleClusterPlanner(dsRef, schemas, mapperRef, earliestRetainedTimestampFn = 0, queryConfig)

  val promQlQueryParams = PromQlQueryParams("sum(heap_usage)", 100, 1, 1000)

  val f1 = Seq(ColumnFilter("__name__", Filter.Equals("http_request_duration_seconds_bucket")),
    ColumnFilter("job", Filter.Equals("myService")),
    ColumnFilter("le", Filter.Equals("0.3")))

  val to = System.currentTimeMillis()
  val from = to - 50000

  val intervalSelector = IntervalSelector(from, to)

  val raw1 = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
  val windowed1 = PeriodicSeriesWithWindowing(raw1, from, 1000, to, 5000, RangeFunctionId.Rate)
  val summed1 = Aggregate(AggregationOperator.Sum, windowed1, Nil, Seq("job"))

  def maskDispatcher(input: String) = {
    input.replaceAll("Actor\\[.*\\]", "Actor\\[\\]").replaceAll("\\s+", "")
  }

  it("should generate Scalar exec plan") {
    val lp = Parser.queryToLogicalPlan("scalar(test{job = \"app\"})", 1000, 1000)

    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual (true)
    execPlan.rangeVectorTransformers.size shouldEqual (1)
    execPlan.rangeVectorTransformers.head.isInstanceOf[ScalarFunctionMapper] shouldEqual (true)
    execPlan.printTree()
    val expected =
      """T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1153666897])
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=3, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1153666897])
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=19, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1153666897])
         """.stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate scalar time based plan") {
    val logicalPlan = ScalarTimeBasedPlan(Time, RangeParams(1524855988, 1000, 1524858988))
    val execPlan = engine.materialize(logicalPlan, QueryContext(origQueryParams = promQlQueryParams))
    val expected = "E~TimeScalarGeneratorExec(params=RangeParams(1524855988,1000,1524858988), function=Time) on InProcessPlanDispatcher"
    execPlan.isInstanceOf[TimeScalarGeneratorExec] shouldEqual (true)
    val scalarTimeBasedExec = execPlan.asInstanceOf[TimeScalarGeneratorExec]
    scalarTimeBasedExec.function.shouldEqual(ScalarFunctionId.Time)
    scalarTimeBasedExec.params shouldEqual (RangeParams(1524855988, 1000, 1524858988))
    maskDispatcher(execPlan.printTree()) shouldEqual maskDispatcher(expected)

  }

  it("should generate ScalarOperationMapper exec plan for query http_requests_total + time()") {
    val lp = Parser.queryToLogicalPlan("http_requests_total{job = \"app\"} + time()", 1000, 1000)

    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    val expected =
      """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1510751596])
        |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
        |--FA1~TimeFuncArgs(RangeParams(1000,1000,1000))
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1510751596])
        |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
        |--FA1~TimeFuncArgs(RangeParams(1000,1000,1000))
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None )
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1510751596])""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarOperationMapper for query having binary operation between scalar function and metric") {
    val lp = Parser.queryToLogicalPlan("scalar(http_requests_total{job = \"app\"}) + node_info{job = \"app\"}", 1000, 1000)

    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.children.head.rangeVectorTransformers(1).isInstanceOf[ScalarOperationMapper] shouldEqual true
    val scalarOperationMapper = execPlan.children.head.rangeVectorTransformers(1).asInstanceOf[ScalarOperationMapper]
    scalarOperationMapper.funcParams.head.isInstanceOf[ExecPlanFuncArgs] shouldEqual true
    val execPlanFuncArg = scalarOperationMapper.funcParams.head.asInstanceOf[ExecPlanFuncArgs]
    execPlanFuncArg.execPlan.rangeVectorTransformers.head.isInstanceOf[ScalarFunctionMapper]
    val expected =
      """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])
        |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true)
        |--FA1~
        |--T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=15, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])
        |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true)
        |--FA1~
        |--T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=31, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])""".stripMargin

    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarOperationMapper exec plan for query 10 + http_requests_total") {
    val lp = Parser.queryToLogicalPlan("10 + http_requests_total{job = \"app\"}", 1000, 1000)

    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    val expected =
      """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1110105620])
        |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true)
        |--FA1~StaticFuncArgs(10.0,RangeParams(1000,1000,1000))
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1110105620])
        |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true)
        |--FA1~StaticFuncArgs(10.0,RangeParams(1000,1000,1000))
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1110105620])
        |""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarOperationMapper exec plan for query time() + 10") {
    val lp = Parser.queryToLogicalPlan("time() + 10", 1000, 1000)

    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    val expected =
      """T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
        |-FA1~StaticFuncArgs(10.0,RangeParams(1000,1000,1000))
        |-E~TimeScalarGeneratorExec(params=RangeParams(1000,1000,1000), function=Time) on InProcessPlanDispatcher""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarOperationMapper exec plan for query  http_requests_total + 10") {
    val lp = Parser.queryToLogicalPlan("http_requests_total{job = \"app\"} + 10", 1000, 1000)

    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    val expected =
      """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1245070935])
        |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
        |--FA1~StaticFuncArgs(10.0,RangeParams(1000,1000,1000))
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1245070935])
        |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
        |--FA1~StaticFuncArgs(10.0,RangeParams(1000,1000,1000))
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1245070935])
         """.stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarOperationMapper exec plan for query scalar(http_requests_total) + time()") {
    val lp = Parser.queryToLogicalPlan("scalar(http_requests_total{job = \"app\"}) + time()", 1000, 1000)
    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true)
        |-FA1~
        |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2114470773])
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2114470773])
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2114470773])
        |-E~TimeScalarGeneratorExec(params = RangeParams(1000,1000,1000), function = Time) on InProcessPlanDispatcher""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarOperationMapper exec plan for query scalar(http_requests_total) - scalar(node_info)") {
    val lp = Parser.queryToLogicalPlan("scalar(http_requests_total{job = \"app\"}) - scalar(node_info{job = \"app\"})", 1000, 1000)
    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """T~ScalarOperationMapper(operator=SUB, scalarOnLhs=true)
        |-FA1~
        |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1081611650])
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1081611650])
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1081611650])
        |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1081611650])
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=15, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1081611650])
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=31, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1081611650])""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarOperationMapper exec plan for query scalar(http_requests_total) + 3") {
    val lp = Parser.queryToLogicalPlan("scalar(http_requests_total{job = \"app\"}) + 3", 1000, 1000)
    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
        |-FA1~StaticFuncArgs(3.0,RangeParams(1000,1000,1000))
        |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#856852588])
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#856852588])
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#856852588])
        |""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate vector plan for vector(2000)") {
    val lp = Parser.queryToLogicalPlan("vector(2000)", 1000, 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """T~VectorFunctionMapper(funcParams=List())
        |-E~ScalarFixedDoubleExec(params=RangeParams(1000,1000,1000), value=2000.0) on InProcessPlanDispatcher""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate vector plan for vector(time())") {
    val lp = Parser.queryToLogicalPlan("vector(time())", 1000, 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """T~VectorFunctionMapper(funcParams=List())
        |-E~TimeScalarGeneratorExec(params=RangeParams(1000,1000,1000), function=Time) on InProcessPlanDispatcher""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate InstantVectorFunctionMapper and VectorFunctionMapper for minute(vector(1136239445))") {
    val lp = Parser.queryToLogicalPlan("minute(vector(1136239445))", 1000, 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """T~InstantVectorFunctionMapper(function=Minute)
        |-T~VectorFunctionMapper(funcParams=List())
        |--E~ScalarFixedDoubleExec(params=RangeParams(1000,1000,1000), value=1.136239445E9) on InProcessPlanDispatcher
        |""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate vector plan for days_in_month(vector(1454284800))") {
    val lp = Parser.queryToLogicalPlan("days_in_month(vector(1454284800))", 1000, 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """T~InstantVectorFunctionMapper(function=DaysInMonth)
        |-T~VectorFunctionMapper(funcParams=List())
        |--E~ScalarFixedDoubleExec(params=RangeParams(1000,1000,1000), value=1.4542848E9) on InProcessPlanDispatcher""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate binary join for month(vector(1456790399)) + day_of_month(vector(1456790399))") {
    val lp = Parser.queryToLogicalPlan("month(vector(1456790399)) + day_of_month(vector(1456790399))", 1000, 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher
        |-T~InstantVectorFunctionMapper(function=Month)
        |--T~VectorFunctionMapper(funcParams=List())
        |---E~ScalarFixedDoubleExec(params=RangeParams(1000,1000,1000), value=1.456790399E9) on InProcessPlanDispatcher
        |-T~InstantVectorFunctionMapper(function=DayOfMonth)
        |--T~VectorFunctionMapper(funcParams=List())
        |---E~ScalarFixedDoubleExec(params=RangeParams(1000,1000,1000), value=1.456790399E9) on InProcessPlanDispatcher""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate InstantFunctionMapper when parameter is scalar function") {
    val lp = Parser.queryToLogicalPlan("clamp_max(node_info{job = \"app\"},scalar(http_requests_total{job = \"app\"}))", 1000, 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])
        |-T~InstantVectorFunctionMapper(function=ClampMax)
        |--FA1~
        |--T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=15, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])
        |-T~InstantVectorFunctionMapper(function=ClampMax)
        |--FA1~
        |--T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=31, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarBinaryOperationExec plan for query 1 + 3") {
    val lp = Parser.queryToLogicalPlan("1 + 3", 1000, 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    val expected =
      """E~ScalarBinaryOperationExec(params = RangeParams(1000,1000,1000), operator = ADD, lhs = Left(1.0),
        |rhs = Left(3.0)) on InProcessPlanDispatcher""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarBinaryOperationExec plan for query 1 < bool(3)") {
    val lp = Parser.queryToLogicalPlan("1 < bool(3)", 1000, 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    val expected =
      """E~ScalarBinaryOperationExec(params = RangeParams(1000,1000,1000), operator=LSS_BOOL, lhs=Left(1.0), rhs=Left(3.0)) on
        |InProcessPlanDispatcher""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarOperationMapper exec plan for query 1 < bool(2) + http_requests_total") {
    val lp = Parser.queryToLogicalPlan("1 < bool(2) + http_requests_total{job = \"app\"}", 1000, 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    val expected =
      """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1098511474])
        |-T~ScalarOperationMapper(operator=LSS_BOOL, scalarOnLhs=true)
        |--FA1~StaticFuncArgs(1.0,RangeParams(1000,1000,1000))
        |--T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true)
        |---FA1~StaticFuncArgs(2.0,RangeParams(1000,1000,1000))
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1098511474])
        |-T~ScalarOperationMapper(operator=LSS_BOOL, scalarOnLhs=true)
        |--FA1~StaticFuncArgs(1.0,RangeParams(1000,1000,1000))
        |--T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true)
        |---FA1~StaticFuncArgs(2.0,RangeParams(1000,1000,1000))
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1098511474])
        |""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual maskDispatcher(expected)
  }

  it("should generate ScalarOperationMapper exec plan for query scalar(node_info) > bool(http_requests_total)") {
    val lp = Parser.queryToLogicalPlan("scalar(node_info{job = \"app\"}) > " +
      "bool(http_requests_total{job = \"app\"})", 1000, 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    val expected =
      """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-879546200])
        |-T~ScalarOperationMapper(operator=GTR_BOOL, scalarOnLhs=true)
        |--FA1~
        |--T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-879546200])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=15, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-879546200])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=31, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-879546200])
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-879546200])
        |-T~ScalarOperationMapper(operator=GTR_BOOL, scalarOnLhs=true)
        |--FA1~
        |--T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |---E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-879546200])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=15, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-879546200])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=31, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-879546200])
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-879546200])
        |""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual maskDispatcher(expected)
  }

  it("should generate BinaryJoinExec for query node_info > bool http_requests_total") {
    val lp = Parser.queryToLogicalPlan("node_info{job = \"app\"} > bool" +
      "http_requests_total{job = \"app\"}", 1000, 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    val expected =
      """E~BinaryJoinExec(binaryOp=GTR_BOOL, on=List(), ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1392317349])
        |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=15, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1392317349])
        |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=31, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1392317349])
        |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1392317349])
        |-T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1392317349])
        |""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarBinaryOperationExec plan for query (1 + 2) < bool 3 + 4") {
    val lp = Parser.queryToLogicalPlan("(1 + 2) < bool 3 + 4", 1000, 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    val expected =
      """E~ScalarBinaryOperationExec(params = RangeParams(1000,1000,1000), operator = LSS_BOOL,
        |lhs = Right(params = RangeParams(1000,1000,1000), operator=ADD, lhs=Left(1.0), rhs=Left(2.0)),
        |rhs = Right(params = RangeParams(1000,1000,1000), operator=ADD, lhs=Left(3.0), rhs=Left(4.0)))
        |on InProcessPlanDispatcher""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarBinaryOperationExec plan for query 1 + 2 - 3") {
    val lp = Parser.queryToLogicalPlan("1 + 2 - 3", 1000, 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    val expected =
      """E~ScalarBinaryOperationExec(params = RangeParams(1000,1000,1000), operator=SUB,
        |lhs=Right(params = RangeParams(1000,1000,1000), operator=ADD, lhs=Left(1.0), rhs=Left(2.0)),
        |rhs=Left(3.0)) on InProcessPlanDispatcher""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarBinaryOperationExec plan for query 1 + 2 <bool 3 + 4") {
    val lp = Parser.queryToLogicalPlan("1 + 2 <bool 3 + 4", 1000, 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    val expected =
      """E~ScalarBinaryOperationExec(params = RangeParams(1000,1000,1000), operator = LSS_BOOL,
        |lhs = Right(params = RangeParams(1000,1000,1000), operator=ADD, lhs=Left(1.0), rhs=Left(2.0)),
        |rhs = Right(params = RangeParams(1000,1000,1000), operator=ADD, lhs=Left(3.0), rhs=Left(4.0)))
        |on InProcessPlanDispatcher""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarBinaryOperationExec plan for query (1 + 2) < bool (3 + 4)") {
    val lp = Parser.queryToLogicalPlan("1 + 2 <bool 3 + 4", 1000, 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    val expected =
      """E~ScalarBinaryOperationExec(params = RangeParams(1000,1000,1000), operator = LSS_BOOL,
        |lhs = Right(params = RangeParams(1000,1000,1000), operator=ADD, lhs=Left(1.0), rhs=Left(2.0)),
        |rhs = Right(params = RangeParams(1000,1000,1000), operator=ADD, lhs=Left(3.0), rhs=Left(4.0)))
        |on InProcessPlanDispatcher""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate execPlan for binary join with nested scalar query") {
    val lp = Parser.queryToLogicalPlan("""sum(http_requests_total{job = "app"}) - 10/2""", 1000, 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """T~ScalarOperationMapper(operator=SUB, scalarOnLhs=false)
        |-FA1~
        |-E~ScalarBinaryOperationExec(params = RangeParams(1000,1000,1000), operator = DIV, lhs = Left(10.0), rhs = Left(2.0)) on InProcessPlanDispatcher
        |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List())
        |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-669137818])
        |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-669137818])
        |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-669137818])""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }
}
