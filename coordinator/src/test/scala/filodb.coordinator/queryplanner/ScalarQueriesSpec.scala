package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}

import filodb.coordinator.ShardMapper
import filodb.core.MetricsTestData
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter, PromQlQueryParams, QueryContext, RangeParams}
import filodb.prometheus.parse.Parser
import filodb.query._
import filodb.query.ScalarFunctionId.Time
import filodb.query.exec._

class ScalarQueriesSpec extends FunSpec with Matchers {

  implicit val system = ActorSystem()
  val node = TestProbe().ref

  val mapper = new ShardMapper(32)
  for {i <- 0 until 32} mapper.registerNode(Seq(i), node)

  private def mapperRef = mapper

  val dataset = MetricsTestData.timeseriesDataset
  val dsRef = dataset.ref
  val schemas = Schemas(dataset.schema)

  val engine = new SingleClusterPlanner(dsRef, schemas, mapperRef)

  val queryEngineConfigString = "routing {\n  buddy {\n    http {\n      timeout = 10.seconds\n    }\n  }\n}"

  val queryEngineConfig = ConfigFactory.parseString(queryEngineConfigString)
  val promQlQueryParams = PromQlQueryParams(ConfigFactory.empty, "sum(heap_usage)", 100, 1, 1000, None)


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
    val lp = Parser.queryToLogicalPlan("scalar(test{job = \"app\"})", 1000)

    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[DistConcatExec] shouldEqual (true)
    execPlan.rangeVectorTransformers.size shouldEqual (1)
    execPlan.rangeVectorTransformers.head.isInstanceOf[ScalarFunctionMapper] shouldEqual (true)
    execPlan.printTree()
    val expected =
      """T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |-E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1153666897])
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=3, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1153666897])
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=19, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1153666897])
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
    val lp = Parser.queryToLogicalPlan("http_requests_total{job = \"app\"} + time()", 1000)

    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    val expected =
      """E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1510751596])
        |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
        |--FA1~TimeFuncArgs(RangeParams(1000,1000,1000))
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1510751596])
        |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
        |--FA1~TimeFuncArgs(RangeParams(1000,1000,1000))
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1510751596])""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarOperationMapper for query having binary operation between scalar function and metric") {
    val lp = Parser.queryToLogicalPlan("scalar(http_requests_total{job = \"app\"}) + node_info{job = \"app\"}", 1000)

    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.children.head.rangeVectorTransformers(1).isInstanceOf[ScalarOperationMapper] shouldEqual true
    val scalarOperationMapper = execPlan.children.head.rangeVectorTransformers(1).asInstanceOf[ScalarOperationMapper]
    scalarOperationMapper.funcParams.head.isInstanceOf[ExecPlanFuncArgs] shouldEqual true
    val execPlanFuncArg = scalarOperationMapper.funcParams.head.asInstanceOf[ExecPlanFuncArgs]
    execPlanFuncArg.execPlan.rangeVectorTransformers.head.isInstanceOf[ScalarFunctionMapper]
    val expected =
      """E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])
        |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true)
        |--FA1~
        |--T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |---E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=15, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])
        |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true)
        |--FA1~
        |--T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |---E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=31, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1643642770])""".stripMargin

    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarOperationMapper exec plan for query 10 + http_requests_total") {
    val lp = Parser.queryToLogicalPlan("10 + http_requests_total{job = \"app\"}", 1000)

    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    val expected =
      """E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1110105620])
        |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true)
        |--FA1~StaticFuncArgs(10.0,RangeParams(1000,1000,1000))
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1110105620])
        |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true)
        |--FA1~StaticFuncArgs(10.0,RangeParams(1000,1000,1000))
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1110105620])
        |""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarOperationMapper exec plan for query time() + 10") {
    val lp = Parser.queryToLogicalPlan("time() + 10", 1000)

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
    val lp = Parser.queryToLogicalPlan("http_requests_total{job = \"app\"} + 10", 1000)

    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.printTree()
    val expected =
      """E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1245070935])
        |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
        |--FA1~StaticFuncArgs(10.0,RangeParams(1000,1000,1000))
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1245070935])
        |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
        |--FA1~StaticFuncArgs(10.0,RangeParams(1000,1000,1000))
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1245070935])
         """.stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarOperationMapper exec plan for query scalar(http_requests_total) + time()") {
    val lp = Parser.queryToLogicalPlan("scalar(http_requests_total{job = \"app\"}) + time()", 1000)
    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true)
        |-FA1~
        |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |--E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2114470773])
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2114470773])
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2114470773])
        |-E~TimeScalarGeneratorExec(params = RangeParams(1000,1000,1000), function = Time) on InProcessPlanDispatcher""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarOperationMapper exec plan for query scalar(http_requests_total) - scalar(node_info)") {
    val lp = Parser.queryToLogicalPlan("scalar(http_requests_total{job = \"app\"}) - scalar(node_info{job = \"app\"})", 1000)
    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """T~ScalarOperationMapper(operator=SUB, scalarOnLhs=true)
        |-FA1~
        |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |--E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1081611650])
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1081611650])
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1081611650])
        |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |--E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1081611650])
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=15, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1081611650])
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=31, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1081611650])""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate ScalarOperationMapper exec plan for query scalar(http_requests_total) + 3") {
    val lp = Parser.queryToLogicalPlan("scalar(http_requests_total{job = \"app\"}) + 3", 1000)
    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false)
        |-FA1~StaticFuncArgs(3.0,RangeParams(1000,1000,1000))
        |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |--E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#856852588])
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#856852588])
        |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#856852588])
        |""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate vector plan for vector(2000)") {
    val lp = Parser.queryToLogicalPlan("vector(2000)", 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """T~VectorFunctionMapper(funcParams=List())
        |-E~ScalarFixedDoubleExec(params=RangeParams(1000,1000,1000), value=2000.0) on InProcessPlanDispatcher""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate vector plan for vector(time())") {
    val lp = Parser.queryToLogicalPlan("vector(time())", 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """T~VectorFunctionMapper(funcParams=List())
        |-E~TimeScalarGeneratorExec(params=RangeParams(1000,1000,1000), function=Time) on InProcessPlanDispatcher""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate InstantVectorFunctionMapper and VectorFunctionMapper for minute(vector(1136239445))") {
    val lp = Parser.queryToLogicalPlan("minute(vector(1136239445))", 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """T~InstantVectorFunctionMapper(function=Minute)
        |-T~VectorFunctionMapper(funcParams=List())
        |--E~ScalarFixedDoubleExec(params=RangeParams(1000,1000,1000), value=1.136239445E9) on InProcessPlanDispatcher
        |""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate vector plan for days_in_month(vector(1454284800))") {
    val lp = Parser.queryToLogicalPlan("days_in_month(vector(1454284800))", 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """T~InstantVectorFunctionMapper(function=DaysInMonth)
        |-T~VectorFunctionMapper(funcParams=List())
        |--E~ScalarFixedDoubleExec(params=RangeParams(1000,1000,1000), value=1.4542848E9) on InProcessPlanDispatcher""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }

  it("should generate binary join for month(vector(1456790399)) + day_of_month(vector(1456790399))") {
    val lp = Parser.queryToLogicalPlan("month(vector(1456790399)) + day_of_month(vector(1456790399))", 1000)
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
    val lp = Parser.queryToLogicalPlan("clamp_max(node_info{job = \"app\"},scalar(http_requests_total{job = \"app\"}))", 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val expected =
      """E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])
        |-T~InstantVectorFunctionMapper(function=ClampMax)
        |--FA1~
        |--T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |---E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=15, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])
        |-T~InstantVectorFunctionMapper(function=ClampMax)
        |--FA1~
        |--T~ScalarFunctionMapper(function=Scalar, funcParams=List())
        |---E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])
        |----T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])
        |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, rawSource=true)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=31, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colName=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#79055924])""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual (maskDispatcher(expected))
  }
}
