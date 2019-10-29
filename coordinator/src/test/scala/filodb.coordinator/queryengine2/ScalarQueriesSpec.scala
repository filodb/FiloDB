package filodb.coordinator.queryengine2

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import monix.eval.Task
import monix.execution.Scheduler
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.duration.FiniteDuration
import com.typesafe.config.ConfigFactory
import filodb.coordinator.ShardMapper
import filodb.core.{MetricsTestData}
import filodb.core.query.{ColumnFilter, Filter, RangeParams}
import filodb.prometheus.parse.Parser
import filodb.query
import filodb.query.ScalarFunctionId.Time
import filodb.query._
import filodb.query.exec._

class ScalarQueriesSpec extends FunSpec with Matchers {

  implicit val system = ActorSystem()
  val node = TestProbe().ref

  val mapper = new ShardMapper(32)
  for { i <- 0 until 32 } mapper.registerNode(Seq(i), node)

  private def mapperRef = mapper

  val dataset = MetricsTestData.timeseriesDataset

  val emptyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlan)(implicit sched: Scheduler,
                                          timeout: FiniteDuration): Task[query.QueryResponse] = ???
  }

  val engine = new QueryEngine(dataset, mapperRef, EmptyFailureProvider)

  val queryEngineConfigString = "routing {\n  buddy {\n    http {\n      timeout = 10.seconds\n    }\n  }\n}"

  val queryEngineConfig = ConfigFactory.parseString(queryEngineConfigString)
  val promQlQueryParams = PromQlQueryParams("sum(heap_usage)", 100, 1, 1000, None)


  val f1 = Seq(ColumnFilter("__name__", Filter.Equals("http_request_duration_seconds_bucket")),
    ColumnFilter("job", Filter.Equals("myService")),
    ColumnFilter("le", Filter.Equals("0.3")))

  val to = System.currentTimeMillis()
  val from = to - 50000

  val intervalSelector = IntervalSelector(from, to)

  val raw1 = RawSeries(rangeSelector = intervalSelector, filters= f1, columns = Seq("value"))
  val windowed1 = PeriodicSeriesWithWindowing(raw1, from, 1000, to, 5000, RangeFunctionId.Rate)
  val summed1 = Aggregate(AggregationOperator.Sum, windowed1, Nil, Seq("job"))

  def maskDispatcher(input: String) ={
    input.replaceAll("Actor\\[.*\\]", "Actor\\[\\]").replaceAll("\\s+","")
  }

  it ("should generate Scalar exec plan") {
    val lp = Parser.queryToLogicalPlan("scalar(test{job = \"app\"})", 1000)
    val logicalPlan = ScalarVaryingDoublePlan(summed1,ScalarFunctionId.withName("scalar"), RangeParams(100,2,300))

    // materialized exec plan
    val execPlan = engine.materialize(lp,
      QueryOptions(), promQlQueryParams)
    execPlan.isInstanceOf[DistConcatExec] shouldEqual(true)
    execPlan.rangeVectorTransformers.size shouldEqual (1)
    execPlan.rangeVectorTransformers.head.isInstanceOf[ScalarFunctionMapper] shouldEqual (true)
    execPlan.printTree()
    println("execPlan:" + execPlan.toString)
    val expected = """T~ScalarFunctionMapper(function=Scalar, funcParams=List())
                     |-E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#675877426])
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                     |---E~SelectRawPartitionsExec(dataset=timeseries, shard=3, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#675877426])
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                     |---E~SelectRawPartitionsExec(dataset=timeseries, shard=19, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#675877426])""".stripMargin
    println("expected:" +expected)
    println("matches res:" + maskDispatcher(execPlan.printTree()).equals(maskDispatcher(expected)))
    // execPlan.printTree() should fullyMatch regex (expected.r)
    //matches(expected)
    maskDispatcher(execPlan.printTree()) shouldEqual(maskDispatcher(expected))
  }

  it ("should generate scalar time based plan") {
    val logicalPlan = ScalarTimeBasedPlan(Time,RangeParams(1524855988,1000,1524858988))
    val execPlan = engine.materialize(logicalPlan,
      QueryOptions(), promQlQueryParams)
    val expected = "E~ScalarTimeBasedExec(params=RangeParams(1524855988,1000,1524858988), function=Time) on InProcessPlanDispatcher()"
    execPlan.isInstanceOf[ScalarTimeBasedExec] shouldEqual(true)
    val scalarTimeBasedExec = execPlan.asInstanceOf[ScalarTimeBasedExec]
    scalarTimeBasedExec.function.shouldEqual(ScalarFunctionId.Time)
    scalarTimeBasedExec.params shouldEqual(RangeParams(1524855988,1000,1524858988))
    execPlan.printTree() shouldEqual(expected)

  }

  it ("should generate ScalarOperationMapper exec plan for query http_requests_total + time()") {
    val lp = Parser.queryToLogicalPlan("http_requests_total{job = \"app\"} + time()", 1000)
    val logicalPlan = ScalarVaryingDoublePlan(summed1,ScalarFunctionId.withName("scalar"), RangeParams(100,2,300))

    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryOptions(), promQlQueryParams)
    execPlan.printTree()
    val expected = """E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-954218812])
                     |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false, scalar=List(TimeFuncArgs(RangeParams(1000,1000,1000))))
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                     |---E~SelectRawPartitionsExec(dataset=timeseries, shard=5, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-954218812])
                     |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false, scalar=List(TimeFuncArgs(RangeParams(1000,1000,1000))))
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                     |---E~SelectRawPartitionsExec(dataset=timeseries, shard=21, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-954218812])""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual(maskDispatcher(expected))
  }

  it ("should generate ScalarOperationMapper for query having binary operation between scalar function and metric") {
    val lp = Parser.queryToLogicalPlan("scalar(http_requests_total{job = \"app\"}) + node_info{job = \"app\"}", 1000)

    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryOptions(), promQlQueryParams)
    execPlan.children.head.rangeVectorTransformers(1).isInstanceOf[ScalarOperationMapper] shouldEqual true
   val scalarOperationMapper = execPlan.children.head.rangeVectorTransformers(1).asInstanceOf[ScalarOperationMapper]
    scalarOperationMapper.funcParams.head.isInstanceOf[ExecPlanFuncArgs] shouldEqual true
    val execPlanFuncArg = scalarOperationMapper.funcParams.head.asInstanceOf[ExecPlanFuncArgs]
    execPlanFuncArg.execPlan.rangeVectorTransformers.head.isInstanceOf[ScalarFunctionMapper]
    println("execPlan is:" +  execPlan.printTree())
    val expected = """E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1357837659])
                     |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true, scalar=List(T~ScalarFunctionMapper(function=Scalar, funcParams=List())
                     |-E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1357837659])
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                     |---E~SelectRawPartitionsExec(dataset=timeseries, shard=5, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1357837659])
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                     |---E~SelectRawPartitionsExec(dataset=timeseries, shard=21, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1357837659])
                     |))
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                     |---E~SelectRawPartitionsExec(dataset=timeseries, shard=15, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1357837659])
                     |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true, scalar=List(T~ScalarFunctionMapper(function=Scalar, funcParams=List())
                     |-E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1357837659])
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                     |---E~SelectRawPartitionsExec(dataset=timeseries, shard=5, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1357837659])
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                     |---E~SelectRawPartitionsExec(dataset=timeseries, shard=21, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1357837659])
                     |))
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                     |---E~SelectRawPartitionsExec(dataset=timeseries, shard=31, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1357837659])""".stripMargin

    maskDispatcher(execPlan.printTree()) shouldEqual(maskDispatcher(expected))
  }

  it ("should generate ScalarOperationMapper exec plan for query 10 + http_requests_total") {
    val lp = Parser.queryToLogicalPlan("10 + http_requests_total{job = \"app\"}", 1000)

    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryOptions(), promQlQueryParams)
    execPlan.printTree()
    val expected = """E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#610440026])
                     |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true, scalar=List(StaticFuncArgs(10.0,RangeParams(1000,1000,1000))))
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                     |---E~SelectRawPartitionsExec(dataset=timeseries, shard=5, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#610440026])
                     |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true, scalar=List(StaticFuncArgs(10.0,RangeParams(1000,1000,1000))))
                     |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                     |---E~SelectRawPartitionsExec(dataset=timeseries, shard=21, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#610440026])""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual(maskDispatcher(expected))
  }

  it ("should generate ScalarOperationMapper exec plan for query time() + 10") {
    val lp = Parser.queryToLogicalPlan("time() + 10", 1000)

    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryOptions(), promQlQueryParams)
    execPlan.printTree()
    val expected ="""T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false, scalar=List(StaticFuncArgs(10.0,RangeParams(1000,1000,1000))))
                    |-E~ScalarTimeBasedExec(params=RangeParams(1000,1000,1000), function=Time) on InProcessPlanDispatcher()""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual(maskDispatcher(expected))
  }

  it ("should generate ScalarOperationMapper exec plan for query  http_requests_total + 10") {
    val lp = Parser.queryToLogicalPlan("http_requests_total{job = \"app\"} + 10", 1000)

    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryOptions(), promQlQueryParams)
    execPlan.printTree()
    val expected ="""E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-222306801])
                    |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false, scalar=List(StaticFuncArgs(10.0,RangeParams(1000,1000,1000))))
                    |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                    |---E~SelectRawPartitionsExec(dataset=timeseries, shard=5, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-222306801])
                    |-T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false, scalar=List(StaticFuncArgs(10.0,RangeParams(1000,1000,1000))))
                    |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                    |---E~SelectRawPartitionsExec(dataset=timeseries, shard=21, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-222306801])""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual(maskDispatcher(expected))
  }

  it ("should generate ScalarOperationMapper exec plan for query scalar(http_requests_total) + time()") {
    val lp = Parser.queryToLogicalPlan("scalar(http_requests_total{job = \"app\"}) + time()", 1000)
     println("lp:" + lp)
    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryOptions(), promQlQueryParams)
   println("execPLan is:" +  execPlan.printTree())
    val expected ="""T~ScalarOperationMapper(operator=ADD, scalarOnLhs=true, scalar=List(T~ScalarFunctionMapper(function=Scalar, funcParams=List())
                    |-E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1890119985])
                    |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                    |---E~SelectRawPartitionsExec(dataset=timeseries, shard=5, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1890119985])
                    |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                    |---E~SelectRawPartitionsExec(dataset=timeseries, shard=21, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-1890119985])
                    |))
                    |-E~ScalarTimeBasedExec(params=RangeParams(1000,1000,1000), function=Time) on InProcessPlanDispatcher()""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual(maskDispatcher(expected))
  }

  it ("should generate ScalarOperationMapper exec plan for query scalar(http_requests_total) - scalar(node_info)") {
    val lp = Parser.queryToLogicalPlan("scalar(http_requests_total{job = \"app\"}) - scalar(node_info{job = \"app\"})", 1000)
    println("lp:" + lp)
    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryOptions(), promQlQueryParams)
    println("execPLan is:" +  execPlan.printTree())
    val expected ="""T~ScalarOperationMapper(operator=SUB, scalarOnLhs=true, scalar=List(T~ScalarFunctionMapper(function=Scalar, funcParams=List())
                    |-E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1383603791])
                    |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                    |---E~SelectRawPartitionsExec(dataset=timeseries, shard=5, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1383603791])
                    |--T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                    |---E~SelectRawPartitionsExec(dataset=timeseries, shard=21, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1383603791])
                    |))
                    |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
                    |--E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1383603791])
                    |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                    |----E~SelectRawPartitionsExec(dataset=timeseries, shard=15, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1383603791])
                    |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                    |----E~SelectRawPartitionsExec(dataset=timeseries, shard=31, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(node_info))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1383603791])""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual(maskDispatcher(expected))
  }

  it ("should generate ScalarOperationMapper exec plan for query scalar(http_requests_total) + 3") {
    val lp = Parser.queryToLogicalPlan("scalar(http_requests_total{job = \"app\"}) + 3", 1000)
    println("lp:" + lp)
    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryOptions(), promQlQueryParams)
    println("execPLan is:" +  execPlan.printTree())
    val expected ="""T~ScalarOperationMapper(operator=ADD, scalarOnLhs=false, scalar=List(StaticFuncArgs(3.0,RangeParams(1000,1000,1000))))
                    |-T~ScalarFunctionMapper(function=Scalar, funcParams=List())
                    |--E~DistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#730447375])
                    |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                    |----E~SelectRawPartitionsExec(dataset=timeseries, shard=5, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#730447375])
                    |---T~PeriodicSamplesMapper(start=1000000, step=1000000, end=1000000, window=None, functionId=None, funcParams=List())
                    |----E~SelectRawPartitionsExec(dataset=timeseries, shard=21, schema=timeseries, chunkMethod=TimeRangeChunkScan(700000,1000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(http_requests_total))), colIDs=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#730447375])""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual(maskDispatcher(expected))
  }
  it ("should generate vector plan for vector(2000)") {
    val lp = Parser.queryToLogicalPlan("vector(2000)", 1000)
    println("lp:" + lp)
    // materialized exec plan
    val execPlan = engine.materialize(lp, QueryOptions(), promQlQueryParams)
    println("execPLan is:" +  execPlan.printTree())
    val expected ="""T~VectorFunctionMapper(funcParams=List())
                    |-E~ScalarFixedDoubleExec(params=RangeParams(1000,1000,1000), value=2000.0) on InProcessPlanDispatcher()""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual(maskDispatcher(expected))
  }
  it ("should generate vector plan for vector(time())") {
    val lp = Parser.queryToLogicalPlan("vector(time())", 1000)
    println("lp:" + lp)
    //materialized exec plan
        val execPlan = engine.materialize(lp, QueryOptions(), promQlQueryParams)
        println("execPLan is:" +  execPlan.printTree())
        val expected ="""T~VectorFunctionMapper(funcParams=List())
                        |-E~ScalarTimeBasedExec(params=RangeParams(1000,1000,1000), function=Time) on InProcessPlanDispatcher()""".stripMargin
        maskDispatcher(execPlan.printTree()) shouldEqual(maskDispatcher(expected))
  }

  it ("should generate InstantVectorFunctionMapper and VectorFunctionMapper for minute(vector(1136239445))") {
    val lp = Parser.queryToLogicalPlan("minute(vector(1136239445))", 1000)
    println("lp:" + lp)
  //   materialized exec plan
        val execPlan = engine.materialize(lp, QueryOptions(), promQlQueryParams)
        println("execPLan is:" +  execPlan.printTree())
        val expected ="""T~InstantVectorFunctionMapper(function=Minute, funcParams=List())
                        |-T~VectorFunctionMapper(funcParams=List())
                        |--E~ScalarFixedDoubleExec(params=RangeParams(1000,1000,1000), value=1.136239445E9) on InProcessPlanDispatcher()
                        |""".stripMargin
        maskDispatcher(execPlan.printTree()) shouldEqual(maskDispatcher(expected))
  }
  it ("should generate vector plan for days_in_month(vector(1454284800))") {
    val lp = Parser.queryToLogicalPlan("days_in_month(vector(1454284800))", 1000)
    println("lp:" + lp)
    //   materialized exec plan
    val execPlan = engine.materialize(lp, QueryOptions(), promQlQueryParams)
    println("execPLan is:" +  execPlan.printTree())
    val expected ="""T~InstantVectorFunctionMapper(function=DaysInMonth, funcParams=List())
                    |-T~VectorFunctionMapper(funcParams=List())
                    |--E~ScalarFixedDoubleExec(params=RangeParams(1000,1000,1000), value=1.4542848E9) on InProcessPlanDispatcher()""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual(maskDispatcher(expected))
  }

  it ("should generate binary join for month(vector(1456790399)) + day_of_month(vector(1456790399))") {
    val lp = Parser.queryToLogicalPlan("month(vector(1456790399)) + day_of_month(vector(1456790399))", 1000)
    println("lp:" + lp)
    //   materialized exec plan
    val execPlan = engine.materialize(lp, QueryOptions(), promQlQueryParams)
    println("execPLan is:" +  execPlan.printTree())
    val expected ="""E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher()
                    |-T~InstantVectorFunctionMapper(function=Month, funcParams=List())
                    |--T~VectorFunctionMapper(funcParams=List())
                    |---E~ScalarFixedDoubleExec(params=RangeParams(1000,1000,1000), value=1.456790399E9) on InProcessPlanDispatcher()
                    |-T~InstantVectorFunctionMapper(function=DayOfMonth, funcParams=List())
                    |--T~VectorFunctionMapper(funcParams=List())
                    |---E~ScalarFixedDoubleExec(params=RangeParams(1000,1000,1000), value=1.456790399E9) on InProcessPlanDispatcher()""".stripMargin
    maskDispatcher(execPlan.printTree()) shouldEqual(maskDispatcher(expected))
  }
}
