package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.core.{MetricsTestData, SpreadChange}
import filodb.core.metadata.Schemas
import filodb.core.query.Filter.Equals
import filodb.core.query.{ColumnFilter, PlannerParams, PromQlQueryParams, QueryConfig, QueryContext, RangeParams, RoutingConfig}
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.BinaryOperator.{ADD, LAND}
import filodb.query.InstantFunctionId.Ln
import filodb.query.{BadQueryException, LabelCardinality, LogicalPlan, PlanValidationSpec, SeriesKeysByFilters, TsCardinalities}
import filodb.query.exec._

class MultiPartitionPlannerSpec extends AnyFunSpec with Matchers with PlanValidationSpec{
  private implicit val system = ActorSystem()
  private val node = TestProbe().ref

  private val mapper = new ShardMapper(32)
  for { i <- 0 until 32 } mapper.registerNode(Seq(i), node)

  private def mapperRef = mapper

  private val dataset = MetricsTestData.timeseriesDataset
  private val schemas = Schemas(dataset.schema)

  private val routingConfigString = "routing {\n  remote {\n    http {\n      timeout = 10000\n    }\n  }\n}"
  private val routingConfig = ConfigFactory.parseString(routingConfigString)

  private val config = ConfigFactory.load("application_test.conf")
    .getConfig("filodb.query").withFallback(routingConfig)
  private val queryConfig = QueryConfig(config)
                            .copy(plannerSelector = Some("plannerSelector"),
                              routingConfig = RoutingConfig(supportRemoteRawExport = true))

  val localPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = 0,
    queryConfig, "raw", StaticSpreadProvider(SpreadChange(0, 1)))

  val startSeconds = 1000
  val endSeconds = 10000
  val localPartitionStart = 3000
  val lookbackMs = 300000
  val step = 100

  def partitions(timeRange: TimeRange): List[PartitionAssignment] = List(PartitionAssignment("local", "local-url",
    TimeRange(timeRange.startMs, timeRange.endMs)))

  it ("should not generate PromQlExec plan when partitions are local") {
    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)))

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)))
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("test{job = \"app\"}", TimeStepParams(1000, 100, 2000))

    val promQlQueryParams = PromQlQueryParams("test{job = \"app\"}", 1000, 100, 2000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual (true)
    execPlan.children.length shouldEqual 2
    execPlan.children.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
    execPlan.children.head.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual true
  }

  // this test case is NOT supported yet as we do not support partitions split
  // across time. MultiPartitionPlanner has the API and some preliminary logic written but the plans
  // generated are not correct. This test demonstrates this behavior and shows that the execution plans have
  // a hole in them, ie the data returned will be incomplete
  it ("should generate all PromQlRemoteExec plan") {

    def twoPartitions(timeRange: TimeRange): List[PartitionAssignment] = List(
      PartitionAssignment("remote", "remote-url", TimeRange(startSeconds * 1000 - lookbackMs,
        localPartitionStart * 1000 - 1)), PartitionAssignment("remote2", "remote-url2",
        TimeRange(localPartitionStart * 1000, endSeconds * 1000)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
       if (routingKey.equals(Map("job" -> "app"))) twoPartitions(timeRange)
       else Nil
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] = twoPartitions(timeRange)

    }
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("test{job = \"app\"}", TimeStepParams(startSeconds, step, endSeconds))

    val promQlQueryParams = PromQlQueryParams("test{job = \"app\"}", startSeconds, step, endSeconds)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams, plannerParams =
      PlannerParams(processMultiPartition = true)))
    val expectedPlanTree = s"""E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                              |-E~PromQlRemoteExec(PromQlQueryParams(test{job = "app"},1000,100,2999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                              |-T~PeriodicSamplesMapper(start=3000000, step=100000, end=3599000, window=None, functionId=None, rawSource=false, offsetMs=None)
                              |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                              |---E~PromQlRemoteExec(PromQlQueryParams(test{job="app"}[900s],3599,1,3599,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                              |---E~PromQlRemoteExec(PromQlQueryParams(test{job="app"}[900s],3599,1,3599,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url2, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                              |-E~PromQlRemoteExec(PromQlQueryParams(test{job = "app"},3600,100,10000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url2, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))""".stripMargin

    validatePlan(execPlan, expectedPlanTree)
  }

  it ("should generate simple plan for one local partition for TopLevelSubquery") {
    val p1StartSecs = 1000
    val p1EndSecs = 12000
    val stepSecs = 100
    val queryStartSecs = 12000
    val subqueryLookbackSecs = 9000
    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)))

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)))
    }
    val engine = new MultiPartitionPlanner(
      partitionLocationProvider, localPlanner, "local", dataset, queryConfig
    )
    val lp = Parser.queryRangeToLogicalPlan(
      """test{job = "app"}[9000s:100s]""", TimeStepParams(queryStartSecs, 0, queryStartSecs)
    )
    val promQlQueryParams = PromQlQueryParams("""test{job = "app"}[9000s:100s]""", queryStartSecs, 0, queryStartSecs)
    val execPlan = engine.materialize(
      lp,
      QueryContext(origQueryParams = promQlQueryParams, plannerParams = PlannerParams(processMultiPartition = true))
    )
    val expectedPlan =
      """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1820520048],raw)
         |-T~PeriodicSamplesMapper(start=3000000, step=100000, end=12000000, window=None, functionId=None, rawSource=true, offsetMs=None)
         |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=3, chunkMethod=TimeRangeChunkScan(2700000,12000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1820520048],raw)
         |-T~PeriodicSamplesMapper(start=3000000, step=100000, end=12000000, window=None, functionId=None, rawSource=true, offsetMs=None)
         |--E~MultiSchemaPartitionsExec(dataset=timeseries, shard=19, chunkMethod=TimeRangeChunkScan(2700000,12000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1820520048],raw)""".stripMargin
    validatePlan(execPlan, expectedPlan)
  }

  // not supported in production
  // returns incomplete results
  // 12/07/21
  //  it ("should generate time split PromQlRemoteExec plans for TopLevelSubquery") {
  //    val p1StartSecs = 1000
  //    val p1EndSecs = 6999
  //    val p2StartSecs = 7000
  //    val p2EndSecs = 15000
  //    val stepSecs = 100
  //    val queryStartSecs = 12000
  //    val subqueryLookbackSecs = 9000
  //
  //    def twoPartitions(): List[PartitionAssignment] = List(
  //      PartitionAssignment("remote", "remote-url", TimeRange(p1StartSecs * 1000, p1EndSecs * 1000)),
  //      PartitionAssignment("remote2", "remote-url2", TimeRange(p2StartSecs * 1000, p2EndSecs * 1000))
  //    )
  //
  //    val partitionLocationProvider = new PartitionLocationProvider {
  //      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
  //        if (routingKey.equals(Map("job" -> "app"))) twoPartitions().filter(
  //          (p: PartitionAssignment) => {
  //            val startWithinPartition = (p.timeRange.startMs <= timeRange.startMs) && (p.timeRange.endMs > timeRange.startMs)
  //            val endWithinPartition = (p.timeRange.startMs <= timeRange.endMs) && (p.timeRange.endMs > timeRange.endMs)
  //            val partitionWithinInterval = (p.timeRange.startMs >= timeRange.startMs) && (p.timeRange.endMs < timeRange.endMs)
  //            startWithinPartition || endWithinPartition || partitionWithinInterval
  //          })
  //        else Nil
  //      }
  //      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] = twoPartitions()
  //    }
  //    val engine = new MultiPartitionPlanner(
  //      partitionLocationProvider, localPlanner, "local", dataset, queryConfig
  //    )
  //    val lp = Parser.queryRangeToLogicalPlan(
  //      """test{job = "app"}[9000s:100s]""", TimeStepParams(queryStartSecs, 0, queryStartSecs)
  //    )
  //    val promQlQueryParams = PromQlQueryParams("""test{job = "app"}[9000s:100s]""", queryStartSecs, 0, queryStartSecs)
  //    val execPlan = engine.materialize(
  //      lp,
  //      QueryContext(origQueryParams = promQlQueryParams, plannerParams = PlannerParams(processMultiPartition = true))
  //    )
  //    // note that start of the second partition is 7400, not 7000 that we would logically expect
  //    // this comes  from the logic in MultiPartitionPlannerSpec.materializePeriodicAndRawSeries()
  //    // val numStepsInPrevPartition = (p.timeRange.startMs - prevPartitionStart + lookBackMs) / stepMs
  //    // (7000 - 3000 +300) / 100 = 43
  //    // val lastPartitionInstant = prevPartitionStart + numStepsInPrevPartition * stepMs
  //    //  3000 + 43*100 = 7300
  //    // val start = lastPartitionInstant + stepMs
  //    // 7300+100=7400
  //    // overall the above is broken, in this particular query, we don't even have a lookback technically, it
  //    // comes from the stalesness interval that is infused into the periodic series logical plan in the
  //    // constructor when we create PeriodicSeries in Vector.
  //    // Even if we did not have stale interval baked in the PeriodicSeries, it would be added by method
  //    // LogicalPlanUtils.getLookBackMillis()
  //    // ie if logical plan does not return lookback, default stale interval would be used instead
  //    val expectedPlan =
  //      """E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5ae1c281)
  //      |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"},3000,100,6999,None,false), PlannerParams(filodb,None,None,None,None,30000,1000000,100000,100000,18000000,None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5ae1c281)
  //      |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"},7400,100,15000,None,false), PlannerParams(filodb,None,None,None,None,30000,1000000,100000,100000,18000000,None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url2, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@5ae1c281)""".stripMargin
  //    validatePlan(execPlan, expectedPlan)
  //  }

  // not supported in production
  // currently returns incomplete results
  // 12/07/21
  //  it ("should generate timesplit local and PromQlRemoteExec plan for TopLevelSubquery") {
  //    val p1StartSecs = 1000
  //    val p1EndSecs = 6999
  //    val p2StartSecs = 7000
  //    val p2EndSecs = 15000
  //    val stepSecs = 100
  //    val queryStartSecs = 12000
  //    val subqueryLookbackSecs = 9000
  //
  //    def twoPartitions(): List[PartitionAssignment] = List(
  //      PartitionAssignment("remote", "remote-url", TimeRange(p1StartSecs * 1000, p1EndSecs * 1000)),
  //      PartitionAssignment("local", "local-url", TimeRange(p2StartSecs * 1000, p2EndSecs * 1000))
  //    )
  //
  //    val partitionLocationProvider = new PartitionLocationProvider {
  //      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
  //        if (routingKey.equals(Map("job" -> "app"))) twoPartitions().filter(
  //          (p: PartitionAssignment) => {
  //            val startWithinPartition = (p.timeRange.startMs <= timeRange.startMs) && (p.timeRange.endMs > timeRange.startMs)
  //            val endWithinPartition = (p.timeRange.startMs <= timeRange.endMs) && (p.timeRange.endMs > timeRange.endMs)
  //            val partitionWithinInterval = (p.timeRange.startMs >= timeRange.startMs) && (p.timeRange.endMs < timeRange.endMs)
  //            startWithinPartition || endWithinPartition || partitionWithinInterval
  //          })
  //        else Nil
  //      }
  //      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] = twoPartitions()
  //    }
  //    val engine = new MultiPartitionPlanner(
  //      partitionLocationProvider, localPlanner, "local", dataset, queryConfig
  //    )
  //    val lp = Parser.queryRangeToLogicalPlan(
  //      """test{job = "app"}[9000s:100s]""", TimeStepParams(queryStartSecs, 0, queryStartSecs)
  //    )
  //    val promQlQueryParams = PromQlQueryParams("""test{job = "app"}[9000s:100s]""", queryStartSecs, 0, queryStartSecs)
  //    val execPlan = engine.materialize(
  //      lp,
  //      QueryContext(origQueryParams = promQlQueryParams, plannerParams = PlannerParams(processMultiPartition = true))
  //    )
  //    // same as in "should generate timesplit PromQlRemoteExec plans for TopLevelSubquery"
  //    // start  of 7400 does not make sense, instead we should have done:
  //    // E~StitchRvsExec
  //    // |-T~PeriodicSamplesMapper
  //    // |--E~MultiSchemaParitionsExec //essentially raw data
  //    // |--E~PromQLRemoteExec //essentially raw data
  //    // if extracting raw data to some node above is too expensive
  //    // we could have extracted raw data only for the overlap though it's going to be considerably more complex
  //    val expectedPlan =
  //      """E~StitchRvsExec() on InProcessPlanDispatcher(filodb.core.query.QueryConfig@62b57479)
  //         |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2031724201],raw)
  //         |--T~PeriodicSamplesMapper(start=7400000, step=100000, end=15000000, window=None, functionId=None, rawSource=true, offsetMs=None)
  //         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=3, chunkMethod=TimeRangeChunkScan(7100000,15000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2031724201],raw)
  //         |--T~PeriodicSamplesMapper(start=7400000, step=100000, end=15000000, window=None, functionId=None, rawSource=true, offsetMs=None)
  //         |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=19, chunkMethod=TimeRangeChunkScan(7100000,15000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-2031724201],raw)
  //         |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"},3000,100,6999,None,false), PlannerParams(filodb,None,None,None,None,30000,1000000,100000,100000,18000000,None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@62b57479)""".stripMargin
  //    validatePlan(execPlan, expectedPlan)
  //  }

  // the only way we might hit two partitions that are not time split is if we have a binary join, ie one time series
  // lives in one partition, and another one lives in another.
  it ("should generate plan over remote and local partitions which are NOT time split for TopLevelSubquery") {
    val stepSecs = 100
    val queryStartSecs = 12000
    val subqueryLookbackSecs = 9000

    def partitions(timeRange: TimeRange): List[PartitionAssignment] = List(PartitionAssignment("remote", "remote-url",
      TimeRange(timeRange.startMs, timeRange.endMs)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app1")))
          List(PartitionAssignment("remote", "remote-url", TimeRange(timeRange.startMs,timeRange.endMs)))
        else
          List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs,timeRange.endMs)))
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val engine = new MultiPartitionPlanner(
      partitionLocationProvider, localPlanner, "local", dataset, queryConfig
    )
    val query = """(test{job = "app1"} + test{job = "app2"})[9000s:100s]"""
    val lp = Parser.queryRangeToLogicalPlan(
      query,
      TimeStepParams(queryStartSecs, step, queryStartSecs),
      Parser.Antlr
    )
    val promQlQueryParams = PromQlQueryParams(query, queryStartSecs, step, queryStartSecs)
    val execPlan = engine.materialize(
      lp, QueryContext(
        origQueryParams = promQlQueryParams,  plannerParams = PlannerParams(processMultiPartition = true)
      )
    )
    val expectedPlan =
    """E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
      |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app1"},3000,100,12000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
      |-E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-763739679],raw)
      |--T~PeriodicSamplesMapper(start=3000000, step=100000, end=12000000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=15, chunkMethod=TimeRangeChunkScan(2700000,12000000), filters=List(ColumnFilter(job,Equals(app2)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-763739679],raw)
      |--T~PeriodicSamplesMapper(start=3000000, step=100000, end=12000000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=31, chunkMethod=TimeRangeChunkScan(2700000,12000000), filters=List(ColumnFilter(job,Equals(app2)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-763739679],raw)""".stripMargin
    validatePlan(execPlan, expectedPlan)
  }

  // the only way we might hit two partitions that are not time split is if we have a binary join, ie one time series
  // lives in one partition, and another one lives in another.
  it ("should generate plan over two remote partitions which are NOT time split for TopLevelSubquery") {
    val stepSecs = 100
    val queryStartSecs = 12000
    val subqueryLookbackSecs = 9000

    def partitions(timeRange: TimeRange): List[PartitionAssignment] = List(PartitionAssignment("remote", "remote-url",
      TimeRange(timeRange.startMs, timeRange.endMs)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app1")))
          List(PartitionAssignment("remote1", "remote-url1", TimeRange(timeRange.startMs,timeRange.endMs)))
        else
          List(PartitionAssignment("remote2", "remote-url2", TimeRange(timeRange.startMs,timeRange.endMs)))
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val engine = new MultiPartitionPlanner(
      partitionLocationProvider, localPlanner, "local", dataset, queryConfig
    )
    val query = """(test{job = "app1"} + test{job = "app2"})[9000s:100s]"""
    val lp = Parser.queryRangeToLogicalPlan(
      query,
      TimeStepParams(queryStartSecs, step, queryStartSecs),
      Parser.Antlr
    )
    val promQlQueryParams = PromQlQueryParams(query, queryStartSecs, step, queryStartSecs)
    val execPlan = engine.materialize(
      lp, QueryContext(
        origQueryParams = promQlQueryParams,  plannerParams = PlannerParams(processMultiPartition = true)
      )
    )
    execPlan.printTree()
    val expectedPlan = {
      """E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
        |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app1"},3000,100,12000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
        |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app2"},3000,100,12000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url2, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))""".stripMargin
    }
    validatePlan(execPlan, expectedPlan)
  }

  // the only way we might hit two partitions that are not time split is if we have a binary join, ie one time series
  // lives in one partition, and another one lives in another.
  it ("should generate plan over remote and local partitions which are NOT time split for TopLevelSubquery /w func") {
    val stepSecs = 100
    val queryStartSecs = 12000
    val subqueryLookbackSecs = 9000

    def partitions(timeRange: TimeRange): List[PartitionAssignment] = List(PartitionAssignment("remote", "remote-url",
      TimeRange(timeRange.startMs, timeRange.endMs)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app1")))
          List(PartitionAssignment("remote", "remote-url", TimeRange(timeRange.startMs,timeRange.endMs)))
        else
          List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs,timeRange.endMs)))
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val engine = new MultiPartitionPlanner(
      partitionLocationProvider, localPlanner, "local", dataset, queryConfig
    )
    val query = """sum(test{job = "app1"} + test{job = "app2"})[9000s:100s]"""
    val lp = Parser.queryRangeToLogicalPlan(
      query,
      TimeStepParams(queryStartSecs, step, queryStartSecs),
      Parser.Antlr
    )
    val promQlQueryParams = PromQlQueryParams(query, queryStartSecs, step, queryStartSecs)
    val execPlan = engine.materialize(
      lp, QueryContext(
        origQueryParams = promQlQueryParams,  plannerParams = PlannerParams(processMultiPartition = true)
      )
    )
    val expectedPlan =
      """T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(12000,100,12000))
        |-E~MultiPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
        |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#887456173],raw)
        |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |----E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#887456173],raw)
        |-----T~PeriodicSamplesMapper(start=3000000, step=100000, end=12000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=7, chunkMethod=TimeRangeChunkScan(2700000,12000000), filters=List(ColumnFilter(job,Equals(app1)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#887456173],raw)
        |-----T~PeriodicSamplesMapper(start=3000000, step=100000, end=12000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=23, chunkMethod=TimeRangeChunkScan(2700000,12000000), filters=List(ColumnFilter(job,Equals(app1)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#887456173],raw)
        |-----T~PeriodicSamplesMapper(start=3000000, step=100000, end=12000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=15, chunkMethod=TimeRangeChunkScan(2700000,12000000), filters=List(ColumnFilter(job,Equals(app2)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#887456173],raw)
        |-----T~PeriodicSamplesMapper(start=3000000, step=100000, end=12000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=31, chunkMethod=TimeRangeChunkScan(2700000,12000000), filters=List(ColumnFilter(job,Equals(app2)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#887456173],raw)
        |--E~PromQlRemoteExec(PromQlQueryParams(sum(test{job = "app1"} + test{job = "app2"})[9000s:100s],12000,100,12000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,true,true,false,false,true,10,false,true), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))""".stripMargin
    validatePlan(execPlan, expectedPlan)
  }


  it ("one remote partition should work for SubqueryWithWindowing") {
    val stepSecs = 120
    val queryStartSecs = 1200
    val queryEndSecs = 1800
    def onePartition(timeRange: TimeRange): List[PartitionAssignment] = List(
      PartitionAssignment(
        "remote", "remote-url", TimeRange(startSeconds * 1000 - lookbackMs, endSeconds * 1000)
      )
    )
    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app"))) onePartition(timeRange)
        else Nil
      }
      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] = onePartition(timeRange)
    }
    val query = "avg_over_time(test{job = \"app\"}[10m:1m])"
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(queryStartSecs, step, queryEndSecs), Parser.Antlr)
    val promQlQueryParams = PromQlQueryParams(query, queryStartSecs, step, queryEndSecs)
    val execPlan = engine.materialize(
      lp,
      QueryContext(origQueryParams = promQlQueryParams, plannerParams = PlannerParams(processMultiPartition = true))
    )
    val expectedPlan =
      """E~PromQlRemoteExec(PromQlQueryParams(avg_over_time(test{job = "app"}[10m:1m]),1200,100,1800,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))"""

    validatePlan(execPlan, expectedPlan)
  }

  it ("one local partition should work for SubqueryWithWindowing") {
    val stepSecs = 120
    val queryStartSecs = 1200
    val queryEndSecs = 1800
    val p1StartSecs = 600
    val p1EndSecs = 1800

    def onePartition(timeRange: TimeRange): List[PartitionAssignment] = List(
      PartitionAssignment(
        "local", "local-url", TimeRange(p1StartSecs * 1000, p1EndSecs * 1000)
      )
    )
    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app"))) onePartition(timeRange)
        else Nil
      }
      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] = onePartition(timeRange)
    }
    val query = "avg_over_time(test{job = \"app\"}[10m:1m])"
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(queryStartSecs, stepSecs, queryEndSecs), Parser.Antlr)
    val promQlQueryParams = PromQlQueryParams(query, queryStartSecs, stepSecs, queryEndSecs)
    val execPlan = engine.materialize(
      lp,
      QueryContext(origQueryParams = promQlQueryParams, plannerParams = PlannerParams(processMultiPartition = true))
    )
    val expectedPlan = {
      """E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#64113238],raw)
        |-T~PeriodicSamplesMapper(start=1200000, step=120000, end=1800000, window=Some(600000), functionId=Some(AvgOverTime), rawSource=false, offsetMs=None)
        |--T~PeriodicSamplesMapper(start=600000, step=60000, end=1800000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=3, chunkMethod=TimeRangeChunkScan(300000,1800000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#64113238],raw)
        |-T~PeriodicSamplesMapper(start=1200000, step=120000, end=1800000, window=Some(600000), functionId=Some(AvgOverTime), rawSource=false, offsetMs=None)
        |--T~PeriodicSamplesMapper(start=600000, step=60000, end=1800000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |---E~MultiSchemaPartitionsExec(dataset=timeseries, shard=19, chunkMethod=TimeRangeChunkScan(300000,1800000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#64113238],raw)""".stripMargin
    }
    validatePlan(execPlan, expectedPlan)
  }

  it ("local and remote partition should work for SubqueryWithWindowing") {
    val stepSecs = 120
    val queryStartSecs = 1200
    val queryEndSecs = 1800
    val pStartSecs = 600
    val pEndSecs = 1800


    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app1")))
          List(PartitionAssignment("remote", "remote-url", TimeRange(pStartSecs * 1000, pEndSecs * 1000)))
        else
          List(PartitionAssignment("local", "local-url", TimeRange(pStartSecs * 1000, pEndSecs * 1000)))
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val query = """avg_over_time((test{job = "app1"} + test{job = "app2"})[10m:1m])"""
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(queryStartSecs, stepSecs, queryEndSecs), Parser.Antlr)
    val promQlQueryParams = PromQlQueryParams(query, queryStartSecs, stepSecs, queryEndSecs)
    val execPlan = engine.materialize(
      lp,
      QueryContext(origQueryParams = promQlQueryParams, plannerParams = PlannerParams(processMultiPartition = true))
    )
    val expectedPlan =
      """T~PeriodicSamplesMapper(start=1200000, step=120000, end=1800000, window=Some(600000), functionId=Some(AvgOverTime), rawSource=false, offsetMs=None)
        |-E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
        |--E~PromQlRemoteExec(PromQlQueryParams(test{job="app1"},600,60,1800,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
        |--E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-763739679],raw)
        |---T~PeriodicSamplesMapper(start=600000, step=60000, end=1800000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=15, chunkMethod=TimeRangeChunkScan(300000,1800000), filters=List(ColumnFilter(job,Equals(app2)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-763739679],raw)
        |---T~PeriodicSamplesMapper(start=600000, step=60000, end=1800000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=31, chunkMethod=TimeRangeChunkScan(300000,1800000), filters=List(ColumnFilter(job,Equals(app2)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-763739679],raw)""".stripMargin
    validatePlan(execPlan, expectedPlan)
  }

  it ("local and remote partition should work for SubqueryWithWindowing with nested subqueries") {
    val stepSecs = 120
    val queryStartSecs = 1200
    val queryEndSecs = 1800
    val pStartSecs = 600
    val pEndSecs = 1800


    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app1")))
          List(PartitionAssignment("remote", "remote-url", TimeRange(pStartSecs * 1000, pEndSecs * 1000)))
        else
          List(PartitionAssignment("local", "local-url", TimeRange(pStartSecs * 1000, pEndSecs * 1000)))
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val query = """min_over_time((sum_over_time(sum(test{job = "app1"})[10m:1m]) + sum_over_time(sum(test{job = "app2"})[10m:1m]))[10m:1m])"""
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(queryStartSecs, stepSecs, queryEndSecs), Parser.Antlr)
    val promQlQueryParams = PromQlQueryParams(query, queryStartSecs, stepSecs, queryEndSecs)
    val execPlan = engine.materialize(
      lp,
      QueryContext(origQueryParams = promQlQueryParams, plannerParams = PlannerParams(processMultiPartition = true))
    )
    val expectedPlan =
      """T~PeriodicSamplesMapper(start=1200000, step=120000, end=1800000, window=Some(600000), functionId=Some(MinOverTime), rawSource=false, offsetMs=None)
        |-E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
        |--E~PromQlRemoteExec(PromQlQueryParams(sum_over_time(sum(test{job="app1"})[600s:60s]),600,60,1800,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
        |--T~PeriodicSamplesMapper(start=600000, step=60000, end=1800000, window=Some(600000), functionId=Some(SumOverTime), rawSource=false, offsetMs=None)
        |---T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(0,60,1800))
        |----E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-763739679],raw)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |------T~PeriodicSamplesMapper(start=0, step=60000, end=1800000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=15, chunkMethod=TimeRangeChunkScan(-300000,1800000), filters=List(ColumnFilter(job,Equals(app2)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-763739679],raw)
        |-----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |------T~PeriodicSamplesMapper(start=0, step=60000, end=1800000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=31, chunkMethod=TimeRangeChunkScan(-300000,1800000), filters=List(ColumnFilter(job,Equals(app2)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-763739679],raw)""".stripMargin
    validatePlan(execPlan, expectedPlan)
  }

  it ("should generate only local exec for fixed scalar queries") {
    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("time()", TimeStepParams(1000, 100, 2000))

    val promQlQueryParams = PromQlQueryParams("time()", 1000, 100, 2000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams, plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[TimeScalarGeneratorExec] shouldEqual (true)
  }

  it ("should generate BinaryJoinExec plan when lhs and rhs are in local partition") {
    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("test1{job = \"app\"} + test2{job = \"app\"}",
      TimeStepParams(1000, 100, 2000))

    val promQlQueryParams = PromQlQueryParams("test1{job = \"app\"} + test2{job = \"app\"}", 1000, 100, 2000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
  }

  it ("should have equal hashcode for identical getColumnFilterGroup") {

    val lp1 = Parser.queryRangeToLogicalPlan("test1{inst = \"inst-001\", job = \"app\", host = \"localhost\"}",
      TimeStepParams(1000, 100, 2000))

    val lp2 = Parser.queryRangeToLogicalPlan("test1{job = \"app\", host = \"localhost\", inst = \"inst-001\"}",
      TimeStepParams(3000, 100, 5000))

    val res1 = LogicalPlan.getColumnFilterGroup(lp1)
    val res2 = LogicalPlan.getColumnFilterGroup(lp2)

    res1.size.shouldEqual(1)
    res1(0).size.shouldEqual(4)
    res2.size.shouldEqual(1)
    res2(0).size.shouldEqual(4)

    res1(0).hashCode() shouldEqual res2(0).hashCode()

  }

  it ("should generate PromQlRemoteExec plan for BinaryJoin when lhs and rhs are in same remote partition") {
    def partitions(timeRange: TimeRange): List[PartitionAssignment] = List(PartitionAssignment("remote", "remote-url",
      TimeRange(timeRange.startMs, timeRange.endMs)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("test1{job = \"app\"} + test2{job = \"app\"}",
      TimeStepParams(1000, 100, 10000))

    val promQlQueryParams = PromQlQueryParams("test1{job = \"app\"} + test2{job = \"app\"}", 1000, 100, 10000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    val queryParams = execPlan.asInstanceOf[PromQlRemoteExec].queryContext.origQueryParams.
      asInstanceOf[PromQlQueryParams]
    queryParams.startSecs shouldEqual 1000
    queryParams.endSecs shouldEqual 10000
  }

  it ("should generate PromQLGrpcRemote plan for BinaryJoin when lhs and rhs are in same remote partition and grpc is enabled") {
    def partitions(timeRange: TimeRange): List[PartitionAssignment] = List(PartitionAssignment("remote", "remote-url",
      TimeRange(timeRange.startMs, timeRange.endMs), grpcEndPoint = Some("grpcEndpoint")))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("test1{job = \"app\"} + test2{job = \"app\"}",
      TimeStepParams(1000, 100, 10000))

    val promQlQueryParams = PromQlQueryParams("test1{job = \"app\"} + test2{job = \"app\"}", 1000, 100, 10000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[PromQLGrpcRemoteExec] shouldEqual (true)
    val queryParams = execPlan.asInstanceOf[PromQLGrpcRemoteExec].queryContext.origQueryParams.
      asInstanceOf[PromQlQueryParams]
    queryParams.startSecs shouldEqual 1000
    queryParams.endSecs shouldEqual 10000
    execPlan.asInstanceOf[PromQLGrpcRemoteExec].plannerSelector shouldEqual "plannerSelector"
  }

  it ("should generate Exec plan for Metadata query without shardkey") {
    def partitions(timeRange: TimeRange): List[PartitionAssignment] =
      List(PartitionAssignment("remote", "remote-url",
        TimeRange(startSeconds * 1000, localPartitionStart * 1000 - 1)),
        PartitionAssignment("local", "local-url", TimeRange(localPartitionStart * 1000, endSeconds * 1000)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val dataset = MetricsTestData.timeseriesDatasetMultipleShardKeys
    val schemas = Schemas(dataset.schema)
    val localPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = 0,
      queryConfig, "raw")
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.metadataQueryToLogicalPlan("http_requests_total{method=\"GET\"}",
      TimeStepParams(startSeconds, step, endSeconds))

    val promQlQueryParams = PromQlQueryParams(
      "http_requests_total{method=\"GET\"}", startSeconds, step, endSeconds)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[PartKeysDistConcatExec] shouldEqual (true)
    execPlan.children(0).isInstanceOf[PartKeysDistConcatExec] shouldEqual(true)
    execPlan.children(1).isInstanceOf[MetadataRemoteExec] shouldEqual(true)

    val queryParams = execPlan.children(1).asInstanceOf[MetadataRemoteExec].queryContext.origQueryParams.
      asInstanceOf[PromQlQueryParams]

    queryParams.startSecs shouldEqual(startSeconds)
    queryParams.endSecs shouldEqual(localPartitionStart - 1)
    execPlan.children(0).asInstanceOf[PartKeysDistConcatExec].children(0).asInstanceOf[PartKeysExec].start shouldEqual
      (localPartitionStart * 1000)
    execPlan.children(0).asInstanceOf[PartKeysDistConcatExec].children(0).asInstanceOf[PartKeysExec].end shouldEqual
      (endSeconds * 1000)
  }

  it ("should generate Exec plan for Metadata query with partial shardkey") {
    def partitions(timeRange: TimeRange): List[PartitionAssignment] =
      List(PartitionAssignment("remote", "remote-url",
        TimeRange(startSeconds * 1000, localPartitionStart * 1000 - 1)),
        PartitionAssignment("local", "local-url", TimeRange(localPartitionStart * 1000, endSeconds * 1000)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val dataset = MetricsTestData.timeseriesDatasetMultipleShardKeys
    val schemas = Schemas(dataset.schema)
    val localPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = 0,
      queryConfig, "raw")
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.metadataQueryToLogicalPlan("http_requests_total{_ws_=\"demo\", method=\"GET\"}",
      TimeStepParams(startSeconds, step, endSeconds))

    val promQlQueryParams = PromQlQueryParams(
      "http_requests_total{_ws_=\"demo\", method=\"GET\"}", startSeconds, step, endSeconds)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[PartKeysDistConcatExec] shouldEqual (true)
    execPlan.children(0).isInstanceOf[PartKeysDistConcatExec] shouldEqual(true)
    execPlan.children(1).isInstanceOf[MetadataRemoteExec] shouldEqual(true)

    val queryParams = execPlan.children(1).asInstanceOf[MetadataRemoteExec].queryContext.origQueryParams.
      asInstanceOf[PromQlQueryParams]

    queryParams.startSecs shouldEqual(startSeconds)
    queryParams.endSecs shouldEqual(localPartitionStart - 1)
    execPlan.children(0).asInstanceOf[PartKeysDistConcatExec].children(0).asInstanceOf[PartKeysExec].start shouldEqual
      (localPartitionStart * 1000)
    execPlan.children(0).asInstanceOf[PartKeysDistConcatExec].children(0).asInstanceOf[PartKeysExec].end shouldEqual
      (endSeconds * 1000)
  }

  it ("should generate Exec plan for Metadata query") {
    def partitions(timeRange: TimeRange): List[PartitionAssignment] =
      List(PartitionAssignment("remote", "remote-url",
      TimeRange(startSeconds * 1000, localPartitionStart * 1000 - 1)),
      PartitionAssignment("local", "local-url", TimeRange(localPartitionStart * 1000, endSeconds * 1000)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.metadataQueryToLogicalPlan("http_requests_total{job=\"prometheus\", method=\"GET\"}",
      TimeStepParams(startSeconds, step, endSeconds))

    val promQlQueryParams = PromQlQueryParams(
      "http_requests_total{job=\"prometheus\", method=\"GET\"}", startSeconds, step, endSeconds)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[PartKeysDistConcatExec] shouldEqual (true)
    execPlan.children(0).isInstanceOf[PartKeysDistConcatExec] shouldEqual(true)
    execPlan.children(1).isInstanceOf[MetadataRemoteExec] shouldEqual(true)

    val queryParams = execPlan.children(1).asInstanceOf[MetadataRemoteExec].queryContext.origQueryParams.
      asInstanceOf[PromQlQueryParams]

    queryParams.startSecs shouldEqual(startSeconds)
    queryParams.endSecs shouldEqual(localPartitionStart - 1)
    execPlan.children(0).asInstanceOf[PartKeysDistConcatExec].children(0).asInstanceOf[PartKeysExec].start shouldEqual
      (localPartitionStart * 1000)
    execPlan.children(0).asInstanceOf[PartKeysDistConcatExec].children(0).asInstanceOf[PartKeysExec].end shouldEqual
      (endSeconds * 1000)
  }

  it ("should generate all PromQlRemoteExec from 3 assignments") {
    val startSeconds = 1000
    val endSeconds = 10000
    val secondPartitionStart = 4000
    val thirdPartitionStart = 7000
    val lookbackMs = 300000
    val step = 100
    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app")))
          List(PartitionAssignment("remote1", "remote-url1", TimeRange(startSeconds * 1000 - lookbackMs,
             secondPartitionStart * 1000 - 1)),
            PartitionAssignment("remote2", "remote-url2", TimeRange(secondPartitionStart * 1000,
              thirdPartitionStart * 1000 - 1)),
            PartitionAssignment("remote3", "remote-url3", TimeRange(thirdPartitionStart * 1000, endSeconds * 1000)))
        else Nil
      }
      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("remote1", "remote-url1", TimeRange(startSeconds * 1000 - lookbackMs,
          secondPartitionStart * 1000 - 1)),
          PartitionAssignment("remote2", "remote-url2", TimeRange(secondPartitionStart * 1000,
            thirdPartitionStart * 1000 - 1)),
          PartitionAssignment("local", "local-url", TimeRange(thirdPartitionStart * 1000, endSeconds * 1000)))
    }
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("test{job = \"app\"}", TimeStepParams(startSeconds, step, endSeconds))
    val promQlQueryParams = PromQlQueryParams("test{job = \"app\"}", startSeconds, step, endSeconds)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))
    // Even a three partition span works with stitch filling in the missing gaps
    val expectedRawPlan = s"""E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                             |-E~PromQlRemoteExec(PromQlQueryParams(test{job = "app"},1000,100,3999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                             |-T~PeriodicSamplesMapper(start=4000000, step=100000, end=4599000, window=None, functionId=None, rawSource=false, offsetMs=None)
                             |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                             |---E~PromQlRemoteExec(PromQlQueryParams(test{job="app"}[900s],4599,1,4599,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                             |---E~PromQlRemoteExec(PromQlQueryParams(test{job="app"}[900s],4599,1,4599,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url2, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                             |---E~PromQlRemoteExec(PromQlQueryParams(test{job="app"}[900s],4599,1,4599,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url3, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                             |-E~PromQlRemoteExec(PromQlQueryParams(test{job = "app"},4600,100,6999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url2, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                             |-T~PeriodicSamplesMapper(start=7000000, step=100000, end=7599000, window=None, functionId=None, rawSource=false, offsetMs=None)
                             |--E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                             |---E~PromQlRemoteExec(PromQlQueryParams(test{job="app"}[900s],7599,1,7599,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                             |---E~PromQlRemoteExec(PromQlQueryParams(test{job="app"}[900s],7599,1,7599,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url2, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                             |---E~PromQlRemoteExec(PromQlQueryParams(test{job="app"}[900s],7599,1,7599,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url3, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                             |-E~PromQlRemoteExec(PromQlQueryParams(test{job = "app"},7600,100,10000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url3, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))""".stripMargin
    validatePlan(execPlan, expectedRawPlan)
  }

  it ("should generate all PromQlRemoteExec plan for instant queries") {
    val startSeconds = 1000
    val endSeconds = 1000
    val localPartitionStartSec= 950
    val lookbackMs = 100000
    val step = 1

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app")))
          List(PartitionAssignment("remote1", "remote-url1", TimeRange(startSeconds * 1000 - lookbackMs,
            localPartitionStartSec * 1000 - 1)), PartitionAssignment("remote2", "remote-url2",
            TimeRange(localPartitionStartSec * 1000, endSeconds * 1000)))
        else Nil
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("remote", "remote-url", TimeRange(startSeconds * 1000 - lookbackMs,
          localPartitionStartSec * 1000 - 1)), PartitionAssignment("local", "local-url",
          TimeRange(localPartitionStartSec * 1000, endSeconds * 1000)))
    }
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("test{job = \"app\"}[100s]", TimeStepParams(startSeconds, step, endSeconds))

    val promQlQueryParams = PromQlQueryParams("test{job = \"app\"}[100s]", startSeconds, step, endSeconds)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val expectedPlanString = s"""E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                                |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"}[100s],1000,1,1000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
                                |-E~PromQlRemoteExec(PromQlQueryParams(test{job="app"}[100s],1000,1,1000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url2, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))""".stripMargin
    validatePlan(execPlan, expectedPlanString)

  }

  // TODO: The range query where the end time of a query is less than the lookback (5 mins default) of the partition
  //  start time of the next partition, the data is not stitched. Calling this out for a future fix and it is not
  //  a major concern for now, following is the pictorial representation of the scenario
  //
  //                           Partition movement-> |           |  <- lookback from partition assignment ends
  //     |------------------------------------------|-----------|----------------------------.....|
  //                                        ^             ^
  //                                   Query start    Query end
//  it ("should generate second Exec with start and end time equal to query end time when query duration is less" +
//    "than or equal to lookback ") {
//
//    val startSeconds = 1594309980L
//    val endSeconds = 1594310280L
//    val localPartitionStartMs: Long = 1594309980001L
//    val step = 15L
//
//    val partitionLocationProvider = new PartitionLocationProvider {
//      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
//        if (routingKey.equals(Map("job" -> "app"))) List(
//          PartitionAssignment("remote1", "remote-url", TimeRange(startSeconds * 1000 - lookbackMs,
//            localPartitionStartMs - 1)), PartitionAssignment("remote2", "remote-url",
//            TimeRange(localPartitionStartMs, endSeconds * 1000)))
//        else Nil
//      }
//
//      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] = List(
//        PartitionAssignment("remote1", "remote-url", TimeRange(startSeconds * 1000 - lookbackMs,
//          localPartitionStartMs - 1)), PartitionAssignment("remote2", "remote-url",
//          TimeRange(localPartitionStartMs, endSeconds * 1000)))
//
//    }
//    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
//    val lp = Parser.queryRangeToLogicalPlan("test{job = \"app\"}", TimeStepParams(startSeconds, step, endSeconds))
//
//    val promQlQueryParams = PromQlQueryParams("test{job = \"app\"}", startSeconds, step, endSeconds)
//
//    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams, plannerParams =
//      PlannerParams(processMultiPartition = true)))
//    val stitchRvsExec = execPlan.asInstanceOf[StitchRvsExec]
//    stitchRvsExec.children.size shouldEqual (2)
//    stitchRvsExec.children(0).isInstanceOf[PromQlRemoteExec] shouldEqual (true)
//    stitchRvsExec.children(1).isInstanceOf[PromQlRemoteExec] shouldEqual (true)
//
//
//    val remoteExec = stitchRvsExec.children(0).asInstanceOf[PromQlRemoteExec]
//    val queryParams = remoteExec.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
//    queryParams.startSecs shouldEqual startSeconds
//    queryParams.endSecs shouldEqual (localPartitionStartMs - 1) / 1000
//    queryParams.stepSecs shouldEqual step
//    remoteExec.queryContext.plannerParams.processFailure shouldEqual true
//    remoteExec.queryContext.plannerParams.processMultiPartition shouldEqual false
//    remoteExec.queryEndpoint shouldEqual "remote-url"
//
//    val remoteExec2 = stitchRvsExec.children(1).asInstanceOf[PromQlRemoteExec]
//    val queryParams2 = remoteExec2.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
//    queryParams2.startSecs shouldEqual endSeconds
//    queryParams2.endSecs shouldEqual endSeconds
//    queryParams2.stepSecs shouldEqual step
//    remoteExec2.queryContext.plannerParams.processFailure shouldEqual true
//    remoteExec2.queryContext.plannerParams.processMultiPartition shouldEqual false
//    remoteExec2.queryEndpoint shouldEqual "remote-url"
//
//  }

  it ("should generate Exec plan for Metadata Label values query") {
    def partitions(timeRange: TimeRange): List[PartitionAssignment] =
      List(PartitionAssignment("remote", "remote-url",
        TimeRange(startSeconds * 1000, localPartitionStart * 1000 - 1)),
        PartitionAssignment("local", "local-url", TimeRange(localPartitionStart * 1000, endSeconds * 1000)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)

    val lp = Parser.labelValuesQueryToLogicalPlan(Seq("""__metric__"""), Some("""_ws_="demo""""), TimeStepParams(startSeconds, step, endSeconds) )

    val promQlQueryParams = PromQlQueryParams("", startSeconds, step, endSeconds, Some("/api/v2/label/values"))

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[LabelValuesDistConcatExec] shouldEqual (true)
    execPlan.children(0).isInstanceOf[LabelValuesDistConcatExec] shouldEqual(true)
    execPlan.children(1).isInstanceOf[MetadataRemoteExec] shouldEqual(true)

    val expectedUrlParams = Map("filter" -> """_ws_="demo"""", "labels" -> "__metric__")
    execPlan.children(1).asInstanceOf[MetadataRemoteExec].urlParams shouldEqual(expectedUrlParams) // Filter values
                                                                                                  // should have quotes
    execPlan.children(1).asInstanceOf[MetadataRemoteExec].queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      endSecs shouldEqual(localPartitionStart - 1)
    execPlan.children(0).asInstanceOf[LabelValuesDistConcatExec].children(0).asInstanceOf[LabelValuesExec].startMs shouldEqual
      (localPartitionStart * 1000)
    execPlan.children(0).asInstanceOf[LabelValuesDistConcatExec].children(0).asInstanceOf[LabelValuesExec].endMs shouldEqual
      (endSeconds * 1000)
  }

  it("should generate correct ExecPlan for TsCardinalities query version 2") {
    def partitions(timeRange: TimeRange): List[PartitionAssignment] =
      List(PartitionAssignment("remote", "remote-url",
        TimeRange(startSeconds * 1000, localPartitionStart * 1000 - 1)),
        PartitionAssignment("local", "local-url", TimeRange(localPartitionStart * 1000, endSeconds * 1000)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter],
                                         timeRange: TimeRange):List[PartitionAssignment] =
        partitions(timeRange)
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local",
      dataset, queryConfig)
    val lp = TsCardinalities(Seq("a", "b"), 3, Seq("longtime-prometheus","recordingrules-prometheus_rules_1m")
    , "raw,recordingrules")
    val promQlQueryParams = PromQlQueryParams("", startSeconds, step, endSeconds,
      Some("/api/v2/metering/cardinality/timeseries"))
    val expectedUrlParams = Map("match[]" -> """{_ws_="a",_ns_="b"}""", "numGroupByFields" -> "3","verbose" -> "true",
      "datasets" -> "raw,recordingrules")

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams, plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[TsCardReduceExec] shouldEqual (true)
    execPlan.children(0).isInstanceOf[TsCardReduceExec] shouldEqual (true)
    execPlan.children(1).isInstanceOf[MetadataRemoteExec] shouldEqual (true)
    execPlan.children(1).asInstanceOf[MetadataRemoteExec].urlParams shouldEqual (expectedUrlParams)
  }

  it ("should generate multipartition BinaryJoin") {
    def partitions(timeRange: TimeRange): List[PartitionAssignment] = List(PartitionAssignment("remote", "remote-url",
      TimeRange(timeRange.startMs, timeRange.endMs)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app1"))) List(
          PartitionAssignment("remote", "remote-url", TimeRange(timeRange.startMs,
            timeRange.endMs)))
        else List(
          PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs,
            timeRange.endMs)))
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("""test1{job = "app1"} + test2{job = "app2"}""",
      TimeStepParams(1000, 100, 10000))

    val promQlQueryParams = PromQlQueryParams("""test1{job = "app1"} + test2{job = "app2"}""", 1000, 100, 10000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    execPlan.asInstanceOf[BinaryJoinExec].lhs.head.isInstanceOf[PromQlRemoteExec] shouldEqual(true)
    execPlan.asInstanceOf[BinaryJoinExec].rhs.head.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual(true)

    execPlan.asInstanceOf[BinaryJoinExec].lhs.head.asInstanceOf[PromQlRemoteExec].queryContext.origQueryParams.asInstanceOf
      [PromQlQueryParams].promQl shouldEqual("""test1{job="app1"}""")

    execPlan.asInstanceOf[BinaryJoinExec].rhs.head.asInstanceOf[LocalPartitionDistConcatExec].children.head.
      asInstanceOf[MultiSchemaPartitionsExec].filters.contains(ColumnFilter("job", Equals("app2"))) shouldEqual(true)

  }

  it ("should generate local Exec plan when partitions list is empty") {
    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        List.empty

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        List.empty
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("test{job = \"app\"}", TimeStepParams(1000, 100, 2000))

    val promQlQueryParams = PromQlQueryParams("test{job = \"app\"}", 1000, 100, 2000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual (true)
    execPlan.children.length shouldEqual 2
    execPlan.children.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
    execPlan.children.head.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual true
  }

  it ("should generate local Exec plan for Metadata Label values query when partitions list is empty") {

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        List.empty

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        List.empty
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)

    val lp = Parser.labelValuesQueryToLogicalPlan(Seq("""__metric__"""), Some("""_ws_="demo""""), TimeStepParams(startSeconds, step, endSeconds) )

    val promQlQueryParams = PromQlQueryParams("", startSeconds, step, endSeconds, Some("/api/v2/label/values"))

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[LabelValuesDistConcatExec] shouldEqual (true)
  }

  it ("should generate local Exec plan for Metadata query when partitions list is empty") {
    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        List.empty

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] = List.empty
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.metadataQueryToLogicalPlan("http_requests_total{job=\"prometheus\", method=\"GET\"}",
      TimeStepParams(startSeconds, step, endSeconds))

    val promQlQueryParams = PromQlQueryParams(
      "http_requests_total{job=\"prometheus\", method=\"GET\"}", startSeconds, step, endSeconds)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[PartKeysDistConcatExec] shouldEqual (true)
    execPlan.children(0).isInstanceOf[PartKeysExec] shouldEqual(true)
    execPlan.children(1).isInstanceOf[PartKeysExec] shouldEqual(true)
  }

  it ("should add config to InprocessDispatcher") {

    val startSeconds = 1594309980L
    val endSeconds = 1594310280L
    val localPartitionStartMs: Long = 1594309980001L
    val step = 15L

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app"))) List(
          PartitionAssignment("remote", "remote-url", TimeRange(startSeconds * 1000 - lookbackMs,
            endSeconds * 1000)))
        else Nil
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] = List(
        PartitionAssignment("remote", "remote-url", TimeRange(startSeconds * 1000 - lookbackMs,
          localPartitionStartMs - 1)), PartitionAssignment("remote", "remote-url",
          TimeRange(localPartitionStartMs, endSeconds * 1000)))

    }
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("test{job = \"app\"}", TimeStepParams(startSeconds, step, endSeconds))

    val promQlQueryParams = PromQlQueryParams("test{job = \"app\"}", startSeconds, step, endSeconds)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams, plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    val config = execPlan.dispatcher.asInstanceOf[InProcessPlanDispatcher].queryConfig
    config.fastReduceMaxWindows shouldEqual(queryConfig.fastReduceMaxWindows)
  }

  private def getPlannerForMetadataQueryTests = {
    def partitions(timeRange: TimeRange): List[PartitionAssignment] =
      List(PartitionAssignment("remote", "remote-url",
        TimeRange(startSeconds * 1000, localPartitionStart * 1000 - 1)),
        PartitionAssignment("local", "local-url", TimeRange(localPartitionStart * 1000, endSeconds * 1000)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    (startSeconds, endSeconds, engine)
  }


  it("should materialize SeriesKeysByFilters query correctly") {
    val (startSeconds: Int, endSeconds: Int, engine: MultiPartitionPlanner) = getPlannerForMetadataQueryTests
    val lv = SeriesKeysByFilters(ColumnFilter("job", Equals("app"))::ColumnFilter("__name__", Equals("test"))::Nil,
      true, startSeconds * 1000 , endSeconds * 1000)
    val promQl = """{job="app",__name__="test"}"""
    val promQlQueryParams = PromQlQueryParams(promQl, startSeconds, step, endSeconds)
    val execPlan = engine.materialize(lv, QueryContext(origQueryParams = promQlQueryParams, plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[PartKeysDistConcatExec] shouldEqual true
    execPlan.children.size shouldEqual 2
    val expectedUrlParams = Map("match[]" -> promQl)
    execPlan.children(1).asInstanceOf[MetadataRemoteExec].urlParams shouldEqual(expectedUrlParams)
    execPlan.children(1).asInstanceOf[MetadataRemoteExec].queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      endSecs shouldEqual(localPartitionStart - 1)
    execPlan.children(0).asInstanceOf[PartKeysDistConcatExec]
      .children(0).asInstanceOf[PartKeysExec].start shouldEqual (localPartitionStart * 1000)
    execPlan.children(0).asInstanceOf[PartKeysDistConcatExec]
      .children(0).asInstanceOf[PartKeysExec].end shouldEqual (endSeconds * 1000)
  }

  it("should materialize LabelNames query correctly") {
    val (startSeconds: Int, endSeconds: Int, engine: MultiPartitionPlanner) = getPlannerForMetadataQueryTests

    val promQl = """{job="app",__name__="test"}"""
    val lv = Parser.labelNamesQueryToLogicalPlan(promQl, TimeStepParams(startSeconds, step, endSeconds))

    val promQlQueryParams = PromQlQueryParams(promQl, startSeconds, step, endSeconds, Some("/api/v2/labels/name"))
    val execPlan = engine.materialize(lv, QueryContext(origQueryParams = promQlQueryParams, plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[LabelNamesDistConcatExec] shouldEqual true
    execPlan.children.size shouldEqual 2

    val expectedUrlParams = Map("match[]" -> promQl)
    execPlan.children(1).asInstanceOf[MetadataRemoteExec].urlParams shouldEqual(expectedUrlParams)
    execPlan.children(1).asInstanceOf[MetadataRemoteExec].queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      endSecs shouldEqual(localPartitionStart - 1)
    execPlan.children(0).asInstanceOf[LabelNamesDistConcatExec]
      .children(0).asInstanceOf[LabelNamesExec].startMs shouldEqual (localPartitionStart * 1000)
    execPlan.children(0).asInstanceOf[LabelNamesDistConcatExec]
      .children(0).asInstanceOf[LabelNamesExec].endMs shouldEqual (endSeconds * 1000)
  }

  it("should materialize LabelNames query without metric filter correctly") {
    val (startSeconds: Int, endSeconds: Int, engine: MultiPartitionPlanner) = getPlannerForMetadataQueryTests

    val promQl = """{job="app"}"""
    val lv = Parser.labelNamesQueryToLogicalPlan(promQl, TimeStepParams(startSeconds, step, endSeconds))

    val promQlQueryParams = PromQlQueryParams(promQl, startSeconds, step, endSeconds, Some("/api/v2/labels/name"))
    val execPlan = engine.materialize(lv, QueryContext(origQueryParams = promQlQueryParams, plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[LabelNamesDistConcatExec] shouldEqual true
    execPlan.children.size shouldEqual 2

    val expectedUrlParams = Map("match[]" -> promQl)
    execPlan.children(1).asInstanceOf[MetadataRemoteExec].urlParams shouldEqual (expectedUrlParams)
    execPlan.children(1).asInstanceOf[MetadataRemoteExec].queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      endSecs shouldEqual (localPartitionStart - 1)
    execPlan.children(0).asInstanceOf[LabelNamesDistConcatExec]
      .children(0).asInstanceOf[LabelNamesExec].startMs shouldEqual (localPartitionStart * 1000)
    execPlan.children(0).asInstanceOf[LabelNamesDistConcatExec]
      .children(0).asInstanceOf[LabelNamesExec].endMs shouldEqual (endSeconds * 1000)
  }

  it("should materialize LabelNames with empty query correctly") {
    val (startSeconds: Int, endSeconds: Int, engine: MultiPartitionPlanner) = getPlannerForMetadataQueryTests

    val promQl = """"""
    val lv = Parser.labelNamesQueryToLogicalPlan(promQl, TimeStepParams(startSeconds, step, endSeconds))

    val promQlQueryParams = PromQlQueryParams(promQl, startSeconds, step, endSeconds, Some("/api/v2/labels/name"))
    val execPlan = engine.materialize(lv, QueryContext(origQueryParams = promQlQueryParams, plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.isInstanceOf[LabelNamesDistConcatExec] shouldEqual true
    execPlan.children.size shouldEqual 2

    val expectedUrlParams = Map("match[]" -> "{}")
    execPlan.children(1).asInstanceOf[MetadataRemoteExec].urlParams shouldEqual (expectedUrlParams)
    execPlan.children(1).asInstanceOf[MetadataRemoteExec].queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      endSecs shouldEqual (localPartitionStart - 1)
    execPlan.children(0).asInstanceOf[LabelNamesDistConcatExec]
      .children(0).asInstanceOf[LabelNamesExec].startMs shouldEqual (localPartitionStart * 1000)
    execPlan.children(0).asInstanceOf[LabelNamesDistConcatExec]
      .children(0).asInstanceOf[LabelNamesExec].endMs shouldEqual (endSeconds * 1000)
  }

  it ("should generate correct plan for multipartition BinaryJoin with instant function") {
    def partitions(timeRange: TimeRange): List[PartitionAssignment] = List(PartitionAssignment("remote", "remote-url",
      TimeRange(timeRange.startMs, timeRange.endMs)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app1"))) List(
          PartitionAssignment("remote", "remote-url", TimeRange(timeRange.startMs,
            timeRange.endMs)))
        else List(
          PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs,
            timeRange.endMs)))
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("""ln(test1{job = "app1"} + test2{job = "app2"})""",
      TimeStepParams(1000, 100, 10000))

    val promQlQueryParams = PromQlQueryParams("""ln(test1{job = "app1"} + test2{job = "app2"})""", 1000, 100, 10000)

//    Sample plan generated for an instant function applied to a Multi Partition Binary Join

//    T~InstantVectorFunctionMapper(function=Ln)
//    -E~BinaryJoinExec(binaryOp=ADD, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@43f03c23)
//    --E~PromQlRemoteExec(PromQlQueryParams(test1{job="app1"},1000,100,10000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@43f03c23)
//    --E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1986332149],raw)
//      ---T~PeriodicSamplesMapper(start=1000000, step=100000, end=10000000, window=None, functionId=None, rawSource=true, offsetMs=None)
//    ----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=13, chunkMethod=TimeRangeChunkScan(700000,10000000), filters=List(ColumnFilter(job,Equals(app2)), ColumnFilter(__name__,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1986332149],raw)
//      ---T~PeriodicSamplesMapper(start=1000000, step=100000, end=10000000, window=None, functionId=None, rawSource=true, offsetMs=None)
//    ----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=29, chunkMethod=TimeRangeChunkScan(700000,10000000), filters=List(ColumnFilter(job,Equals(app2)), ColumnFilter(__name__,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1986332149],raw)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    execPlan.asInstanceOf[BinaryJoinExec].binaryOp shouldBe ADD
    execPlan.asInstanceOf[BinaryJoinExec].lhs.head.isInstanceOf[PromQlRemoteExec] shouldEqual(true)
    execPlan.asInstanceOf[BinaryJoinExec].rhs.head.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual(true)

    execPlan.asInstanceOf[BinaryJoinExec].lhs.head.asInstanceOf[PromQlRemoteExec].queryContext.origQueryParams.asInstanceOf
      [PromQlQueryParams].promQl shouldEqual("""test1{job="app1"}""")

    execPlan.asInstanceOf[BinaryJoinExec].rhs.head.asInstanceOf[LocalPartitionDistConcatExec].children.head.
      asInstanceOf[MultiSchemaPartitionsExec].filters.contains(ColumnFilter("job", Equals("app2"))) shouldEqual(true)

    execPlan.rangeVectorTransformers.nonEmpty shouldBe true
    execPlan.rangeVectorTransformers.head shouldBe InstantVectorFunctionMapper(Ln, Nil)

  }

  it ("should generate correct plan for multipartition set operation with absent function") {
    def partitions(timeRange: TimeRange): List[PartitionAssignment] = List(PartitionAssignment("remote", "remote-url",
      TimeRange(timeRange.startMs, timeRange.endMs)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app1"))) List(
          PartitionAssignment("remote", "remote-url", TimeRange(timeRange.startMs,
            timeRange.endMs)))
        else List(
          PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs,
            timeRange.endMs)))
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("""absent(test1{job = "app1"} and test2{job = "app2"})""",
      TimeStepParams(1000, 100, 10000))

    val promQlQueryParams = PromQlQueryParams("""absent(test1{job = "app1"} and test2{job = "app2"})""", 1000, 100, 10000)

    //    Sample plan generated for an instant function applied to a Multi Partition Set Operation with Absent

//    T~AbsentFunctionMapper(columnFilter=List() rangeParams=RangeParams(1000,100,10000) metricColumn=__name__)
//    -E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@6048e26a)
//    --T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List(job))
//    ---E~SetOperatorExec(binaryOp=LAND, on=List(), ignoring=List()) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@6048e26a)
//    ----E~PromQlRemoteExec(PromQlQueryParams(test1{job="app1"},1000,100,10000,None,false), PlannerParams(filodb,None,None,None,None,60000,1000000,100000,100000,18000000,None,None,None,false,86400000,86400000,false,true,false,false), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(filodb.core.query.QueryConfig@6048e26a)
//    ----E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-529259697],raw)
//      -----T~PeriodicSamplesMapper(start=1000000, step=100000, end=10000000, window=None, functionId=None, rawSource=true, offsetMs=None)
//    ------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=13, chunkMethod=TimeRangeChunkScan(700000,10000000), filters=List(ColumnFilter(job,Equals(app2)), ColumnFilter(__name__,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-529259697],raw)
//      -----T~PeriodicSamplesMapper(start=1000000, step=100000, end=10000000, window=None, functionId=None, rawSource=true, offsetMs=None)
//    ------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=29, chunkMethod=TimeRangeChunkScan(700000,10000000), filters=List(ColumnFilter(job,Equals(app2)), ColumnFilter(__name__,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#-529259697],raw)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))
    execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual true
    execPlan.rangeVectorTransformers.nonEmpty shouldBe true
    execPlan.rangeVectorTransformers.head shouldBe AbsentFunctionMapper(Nil, RangeParams(1000, 100, 10000), "__name__")
    val childPlan = execPlan.children.head
    childPlan.isInstanceOf[SetOperatorExec] shouldEqual (true)
    childPlan.asInstanceOf[SetOperatorExec].lhs.head.isInstanceOf[PromQlRemoteExec] shouldEqual(true)
    childPlan.asInstanceOf[SetOperatorExec].rhs.head.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual(true)
    childPlan.asInstanceOf[SetOperatorExec].binaryOp shouldBe LAND

    childPlan.asInstanceOf[SetOperatorExec].lhs.head.asInstanceOf[PromQlRemoteExec].queryContext.origQueryParams.asInstanceOf
      [PromQlQueryParams].promQl shouldEqual("""test1{job="app1"}""")

    childPlan.asInstanceOf[SetOperatorExec].rhs.head.asInstanceOf[LocalPartitionDistConcatExec].children.head.
      asInstanceOf[MultiSchemaPartitionsExec].filters.contains(ColumnFilter("job", Equals("app2"))) shouldEqual(true)
  }

  it("should materialize a multi level multi partition binary join correctly") {
    def partitions(timeRange: TimeRange): List[PartitionAssignment] = List(PartitionAssignment("remote", "remote-url",
      TimeRange(timeRange.startMs, timeRange.endMs)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app2"))) List(
          PartitionAssignment("remote-1", "remote-url-1", TimeRange(timeRange.startMs,
            timeRange.endMs)))
        else if (routingKey.equals(Map("job" -> "app3"))) List(
          PartitionAssignment("remote-2", "remote-url-2", TimeRange(timeRange.startMs,
            timeRange.endMs)))
        else List(
          PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs,
            timeRange.endMs)))
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val query =
      """sum(test1{job = "app1"}) * sum(test2{job = "app1"}) +
        |ln(sum(test3{job = "app2"}) + sum(test4{job = "app3"}))""".stripMargin
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(1000, 100, 10000))

    val promQlQueryParams = PromQlQueryParams(query, 1000, 100, 10000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))


    // The above query has two binary joins, the top level is + with another binary join with * being its LHS given
    // operator precedence. Since app1 and local, it should be materialized by local planner, in our case the entire
    // binary join with * should get pushed down to local planner.
    // RHS is a multi partition operation making two remote calls, one for job="app2" and another for  job="app3".
    // In this case the aggregation should be pushed to these remote partitions and the Binary join + and applying
    // the instant Function Ln should happen in query service (use InProcessPlanDispatcher). Finally the top level
    // binary join for + should be done in process

    val expectedPlan =
    """E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
      |-E~BinaryJoinExec(binaryOp=MUL, on=None, ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#847971057],raw)
      |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1000,100,10000))
      |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#847971057],raw)
      |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
      |-----T~PeriodicSamplesMapper(start=1000000, step=100000, end=10000000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=12, chunkMethod=TimeRangeChunkScan(700000,10000000), filters=List(ColumnFilter(job,Equals(app1)), ColumnFilter(__name__,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#847971057],raw)
      |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
      |-----T~PeriodicSamplesMapper(start=1000000, step=100000, end=10000000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=28, chunkMethod=TimeRangeChunkScan(700000,10000000), filters=List(ColumnFilter(job,Equals(app1)), ColumnFilter(__name__,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#847971057],raw)
      |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1000,100,10000))
      |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#847971057],raw)
      |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
      |-----T~PeriodicSamplesMapper(start=1000000, step=100000, end=10000000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,10000000), filters=List(ColumnFilter(job,Equals(app1)), ColumnFilter(__name__,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#847971057],raw)
      |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
      |-----T~PeriodicSamplesMapper(start=1000000, step=100000, end=10000000, window=None, functionId=None, rawSource=true, offsetMs=None)
      |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,10000000), filters=List(ColumnFilter(job,Equals(app1)), ColumnFilter(__name__,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#847971057],raw)
      |-T~InstantVectorFunctionMapper(function=Ln)
      |--E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
      |---E~PromQlRemoteExec(PromQlQueryParams(sum(test3{job="app2"}),1000,100,10000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url-1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
      |---E~PromQlRemoteExec(PromQlQueryParams(sum(test4{job="app3"}),1000,100,10000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url-2, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))""".stripMargin

    validatePlan(execPlan, expectedPlan)
  }


  it("should push multi-namespace portion of query when all of it is in one partition") {
    def partitions(timeRange: TimeRange): List[PartitionAssignment] = List(PartitionAssignment("remote", "remote-url",
      TimeRange(timeRange.startMs, timeRange.endMs)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app2"))) List(
          PartitionAssignment("remote-1", "remote-url-1", TimeRange(timeRange.startMs,
            timeRange.endMs)))
        else List(
          PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs,
            timeRange.endMs)))
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val query =
      """sum(test1{job = "app1"}) * sum(test2{job = "app1"}) +
        |ln(sum(test3{job = "app2"}) + sum(test4{job = "app2"}))""".stripMargin
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(1000, 100, 10000))

    val promQlQueryParams = PromQlQueryParams(query, 1000, 100, 10000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams, plannerParams =
      PlannerParams(processMultiPartition = true)))


    // The above query has two binary joins, the top level is + with another binary join with * being its LHS given
    // operator precedence. Since app1 and local, it should be materialized by local planner, in our case the entire
    // binary join with * should get pushed down to local planner.
    // RHS is a single remote partition operation making one remote call. In this case the aggregation should be pushed
    // to this remote partitions. Finally the top level binary join for + should be done in process

    val expectedPlan =
      """E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
        |-E~BinaryJoinExec(binaryOp=MUL, on=None, ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#847971057],raw)
        |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1000,100,10000))
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#847971057],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1000000, step=100000, end=10000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=12, chunkMethod=TimeRangeChunkScan(700000,10000000), filters=List(ColumnFilter(job,Equals(app1)), ColumnFilter(__name__,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#847971057],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1000000, step=100000, end=10000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=28, chunkMethod=TimeRangeChunkScan(700000,10000000), filters=List(ColumnFilter(job,Equals(app1)), ColumnFilter(__name__,Equals(test1))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#847971057],raw)
        |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(1000,100,10000))
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#847971057],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1000000, step=100000, end=10000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=5, chunkMethod=TimeRangeChunkScan(700000,10000000), filters=List(ColumnFilter(job,Equals(app1)), ColumnFilter(__name__,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#847971057],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=1000000, step=100000, end=10000000, window=None, functionId=None, rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=21, chunkMethod=TimeRangeChunkScan(700000,10000000), filters=List(ColumnFilter(job,Equals(app1)), ColumnFilter(__name__,Equals(test2))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#847971057],raw)
        |-E~PromQlRemoteExec(PromQlQueryParams(ln((sum(test3{job="app2"}) + sum(test4{job="app2"}))),1000,100,10000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url-1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))""".stripMargin

    validatePlan(execPlan, expectedPlan)
  }

  it("should push entire query to remote partition when all of it is in one partition") {
    def partitions(timeRange: TimeRange): List[PartitionAssignment] = List(PartitionAssignment("remote", "remote-url",
      TimeRange(timeRange.startMs, timeRange.endMs)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app2"))) List(
          PartitionAssignment("remote-1", "remote-url-1", TimeRange(timeRange.startMs,
            timeRange.endMs)))
        else List(
          PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs,
            timeRange.endMs)))
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val query =
      """sum(test1{job = "app2"}) * sum(test2{job = "app2"}) +
        |ln(sum(test3{job = "app2"}) + sum(test4{job = "app2"}))""".stripMargin.replaceAll("\n", "")
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(1000, 100, 10000))

    val promQlQueryParams = PromQlQueryParams(query, 1000, 100, 10000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams, plannerParams =
      PlannerParams(processMultiPartition = true)))


    // Above Binary join should push the entire query to remote partition

    val expectedPlan =
      """E~PromQlRemoteExec(PromQlQueryParams(sum(test1{job = "app2"}) * sum(test2{job = "app2"}) +ln(sum(test3{job = "app2"}) + sum(test4{job = "app2"})),1000,100,10000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url-1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))"""

    validatePlan(execPlan, expectedPlan)
  }


  it("should materialize to local or remote based on where the job") {
    def partitions(timeRange: TimeRange): List[PartitionAssignment] = List(PartitionAssignment("remote", "remote-url",
      TimeRange(timeRange.startMs, timeRange.endMs)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app2"))) List(
          PartitionAssignment("remote-1", "remote-url-1", TimeRange(timeRange.startMs,
            timeRange.endMs)))
        else List(
          PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs,
            timeRange.endMs)))
      }

      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)
    }

    val mpPlanner = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)

    val localPlan = LabelCardinality(Seq(ColumnFilter("job", Equals("app1")), ColumnFilter("__name__", Equals("test"))),
      startSeconds * 1000, endSeconds * 1000)

    val localExecPlan = mpPlanner.materialize(localPlan,
      QueryContext(PromQlQueryParams("test{job=\"app1\"}", 1000, 100, 10000), plannerParams =
        PlannerParams(processMultiPartition = true)))

    val expectedLocalPlan =
      s"""T~LabelCardinalityPresenter(LabelCardinalityPresenter)
         |-E~LabelCardinalityReduceExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1403863627],raw)
         |--E~LabelCardinalityExec(shard=7, filters=List(ColumnFilter(job,Equals(app1)), ColumnFilter(__name__,Equals(test))), limit=1000000, startMs=1000000, endMs=10000000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1403863627],raw)
         |--E~LabelCardinalityExec(shard=23, filters=List(ColumnFilter(job,Equals(app1)), ColumnFilter(__name__,Equals(test))), limit=1000000, startMs=1000000, endMs=10000000) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#1403863627],raw)""".stripMargin

    validatePlan(localExecPlan, expectedLocalPlan)

    val remotePlan = LabelCardinality(Seq(ColumnFilter("job", Equals("app2")), ColumnFilter("__name__", Equals("test"))),
      startSeconds * 1000, endSeconds * 1000)

    val remoteExecPlan = mpPlanner.materialize(remotePlan,
      QueryContext(PromQlQueryParams("test{job=\"app2\"}", 1000, 100, 10000), plannerParams =
        PlannerParams(processMultiPartition = true)))


    val expectedRemotePlan = """E~MetadataRemoteExec(PromQlQueryParams(test{job="app2"},1000,100,10000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url-1, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))"""
    validatePlan(remoteExecPlan, expectedRemotePlan)
  }

  it("should generate generate a raw export from remote from multiple partitions and stitch") {

    val p1StartSecs = 1000
    val p1EndSecs = 6999
    val p2StartSecs = 7000
    val p2EndSecs = 15000
    val stepSecs = 100
    val queryStartSecs = 12000
    val subqueryLookbackSecs = 9000

    def twoPartitions(): List[PartitionAssignment] = List(
      PartitionAssignment("remote", "remote-url", TimeRange(p1StartSecs * 1000, p1EndSecs * 1000)),
      PartitionAssignment("local", "local-url", TimeRange(p2StartSecs * 1000, p2EndSecs * 1000))
    )

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app"))) twoPartitions().filter(
          (p: PartitionAssignment) => {
            val startWithinPartition = (p.timeRange.startMs <= timeRange.startMs) && (p.timeRange.endMs > timeRange.startMs)
            val endWithinPartition = (p.timeRange.startMs <= timeRange.endMs) && (p.timeRange.endMs > timeRange.endMs)
            val partitionWithinInterval = (p.timeRange.startMs >= timeRange.startMs) && (p.timeRange.endMs < timeRange.endMs)
            startWithinPartition || endWithinPartition || partitionWithinInterval
          })
        else Nil
      }

      def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter],
                                timeRange: TimeRange): List[PartitionAssignment] = twoPartitions()
    }

    val expectedPlanWithRemoteExport =
      s"""E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
         |-E~PromQlRemoteExec(PromQlQueryParams(sum(rate(test{job = "app"}[10m])),1000,100,6999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
         |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(7000,100,7899))
         |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
         |----T~PeriodicSamplesMapper(start=7000000, step=100000, end=7899000, window=Some(600000), functionId=Some(Rate), rawSource=false, offsetMs=None)
         |-----E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
         |------E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#885676802],raw)
         |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=3, chunkMethod=TimeRangeChunkScan(6399000,7899000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#885676802],raw)
         |-------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=19, chunkMethod=TimeRangeChunkScan(6399000,7899000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#885676802],raw)
         |------E~PromQlRemoteExec(PromQlQueryParams(test{job="app"}[1500s],7899,1,7899,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
         |-T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(7900,100,10000))
         |--E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#885676802],raw)
         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
         |----T~PeriodicSamplesMapper(start=7900000, step=100000, end=10000000, window=Some(600000), functionId=Some(Rate), rawSource=true, offsetMs=None)
         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=3, chunkMethod=TimeRangeChunkScan(7300000,10000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#885676802],raw)
         |---T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
         |----T~PeriodicSamplesMapper(start=7900000, step=100000, end=10000000, window=Some(600000), functionId=Some(Rate), rawSource=true, offsetMs=None)
         |-----E~MultiSchemaPartitionsExec(dataset=timeseries, shard=19, chunkMethod=TimeRangeChunkScan(7300000,10000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-1#885676802],raw)""".stripMargin

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local",
      dataset, queryConfig.copy(routingConfig = queryConfig.routingConfig.copy(supportRemoteRawExport = true)))
    val query1 = "sum(rate(test{job = \"app\"}[10m]))"
    val lp1 = Parser.queryRangeToLogicalPlan(query1, TimeStepParams(1000, stepSecs, 10000))

    val promQlQueryParams = PromQlQueryParams(query1, 1000, 100, 10000)
    val execPlan1 = engine.materialize(lp1, QueryContext(origQueryParams = promQlQueryParams, plannerParams =
      PlannerParams(processMultiPartition = true)))
    validatePlan(execPlan1, expectedPlanWithRemoteExport)

    val expectedPlanWithRemoteExec1 =
      """E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
        |-E~PromQlRemoteExec(PromQlQueryParams(sum(rate(test{job = "app"}[10m])) + sum(rate(bar{job = "app"}[5m])),1000,100,6999,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
        |-E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
        |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(7000,100,7899))
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=7000000, step=100000, end=7899000, window=Some(600000), functionId=Some(Rate), rawSource=false, offsetMs=None)
        |------E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
        |-------E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#479288168],raw)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=3, chunkMethod=TimeRangeChunkScan(6399000,7899000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#479288168],raw)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=19, chunkMethod=TimeRangeChunkScan(6399000,7899000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#479288168],raw)
        |-------E~PromQlRemoteExec(PromQlQueryParams(test{job="app"}[1500s],7899,1,7899,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
        |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(7000,100,7899))
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=7000000, step=100000, end=7899000, window=Some(300000), functionId=Some(Rate), rawSource=false, offsetMs=None)
        |------E~StitchRvsExec() on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
        |-------E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#479288168],raw)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(6699000,7899000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#479288168],raw)
        |--------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(6699000,7899000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#479288168],raw)
        |-------E~PromQlRemoteExec(PromQlQueryParams(bar{job="app"}[1200s],7899,1,7899,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,true,false,false,true,10,false,true), queryEndpoint=remote-url, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,None,Some(10000),None,None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(true,3 days,true,300000)))
        |-E~BinaryJoinExec(binaryOp=ADD, on=None, ignoring=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#479288168],raw)
        |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(7900,100,10000))
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#479288168],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=7900000, step=100000, end=10000000, window=Some(600000), functionId=Some(Rate), rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=3, chunkMethod=TimeRangeChunkScan(7300000,10000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#479288168],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=7900000, step=100000, end=10000000, window=Some(600000), functionId=Some(Rate), rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=19, chunkMethod=TimeRangeChunkScan(7300000,10000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(test))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#479288168],raw)
        |--T~AggregatePresenter(aggrOp=Sum, aggrParams=List(), rangeParams=RangeParams(7900,100,10000))
        |---E~LocalPartitionReduceAggregateExec(aggrOp=Sum, aggrParams=List()) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#479288168],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=7900000, step=100000, end=10000000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=11, chunkMethod=TimeRangeChunkScan(7600000,10000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#479288168],raw)
        |----T~AggregateMapReduce(aggrOp=Sum, aggrParams=List(), without=List(), by=List())
        |-----T~PeriodicSamplesMapper(start=7900000, step=100000, end=10000000, window=Some(300000), functionId=Some(Rate), rawSource=true, offsetMs=None)
        |------E~MultiSchemaPartitionsExec(dataset=timeseries, shard=27, chunkMethod=TimeRangeChunkScan(7600000,10000000), filters=List(ColumnFilter(job,Equals(app)), ColumnFilter(__name__,Equals(bar))), colName=None, schema=None) on ActorPlanDispatcher(Actor[akka://default/system/testProbe-3#479288168],raw)""".stripMargin

    val query2 = "sum(rate(test{job = \"app\"}[10m])) + sum(rate(bar{job = \"app\"}[5m]))"
    val lp2 = Parser.queryRangeToLogicalPlan(query2, TimeStepParams(2000, stepSecs, 10000))

    val promQlQueryParams2 = PromQlQueryParams(query2, 1000, 100, 10000)
    val execPlan2 = engine.materialize(lp2, QueryContext(origQueryParams = promQlQueryParams2, plannerParams =
      PlannerParams(processMultiPartition = true)))

   validatePlan(execPlan2, expectedPlanWithRemoteExec1)


    val query4 = "sum(rate(test{job = \"app\"}[10m])) + sum(rate(bar{job = \"app\"}[5m] offset 5m))"
    val lp4 = Parser.queryRangeToLogicalPlan(query4, TimeStepParams(2000, stepSecs, 10000))

    val promQlQueryParams4 = PromQlQueryParams(query4, 1000, 100, 10000)
    intercept[BadQueryException] {
      // Expecting to see Exception when we use BinaryJoin with offsets, technically this too should not be a big deal
      // as we need to identify the right window, however this was not supported even before the change and it is ok to
      // leave it unaddressed in the first phase as its just Binary joins with offsets
      engine.materialize(lp4, QueryContext(origQueryParams = promQlQueryParams4, plannerParams =
        PlannerParams(processMultiPartition = true)))
    }

  }

  it ("should give the correct routing keys") {
    val dummyPartitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = ???
      override def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] = ???
    }
    val mpp = new MultiPartitionPlanner(dummyPartitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("""foo{job=~"abc|def"} + foo{job="ghi"}""", TimeStepParams(1000, 100, 2000))
    val expected = Set(Map("job" -> "abc"), Map("job" -> "def"), Map("job" -> "ghi"))
    mpp.getRoutingKeys(lp) shouldEqual expected
  }
}
