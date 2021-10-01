package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import filodb.coordinator.ShardMapper
import filodb.core.MetricsTestData
import filodb.core.metadata.Schemas
import filodb.core.query.Filter.Equals
import filodb.core.query.{ColumnFilter, PlannerParams, PromQlQueryParams, QueryConfig, QueryContext}
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.LogicalPlan
import filodb.query.exec._

class MultiPartitionPlannerSpec extends AnyFunSpec with Matchers {
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
  private val queryConfig = new QueryConfig(config)

  val localPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = 0,
    queryConfig, "raw")

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

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
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

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] = twoPartitions(timeRange)

    }
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("test{job = \"app\"}", TimeStepParams(startSeconds, step, endSeconds))

    val promQlQueryParams = PromQlQueryParams("test{job = \"app\"}", startSeconds, step, endSeconds)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams, plannerParams =
      PlannerParams(processMultiPartition = true)))

    val stitchRvsExec = execPlan.asInstanceOf[StitchRvsExec]
    stitchRvsExec.children.size shouldEqual (2)
    stitchRvsExec.children(0).isInstanceOf[PromQlRemoteExec] shouldEqual true
    stitchRvsExec.children(1).isInstanceOf[PromQlRemoteExec] shouldEqual true

    val remoteExec1 = stitchRvsExec.children(0).asInstanceOf[PromQlRemoteExec]
    val queryParams1 = remoteExec1.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams1.startSecs shouldEqual startSeconds
    queryParams1.endSecs shouldEqual (localPartitionStart - 1)
    queryParams1.stepSecs shouldEqual step
    remoteExec1.queryContext.plannerParams.processFailure shouldEqual true
    remoteExec1.queryContext.plannerParams.processMultiPartition shouldEqual false
    remoteExec1.queryEndpoint shouldEqual "remote-url"

    val expectedStartMs = ((startSeconds*1000) to (endSeconds*1000) by (step*1000)).find { instant =>
      instant - lookbackMs > (localPartitionStart * 1000)
    }.get

    val remoteExec2 = stitchRvsExec.children(1).asInstanceOf[PromQlRemoteExec]
    val queryParams2 = remoteExec2.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams2.startSecs shouldEqual (expectedStartMs / 1000)
    queryParams2.endSecs shouldEqual endSeconds
    queryParams2.stepSecs shouldEqual step
    remoteExec2.queryContext.plannerParams.processFailure shouldEqual true
    remoteExec2.queryContext.plannerParams.processMultiPartition shouldEqual false
    remoteExec2.queryEndpoint shouldEqual "remote-url2"

  }

  it ("should generate all PromQlRemoteExec plan for TopLevelSubquery") {

    def twoPartitions(timeRange: TimeRange): List[PartitionAssignment] = List(
      PartitionAssignment("remote", "remote-url", TimeRange(startSeconds * 1000 - lookbackMs,
        localPartitionStart * 1000 - 1)), PartitionAssignment("remote2", "remote-url2",
        TimeRange(localPartitionStart * 1000, endSeconds * 1000)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app"))) twoPartitions(timeRange)
        else Nil
      }

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] = twoPartitions(timeRange)

    }
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("test{job = \"app\"}[9000s:100s]", TimeStepParams(endSeconds, step, endSeconds))

    val promQlQueryParams = PromQlQueryParams("test{job = \"app\"}[9000s:100s]", endSeconds, step, endSeconds)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams, plannerParams =
      PlannerParams(processMultiPartition = true)))

    val stitchRvsExec = execPlan.asInstanceOf[StitchRvsExec]
    stitchRvsExec.children.size shouldEqual (2)
    stitchRvsExec.children(0).isInstanceOf[PromQlRemoteExec] shouldEqual true
    stitchRvsExec.children(1).isInstanceOf[PromQlRemoteExec] shouldEqual true

    val remoteExec1 = stitchRvsExec.children(0).asInstanceOf[PromQlRemoteExec]
    val queryParams1 = remoteExec1.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams1.startSecs shouldEqual endSeconds
    queryParams1.endSecs shouldEqual endSeconds
    queryParams1.stepSecs shouldEqual step
    remoteExec1.queryContext.plannerParams.processFailure shouldEqual true
    remoteExec1.queryContext.plannerParams.processMultiPartition shouldEqual false
    remoteExec1.queryEndpoint shouldEqual "remote-url"

    val expectedStartMs = ((startSeconds*1000) to (endSeconds*1000) by (step*1000)).find { instant =>
      instant - lookbackMs > (localPartitionStart * 1000)
    }.get

    val remoteExec2 = stitchRvsExec.children(1).asInstanceOf[PromQlRemoteExec]
    val queryParams2 = remoteExec2.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams2.startSecs shouldEqual endSeconds
    queryParams2.endSecs shouldEqual endSeconds
    queryParams2.stepSecs shouldEqual step
    remoteExec2.queryContext.plannerParams.processFailure shouldEqual true
    remoteExec2.queryContext.plannerParams.processMultiPartition shouldEqual false
    remoteExec2.queryEndpoint shouldEqual "remote-url2"

  }

  it ("should not generate PromQlExec plan when partitions are local for TopLevelSubquery") {
    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)))

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)))
    }

    val engine = new MultiPartitionPlanner(
      partitionLocationProvider, localPlanner, "local", dataset, queryConfig
    )
    val query = """test{job = "app"}[100m:10m]"""
    val lp = Parser.queryRangeToLogicalPlan(
      query,
      TimeStepParams(endSeconds, step, endSeconds)
    )

    val promQlQueryParams = PromQlQueryParams(query, endSeconds, step, endSeconds)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))

    execPlan.printTree()

    execPlan.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual (true)
    execPlan.children.length shouldEqual 2
    execPlan.children.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
    execPlan.children.head.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual true

    val distConcatPlan = execPlan.asInstanceOf[LocalPartitionDistConcatExec]

    val localExec1 = distConcatPlan.children(0).asInstanceOf[MultiSchemaPartitionsExec]
    val queryParams1 = localExec1.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams1.startSecs shouldEqual endSeconds
    queryParams1.endSecs shouldEqual endSeconds
    queryParams1.stepSecs shouldEqual step
    val samplesMapper1 = localExec1.rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper]
    samplesMapper1.startMs shouldEqual 4200000
    samplesMapper1.endMs shouldEqual 9600000
    samplesMapper1.stepMs shouldEqual 10*60*1000

    val localExec2 = distConcatPlan.children(1).asInstanceOf[MultiSchemaPartitionsExec]
    val queryParams2 = localExec2.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams2.startSecs shouldEqual endSeconds
    queryParams2.endSecs shouldEqual endSeconds
    queryParams2.stepSecs shouldEqual step
    val samplesMapper2 = localExec2.rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper]
    samplesMapper2.startMs shouldEqual 4200000
    samplesMapper2.endMs shouldEqual 9600000
    samplesMapper2.stepMs shouldEqual 10*60*1000
  }

  it ("should generate both PromQlExec and MultiSchemaPartitionsExec for TopLevelSubquery") {
    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        List(
          PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)),
          PartitionAssignment("remote", "remote-url", TimeRange(timeRange.startMs, timeRange.endMs))
        )

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)))
    }

    val engine = new MultiPartitionPlanner(
      partitionLocationProvider, localPlanner, "local", dataset, queryConfig
    )
    val query = """test{job = "app"}[100m:10m]"""
    val lp = Parser.queryRangeToLogicalPlan(
      query,
      TimeStepParams(endSeconds, step, endSeconds)
    )

    val promQlQueryParams = PromQlQueryParams(query, endSeconds, step, endSeconds)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,  plannerParams =
      PlannerParams(processMultiPartition = true)))

    println(execPlan.printTree())

    execPlan.isInstanceOf[StitchRvsExec] shouldEqual (true)
    execPlan.children.length shouldEqual 2
    execPlan.children(0).isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
    execPlan.children(1).isInstanceOf[PromQlRemoteExec] shouldEqual true

    val distConcatPlan = execPlan.children(0).asInstanceOf[LocalPartitionDistConcatExec]

    val localExec = distConcatPlan.children(0).asInstanceOf[MultiSchemaPartitionsExec]
    val localQueryParams = localExec.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    localQueryParams.startSecs shouldEqual endSeconds
    localQueryParams.endSecs shouldEqual endSeconds
    localQueryParams.stepSecs shouldEqual step
    val localSamplesMapper = localExec.rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper]
    localSamplesMapper.startMs shouldEqual 4200000
    localSamplesMapper.endMs shouldEqual 9600000
    localSamplesMapper.stepMs shouldEqual 10*60*1000

    val remoteExec = execPlan.children(1).asInstanceOf[PromQlRemoteExec]
    val remoteQueryParams = remoteExec.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    remoteQueryParams.startSecs shouldEqual endSeconds
    remoteQueryParams.endSecs shouldEqual endSeconds
    remoteQueryParams.stepSecs shouldEqual step
  }

  it ("one partition should work for SubqueryWithWindowing") {

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

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] = onePartition(timeRange)

    }
    val query = "avg_over_time(test{job = \"app\"}[10m:1m])"
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSeconds, step, endSeconds))

    val promQlQueryParams = PromQlQueryParams(query, startSeconds, step, endSeconds)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams, plannerParams =
      PlannerParams(processMultiPartition = true)))

    // subqueries cannot be stitched, this test is to verify that subquery can be executed against one remote partition
    val exec = execPlan.asInstanceOf[PromQlRemoteExec]

    val queryParams1 = exec.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams1.startSecs shouldEqual startSeconds
    queryParams1.endSecs shouldEqual endSeconds
    queryParams1.stepSecs shouldEqual step
    exec.queryContext.plannerParams.processFailure shouldEqual true
    exec.queryContext.plannerParams.processMultiPartition shouldEqual false
    exec.queryEndpoint shouldEqual "remote-url"
  }

  it ("should generate only local exec for fixed scalar queries") {
    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
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

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
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

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
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

  it ("should generate Exec plan for Metadata query") {
    def partitions(timeRange: TimeRange): List[PartitionAssignment] =
      List(PartitionAssignment("remote", "remote-url",
      TimeRange(startSeconds * 1000, localPartitionStart * 1000 - 1)),
      PartitionAssignment("local", "local-url", TimeRange(localPartitionStart * 1000, endSeconds * 1000)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
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
      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
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
    val stitchRvsExec = execPlan.asInstanceOf[StitchRvsExec]
    stitchRvsExec.children.size shouldEqual (3)
    stitchRvsExec.children(0).isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    stitchRvsExec.children(1).isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    stitchRvsExec.children(2).isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    val remoteExec1 = stitchRvsExec.children(0).asInstanceOf[PromQlRemoteExec]
    val queryParams1 = remoteExec1.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams1.startSecs shouldEqual startSeconds
    queryParams1.endSecs shouldEqual 3999
    queryParams1.stepSecs shouldEqual step
    remoteExec1.queryContext.plannerParams.processFailure shouldEqual true
    remoteExec1.queryContext.plannerParams.processMultiPartition shouldEqual false
    remoteExec1.queryEndpoint shouldEqual "remote-url1"
    val remoteExec2 = stitchRvsExec.children(1).asInstanceOf[PromQlRemoteExec]

    val expectedStartMs1 = ((startSeconds*1000) to (endSeconds*1000) by (step*1000)).find { instant =>
      instant - lookbackMs > (secondPartitionStart * 1000)
    }.get

    val expectedStartMs2 = ((startSeconds*1000) to (endSeconds*1000) by (step*1000)).find { instant =>
      instant - lookbackMs > (thirdPartitionStart * 1000)
    }.get

    val queryParams2 = remoteExec2.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams2.startSecs shouldEqual expectedStartMs1 / 1000
    queryParams2.endSecs shouldEqual 6999
    queryParams2.stepSecs shouldEqual step
    remoteExec2.queryContext.plannerParams.processFailure shouldEqual true
    remoteExec2.queryContext.plannerParams.processMultiPartition shouldEqual false
    remoteExec2.queryEndpoint shouldEqual "remote-url2"

    val remoteExec3 = stitchRvsExec.children(2).asInstanceOf[PromQlRemoteExec]
    val queryParams3 = remoteExec3.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams3.startSecs shouldEqual expectedStartMs2 / 1000
    queryParams3.endSecs shouldEqual endSeconds
    queryParams3.stepSecs shouldEqual step
    remoteExec3.queryContext.plannerParams.processFailure shouldEqual true
    remoteExec3.queryContext.plannerParams.processMultiPartition shouldEqual false
    remoteExec3.queryEndpoint shouldEqual "remote-url3"

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

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("remote", "remote-url", TimeRange(startSeconds * 1000 - lookbackMs,
          localPartitionStartSec * 1000 - 1)), PartitionAssignment("local", "local-url",
          TimeRange(localPartitionStartSec * 1000, endSeconds * 1000)))
    }
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("test{job = \"app\"}[100s]", TimeStepParams(startSeconds, step, endSeconds))

    val promQlQueryParams = PromQlQueryParams("test{job = \"app\"}", startSeconds, step, endSeconds)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams,
      plannerParams = PlannerParams(processMultiPartition = true)))

    val stitchRvsExec = execPlan.asInstanceOf[StitchRvsExec]
    stitchRvsExec.children.size shouldEqual (2)
    stitchRvsExec.children(0).isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    stitchRvsExec.children(1).isInstanceOf[PromQlRemoteExec] shouldEqual (true)


    // Instant/Raw queries will have same start and end point in all partitions as we want to fetch raw data
    val remoteExec1 = stitchRvsExec.children(0).asInstanceOf[PromQlRemoteExec]
    val queryParams1 = remoteExec1.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams1.startSecs shouldEqual startSeconds
    queryParams1.endSecs shouldEqual endSeconds
    queryParams1.stepSecs shouldEqual step
    remoteExec1.queryContext.plannerParams.processFailure shouldEqual true
    remoteExec1.queryContext.plannerParams.processMultiPartition shouldEqual false
    remoteExec1.queryEndpoint shouldEqual "remote-url1"

    val remoteExec2 = stitchRvsExec.children(1).asInstanceOf[PromQlRemoteExec]
    val queryParams2 = remoteExec1.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams2.startSecs shouldEqual startSeconds
    queryParams2.endSecs shouldEqual endSeconds
    queryParams2.stepSecs shouldEqual step
    remoteExec2.queryContext.plannerParams.processFailure shouldEqual true
    remoteExec2.queryContext.plannerParams.processMultiPartition shouldEqual false
    remoteExec2.queryEndpoint shouldEqual "remote-url2"

  }

  it ("should generate second Exec with start and end time equal to query end time when query duration is less" +
    "than or equal to lookback ") {

    val startSeconds = 1594309980L
    val endSeconds = 1594310280L
    val localPartitionStartMs: Long = 1594309980001L
    val step = 15L

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app"))) List(
          PartitionAssignment("remote", "remote-url", TimeRange(startSeconds * 1000 - lookbackMs,
            localPartitionStartMs - 1)), PartitionAssignment("remote", "remote-url",
            TimeRange(localPartitionStartMs, endSeconds * 1000)))
        else Nil
      }

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] = List(
        PartitionAssignment("remote", "remote-url", TimeRange(startSeconds * 1000 - lookbackMs,
          localPartitionStartMs - 1)), PartitionAssignment("remote", "remote-url",
          TimeRange(localPartitionStartMs, endSeconds * 1000)))

    }
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset, queryConfig)
    val lp = Parser.queryRangeToLogicalPlan("test{job = \"app\"}", TimeStepParams(startSeconds, step, endSeconds))

    val promQlQueryParams = PromQlQueryParams("test{job = \"app\"}", startSeconds, step, endSeconds)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams, plannerParams =
      PlannerParams(processMultiPartition = true)))
    val stitchRvsExec = execPlan.asInstanceOf[StitchRvsExec]
    stitchRvsExec.children.size shouldEqual (2)
    stitchRvsExec.children(0).isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    stitchRvsExec.children(1).isInstanceOf[PromQlRemoteExec] shouldEqual (true)


    val remoteExec = stitchRvsExec.children(0).asInstanceOf[PromQlRemoteExec]
    val queryParams = remoteExec.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams.startSecs shouldEqual startSeconds
    queryParams.endSecs shouldEqual (localPartitionStartMs - 1) / 1000
    queryParams.stepSecs shouldEqual step
    remoteExec.queryContext.plannerParams.processFailure shouldEqual true
    remoteExec.queryContext.plannerParams.processMultiPartition shouldEqual false
    remoteExec.queryEndpoint shouldEqual "remote-url"

    val remoteExec2 = stitchRvsExec.children(1).asInstanceOf[PromQlRemoteExec]
    val queryParams2 = remoteExec2.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams2.startSecs shouldEqual endSeconds
    queryParams2.endSecs shouldEqual endSeconds
    queryParams2.stepSecs shouldEqual step
    remoteExec2.queryContext.plannerParams.processFailure shouldEqual true
    remoteExec2.queryContext.plannerParams.processMultiPartition shouldEqual false
    remoteExec2.queryEndpoint shouldEqual "remote-url"

  }

  it ("should generate Exec plan for Metadata Label values query") {
    def partitions(timeRange: TimeRange): List[PartitionAssignment] =
      List(PartitionAssignment("remote", "remote-url",
        TimeRange(startSeconds * 1000, localPartitionStart * 1000 - 1)),
        PartitionAssignment("local", "local-url", TimeRange(localPartitionStart * 1000, endSeconds * 1000)))

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        partitions(timeRange)

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
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

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
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

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
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

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
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

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] = List.empty
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

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] = List(
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
}
