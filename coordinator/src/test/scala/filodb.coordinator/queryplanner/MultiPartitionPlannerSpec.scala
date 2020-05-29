package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}

import filodb.coordinator.ShardMapper
import filodb.core.MetricsTestData
import filodb.core.metadata.Schemas
import filodb.core.query.{PromQlQueryParams, QueryConfig, QueryContext}
import filodb.core.store.TimeRangeChunkScan
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.exec._

class MultiPartitionPlannerSpec extends FunSpec with Matchers {
  private implicit val system = ActorSystem()
  private val node = TestProbe().ref

  private val mapper = new ShardMapper(32)
  for { i <- 0 until 32 } mapper.registerNode(Seq(i), node)

  private def mapperRef = mapper

  private val dataset = MetricsTestData.timeseriesDataset
  private val schemas = Schemas(dataset.schema)
  private val config = ConfigFactory.load("application_test.conf").getConfig("filodb.query")
  private val queryConfig = new QueryConfig(config)

  val localPlanner = new SingleClusterPlanner(dataset.ref, schemas, mapperRef, earliestRetainedTimestampFn = 0,
    queryConfig)

  it ("should not generate PromQlExec plan when partitions are local") {

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)))

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)))
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset)
    val lp = Parser.queryRangeToLogicalPlan("test{job = \"app\"}", TimeStepParams(1000, 100, 2000))

    val promQlQueryParams = PromQlQueryParams(ConfigFactory.empty, "test{job = \"app\"}", 1000, 100, 2000, None)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[DistConcatExec] shouldEqual (true)
    execPlan.children.length shouldEqual 2
    execPlan.children.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
    execPlan.children.head.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual true
  }

  it ("should generate local & PromQlExec plan") {
    val startSeconds = 1000
    val endSeconds = 10000
    val lookbackMs = 300000
    val step = 100

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("remote", "remote-url", TimeRange(timeRange.startMs, startSeconds * 1000 + 2000)),
          PartitionAssignment("local", "local-url", TimeRange(startSeconds * 1000 + 3000, timeRange.endMs)))

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("remote", "remote-url", TimeRange(timeRange.startMs, timeRange.startMs + 2000)),
          PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs + 3000, timeRange.endMs)))
    }
    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset)
    val lp = Parser.queryRangeToLogicalPlan("test{job = \"app\"}", TimeStepParams(startSeconds, step, endSeconds))

    val promQlQueryParams = PromQlQueryParams(ConfigFactory.empty, "test{job = \"app\"}", startSeconds, step,
      endSeconds, None)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    val stitchRvsExec = execPlan.asInstanceOf[StitchRvsExec]
    stitchRvsExec.children.size shouldEqual (2)
    stitchRvsExec.children(0).isInstanceOf[DistConcatExec] shouldEqual (true)
    stitchRvsExec.children(1).isInstanceOf[PromQlExec] shouldEqual (true)


    val remoteExec = stitchRvsExec.children(1).asInstanceOf[PromQlExec]
    remoteExec.params.startSecs shouldEqual startSeconds
    remoteExec.params.endSecs shouldEqual (startSeconds * 1000 + 2000) / 1000
    remoteExec.params.stepSecs shouldEqual step
    remoteExec.params.processFailure shouldEqual true
    remoteExec.params.processRouting shouldEqual false
    remoteExec.params.config.getString("endpoint") shouldEqual "remote-url"

    val localExec = stitchRvsExec.children(0).asInstanceOf[DistConcatExec]
    localExec.isInstanceOf[DistConcatExec] shouldEqual (true)
    localExec.children.length shouldEqual 2
    localExec.children.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
    localExec.children.head.asInstanceOf[MultiSchemaPartitionsExec].
      chunkMethod.asInstanceOf[TimeRangeChunkScan].startTime shouldEqual (startSeconds * 1000 + 3000)
    localExec.children.head.asInstanceOf[MultiSchemaPartitionsExec].
      chunkMethod.asInstanceOf[TimeRangeChunkScan].endTime shouldEqual (endSeconds * 1000)
    localExec.children.head.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual true
    localExec.children.head.rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper].start shouldEqual
      (startSeconds * 1000 + 3000 + lookbackMs)
    localExec.children.head.rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper].end shouldEqual
      (endSeconds * 1000)

  }

  it ("should generate only local exec for fixed scalar queries") {

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)))

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)))
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset)
    val lp = Parser.queryRangeToLogicalPlan("time()", TimeStepParams(1000, 100, 2000))

    val promQlQueryParams = PromQlQueryParams(ConfigFactory.empty, "time()", 1000, 100, 2000, None)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[TimeScalarGeneratorExec] shouldEqual (true)
  }

  it ("should generate BinaryJoinExec plan when lhs and rhs are in local partition") {

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)))

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)))
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset)
    val lp = Parser.queryRangeToLogicalPlan("test1{job = \"app\"} + test2{job = \"app\"}",
      TimeStepParams(1000, 100, 2000))

    val promQlQueryParams = PromQlQueryParams(ConfigFactory.empty, "test1{job = \"app\"} + test2{job = \"app\"}",
      1000, 100, 2000, None)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
  }

  it ("should generate PromQlExec plan for BinaryJoin when lhs and rhs are in same remote partition") {

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("remote", "remote-url", TimeRange(timeRange.startMs, timeRange.endMs)))

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("remote", "remote-url", TimeRange(timeRange.startMs, timeRange.endMs)))
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset)
    val lp = Parser.queryRangeToLogicalPlan("test1{job = \"app\"} + test2{job = \"app\"}",
      TimeStepParams(1000, 100, 10000))

    val promQlQueryParams = PromQlQueryParams(ConfigFactory.empty, "test1{job = \"app\"} + test2{job = \"app\"}",
      1000, 100, 10000, None)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlExec] shouldEqual (true)
    execPlan.asInstanceOf[PromQlExec].params.startSecs shouldEqual 1
    execPlan.asInstanceOf[PromQlExec].params.endSecs shouldEqual 10
  }

  it ("should generate Exec plan for Metadata query") {

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)))

      override def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment] =
        List(PartitionAssignment("local", "local-url", TimeRange(timeRange.startMs, timeRange.endMs)))
    }

    val engine = new MultiPartitionPlanner(partitionLocationProvider, localPlanner, "local", dataset)
    val lp = Parser.metadataQueryToLogicalPlan("http_requests_total{job=\"prometheus\", method=\"GET\"}",
      TimeStepParams(1000, 100, 2000))

    val promQlQueryParams = PromQlQueryParams(ConfigFactory.empty,
      "http_requests_total{job=\"prometheus\", method=\"GET\"}", 1000, 100, 2000, None)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PartKeysDistConcatExec] shouldEqual (true)
  }
}
