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

  private val routingConfigString = "routing {\n  remote {\n    http {\n      timeout = 10000\n    }\n  }\n}"
  private val routingConfig = ConfigFactory.parseString(routingConfigString)

  private val config = ConfigFactory.load("application_test.conf")
    .getConfig("filodb.query").withFallback(routingConfig)
  private val queryConfig = new QueryConfig(config)

  val localPlanner = new SingleClusterPlanner(dataset.ref, schemas, mapperRef, earliestRetainedTimestampFn = 0,
    queryConfig)

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

    val promQlQueryParams = PromQlQueryParams("test{job = \"app\"}", 1000, 100, 2000, processMultiPartition = true)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[DistConcatExec] shouldEqual (true)
    execPlan.children.length shouldEqual 2
    execPlan.children.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
    execPlan.children.head.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual true
  }

  it ("should generate local & PromQlRemoteExec plan") {

    def twoPartitions(timeRange: TimeRange): List[PartitionAssignment] = List(
      PartitionAssignment("remote", "remote-url", TimeRange(startSeconds * 1000 - lookbackMs,
        localPartitionStart * 1000 - 1)), PartitionAssignment("local", "local-url",
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

    val promQlQueryParams = PromQlQueryParams("test{job = \"app\"}", startSeconds, step, endSeconds,
      processMultiPartition = true)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    val stitchRvsExec = execPlan.asInstanceOf[StitchRvsExec]
    stitchRvsExec.children.size shouldEqual (2)
    stitchRvsExec.children(0).isInstanceOf[DistConcatExec] shouldEqual (true)
    stitchRvsExec.children(1).isInstanceOf[PromQlRemoteExec] shouldEqual (true)


    val remoteExec = stitchRvsExec.children(1).asInstanceOf[PromQlRemoteExec]
    remoteExec.params.startSecs shouldEqual startSeconds
    remoteExec.params.endSecs shouldEqual (localPartitionStart - 1)
    remoteExec.params.stepSecs shouldEqual step
    remoteExec.params.processFailure shouldEqual true
    remoteExec.params.processMultiPartition shouldEqual false
    remoteExec.queryEndpoint shouldEqual "remote-url"

    val localExec = stitchRvsExec.children(0).asInstanceOf[DistConcatExec]
    localExec.isInstanceOf[DistConcatExec] shouldEqual (true)
    localExec.children.length shouldEqual 2
    localExec.children.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true

    val expectedStartMs = ((startSeconds*1000) to (endSeconds*1000) by (step*1000)).find { instant =>
      instant - lookbackMs > (localPartitionStart * 1000)
    }.get

    localExec.children.head.asInstanceOf[MultiSchemaPartitionsExec].
      chunkMethod.asInstanceOf[TimeRangeChunkScan].startTime shouldEqual (expectedStartMs - lookbackMs)
    localExec.children.head.asInstanceOf[MultiSchemaPartitionsExec].
      chunkMethod.asInstanceOf[TimeRangeChunkScan].endTime shouldEqual (endSeconds * 1000)
    localExec.children.head.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual true
    localExec.children.head.rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper].start shouldEqual
      (expectedStartMs)
    localExec.children.head.rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper].end shouldEqual
      (endSeconds * 1000)

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

    val promQlQueryParams = PromQlQueryParams("time()", 1000, 100, 2000, processMultiPartition = true)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

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

    val promQlQueryParams = PromQlQueryParams("test1{job = \"app\"} + test2{job = \"app\"}", 1000, 100, 2000,
      processMultiPartition = true)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
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

    val promQlQueryParams = PromQlQueryParams("test1{job = \"app\"} + test2{job = \"app\"}", 1000, 100, 10000,
      processMultiPartition = true)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    execPlan.asInstanceOf[PromQlRemoteExec].params.startSecs shouldEqual 1000
    execPlan.asInstanceOf[PromQlRemoteExec].params.endSecs shouldEqual 10000
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
      "http_requests_total{job=\"prometheus\", method=\"GET\"}", startSeconds, step, endSeconds,
      processMultiPartition = true)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PartKeysDistConcatExec] shouldEqual (true)
    execPlan.children(0).isInstanceOf[PartKeysDistConcatExec] shouldEqual(true)
    execPlan.children(1).isInstanceOf[MetadataRemoteExec] shouldEqual(true)

    execPlan.children(1).asInstanceOf[MetadataRemoteExec].params.startSecs shouldEqual(startSeconds)
    execPlan.children(1).asInstanceOf[MetadataRemoteExec].params.endSecs shouldEqual(localPartitionStart - 1)
    execPlan.children(0).asInstanceOf[PartKeysDistConcatExec].children(0).asInstanceOf[PartKeysExec].start shouldEqual
      (localPartitionStart * 1000)
    execPlan.children(0).asInstanceOf[PartKeysDistConcatExec].children(0).asInstanceOf[PartKeysExec].end shouldEqual
      (endSeconds * 1000)
  }

  it ("should generate local & PromQlRemoteExec from 3 assignments") {
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
            PartitionAssignment("local", "local-url", TimeRange(thirdPartitionStart * 1000, endSeconds * 1000)))
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
    val promQlQueryParams = PromQlQueryParams("test{job = \"app\"}", startSeconds, step, endSeconds,
      processMultiPartition = true)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    val stitchRvsExec = execPlan.asInstanceOf[StitchRvsExec]
    stitchRvsExec.children.size shouldEqual (3)
    stitchRvsExec.children(0).isInstanceOf[DistConcatExec] shouldEqual (true)
    stitchRvsExec.children(1).isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    stitchRvsExec.children(2).isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    val remoteExec1 = stitchRvsExec.children(1).asInstanceOf[PromQlRemoteExec]
    remoteExec1.params.startSecs shouldEqual startSeconds
    remoteExec1.params.endSecs shouldEqual 3999
    remoteExec1.params.stepSecs shouldEqual step
    remoteExec1.params.processFailure shouldEqual true
    remoteExec1.params.processMultiPartition shouldEqual false
    remoteExec1.queryEndpoint shouldEqual "remote-url1"
    val remoteExec2 = stitchRvsExec.children(2).asInstanceOf[PromQlRemoteExec]

    val expectedStartMs1 = ((startSeconds*1000) to (endSeconds*1000) by (step*1000)).find { instant =>
      instant - lookbackMs > (secondPartitionStart * 1000)
    }.get

    val expectedStartMs2 = ((startSeconds*1000) to (endSeconds*1000) by (step*1000)).find { instant =>
      instant - lookbackMs > (thirdPartitionStart * 1000)
    }.get

    remoteExec2.params.startSecs shouldEqual expectedStartMs1 / 1000
    remoteExec2.params.endSecs shouldEqual 6999
    remoteExec2.params.stepSecs shouldEqual step
    remoteExec2.params.processFailure shouldEqual true
    remoteExec2.params.processMultiPartition shouldEqual false
    remoteExec2.queryEndpoint shouldEqual "remote-url2"
    val localExec = stitchRvsExec.children(0).asInstanceOf[DistConcatExec]
    localExec.isInstanceOf[DistConcatExec] shouldEqual (true)
    localExec.children.length shouldEqual 2
    localExec.children.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true

    localExec.children.head.rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper].start shouldEqual
      expectedStartMs2
    localExec.children.head.asInstanceOf[MultiSchemaPartitionsExec].
      chunkMethod.asInstanceOf[TimeRangeChunkScan].startTime shouldEqual expectedStartMs2 - lookbackMs
    localExec.children.head.asInstanceOf[MultiSchemaPartitionsExec].
      chunkMethod.asInstanceOf[TimeRangeChunkScan].endTime shouldEqual (endSeconds * 1000)
    localExec.children.head.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual true

    localExec.children.head.rangeVectorTransformers.head.asInstanceOf[PeriodicSamplesMapper].end shouldEqual
      (endSeconds * 1000)
  }

  it ("should generate local & PromQlRemoteExec plan for instant queries") {
    val startSeconds = 1000
    val endSeconds = 1000
    val localPartitionStartSec= 950
    val lookbackMs = 100000
    val step = 1

    val partitionLocationProvider = new PartitionLocationProvider {
      override def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment] = {
        if (routingKey.equals(Map("job" -> "app")))
          List(PartitionAssignment("remote", "remote-url", TimeRange(startSeconds * 1000 - lookbackMs,
            localPartitionStartSec * 1000 - 1)), PartitionAssignment("local", "local-url",
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

    val promQlQueryParams = PromQlQueryParams("test{job = \"app\"}", startSeconds, step, endSeconds,
      processMultiPartition = true)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    val stitchRvsExec = execPlan.asInstanceOf[StitchRvsExec]
    stitchRvsExec.children.size shouldEqual (2)
    stitchRvsExec.children(0).isInstanceOf[DistConcatExec] shouldEqual (true)
    stitchRvsExec.children(1).isInstanceOf[PromQlRemoteExec] shouldEqual (true)


    // Instant/Raw queries will have same start and end point in all partitions as we want to fetch raw data
    val remoteExec = stitchRvsExec.children(1).asInstanceOf[PromQlRemoteExec]
    remoteExec.params.startSecs shouldEqual startSeconds
    remoteExec.params.endSecs shouldEqual endSeconds
    remoteExec.params.stepSecs shouldEqual step
    remoteExec.params.processFailure shouldEqual true
    remoteExec.params.processMultiPartition shouldEqual false
    remoteExec.queryEndpoint shouldEqual "remote-url"

    val localExec = stitchRvsExec.children(0).asInstanceOf[DistConcatExec]
    localExec.isInstanceOf[DistConcatExec] shouldEqual (true)
    localExec.children.length shouldEqual 2
    localExec.children.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true

    localExec.children.head.asInstanceOf[MultiSchemaPartitionsExec].
      chunkMethod.asInstanceOf[TimeRangeChunkScan].startTime shouldEqual (startSeconds * 1000 - lookbackMs)
    localExec.children.head.asInstanceOf[MultiSchemaPartitionsExec].
      chunkMethod.asInstanceOf[TimeRangeChunkScan].endTime shouldEqual (endSeconds * 1000)

  }
}
