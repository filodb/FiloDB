package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import filodb.coordinator.ShardMapper
import filodb.core.{DatasetRef, MetricsTestData}
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter, PlannerParams, PromQlQueryParams, QueryConfig, QueryContext}
import filodb.core.store.TimeRangeChunkScan
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query._
import filodb.query.exec._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

// scalastyle:off line.size.limit
class HighAvailabilityPlannerSpec extends AnyFunSpec with Matchers with PlanValidationSpec {

  private implicit val system: ActorSystem = ActorSystem()
  private val node = TestProbe().ref

  private val mapper = new ShardMapper(32)
  for { i <- 0 until 32 } mapper.registerNode(Seq(i), node)

  private def mapperRef = mapper

  private val dataset = MetricsTestData.timeseriesDataset
  private val dsRef = dataset.ref
  private val schemas = Schemas(dataset.schema)

  private val routingConfigString = "routing {partition_name = p1 \n  remote {\n    http {\n" +
      "      endpoint = localhost\n      timeout = 10000\n    }\n  }\n}"

  private val routingConfig = ConfigFactory.parseString(routingConfigString)

  private val config = ConfigFactory.load("application_test.conf").getConfig("filodb.query").
    withFallback(routingConfig).resolve()
  private val queryConfig = QueryConfig(config).copy(plannerSelector = Some("plannerSelector"))
  /*
  This is the PromQL

  sum(rate(http_request_duration_seconds_bucket{job="myService",le="0.3"}[5m])) by (job)
   /
  sum(rate(http_request_duration_seconds_count{job="myService"}[5m])) by (job)
  */
  private val f1 = Seq(ColumnFilter("__name__", Filter.Equals("http_request_duration_seconds_bucket")),
    ColumnFilter("job", Filter.Equals("myService")),
    ColumnFilter("le", Filter.Equals("0.3")))

  private val promQlQueryParams = PromQlQueryParams("sum(heap_usage0)", 100, 1, 1000)

  val localPlanner = new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = 0, queryConfig,
    "raw")

  it("should not generate PromQlExec plan when local overlapping failure is smaller") {
    val to = 10000
    val from = 100
    val intervalSelector = IntervalSelector(from, to)
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from, 100, to, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, AggregateClause.byOpt(Seq("job")))

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("remote", datasetRef,
          TimeRange(1500, 4000), true),
          FailureTimeRange("local", datasetRef, //Removed
          TimeRange(2000, 3000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(5000, 6000), true))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)

    // Should ignore smaller local failure which is from 1500 - 4000 and generate local exec plan
    val reduceAggregateExec = execPlan.asInstanceOf[LocalPartitionReduceAggregateExec]

    reduceAggregateExec.children.length shouldEqual (2) //default spread is 1 so 2 shards

    reduceAggregateExec.children.foreach { l1 =>
      l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
      val l1Exec = l1.asInstanceOf[MultiSchemaPartitionsExec]
      SingleClusterPlannerSpec.validateRangeVectorTransformersForPeriodicSeriesWithWindowingLogicalPlan(l1Exec)
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].startMs shouldEqual (0)
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].endMs shouldEqual (10000)
    }
  }

  it("should generate only PromQlExec when failure is present only in local") {
    val to = 10000
    val from = 100
    val intervalSelector = IntervalSelector(from, to)
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from, 100, to, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, AggregateClause.byOpt(Seq("job")))

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(1000, 6000), false))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    val queryParams = execPlan.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams.startSecs shouldEqual(from/1000)
    queryParams.endSecs shouldEqual(to/1000)
  }


  it("should generate PromQLGrpcExec when failure is present only in local and remote gRPC is configured") {
    val to = 10000
    val from = 100
    val intervalSelector = IntervalSelector(from, to)
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from, 100, to, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, AggregateClause.byOpt(Seq("job")))

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(1000, 6000), false))
      }
    }

    val queryConfigWithGrpcEndpoint = QueryConfig(
      config.withValue("routing.remote.grpc.endpoint", ConfigValueFactory.fromAnyRef("grpcEndpoint")))
      .copy(plannerSelector = Some("plannerSelector"))

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider,
      queryConfigWithGrpcEndpoint,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQLGrpcRemoteExec] shouldEqual (true)
    val queryParams = execPlan.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams.startSecs shouldEqual(from/1000)
    queryParams.endSecs shouldEqual(to/1000)
    execPlan.queryContext.plannerParams.processFailure shouldEqual false
    execPlan.asInstanceOf[PromQLGrpcRemoteExec].channel.authority() shouldEqual "grpcEndpoint"
    execPlan.asInstanceOf[PromQLGrpcRemoteExec].plannerSelector shouldEqual "plannerSelector"
  }

  it("should generate PromQLRemoteExec when failure is present in local, remote gRPC is configured " +
    "but partition is denied") {
    val to = 10000
    val from = 100
    val intervalSelector = IntervalSelector(from, to)
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from, 100, to, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, AggregateClause.byOpt(Seq("job")))

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(1000, 6000), false))
      }
    }

    val queryConfigWithGrpcEndpoint = QueryConfig(
      config.withValue("routing.remote.grpc.endpoint", ConfigValueFactory.fromAnyRef("grpcEndpoint")))
      .copy(grpcPartitionsDenyList = Set("*"), plannerSelector = Some("plannerSelector"))

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider,
      queryConfigWithGrpcEndpoint,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    val queryParams = execPlan.queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams.startSecs shouldEqual(from/1000)
    queryParams.endSecs shouldEqual(to/1000)
    execPlan.queryContext.plannerParams.processFailure shouldEqual false
  }

  it("should generate RemoteExecPlan with RawSeries time according to lookBack") {
    val to = 2000000
    val from = 1000000
    val intervalSelector = IntervalSelector(from, to) // Lookback of 50000
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"), Some(50000))
    val windowed = PeriodicSeriesWithWindowing(raw, from, 100, to, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, AggregateClause.byOpt(Seq("job")))
    val promQlQueryParams = PromQlQueryParams("", from/1000, 1, to/1000)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(1060000, 1090000), true))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[StitchRvsExec] shouldEqual (true)

    val stitchRvsExec = execPlan.asInstanceOf[StitchRvsExec]
    stitchRvsExec.children.size shouldEqual (2)
    stitchRvsExec.children(0).isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
    stitchRvsExec.children(1).isInstanceOf[PromQlRemoteExec] shouldEqual (true)

    val child1 = stitchRvsExec.children(0).asInstanceOf[LocalPartitionReduceAggregateExec]
    val child2 = stitchRvsExec.children(1).asInstanceOf[PromQlRemoteExec]

    child1.children.length shouldEqual (2) //default spread is 1 so 2 shards

    child1.children.foreach { l1 =>
      l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
      l1.asInstanceOf[MultiSchemaPartitionsExec].chunkMethod.asInstanceOf[TimeRangeChunkScan].startTime shouldEqual
        1010000
      l1.asInstanceOf[MultiSchemaPartitionsExec].chunkMethod.asInstanceOf[TimeRangeChunkScan].endTime shouldEqual
        2000000
      val l1Exec = l1.asInstanceOf[MultiSchemaPartitionsExec]
      SingleClusterPlannerSpec.validateRangeVectorTransformersForPeriodicSeriesWithWindowingLogicalPlan(l1Exec)
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].startMs shouldEqual (1060000)
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].endMs shouldEqual (2000000)
    }
    val queryParams = child2.queryContext.origQueryParams.
      asInstanceOf[PromQlQueryParams]

    queryParams.startSecs shouldEqual from/1000
    queryParams.endSecs shouldEqual (1060000-1)/1000
    child2.queryContext.plannerParams.processFailure shouldEqual(false)
  }

  it("should generate only PromQlExec when local failure starts before query time") {
    val to = 10000
    val from = 100
    val intervalSelector = IntervalSelector(from, to)
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from, 100, to, 10000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, AggregateClause.byOpt(Seq("job")))

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(50, 200), false))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    val queryParams = execPlan.asInstanceOf[PromQlRemoteExec].queryContext.origQueryParams.
      asInstanceOf[PromQlQueryParams]
    queryParams.startSecs shouldEqual(from/1000)
    queryParams.endSecs shouldEqual(to/1000)
  }

  it("should generate only PromQlExec when local failure timerange coincide with query time range") {
    val to = 10000
    val from = 100
    val intervalSelector = IntervalSelector(from, to)
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from, 100, to, 10000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, AggregateClause.byOpt(Seq("job")))

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(100, 10000), false))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)


    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    val queryParams = execPlan.asInstanceOf[PromQlRemoteExec].queryContext.origQueryParams.
      asInstanceOf[PromQlQueryParams]
    queryParams.startSecs shouldEqual(from/1000)
    queryParams.endSecs shouldEqual(to/1000)
  }

  it("should generate only PromQlExec when local failure starts before query end time and ends after query end time") {
    val to = 10000
    val from = 100
    val intervalSelector = IntervalSelector(from, to)
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from, 100, to, 10000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, AggregateClause.byOpt(Seq("job")))

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(5000, 20000), false))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    val queryParams =execPlan.asInstanceOf[PromQlRemoteExec].queryContext.origQueryParams.
      asInstanceOf[PromQlQueryParams]
    queryParams.startSecs shouldEqual(from/1000)
    queryParams.endSecs shouldEqual(to/1000)
  }

  it("should generate PromQlExecPlan and LocalPlan with RawSeries time according to lookBack and step") {
    val to = 2000
    val from = 900
    val lookBack = 300000
    val step = 60
    val intervalSelector = IntervalSelector(from * 1000 - lookBack , to * 1000) // Lookback of 300
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from * 1000, step * 1000, to * 1000, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, AggregateClause.byOpt(Seq("job")))
    val promQlQueryParams = PromQlQueryParams("dummy query", from, step, to)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(1060000, 1090000), true))
      }
    }
    //900K to 1020K and 1020+60 k to 2000K

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[StitchRvsExec] shouldEqual (true)

    val stitchRvsExec = execPlan.asInstanceOf[StitchRvsExec]
    stitchRvsExec.children.size shouldEqual 2
    stitchRvsExec.children(0).isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)
    stitchRvsExec.children(1).isInstanceOf[PromQlRemoteExec] shouldEqual (true)

    val child1 = stitchRvsExec.children(0).asInstanceOf[LocalPartitionReduceAggregateExec]
    val child2 = stitchRvsExec.children(1).asInstanceOf[PromQlRemoteExec]

    child1.children.length shouldEqual 2 //default spread is 1 so 2 shards

    child1.children.foreach { l1 =>
      l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
      l1.asInstanceOf[MultiSchemaPartitionsExec].chunkMethod.asInstanceOf[TimeRangeChunkScan].startTime shouldEqual
        (1080000-lookBack)
      l1.asInstanceOf[MultiSchemaPartitionsExec].chunkMethod.asInstanceOf[TimeRangeChunkScan].endTime shouldEqual
        2000000
      val l1Exec = l1.asInstanceOf[MultiSchemaPartitionsExec]
      SingleClusterPlannerSpec.validateRangeVectorTransformersForPeriodicSeriesWithWindowingLogicalPlan(l1Exec)
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].startMs shouldEqual 1080000
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].endMs shouldEqual 2000000
    }

    val queryParams = child2.asInstanceOf[PromQlRemoteExec].queryContext.origQueryParams.
      asInstanceOf[PromQlQueryParams]
    queryParams.startSecs shouldEqual 900
    queryParams.endSecs shouldEqual 1020
    queryParams.stepSecs shouldEqual 60
    child2.asInstanceOf[PromQlRemoteExec].queryContext.plannerParams.processFailure shouldEqual(false)
  }

  it("should generate only PromQlExecPlan when second remote ends after query end time") {
    val to = 2000
    val from = 900
    val lookBack = 300000
    val step = 60
    val intervalSelector = IntervalSelector(from * 1000 - lookBack , to * 1000) // Lookback of 300
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from * 1000, step * 1000, to * 1000, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, AggregateClause.byOpt(Seq("job")))
    val promQlQueryParams = PromQlQueryParams("dummy query", from, step, to)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(2000000, 2500000), true))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual true

    val child = execPlan.asInstanceOf[PromQlRemoteExec]
    val queryParams = child.asInstanceOf[PromQlRemoteExec].queryContext.origQueryParams.
      asInstanceOf[PromQlQueryParams]
    queryParams.startSecs shouldEqual 900
    queryParams.endSecs shouldEqual 1980
    queryParams.stepSecs shouldEqual 60
    child.asInstanceOf[PromQlRemoteExec].queryContext.plannerParams.processFailure shouldEqual false
  }

  it("should not do routing for InstantQueries when there are local and remote failures") {
    val to = 900
    val from = 900
    val lookBack = 300000
    val step = 1000
    val intervalSelector = IntervalSelector(from * 1000 - lookBack , to * 1000) // Lookback of 300
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from * 1000, step * 1000, to * 1000, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, AggregateClause.byOpt(Seq("job")))
    val promQlQueryParams = PromQlQueryParams("dummy query", from, step, to)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(2000000, 2500000), true))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)

    val reduceAggregateExec = execPlan.asInstanceOf[LocalPartitionReduceAggregateExec]

    reduceAggregateExec.children.length shouldEqual (2) //default spread is 1 so 2 shards

    reduceAggregateExec.children.foreach { l1 =>
      l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
      val l1Exec = l1.asInstanceOf[MultiSchemaPartitionsExec]
      SingleClusterPlannerSpec.validateRangeVectorTransformersForPeriodicSeriesWithWindowingLogicalPlan(l1Exec)
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].startMs shouldEqual from *1000
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].endMs shouldEqual  to * 1000
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
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, AggregateClause.byOpt(Seq("job")))
    val promQlQueryParams = PromQlQueryParams("dummy query", from, step, to)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(2000000, 2500000), false))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual true

    val child = execPlan.asInstanceOf[PromQlRemoteExec]
    val queryParams = child.asInstanceOf[PromQlRemoteExec].queryContext.origQueryParams.
      asInstanceOf[PromQlQueryParams]
    queryParams.startSecs shouldEqual from
    queryParams.endSecs shouldEqual to
    queryParams.stepSecs shouldEqual step
    child.asInstanceOf[PromQlRemoteExec].queryContext.plannerParams.processFailure shouldEqual false
  }

  it("should work with offset") {
    val t = TimeStepParams(700, 1000, 10000)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef, TimeRange(100000, 200000), false))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val lp1 = Parser.queryRangeToLogicalPlan("http_requests_total{job = \"app\"}", t)
    val execPlan1 = engine.materialize(lp1, QueryContext(origQueryParams = promQlQueryParams))
    execPlan1.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual (true) // No routing as failure is before query start time

    val lp2 = Parser.queryRangeToLogicalPlan("http_requests_total{job = \"app\"} offset 10m", t)
    val execPlan2 = engine.materialize(lp2, QueryContext(origQueryParams = promQlQueryParams))
    // Because of offset starts time would be (700 - 600) = 100 seconds where there is failure
    // So PromQlExec is generated instead of local LocalPartitionDistConcatExec. PromQlExec will have original query and start time
    // Start time with offset will be calculated by buddy pod
    execPlan2.isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    val queryParams = execPlan2.asInstanceOf[PromQlRemoteExec].queryContext.origQueryParams.
      asInstanceOf[PromQlQueryParams]
    queryParams.startSecs shouldEqual(700)
    queryParams.endSecs shouldEqual(10000)
  }

  it("should generate PromQlExec for metadata queries") {
    val to = 10000
    val from = 100
    val intervalSelector = IntervalSelector(from, to)
    val lp = Parser.metadataQueryToLogicalPlan("http_requests_total{job=\"prometheus\", method=\"GET\"}",
      TimeStepParams(from, 20, to))

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(from * 1000, (from + 200) * 1000), false))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[MetadataRemoteExec] shouldEqual (true)
    val queryParams = execPlan.asInstanceOf[MetadataRemoteExec].queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryParams.startSecs shouldEqual (from)
    queryParams.endSecs shouldEqual (to)

  }

  it("should generate MetadataRemoteExec for TsCardinalities when local failure is present") {
    val lp = TsCardinalities(Seq("ws_foo", "ns_bar"), 3)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        // Return a failure that overlaps with the synthetic time range (now-5min, now)
        Seq(FailureTimeRange("local", datasetRef, queryTimeRange, false))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[MetadataRemoteExec] shouldEqual true
    execPlan.queryContext.plannerParams.processFailure shouldEqual false

    val remoteExec = execPlan.asInstanceOf[MetadataRemoteExec]
    remoteExec.urlParams.contains("numGroupByFields") shouldEqual true
    remoteExec.urlParams("numGroupByFields") shouldEqual "3"
    remoteExec.urlParams.contains("match[]") shouldEqual true

    // Validate the complete plan tree
    val expected =
      """E~MetadataRemoteExec(PromQlQueryParams(sum(heap_usage0),100,1,1000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,1000000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,false,false,false,true,10,false,true,TreeSet(),LegacyFailoverMode,None,None,None,None), queryEndpoint=localhost, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(p1),Some(10000),Some(localhost),None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(false,1800000 milliseconds,true,0,Set(),_ws_),CachingConfig(true,2048),false))""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should execute TsCardinalities locally when no failures are present") {
    val lp = TsCardinalities(Seq("ws_foo", "ns_bar"), 3)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq.empty
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    // No failures — should stay local
    execPlan.isInstanceOf[MetadataRemoteExec] shouldEqual false
    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual false
  }

  it("should route TsCardinalities to buddy and set processFailure=false to prevent loop") {
    val lp = TsCardinalities(Seq("ws_foo", "ns_bar"), 3)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef, queryTimeRange, false))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[MetadataRemoteExec] shouldEqual true
    // Verify loop prevention: buddy should not attempt failover again
    execPlan.queryContext.plannerParams.processFailure shouldEqual false
    execPlan.queryContext.plannerParams.processMultiPartition shouldEqual false
  }

  it("should execute TsCardinalities locally when processMultiPartition=false (buddy received query)") {
    val lp = TsCardinalities(Seq("ws_foo", "ns_bar"), 3)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef, queryTimeRange, false))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    // Simulate buddy receiving the rerouted query: processFailure=false skips HA routing
    val qContext = QueryContext(origQueryParams = promQlQueryParams,
      plannerParams = PlannerParams(processFailure = false, processMultiPartition = false))
    val execPlan = engine.materialize(lp, qContext)

    // Should execute locally even though failures exist — processFailure=false bypasses HA
    execPlan.isInstanceOf[MetadataRemoteExec] shouldEqual false
    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual false
  }

  it("should execute TsCardinalities locally when only remote failures exist") {
    val lp = TsCardinalities(Seq("ws_foo", "ns_bar"), 3)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        // Only remote/buddy failures — local is healthy
        Seq(FailureTimeRange("remote", datasetRef, queryTimeRange, true))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    // Remote failure only — should stay local, not route to unhealthy buddy
    execPlan.isInstanceOf[MetadataRemoteExec] shouldEqual false
    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual false
  }

  it("should route TsCardinalities to buddy when both local and remote failures exist") {
    val lp = TsCardinalities(Seq("ws_foo", "ns_bar"), 3)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        // Both local and remote have failures
        Seq(
          FailureTimeRange("local", datasetRef, queryTimeRange, false),
          FailureTimeRange("remote", datasetRef, queryTimeRange, true)
        )
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    // Local failure exists — routes to buddy even if buddy also has failures.
    // materializeLegacy only considers local failures for routing decisions.
    execPlan.isInstanceOf[MetadataRemoteExec] shouldEqual true

    // Validate the complete plan tree
    val expected =
      """E~MetadataRemoteExec(PromQlQueryParams(sum(heap_usage0),100,1,1000,None,false), PlannerParams(filodb,None,None,None,None,60000,PerQueryLimits(1000000,1000000,18000000,100000,100000,300000000,1000000,200000000),PerQueryLimits(50000,1000000,15000000,50000,50000,150000000,500000,100000000),None,None,None,false,86400000,86400000,false,false,false,false,true,10,false,true,TreeSet(),LegacyFailoverMode,None,None,None,None), queryEndpoint=localhost, requestTimeoutMs=10000) on InProcessPlanDispatcher(QueryConfig(10 seconds,300000,1,50,antlr,true,true,Some(p1),Some(10000),Some(localhost),None,25,true,false,true,Set(),Some(plannerSelector),Map(filodb-query-exec-metadataexec -> 65536, filodb-query-exec-aggregate-large-container -> 65536),RoutingConfig(false,1800000 milliseconds,true,0,Set(),_ws_),CachingConfig(true,2048),false))""".stripMargin
    validatePlan(execPlan, expected)
  }

  it("should route TsCardinalities with multiple datasets to buddy on local failure") {
    val lp = TsCardinalities(Seq("ws_foo", "ns_bar"), 3,
      Seq("raw", "recordingrules"), "raw,recordingrules")

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef, queryTimeRange, false))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[MetadataRemoteExec] shouldEqual true
    val remoteExec = execPlan.asInstanceOf[MetadataRemoteExec]
    // Verify datasets are passed through in URL params
    remoteExec.urlParams("datasets") shouldEqual "raw,recordingrules"
    remoteExec.urlParams("numGroupByFields") shouldEqual "3"
  }

  it("should route TsCardinalities with empty shardKeyPrefix to buddy on local failure") {
    // Empty prefix (numGroupByFields=1) — dispatched to all partitions at MPP level,
    // but at HA level it's the same all-or-nothing routing
    val lp = TsCardinalities(Seq(), 1)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef, queryTimeRange, false))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, mapperRef, failureProvider, queryConfig,
      workUnit = null, buddyWorkUnit = null, clusterName = null, useShardLevelFailover = false)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[MetadataRemoteExec] shouldEqual true
    val remoteExec = execPlan.asInstanceOf[MetadataRemoteExec]
    remoteExec.urlParams("numGroupByFields") shouldEqual "1"
  }
}
