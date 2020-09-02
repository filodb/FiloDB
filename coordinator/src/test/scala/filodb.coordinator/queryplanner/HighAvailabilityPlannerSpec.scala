package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

import filodb.coordinator.ShardMapper
import filodb.core.{DatasetRef, MetricsTestData}
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter, PromQlQueryParams, QueryConfig, QueryContext}
import filodb.core.store.TimeRangeChunkScan
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query._
import filodb.query.exec._

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class HighAvailabilityPlannerSpec extends AnyFunSpec with Matchers {

  private implicit val system = ActorSystem()
  private val node = TestProbe().ref

  private val mapper = new ShardMapper(32)
  for { i <- 0 until 32 } mapper.registerNode(Seq(i), node)

  private def mapperRef = mapper

  private val dataset = MetricsTestData.timeseriesDataset
  private val dsRef = dataset.ref
  private val schemas = Schemas(dataset.schema)

  private val routingConfigString = "routing {\n  remote {\n   " +
    " http {\n      endpoint = localhost\n timeout = 10000\n    }\n  }\n}"

  private val routingConfig = ConfigFactory.parseString(routingConfigString)

  private val config = ConfigFactory.load("application_test.conf").getConfig("filodb.query").
    withFallback(routingConfig)
  private val queryConfig = new QueryConfig(config)
  /*
  This is the PromQL

  sum(rate(http_request_duration_seconds_bucket{job="myService",le="0.3"}[5m])) by (job)
   /
  sum(rate(http_request_duration_seconds_count{job="myService"}[5m])) by (job)
  */
  private val f1 = Seq(ColumnFilter("__name__", Filter.Equals("http_request_duration_seconds_bucket")),
    ColumnFilter("job", Filter.Equals("myService")),
    ColumnFilter("le", Filter.Equals("0.3")))

  private val promQlQueryParams = PromQlQueryParams("sum(heap_usage)", 100, 1, 1000)

  val localPlanner = new SingleClusterPlanner(dsRef, schemas, mapperRef, earliestRetainedTimestampFn = 0, queryConfig)

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

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryConfig)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)

    // Should ignore smaller local failure which is from 1500 - 4000 and generate local exec plan
    val reduceAggregateExec = execPlan.asInstanceOf[LocalPartitionReduceAggregateExec]

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

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryConfig)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    execPlan.asInstanceOf[PromQlRemoteExec].params.startSecs shouldEqual(from/1000)
    execPlan.asInstanceOf[PromQlRemoteExec].params.endSecs shouldEqual(to/1000)
  }

  it("should generate RemoteExecPlan with RawSeries time according to lookBack") {
    val to = 2000000
    val from = 1000000
    val intervalSelector = IntervalSelector(from, to) // Lookback of 50000
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"), Some(50000))
    val windowed = PeriodicSeriesWithWindowing(raw, from, 100, to, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, Seq("job"))
    val promQlQueryParams = PromQlQueryParams("", from/1000, 1, to/1000)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(1060000, 1090000), true))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryConfig)

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
      l1.rangeVectorTransformers.size shouldEqual 2
      l1.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].start shouldEqual (1060000)
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].end shouldEqual (2000000)
      l1.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
    }

    child2.params.startSecs shouldEqual from/1000
    child2.params.endSecs shouldEqual (1060000-1)/1000
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

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryConfig)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    execPlan.asInstanceOf[PromQlRemoteExec].params.startSecs shouldEqual(from/1000)
    execPlan.asInstanceOf[PromQlRemoteExec].params.endSecs shouldEqual(to/1000)
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

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryConfig)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    execPlan.asInstanceOf[PromQlRemoteExec].params.startSecs shouldEqual(from/1000)
    execPlan.asInstanceOf[PromQlRemoteExec].params.endSecs shouldEqual(to/1000)
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

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryConfig)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    execPlan.asInstanceOf[PromQlRemoteExec].params.startSecs shouldEqual(from/1000)
    execPlan.asInstanceOf[PromQlRemoteExec].params.endSecs shouldEqual(to/1000)
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
    val promQlQueryParams = PromQlQueryParams("dummy query", from, step, to)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(1060000, 1090000), true))
      }
    }
    //900K to 1020K and 1020+60 k to 2000K

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryConfig)

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
      l1.rangeVectorTransformers.size shouldEqual 2
      l1.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].start shouldEqual 1080000
      l1.rangeVectorTransformers(0).asInstanceOf[PeriodicSamplesMapper].end shouldEqual 2000000
      l1.rangeVectorTransformers(1).isInstanceOf[AggregateMapReduce] shouldEqual true
    }

    child2.params.startSecs shouldEqual 900
    child2.params.endSecs shouldEqual 1020
    child2.params.stepSecs shouldEqual 60
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
    val promQlQueryParams = PromQlQueryParams("dummy query", from, step, to)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(2000000, 2500000), true))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryConfig)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual true

    val child = execPlan.asInstanceOf[PromQlRemoteExec]

    child.params.startSecs shouldEqual 900
    child.params.endSecs shouldEqual 1980
    child.params.stepSecs shouldEqual 60
    child.params.processFailure shouldEqual false
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
    val promQlQueryParams = PromQlQueryParams("dummy query", from, step, to)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(2000000, 2500000), true))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryConfig)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[LocalPartitionReduceAggregateExec] shouldEqual (true)

    val reduceAggregateExec = execPlan.asInstanceOf[LocalPartitionReduceAggregateExec]

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
    val promQlQueryParams = PromQlQueryParams("dummy query", from, step, to)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(2000000, 2500000), false))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryConfig)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual true

    val child = execPlan.asInstanceOf[PromQlRemoteExec]
    child.params.startSecs shouldEqual from
    child.params.endSecs shouldEqual to
    child.params.stepSecs shouldEqual step
    child.params.processFailure shouldEqual false
  }

  it("should work with offset") {
    val t = TimeStepParams(700, 1000, 10000)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef, TimeRange(100000, 200000), false))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryConfig)

    val lp1 = Parser.queryRangeToLogicalPlan("http_requests_total{job = \"app\"}", t)
    val execPlan1 = engine.materialize(lp1, QueryContext(origQueryParams = promQlQueryParams))
    execPlan1.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual (true) // No routing as failure is before query start time

    val lp2 = Parser.queryRangeToLogicalPlan("http_requests_total{job = \"app\"} offset 10m", t)
    val execPlan2 = engine.materialize(lp2, QueryContext(origQueryParams = promQlQueryParams))
    // Because of offset starts time would be (700 - 600) = 100 seconds where there is failure
    // So PromQlExec is generated instead of local LocalPartitionDistConcatExec. PromQlExec will have original query and start time
    // Start time with offset will be calculated by buddy pod
    execPlan2.isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    execPlan2.asInstanceOf[PromQlRemoteExec].params.startSecs shouldEqual(700)
    execPlan2.asInstanceOf[PromQlRemoteExec].params.endSecs shouldEqual(10000)
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

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryConfig)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[MetadataRemoteExec] shouldEqual (true)
    execPlan.asInstanceOf[MetadataRemoteExec].params.startSecs shouldEqual (from)
    execPlan.asInstanceOf[MetadataRemoteExec].params.endSecs shouldEqual (to)

  }
}
