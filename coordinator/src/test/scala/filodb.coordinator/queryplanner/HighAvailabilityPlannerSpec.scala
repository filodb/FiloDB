package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}
import filodb.coordinator.ShardMapper
import filodb.core.{DatasetRef, MetricsTestData}
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter, PromQlQueryParams, QueryContext}
import filodb.core.store.TimeRangeChunkScan
import filodb.query._
import filodb.query.exec._

class HighAvailabilityPlannerSpec extends FunSpec with Matchers {

  private implicit val system = ActorSystem()
  private val node = TestProbe().ref

  private val mapper = new ShardMapper(32)
  for { i <- 0 until 32 } mapper.registerNode(Seq(i), node)

  private def mapperRef = mapper

  private val dataset = MetricsTestData.timeseriesDataset
  private val dsRef = dataset.ref
  private val schemas = Schemas(dataset.schema)

  private val queryEngineConfigString = "routing {\n  buddy {\n    http {\n      timeout = 10.seconds\n    }\n  }\n}"

  private val queryEngineConfig = ConfigFactory.parseString(queryEngineConfigString)
  /*
  This is the PromQL

  sum(rate(http_request_duration_seconds_bucket{job="myService",le="0.3"}[5m])) by (job)
   /
  sum(rate(http_request_duration_seconds_count{job="myService"}[5m])) by (job)
  */
  private val f1 = Seq(ColumnFilter("__name__", Filter.Equals("http_request_duration_seconds_bucket")),
    ColumnFilter("job", Filter.Equals("myService")),
    ColumnFilter("le", Filter.Equals("0.3")))

  private val promQlQueryParams = PromQlQueryParams("sum(heap_usage)", 100, 1, 1000, None)

  val localPlanner = new SingleClusterPlanner(dsRef, schemas, mapperRef)

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

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[ReduceAggregateExec] shouldEqual (true)

    // Should ignore smaller local failure which is from 1500 - 4000 and generate local exec plan
    val reduceAggregateExec = execPlan.asInstanceOf[ReduceAggregateExec]

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

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryEngineConfig)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlExec] shouldEqual (true)
    execPlan.asInstanceOf[PromQlExec].params.startSecs shouldEqual(from/1000)
    execPlan.asInstanceOf[PromQlExec].params.endSecs shouldEqual(to/1000)
  }

  it("should generate RemotExecPlan with RawSeries time according to lookBack") {
    val to = 2000000
    val from = 1000000
    val intervalSelector = IntervalSelector(from - 50000 , to) // Lookback of 50000
    val raw = RawSeries(rangeSelector = intervalSelector, filters = f1, columns = Seq("value"))
    val windowed = PeriodicSeriesWithWindowing(raw, from, 100, to, 5000, RangeFunctionId.Rate)
    val summed = Aggregate(AggregationOperator.Sum, windowed, Nil, Seq("job"))
    val promQlQueryParams = PromQlQueryParams("", from/1000, 1, to/1000, None)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(1060000, 1090000), true))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryEngineConfig)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[StitchRvsExec] shouldEqual (true)

    val stitchRvsExec = execPlan.asInstanceOf[StitchRvsExec]
    stitchRvsExec.children.size shouldEqual (2)
    stitchRvsExec.children(0).isInstanceOf[ReduceAggregateExec] shouldEqual (true)
    stitchRvsExec.children(1).isInstanceOf[PromQlExec] shouldEqual (true)

    val child1 = stitchRvsExec.children(0).asInstanceOf[ReduceAggregateExec]
    val child2 = stitchRvsExec.children(1).asInstanceOf[PromQlExec]

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

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryEngineConfig)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlExec] shouldEqual (true)
    execPlan.asInstanceOf[PromQlExec].params.startSecs shouldEqual(from/1000)
    execPlan.asInstanceOf[PromQlExec].params.endSecs shouldEqual(to/1000)
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

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryEngineConfig)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlExec] shouldEqual (true)
    execPlan.asInstanceOf[PromQlExec].params.startSecs shouldEqual(from/1000)
    execPlan.asInstanceOf[PromQlExec].params.endSecs shouldEqual(to/1000)
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

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryEngineConfig)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlExec] shouldEqual (true)
    execPlan.asInstanceOf[PromQlExec].params.startSecs shouldEqual(from/1000)
    execPlan.asInstanceOf[PromQlExec].params.endSecs shouldEqual(to/1000)
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
    val promQlQueryParams = PromQlQueryParams("dummy query", from, step, to, None)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(1060000, 1090000), true))
      }
    }
    //900K to 1020K and 1020+60 k to 2000K

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryEngineConfig)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[StitchRvsExec] shouldEqual (true)

    val stitchRvsExec = execPlan.asInstanceOf[StitchRvsExec]
    stitchRvsExec.children.size shouldEqual 2
    stitchRvsExec.children(0).isInstanceOf[ReduceAggregateExec] shouldEqual (true)
    stitchRvsExec.children(1).isInstanceOf[PromQlExec] shouldEqual (true)

    val child1 = stitchRvsExec.children(0).asInstanceOf[ReduceAggregateExec]
    val child2 = stitchRvsExec.children(1).asInstanceOf[PromQlExec]

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
    val promQlQueryParams = PromQlQueryParams("dummy query", from, step, to, None)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(2000000, 2500000), true))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryEngineConfig)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlExec] shouldEqual true

    val child = execPlan.asInstanceOf[PromQlExec]

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
    val promQlQueryParams = PromQlQueryParams("dummy query", from, step, to, None)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(2000000, 2500000), true))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryEngineConfig)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[ReduceAggregateExec] shouldEqual (true)

    val reduceAggregateExec = execPlan.asInstanceOf[ReduceAggregateExec]

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
    val promQlQueryParams = PromQlQueryParams("dummy query", from, step, to, None)

    val failureProvider = new FailureProvider {
      override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
        Seq(FailureTimeRange("local", datasetRef,
          TimeRange(910000, 1030000), false), FailureTimeRange("remote", datasetRef,
          TimeRange(2000000, 2500000), false))
      }
    }

    val engine = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryEngineConfig)

    val execPlan = engine.materialize(summed, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.isInstanceOf[PromQlExec] shouldEqual true

    val child = execPlan.asInstanceOf[PromQlExec]
    child.params.startSecs shouldEqual from
    child.params.endSecs shouldEqual to
    child.params.stepSecs shouldEqual step
    child.params.processFailure shouldEqual false
  }
}
