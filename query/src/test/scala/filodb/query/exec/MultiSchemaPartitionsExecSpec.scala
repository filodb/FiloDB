package filodb.query.exec

import com.typesafe.config.ConfigFactory
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SchemaMismatch, SomeData, TimeSeriesMemStore}
import filodb.core.metadata.Column.ColumnType.{DoubleColumn, HistogramColumn, LongColumn, TimestampColumn}
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.core.store.{AllChunkScan, ChunkSource, InMemoryMetaStore, NullColumnStore, TimeRangeChunkScan}
import filodb.core.{DatasetRef, GlobalConfig, QueryTimeoutException, TestData, Types}
import filodb.memory.MemFactory
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.query._
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._
import monix.reactive.Observable

object MultiSchemaPartitionsExecSpec {
  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlanWithClientParams, source: ChunkSource)
                         (implicit sched: Scheduler): Task[QueryResponse] = ???

    override def clusterName: String = ???

    override def isLocalCall: Boolean = false

    override def dispatchStreaming(plan: ExecPlanWithClientParams,
                                   source: ChunkSource)(implicit sched: Scheduler): Observable[StreamQueryResponse] = ???
  }

  val dsRef = DatasetRef("raw-metrics")
  val dummyPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher, dsRef, 0, Nil, AllChunkScan, "_metric_")

  val builder = new RecordBuilder(MemFactory.onHeapFactory)
}

class MultiSchemaPartitionsExecSpec extends AnyFunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  import MultiSchemaPartitionsExecSpec._
  import Schemas.promCounter
  import ZeroCopyUTF8String._
  import filodb.core.{MachineMetricsData => MMD}

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)
  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))

  val metric = "http_req_total"
  val partKeyLabelValues = Map("job" -> "myCoolService", "instance" -> "someHost:8787", "host" -> "host-1")
  val partKeyKVWithMetric = partKeyLabelValues ++ Map("_metric_" -> metric)
  val partTagsUTF8 = partKeyLabelValues.map { case (k, v) => (k.utf8, v.utf8) }
  val now = System.currentTimeMillis()
  val numRawSamples = 1000
  val reportingInterval = 10000
  val tuples = (numRawSamples until 0).by(-1).map { n =>
    (now - n * reportingInterval, n.toDouble)
  }
  val schemas = Schemas(promCounter.partition,
                        Map(promCounter.name -> promCounter,
                            "histogram" -> MMD.histDataset.schema,
                            Schemas.dsGauge.name -> Schemas.dsGauge))

  // NOTE: due to max-chunk-size in storeConf = 100, this will make (numRawSamples / 100) chunks
  // Be sure to reset the builder; it is in an Object so static and shared amongst tests
  builder.reset()
  tuples.map { t => SeqRowReader(Seq(t._1, t._2, metric.utf8, partTagsUTF8)) }
        .foreach(builder.addFromReader(_, promCounter))
  val container = builder.allContainers.head

  val mmdBuilder = new RecordBuilder(MemFactory.onHeapFactory)
  val mmdTuples = MMD.linearMultiSeries().take(100)
  val mmdSomeData = MMD.records(MMD.dataset1, mmdTuples)
  val histData = MMD.linearHistSeries().take(100)
  val histMaxMinData = MMD.histMaxMin(histData)

  val histDataDisabledWS = MMD.linearHistSeries(ws = GlobalConfig.workspacesDisabledForMaxMin.get.head).take(100)
  val histMaxMinDataDisabledWS = MMD.histMaxMin(histDataDisabledWS)

  implicit val execTimeout = 5.seconds

  override def beforeAll(): Unit = {
    memStore.setup(dsRef, schemas, 0, TestData.storeConf, 2)
    memStore.ingest(dsRef, 0, SomeData(container, 0))
    memStore.ingest(dsRef, 0, MMD.records(MMD.histDataset, histData))

    // set up shard, but do not ingest data to simulate an empty shard
    memStore.setup(dsRef, schemas, 1, TestData.storeConf, 2)

    memStore.setup(MMD.dataset1.ref, Schemas(MMD.schema1), 0, TestData.storeConf, 1)
    memStore.ingest(MMD.dataset1.ref, 0, mmdSomeData)
    memStore.setup(MMD.histMaxMinDS.ref, Schemas(MMD.histMaxMinDS.schema), 0, TestData.storeConf, 1)
    memStore.ingest(MMD.histMaxMinDS.ref, 0, MMD.records(MMD.histMaxMinDS, histMaxMinData))
    memStore.ingest(MMD.histMaxMinDS.ref, 0, MMD.records(MMD.histMaxMinDS, histMaxMinDataDisabledWS))

    memStore.refreshIndexForTesting(dsRef)
    memStore.refreshIndexForTesting(MMD.dataset1.ref)
    memStore.refreshIndexForTesting(MMD.histMaxMinDS.ref)
  }

  override def afterAll(): Unit = {
    memStore.shutdown()
  }

  it ("should read raw samples from Memstore using AllChunksSelector") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)),
                       ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher,
      dsRef, 0, filters, AllChunkScan, "_metric_")

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, DoubleColumn)
    result.result.size shouldEqual 1
    val partKeyRead = result.result(0).key.labelValues.map(lv => (lv._1.asNewString, lv._2.asNewString))
    partKeyRead shouldEqual partKeyKVWithMetric
    val dataRead = result.result(0).rows.map(r=>(r.getLong(0), r.getDouble(1))).toList
    dataRead shouldEqual tuples
  }

  it ("should read raw samples from Memstore using IntervalSelector") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)),
      ColumnFilter("instance", Filter.NotEquals("SomeJob".utf8)))
    // read from an interval of 100000ms, resulting in 11 samples
    val startTime = now - numRawSamples * reportingInterval
    val endTime   = now - (numRawSamples-10) * reportingInterval

    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher,
                                             dsRef, 0, filters, TimeRangeChunkScan(startTime, endTime), "_metric_")

    querySession.queryStats.clear() // so this can be run as a standalone test
    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 1
    val dataRead = result.result(0).rows.map(r=>(r.getLong(0), r.getDouble(1))).toList
    dataRead shouldEqual tuples.take(11)
    val partKeyRead = result.result(0).key.labelValues.map(lv => (lv._1.asNewString, lv._2.asNewString))
    partKeyRead shouldEqual partKeyKVWithMetric
    querySession.queryStats.getResultBytesCounter().get() shouldEqual 297
    querySession.queryStats.getCpuNanosCounter().get() > 0 shouldEqual true
    querySession.queryStats.getDataBytesScannedCounter().get() shouldEqual 48
    querySession.queryStats.getTimeSeriesScannedCounter().get() shouldEqual 1
  }

  it("should get empty schema if query returns no results") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("_metric_", Filter.Equals("not_a_metric!".utf8)),
                       ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher,
      dsRef, 0, filters, AllChunkScan, "_metric_")

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.isEmpty shouldEqual true
    result.result.size shouldEqual 0
  }

  it ("should read raw Long samples from Memstore using IntervalSelector") {
    import ZeroCopyUTF8String._
    val filters = Seq(ColumnFilter("series", Filter.Equals("Series 1".utf8)))

    // read from an interval of 100000ms, resulting in 11 samples, count column
    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher, MMD.dataset1.ref, 0,
                                             filters, TimeRangeChunkScan(100000L, 150000L),"_metric_", colName = Some("count"))

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, LongColumn)
    result.result.size shouldEqual 1
    val dataRead = result.result(0).rows.map(r=>(r.getLong(0), r.getLong(1))).toList
    dataRead shouldEqual mmdTuples.filter(_(5) == "Series 1").map(r => (r(0), r(4))).take(5)
  }

  it ("should read raw Histogram samples from Memstore using IntervalSelector") {
    import ZeroCopyUTF8String._

    val filters = Seq(ColumnFilter("dc", Filter.Equals("0".utf8)),
                      ColumnFilter("_metric_", Filter.Equals("request-latency".utf8)))
    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher, dsRef, 0,
                                             filters, TimeRangeChunkScan(100000L, 150000L), "_metric_", colName=Some("h"))

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, HistogramColumn)
    result.result.size shouldEqual 1
    val resultIt = result.result(0).rows.map(r=>(r.getLong(0), r.getHistogram(1)))
    val orig = histData.filter(_(5).asInstanceOf[Types.UTF8Map]("dc".utf8) == "0".utf8).map(r => (r(0), r(3))).take(5)
    resultIt.zip(orig.toIterator).foreach { case (res, origData) => res shouldEqual origData }
  }

  it ("should read periodic samples from Memstore") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher,
                                             dsRef, 0, filters, AllChunkScan, "_metric_")
    val start = now - numRawSamples * reportingInterval - 100 // reduce by 100 to not coincide with reporting intervals
    val step = 20000
    val end = now - (numRawSamples-100) * reportingInterval
    execPlan.addRangeVectorTransformer(new PeriodicSamplesMapper(start, step, end, None, None))

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, DoubleColumn)
    // PSM should rename the double column to value always
    result.resultSchema.columns.map(_.name) shouldEqual Seq("timestamp", "value")
    result.result.size shouldEqual 1
    val partKeyRead = result.result(0).key.labelValues.map(lv => (lv._1.asNewString, lv._2.asNewString))
    partKeyRead shouldEqual partKeyKVWithMetric
    val dataRead = result.result(0).rows.map(r=>(r.getLong(0), r.getDouble(1))).toList
    dataRead.map(_._1) shouldEqual (start to end).by(step)

    val validationMap = new java.util.TreeMap[Long, Double]()
    tuples.foreach(s => validationMap.put(s._1, s._2))
    dataRead.foreach{ s =>
      val expected = validationMap.floorEntry(s._1)
      val observed = s._2
      if (expected == null) {
        observed.isNaN shouldEqual true
      } else {
        observed shouldEqual expected.getValue
      }
    }
  }

  it ("should read periodic samples from Memstore with instant query where step == 0") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher,
      dsRef, 0, filters, AllChunkScan, "_metric_")
    val start = now - (numRawSamples-100) * reportingInterval + 1
    val step = 0
    val end = now - (numRawSamples-100) * reportingInterval + 1
    execPlan.addRangeVectorTransformer(new PeriodicSamplesMapper(start, step, end, Some(reportingInterval * 3),
      Some(InternalRangeFunction.SumOverTime)))

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, DoubleColumn)
    // PSM should rename the double column to value always
    result.resultSchema.columns.map(_.name) shouldEqual Seq("timestamp", "value")
    result.result.size shouldEqual 1
    val partKeyRead = result.result(0).key.labelValues.map(lv => (lv._1.asNewString, lv._2.asNewString))
    partKeyRead shouldEqual partKeyKVWithMetric
    val dataRead = result.result(0).rows.map(r=>(r.getLong(0), r.getDouble(1))).toList
    dataRead.map(_._1) shouldEqual Seq(start)
    dataRead.map(_._2) shouldEqual Seq(2703.0)
  }


  it("should read periodic samples from Long column") {
    import ZeroCopyUTF8String._
    val filters = Seq(ColumnFilter("series", Filter.Equals("Series 1".utf8)))
    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher, MMD.dataset1.ref, 0,
                                             filters, AllChunkScan, "_metric_", colName = Some("count"))

    // Raw data like 101000, 111000, ....
    val start = 105000L
    val step = 20000L
    val end = 185000L
    execPlan.addRangeVectorTransformer(new PeriodicSamplesMapper(start, step, end, None, None))

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, DoubleColumn)
    result.result.size shouldEqual 1
    val dataRead = result.result(0).rows.map(r=>(r.getLong(0), r.getDouble(1))).toList
    dataRead.map(_._1) shouldEqual (start to end by step)
    dataRead.map(_._2) shouldEqual (86 to 166).by(20)
  }

  it("should read periodic Histogram samples from Memstore") {
    import ZeroCopyUTF8String._
    val filters = Seq(ColumnFilter("dc", Filter.Equals("0".utf8)),
                      ColumnFilter("_metric_", Filter.Equals("request-latency".utf8)))
    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher, dsRef, 0,
                                             filters, AllChunkScan,"_metric_")   // should default to h column

    val start = 105000L
    val step = 20000L
    val end = 185000L
    execPlan.addRangeVectorTransformer(new PeriodicSamplesMapper(start, step, end, None, None))

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, HistogramColumn)
    result.result.size shouldEqual 1
    val resultIt = result.result(0).rows.map(r=>(r.getLong(0), r.getHistogram(1)))
    val orig = histData.filter(_(5).asInstanceOf[Types.UTF8Map]("dc".utf8) == "0".utf8)
                       .grouped(2).map(_.head)   // Skip every other one, starting with second, since step=2x pace
                       .zip((start to end by step).toIterator).map { case (r, t) => (t, r(3)) }
    resultIt.zip(orig.toIterator).foreach { case (res, origData) => res shouldEqual origData }
  }

  it("should extract bucket from Histogram samples then calculate rate") {
    import ZeroCopyUTF8String._
    val filters = Seq(ColumnFilter("dc", Filter.Equals("0".utf8)),
                      ColumnFilter("_metric_", Filter.Equals("request-latency".utf8)))
    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher, dsRef, 0,
                                             filters, AllChunkScan,"_metric_")   // should default to h column

    val start = 105000L
    val step = 20000L
    val end = 185000L
    execPlan.addRangeVectorTransformer(new InstantVectorFunctionMapper(
                                        InstantFunctionId.HistogramBucket,
                                        Seq(StaticFuncArgs(16.0, RangeParams(0,0,0)))))
    execPlan.addRangeVectorTransformer(new PeriodicSamplesMapper(start, step, end, Some(300 * 1000),  // [5m]
                                         Some(InternalRangeFunction.Rate), rawSource = false))

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, DoubleColumn)
    result.result.size shouldEqual 1
    val resultIt = result.result(0).rows.map(r=>(r.getLong(0), r.getDouble(1)))

    val expected = (start to end by step).zip(Seq(Double.NaN, 0.049167, 0.078333, 0.115278, 0.145))
    resultIt.zip(expected.toIterator).foreach { case (res, exp) =>
      res._1 shouldEqual exp._1
      if (!java.lang.Double.isNaN(exp._2)) res._2 shouldEqual exp._2 +- 0.00001
    }
  }

  it("should return SchemaMismatch QueryError if multiple schemas found in query") {
    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher,
      dsRef, 0, Nil, AllChunkScan, "_metric_")

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryError]
    result.t.getClass shouldEqual classOf[SchemaMismatch]
  }

  it("should select only specified schema if schema option given even if multiple schemas match") {
    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher,
      dsRef, 0, Nil, AllChunkScan, "_metric_", schema = Some("prom-counter"))

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, DoubleColumn)
    result.result.size shouldEqual 1
    val dataRead = result.result(0).rows.map(r=>(r.getLong(0), r.getDouble(1))).toList
    dataRead shouldEqual tuples
  }

  // A lower-level (below coordinator) end to end histogram with max ingestion and querying test
  it("should sum Histogram records with max correctly") {
    val filters = Seq(ColumnFilter("dc", Filter.Equals("0".utf8)), ColumnFilter("_ws_", Filter.Equals("demo".utf8)))
    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher, MMD.histMaxMinDS.ref, 0,
                                             filters, AllChunkScan, "_metric_", colName = Some("h"))

    val start = 105000L
    val step = 20000L
    val end = 185000L
    execPlan.addRangeVectorTransformer(new PeriodicSamplesMapper(start, step, end, Some(300 * 1000),  // [5m]
                                         Some(InternalRangeFunction.SumOverTime)))
    execPlan.addRangeVectorTransformer(AggregateMapReduce(AggregationOperator.Sum, Nil))

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    info(execPlan.printTree())
    // Check that the "inner" SelectRawPartitionsExec has the right schema/columnIDs
    execPlan.finalPlan shouldBe a[SelectRawPartitionsExec]
    execPlan.finalPlan.asInstanceOf[SelectRawPartitionsExec].colIds shouldEqual Seq(0, 3, 5, 4)
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual
      Seq(TimestampColumn, HistogramColumn, DoubleColumn, DoubleColumn)
    result.result.size shouldEqual 1
    val resultIt = result.result(0).rows.map(r=>(r.getLong(0), r.getHistogram(1),
      r.getDouble(2), r.getDouble(3)))

    // For now, just validate that we can read "reasonable" results, ie max should be >= value at head of window
    // Rely on AggrOverTimeFunctionsSpec to actually validate aggregation results
    val orig = histMaxMinData.filter(_(7).asInstanceOf[Types.UTF8Map]("dc".utf8) == "0".utf8)
                       .grouped(2).map(_.head)   // Skip every other one, starting with second, since step=2x pace
                       .zip((start to end by step).toIterator).map { case (r, t) => (t, r(3), r(5), r(4)) }
    resultIt.zip(orig.toIterator).foreach { case (res, origData) =>
      res._3.isNaN shouldEqual false
      res._3 should be >= origData._3.asInstanceOf[Double]
    }

    // Add the histogram_max_quantile function to ExecPlan and make sure results are OK
    execPlan.addRangeVectorTransformer(
      exec.InstantVectorFunctionMapper(InstantFunctionId.HistogramMaxQuantile, Seq(StaticFuncArgs(0.99, RangeParams(0,0,0)))))
    val resp2 = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result2 = resp2.asInstanceOf[QueryResult]
    result2.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, DoubleColumn)
    result2.result.size shouldEqual 1
    val resultIt2 = result2.result(0).rows.map(r=>(r.getLong(0), r.getDouble(1))).toBuffer

    resultIt2.foreach { case (t, v) =>
      v.isNaN shouldEqual false
    }
  }

  it("should extract Histogram with max using Last/None function correctly") {
    val filters = Seq(ColumnFilter("dc", Filter.Equals("0".utf8)), ColumnFilter("_ws_", Filter.Equals("demo".utf8)))
    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher, MMD.histMaxMinDS.ref, 0,
                                             filters, AllChunkScan, "_metric_")   // should default to h column

    val start = 105000L
    val step = 20000L
    val end = 185000L
    execPlan.addRangeVectorTransformer(new PeriodicSamplesMapper(start, step, end, None, None))

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual
      Seq(TimestampColumn, HistogramColumn, DoubleColumn, DoubleColumn)
    result.result.size shouldEqual 1
    val resultIt = result.result(0).rows.map(r=>(r.getLong(0), r.getHistogram(1),
      r.getDouble(2), r.getDouble(3)))

    // For now, just validate that we can read "reasonable" results, ie max should be >= value at head of window
    // Rely on AggrOverTimeFunctionsSpec to actually validate aggregation results
    val orig = histMaxMinData.filter(_(7).asInstanceOf[Types.UTF8Map]("dc".utf8) == "0".utf8)
                       .grouped(2).map(_.head)   // Skip every other one, starting with second, since step=2x pace
                       .zip((start to end by step).toIterator).map { case (r, t) => (t, r(3), r(5), r(4)) }
    resultIt.zip(orig.toIterator).foreach { case (res, origData) =>
      res._3.isNaN shouldEqual false
      res._3 should be >= origData._3.asInstanceOf[Double]
      res._4.isNaN shouldEqual false
      res._4 should be <= origData._3.asInstanceOf[Double]
    }
  }

  it("should not pickup max min columns if ws is disabled") {
    val filters = Seq(ColumnFilter("dc", Filter.Equals("0".utf8)), ColumnFilter("_ws_",
      Filter.Equals(GlobalConfig.workspacesDisabledForMaxMin.get.head.utf8)))
    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher, MMD.histMaxMinDS.ref, 0,
      filters, AllChunkScan, "_metric_", colName = Some("h"))

    val start = 105000L
    val step = 20000L
    val end = 185000L
    execPlan.addRangeVectorTransformer(new PeriodicSamplesMapper(start, step, end, Some(300 * 1000), // [5m]
      Some(InternalRangeFunction.SumOverTime)))
    execPlan.addRangeVectorTransformer(AggregateMapReduce(AggregationOperator.Sum, Nil))
    execPlan.execute(memStore, querySession).runToFuture.futureValue
    info(execPlan.printTree())
    execPlan.finalPlan shouldBe a[SelectRawPartitionsExec]
    // if the ws is disabled for max min calculation, then the max min columns should not be picked up
    execPlan.finalPlan.colIds shouldEqual Seq(0, 3)
  }

  it("test isMaxMinEnabledForWorkspace correctly returns expected values") {
    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher, MMD.histMaxMinDS.ref, 0,
      Seq(), AllChunkScan, "_metric_", colName = Some("h"))
    execPlan.isMaxMinColumnsEnabled(Some("demo")) shouldEqual true
    execPlan.isMaxMinColumnsEnabled(Some(GlobalConfig.workspacesDisabledForMaxMin.get.head)) shouldEqual false
    execPlan.isMaxMinColumnsEnabled(None) shouldEqual false
  }

  it("should return chunk metadata from MemStore") {
    val filters = Seq (ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)),
                       ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    // TODO: SelectChunkInfos should not require a raw schema
    // shard 0 contains data
    val execPlan = SelectChunkInfosExec(QueryContext(), dummyDispatcher,
      dsRef, 0, filters, AllChunkScan, colName = Some("timestamp"))
    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    info(s"resp = $resp")
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 1
    val partKeyRead = result.result(0).key.labelValues.map(lv => (lv._1.asNewString, lv._2.asNewString))
    partKeyRead shouldEqual partKeyKVWithMetric

    // Extract out the numRows, startTime, endTIme and verify
    val infosRead = result.result(0).rows.map { r => (r.getInt(1), r.getLong(2), r.getLong(3), r.getString(5)) }.toList
    infosRead.foreach { i => info(s"  Infos read => $i") }
    val expectedNumChunks = 15
    // One would expect numChunks = numRawSamples / TestData.storeConf.maxChunksSize
    // but we also break chunks when time duration in chunk reaches max.
    infosRead should have length expectedNumChunks
    infosRead.map(_._1) shouldEqual (Seq.fill(expectedNumChunks-1)(67) ++ Seq(62))
    // Last chunk is the writeBuffer which is not encoded
    infosRead.map(_._4).dropRight(1).foreach(_ should include ("DeltaDeltaConst"))

    val startTimes = tuples.grouped(67).map(_.head._1).toBuffer
    infosRead.map(_._2) shouldEqual startTimes
  }

  it("should return empty chunk metadata from MemStore when shard has no data") {
    val filters = Seq (ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    // TODO: SelectChunkInfos should not require a raw schema
    // shard 1 does not contain data
    val execPlan = SelectChunkInfosExec(QueryContext(), dummyDispatcher,
      dsRef, 1, filters, AllChunkScan, colName = Some("timestamp"))
    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    info(s"resp = $resp")
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 0
  }

  it ("should fail with exception BadQueryException") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    // Query returns n ("numRawSamples") samples - Applying Limit (n-1) to fail the query execution
    // with ResponseTooLargeException
    val execPlan = MultiSchemaPartitionsExec(
      QueryContext(plannerParams = PlannerParams(enforcedLimits = PerQueryLimits(execPlanSamples = 999))),
      dummyDispatcher, dsRef, 0, filters, AllChunkScan, "_metric_")
    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryError]
    result.t.getClass shouldEqual classOf[QueryLimitException]
  }

  it("should throw QueryTimeoutException when query processing time is greater than timeout") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("_metric_", Filter.Equals("not_a_metric!".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val execPlan = MultiSchemaPartitionsExec(QueryContext(submitTime = System.currentTimeMillis() - 180000),
      dummyDispatcher, dsRef, 0, filters, AllChunkScan, "_metric_")

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryError]
    result.t.getClass shouldEqual classOf[QueryTimeoutException]
    result.t.getMessage shouldEqual "Query timeout in step1-MultiSchemaPartitionsExec after 180 seconds"
  }

  it ("""should not return range vectors with !="" where column is not present""") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)),
      ColumnFilter("dc", Filter.NotEquals("".utf8)))
    val startTime = now - numRawSamples * reportingInterval
    val endTime   = now - (numRawSamples-10) * reportingInterval

    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher,
      dsRef, 0, filters, TimeRangeChunkScan(startTime, endTime), "_metric_")

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 0
  }

  it ("""should return range vectors with != condition on a label that does not exist and value is non empty""") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)),
      ColumnFilter("host", Filter.NotEquals("host".utf8)),
      ColumnFilter("dc", Filter.NotEquals("us-west".utf8)))  // This label does not exist
    val startTime = now - numRawSamples * reportingInterval
    val endTime   = now - (numRawSamples-10) * reportingInterval

    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher,
      dsRef, 0, filters, TimeRangeChunkScan(startTime, endTime), "_metric_")

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 1
  }

  it ("""should return range vectors when it satisfies NotEquals condition""") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)),
      ColumnFilter("host", Filter.NotEquals("host".utf8)))
    val startTime = now - numRawSamples * reportingInterval
    val endTime   = now - (numRawSamples-10) * reportingInterval

    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher,
      dsRef, 0, filters, TimeRangeChunkScan(startTime, endTime), "_metric_")

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 1
  }

  it ("should return rangevector for prom query to get sum timeseries") {
    import ZeroCopyUTF8String._

    val filters = Seq(ColumnFilter("dc", Filter.Equals("0".utf8)),
      ColumnFilter("_metric_", Filter.Equals("request-latency_sum".utf8)))
    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher, dsRef, 0,
      filters, TimeRangeChunkScan(100000L, 150000L), "_metric_")

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 1
  }

  it ("should return rangevector for prom query to get count timeseries") {
    import ZeroCopyUTF8String._

    val filters = Seq(ColumnFilter("dc", Filter.Equals("0".utf8)),
      ColumnFilter("_metric_", Filter.Equals("request-latency_count".utf8)))
    val execPlan = MultiSchemaPartitionsExec(QueryContext(), dummyDispatcher, dsRef, 0,
      filters, TimeRangeChunkScan(100000L, 150000L), "_metric_")

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.result.head.key.labelValues.get(ZeroCopyUTF8String("metric")).get equals("request-latency")
    result.result.size shouldEqual 1
  }
}

