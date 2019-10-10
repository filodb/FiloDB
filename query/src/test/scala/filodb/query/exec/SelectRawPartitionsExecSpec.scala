package filodb.query.exec

import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import filodb.core.{TestData, Types}
import filodb.core.MetricsTestData._
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SomeData, TimeSeriesMemStore}
import filodb.core.metadata.Schemas
import filodb.core.metadata.Column.ColumnType.{DoubleColumn, HistogramColumn, TimestampColumn}
import filodb.core.query._
import filodb.core.store.{AllChunkScan, InMemoryMetaStore, NullColumnStore, TimeRangeChunkScan}
import filodb.memory.MemFactory
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.query._
import monix.execution.Scheduler

object SelectRawPartitionsExecSpec {
  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlan)
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Task[QueryResponse] = ???
  }

  val dataset = timeseriesDataset

  val dummyPlan = SelectRawPartitionsExec("someQueryId", System.currentTimeMillis, 100, dummyDispatcher,
                    timeseriesDataset.ref, 0, timeseriesSchema, Nil, AllChunkScan, Seq(0, 1))
}

class SelectRawPartitionsExecSpec extends FunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  import ZeroCopyUTF8String._
  import filodb.core.{MachineMetricsData => MMD}
  import SelectRawPartitionsExecSpec._

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))

  val partKeyLabelValues = Map("__name__"->"http_req_total", "job"->"myCoolService", "instance"->"someHost:8787")
  val partTagsUTF8 = partKeyLabelValues.map { case (k, v) => (k.utf8, v.utf8) }
  val now = System.currentTimeMillis()
  val numRawSamples = 1000
  val reportingInterval = 10000
  val tuples = (numRawSamples until 0).by(-1).map { n =>
    (now - n * reportingInterval, n.toDouble)
  }

  // NOTE: due to max-chunk-size in storeConf = 100, this will make (numRawSamples / 100) chunks
  // Be sure to reset the builder; it is in an Object so static and shared amongst tests
  builder.reset()
  tuples.map { t => SeqRowReader(Seq(t._1, t._2, partTagsUTF8)) }.foreach(builder.addFromReader(_, timeseriesSchema))
  val container = builder.allContainers.head

  val mmdBuilder = new RecordBuilder(MemFactory.onHeapFactory)
  val mmdTuples = MMD.linearMultiSeries().take(100)
  val mmdSomeData = MMD.records(MMD.dataset1, mmdTuples)
  val histData = MMD.linearHistSeries().take(100)
  val histMaxData = MMD.histMax(histData)

  implicit val execTimeout = 5.seconds

  override def beforeAll(): Unit = {
    memStore.setup(timeseriesDataset.ref, Schemas(timeseriesSchema), 0, TestData.storeConf)
    memStore.ingest(timeseriesDataset.ref, 0, SomeData(container, 0))
    memStore.setup(MMD.dataset1.ref, Schemas(MMD.schema1), 0, TestData.storeConf)
    memStore.ingest(MMD.dataset1.ref, 0, mmdSomeData)
    memStore.setup(MMD.histDataset.ref, Schemas(MMD.histDataset.schema), 0, TestData.storeConf)
    memStore.ingest(MMD.histDataset.ref, 0, MMD.records(MMD.histDataset, histData))
    memStore.setup(MMD.histMaxDS.ref, Schemas(MMD.histMaxDS.schema), 0, TestData.storeConf)
    memStore.ingest(MMD.histMaxDS.ref, 0, MMD.records(MMD.histMaxDS, histMaxData))
    memStore.refreshIndexForTesting(timeseriesDataset.ref)
    memStore.refreshIndexForTesting(MMD.dataset1.ref)
    memStore.refreshIndexForTesting(MMD.histDataset.ref)
    memStore.refreshIndexForTesting(MMD.histMaxDS.ref)
  }

  override def afterAll(): Unit = {
    memStore.shutdown()
  }

  it ("should read raw samples from Memstore using AllChunksSelector") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
                       ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val execPlan = SelectRawPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher,
      timeseriesDataset.ref, 0, timeseriesSchema, filters, AllChunkScan, Nil)

    val resp = execPlan.execute(memStore, queryConfig).runAsync.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 1
    val partKeyRead = result.result(0).key.labelValues.map(lv => (lv._1.asNewString, lv._2.asNewString))
    partKeyRead shouldEqual partKeyLabelValues
    val dataRead = result.result(0).rows.map(r=>(r.getLong(0), r.getDouble(1))).toList
    dataRead shouldEqual tuples
  }

  it ("should read raw samples from Memstore using IntervalSelector") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    // read from an interval of 100000ms, resulting in 11 samples
    val startTime = now - numRawSamples * reportingInterval
    val endTime   = now - (numRawSamples-10) * reportingInterval

    val execPlan = SelectRawPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher,
                                           timeseriesDataset.ref, 0, timeseriesSchema,
                                           filters, TimeRangeChunkScan(startTime, endTime), Nil)

    val resp = execPlan.execute(memStore, queryConfig).runAsync.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 1
    val dataRead = result.result(0).rows.map(r=>(r.getLong(0), r.getDouble(1))).toList
    dataRead shouldEqual tuples.take(11)
    val partKeyRead = result.result(0).key.labelValues.map(lv => (lv._1.asNewString, lv._2.asNewString))
    partKeyRead shouldEqual partKeyLabelValues
  }

  it ("should read raw Long samples from Memstore using IntervalSelector") {
    import ZeroCopyUTF8String._
    val filters = Seq(ColumnFilter("series", Filter.Equals("Series 1".utf8)))

    // read from an interval of 100000ms, resulting in 11 samples
    val execPlan = SelectRawPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher, MMD.dataset1.ref, 0,
      MMD.dataset1.schema, filters, TimeRangeChunkScan(100000L, 150000L), Seq(4))

    val resp = execPlan.execute(memStore, queryConfig).runAsync.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 1
    val dataRead = result.result(0).rows.map(r=>(r.getLong(0), r.getLong(1))).toList
    dataRead shouldEqual mmdTuples.filter(_(5) == "Series 1").map(r => (r(0), r(4))).take(5)
  }

  it ("should read raw Histogram samples from Memstore using IntervalSelector") {
    import ZeroCopyUTF8String._

    val filters = Seq(ColumnFilter("dc", Filter.Equals("0".utf8)))
    val execPlan = SelectRawPartitionsExec("id1", now, numRawSamples, dummyDispatcher, MMD.histDataset.ref, 0,
      MMD.histDataset.schema, filters, TimeRangeChunkScan(100000L, 150000L), Seq(3))

    val resp = execPlan.execute(memStore, queryConfig).runAsync.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, HistogramColumn)
    result.result.size shouldEqual 1
    val resultIt = result.result(0).rows.map(r=>(r.getLong(0), r.getHistogram(1)))
    val orig = histData.filter(_(5).asInstanceOf[Types.UTF8Map]("dc".utf8) == "0".utf8).map(r => (r(0), r(3))).take(5)
    resultIt.zip(orig.toIterator).foreach { case (res, origData) => res shouldEqual origData }
  }

  it ("should read periodic samples from Memstore") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val execPlan = SelectRawPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher,
                                           timeseriesDataset.ref, 0, timeseriesSchema, filters, AllChunkScan, Nil)
    val start = now - numRawSamples * reportingInterval - 100 // reduce by 100 to not coincide with reporting intervals
    val step = 20000
    val end = now - (numRawSamples-100) * reportingInterval
    execPlan.addRangeVectorTransformer(new PeriodicSamplesMapper(start, step, end, None, None, Nil))

    val resp = execPlan.execute(memStore, queryConfig).runAsync.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 1
    val partKeyRead = result.result(0).key.labelValues.map(lv => (lv._1.asNewString, lv._2.asNewString))
    partKeyRead shouldEqual partKeyLabelValues
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

  it("should read periodic samples from Long column") {
    import ZeroCopyUTF8String._
    val filters = Seq(ColumnFilter("series", Filter.Equals("Series 1".utf8)))
    val execPlan = SelectRawPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher, MMD.dataset1.ref, 0,
      MMD.dataset1.schema, filters, AllChunkScan, Seq(4))

    // Raw data like 101000, 111000, ....
    val start = 105000L
    val step = 20000L
    val end = 185000L
    execPlan.addRangeVectorTransformer(new PeriodicSamplesMapper(start, step, end, None, None, Nil))

    val resp = execPlan.execute(memStore, queryConfig).runAsync.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 1
    val dataRead = result.result(0).rows.map(r=>(r.getLong(0), r.getDouble(1))).toList
    dataRead.map(_._1) shouldEqual (start to end by step)
    dataRead.map(_._2) shouldEqual (86 to 166).by(20)
  }

  it ("should read periodic Histogram samples from Memstore") {
    import ZeroCopyUTF8String._
    val filters = Seq(ColumnFilter("dc", Filter.Equals("0".utf8)))
    val execPlan = SelectRawPartitionsExec("id1", now, numRawSamples, dummyDispatcher, MMD.histDataset.ref, 0,
      MMD.histDataset.schema, filters, AllChunkScan, Seq(3))

    val start = 105000L
    val step = 20000L
    val end = 185000L
    execPlan.addRangeVectorTransformer(new PeriodicSamplesMapper(start, step, end, None, None, Nil))

    val resp = execPlan.execute(memStore, queryConfig).runAsync.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, HistogramColumn)
    result.result.size shouldEqual 1
    val resultIt = result.result(0).rows.map(r=>(r.getLong(0), r.getHistogram(1)))
    val orig = histData.filter(_(5).asInstanceOf[Types.UTF8Map]("dc".utf8) == "0".utf8)
                       .grouped(2).map(_.head)   // Skip every other one, starting with second, since step=2x pace
                       .zip((start to end by step).toIterator).map { case (r, t) => (t, r(3)) }
    resultIt.zip(orig.toIterator).foreach { case (res, origData) => res shouldEqual origData }
  }

  // A lower-level (below coordinator) end to end histogram with max ingestion and querying test
  it("should sum Histogram records with max correctly") {
    val filters = Seq(ColumnFilter("dc", Filter.Equals("0".utf8)))
    val execPlan = SelectRawPartitionsExec("hMax", now, numRawSamples, dummyDispatcher, MMD.histMaxDS.ref, 0,
      MMD.histMaxDS.schema, filters, AllChunkScan, Seq(4))

    val start = 105000L
    val step = 20000L
    val end = 185000L
    execPlan.addRangeVectorTransformer(new PeriodicSamplesMapper(start, step, end, Some(300 * 1000),  // [5m]
                                         Some(InternalRangeFunction.SumOverTime), Nil))
    execPlan.addRangeVectorTransformer(AggregateMapReduce(AggregationOperator.Sum, Nil, Nil, Nil))

    val resp = execPlan.execute(memStore, queryConfig).runAsync.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, HistogramColumn, DoubleColumn)
    result.result.size shouldEqual 1
    val resultIt = result.result(0).rows.map(r=>(r.getLong(0), r.getHistogram(1), r.getDouble(2)))

    // For now, just validate that we can read "reasonable" results, ie max should be >= value at head of window
    // Rely on AggrOverTimeFunctionsSpec to actually validate aggregation results
    val orig = histMaxData.filter(_(6).asInstanceOf[Types.UTF8Map]("dc".utf8) == "0".utf8)
                       .grouped(2).map(_.head)   // Skip every other one, starting with second, since step=2x pace
                       .zip((start to end by step).toIterator).map { case (r, t) => (t, r(4), r(3)) }
    resultIt.zip(orig.toIterator).foreach { case (res, origData) =>
      res._3.isNaN shouldEqual false
      res._3 should be >= origData._3.asInstanceOf[Double]
    }

    // Add the histogram_max_quantile function to ExecPlan and make sure results are OK
    execPlan.addRangeVectorTransformer(
      exec.InstantVectorFunctionMapper(InstantFunctionId.HistogramMaxQuantile, Seq(0.99)))
    val resp2 = execPlan.execute(memStore, queryConfig).runAsync.futureValue
    val result2 = resp2.asInstanceOf[QueryResult]
    result2.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, DoubleColumn)
    result2.result.size shouldEqual 1
    val resultIt2 = result2.result(0).rows.map(r=>(r.getLong(0), r.getDouble(1))).toBuffer

    resultIt2.foreach { case (t, v) =>
      v.isNaN shouldEqual false
    }
  }

  it("should extract Histogram with max using Last/None function correctly") {
    val filters = Seq(ColumnFilter("dc", Filter.Equals("0".utf8)))
    val execPlan = SelectRawPartitionsExec("hMax", now, numRawSamples, dummyDispatcher, MMD.histMaxDS.ref, 0,
      MMD.histMaxDS.schema, filters, AllChunkScan, Seq(4))

    val start = 105000L
    val step = 20000L
    val end = 185000L
    execPlan.addRangeVectorTransformer(new PeriodicSamplesMapper(start, step, end, None, None, Nil))

    val resp = execPlan.execute(memStore, queryConfig).runAsync.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, HistogramColumn, DoubleColumn)
    result.result.size shouldEqual 1
    val resultIt = result.result(0).rows.map(r=>(r.getLong(0), r.getHistogram(1), r.getDouble(2)))

    // For now, just validate that we can read "reasonable" results, ie max should be >= value at head of window
    // Rely on AggrOverTimeFunctionsSpec to actually validate aggregation results
    val orig = histMaxData.filter(_(6).asInstanceOf[Types.UTF8Map]("dc".utf8) == "0".utf8)
                       .grouped(2).map(_.head)   // Skip every other one, starting with second, since step=2x pace
                       .zip((start to end by step).toIterator).map { case (r, t) => (t, r(4), r(3)) }
    resultIt.zip(orig.toIterator).foreach { case (res, origData) =>
      res._3.isNaN shouldEqual false
      res._3 should be >= origData._3.asInstanceOf[Double]
    }

  }

  it ("should return correct result schema") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val execPlan = SelectRawPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher,
                                           timeseriesDataset.ref, 0, timeseriesSchema, filters, AllChunkScan, Nil)
    val resultSchema = execPlan.schema()
    resultSchema.isTimeSeries shouldEqual true
    resultSchema.numRowKeyColumns shouldEqual 1
    resultSchema.length shouldEqual 2
    resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, DoubleColumn)
    resultSchema.columns.map(_.name) shouldEqual Seq("timestamp", "value")
  }

  it("should produce correct schema for histogram RVs with and without max column") {
    // Histogram dataset, no max column
    val noMaxPlan = SelectRawPartitionsExec("someQueryId", System.currentTimeMillis, 100, dummyDispatcher,
                      MMD.histDataset.ref, 0, MMD.histDataset.schema, Nil, AllChunkScan, Seq(3))
    val expected1 = ResultSchema(Seq(ColumnInfo("timestamp", TimestampColumn),
                                     ColumnInfo("h", HistogramColumn)), 1, colIDs = Seq(0, 3))
    noMaxPlan.schemaOfDoExecute() shouldEqual expected1

    // Histogram dataset with max column - should add max to schema automatically
    val maxPlan = SelectRawPartitionsExec("someQueryId", System.currentTimeMillis, 100, dummyDispatcher,
                      MMD.histMaxDS.ref, 0, MMD.histMaxDS.schema, Nil, AllChunkScan, Seq(4))
    val expected2 = ResultSchema(Seq(ColumnInfo("timestamp", TimestampColumn),
                                     ColumnInfo("h", HistogramColumn),
                                     ColumnInfo("max", DoubleColumn)), 1, colIDs = Seq(0, 4, 3))
    maxPlan.schemaOfDoExecute() shouldEqual expected2
  }

  it("should return chunk metadata from MemStore") {
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
                       ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val execPlan = SelectChunkInfosExec("someQueryId", now, numRawSamples, dummyDispatcher,
      timeseriesDataset.ref, 0, timeseriesSchema, filters, AllChunkScan, 0)
    val resp = execPlan.execute(memStore, queryConfig).runAsync.futureValue
    info(s"resp = $resp")
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 1
    val partKeyRead = result.result(0).key.labelValues.map(lv => (lv._1.asNewString, lv._2.asNewString))
    partKeyRead shouldEqual partKeyLabelValues

    // Extract out the numRows, startTime, endTIme and verify
    val infosRead = result.result(0).rows.map { r => (r.getInt(1), r.getLong(2), r.getLong(3), r.getString(5)) }.toList
    infosRead.foreach { i => info(s"  Infos read => $i") }
    val numChunks = numRawSamples / TestData.storeConf.maxChunksSize
    infosRead should have length (numChunks)
    infosRead.map(_._1) shouldEqual Seq.fill(numChunks)(TestData.storeConf.maxChunksSize)
    // Last chunk is the writeBuffer which is not encoded
    infosRead.map(_._4).dropRight(1).foreach(_ should include ("DeltaDeltaConst"))

    val startTimes = tuples.grouped(TestData.storeConf.maxChunksSize).map(_.head._1).toBuffer
    infosRead.map(_._2) shouldEqual startTimes
  }

  it ("should fail with exception BadQueryException") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    // Query returns n ("numRawSamples") samples - Applying Limit (n-1) to fail the query execution
    // with ResponseTooLargeException
    val execPlan = SelectRawPartitionsExec("someQueryId", now, numRawSamples - 1, dummyDispatcher,
      timeseriesDataset.ref, 0, timeseriesSchema, filters, AllChunkScan, Nil)

    val resp = execPlan.execute(memStore, queryConfig).runAsync.futureValue
    val result = resp.asInstanceOf[QueryError]
    result.t.getClass shouldEqual classOf[BadQueryException]
  }

}

