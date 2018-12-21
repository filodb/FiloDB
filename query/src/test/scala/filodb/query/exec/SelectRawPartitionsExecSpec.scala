package filodb.query.exec

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core.MetricsTestData._
import filodb.core.TestData
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SomeData, TimeSeriesMemStore}
import filodb.core.metadata.Column.ColumnType.{DoubleColumn, TimestampColumn}
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.store.{InMemoryMetaStore, NullColumnStore}
import filodb.memory.MemFactory
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.query._

class SelectRawPartitionsExecSpec extends FunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  import ZeroCopyUTF8String._
  import filodb.core.{MachineMetricsData => MMD}

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
  tuples.map { t => SeqRowReader(Seq(t._1, t._2, partTagsUTF8)) }.foreach(builder.addFromReader)
  val container = builder.allContainers.head

  val mmdBuilder = new RecordBuilder(MemFactory.onHeapFactory, MMD.dataset1.ingestionSchema)
  val mmdTuples = MMD.linearMultiSeries().take(100)
  val mmdSomeData = MMD.records(MMD.dataset1, mmdTuples)

  implicit val execTimeout = 5.seconds

  override def beforeAll(): Unit = {
    memStore.setup(timeseriesDataset, 0, TestData.storeConf)
    memStore.ingest(timeseriesDataset.ref, 0, SomeData(container, 0))
    memStore.setup(MMD.dataset1, 0, TestData.storeConf)
    memStore.ingest(MMD.dataset1.ref, 0, mmdSomeData)
    memStore.commitIndexForTesting(timeseriesDataset.ref)
    memStore.commitIndexForTesting(MMD.dataset1.ref)
  }

  override def afterAll(): Unit = {
    memStore.shutdown()
  }

  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlan)
                         (implicit sched: ExecutionContext,
                          timeout: FiniteDuration): Task[QueryResponse] = ???
  }

  it ("should read raw samples from Memstore using AllChunksSelector") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
                       ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val execPlan = SelectRawPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher,
      timeseriesDataset.ref, 0, filters, AllChunks, Seq(0, 1))

    val resp = execPlan.execute(memStore, timeseriesDataset, queryConfig).runAsync.futureValue
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
    val start: BinaryRecord = BinaryRecord(timeseriesDataset, Seq(now - numRawSamples * reportingInterval))
    val end: BinaryRecord = BinaryRecord(timeseriesDataset, Seq(now - (numRawSamples-10) * reportingInterval))

    val execPlan = SelectRawPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher, timeseriesDataset.ref, 0,
      filters, RowKeyInterval(start, end), Seq(0, 1))

    val resp = execPlan.execute(memStore, timeseriesDataset, queryConfig).runAsync.futureValue
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
    val start: BinaryRecord = BinaryRecord(MMD.dataset1, Seq(100000L))
    val end: BinaryRecord = BinaryRecord(MMD.dataset1, Seq(150000L))

    val execPlan = SelectRawPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher, MMD.dataset1.ref, 0,
      filters, RowKeyInterval(start, end), Seq(0, 4))

    val resp = execPlan.execute(memStore, MMD.dataset1, queryConfig).runAsync.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 1
    val dataRead = result.result(0).rows.map(r=>(r.getLong(0), r.getLong(1))).toList
    dataRead shouldEqual mmdTuples.filter(_(5) == "Series 1").map(r => (r(0), r(4))).take(5)
  }

  it ("should read periodic samples from Memstore") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val execPlan = SelectRawPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher, timeseriesDataset.ref, 0,
      filters, AllChunks, Seq(0, 1))
    val start = now - numRawSamples * reportingInterval - 100 // reduce by 100 to not coincide with reporting intervals
    val step = 20000
    val end = now - (numRawSamples-100) * reportingInterval
    execPlan.addRangeVectorTransformer(new PeriodicSamplesMapper(start, step, end, None, None, Nil))

    val resp = execPlan.execute(memStore, timeseriesDataset, queryConfig).runAsync.futureValue
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
      filters, AllChunks, Seq(0, 4))

    // Raw data like 101000, 111000, ....
    val start = 105000L
    val step = 20000L
    val end = 185000L
    execPlan.addRangeVectorTransformer(new PeriodicSamplesMapper(start, step, end, None, None, Nil))

    val resp = execPlan.execute(memStore, MMD.dataset1, queryConfig).runAsync.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 1
    val dataRead = result.result(0).rows.map(r=>(r.getLong(0), r.getDouble(1))).toList
    dataRead.map(_._1) shouldEqual (start to end by step)
    dataRead.map(_._2) shouldEqual (86 to 166).by(20)

  }

  it ("should return correct result schema") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val execPlan = SelectRawPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher, timeseriesDataset.ref, 0,
      filters, AllChunks, Seq(0, 1))
    val resultSchema = execPlan.schema(timeseriesDataset)
    resultSchema.isTimeSeries shouldEqual true
    resultSchema.numRowKeyColumns shouldEqual 1
    resultSchema.length shouldEqual 2
    resultSchema.columns.map(_.colType) shouldEqual Seq(TimestampColumn, DoubleColumn)
    resultSchema.columns.map(_.name) shouldEqual Seq("timestamp", "value")
  }

  it("should return chunk metadata from MemStore") {
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
                       ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val execPlan = SelectChunkInfosExec("someQueryId", now, numRawSamples, dummyDispatcher,
      timeseriesDataset.ref, 0, filters, AllChunks, 0)
    val resp = execPlan.execute(memStore, timeseriesDataset, queryConfig).runAsync.futureValue
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
}

