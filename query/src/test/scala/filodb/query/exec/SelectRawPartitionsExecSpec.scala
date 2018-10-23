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
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SomeData, TimeSeriesMemStore}
import filodb.core.metadata.Column.ColumnType.{DoubleColumn, TimestampColumn}
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.store.{InMemoryMetaStore, NullColumnStore}
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.query._

class SelectRawPartitionsExecSpec extends FunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  import ZeroCopyUTF8String._

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
  tuples.map { t => SeqRowReader(Seq(t._1, t._2, partTagsUTF8)) }.foreach(builder.addFromReader)
  val container = builder.allContainers.head

  implicit val execTimeout = 5.seconds

  override def beforeAll(): Unit = {
    memStore.setup(timeseriesDataset, 0, TestData.storeConf)
    memStore.ingest(timeseriesDataset.ref, 0, SomeData(container, 0))
    memStore.commitIndexForTesting(timeseriesDataset.ref)
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
      timeseriesDataset.ref, 0, filters, AllChunks, Seq("timestamp", "value"))

    val resp = execPlan.execute(memStore, timeseriesDataset, queryConfig).runAsync.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 1
    val partKeyRead = result.result(0).key.labelValues.map(lv => (lv._1.asNewString, lv._2.asNewString))
    partKeyRead shouldEqual partKeyLabelValues
    val dataRead = result.result(0).rows.map(r=>(r.getLong(0), r.getDouble(1))).toList
    dataRead.sorted shouldEqual tuples.sorted // TODO see why rows are not in order
  }

  it ("should read raw samples from Memstore using IntervalSelector") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    // read from an interval of 100000ms, resulting in 11 samples
    val start: BinaryRecord = BinaryRecord(timeseriesDataset, Seq(now - numRawSamples * reportingInterval))
    val end: BinaryRecord = BinaryRecord(timeseriesDataset, Seq(now - (numRawSamples-10) * reportingInterval))

    val execPlan = SelectRawPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher, timeseriesDataset.ref, 0,
      filters, RowKeyInterval(start, end), Seq("timestamp", "value"))

    val resp = execPlan.execute(memStore, timeseriesDataset, queryConfig).runAsync.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 1
    val dataRead = result.result(0).rows.map(r=>(r.getLong(0), r.getDouble(1))).toList
    dataRead shouldEqual tuples.take(11)
    val partKeyRead = result.result(0).key.labelValues.map(lv => (lv._1.asNewString, lv._2.asNewString))
    partKeyRead shouldEqual partKeyLabelValues
  }

  it ("should read periodic samples from Memstore") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val execPlan = SelectRawPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher, timeseriesDataset.ref, 0,
      filters, AllChunks, Seq("timestamp", "value"))
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

  it ("should return correct result schema") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val execPlan = SelectRawPartitionsExec("someQueryId", now, numRawSamples, dummyDispatcher, timeseriesDataset.ref, 0,
      filters, AllChunks, Seq("timestamp", "value"))
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

