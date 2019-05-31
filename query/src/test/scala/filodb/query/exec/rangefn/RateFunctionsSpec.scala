package filodb.query.exec.rangefn

import scala.concurrent.duration._
import scala.util.Random

import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler.Implicits.global
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core.MetricsTestData.{builder, timeseriesDataset}
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.store.{AllChunkScan, InMemoryMetaStore, NullColumnStore}
import filodb.core.TestData
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SomeData, TimeSeriesMemStore}
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.query._
import filodb.query.exec.{PeriodicSamplesMapper, QueueBasedWindow, SelectRawPartitionsExec, TransientRow}
import filodb.query.util.IndexedArrayQueue
import filodb.query.RangeFunctionId.Resets
import filodb.query.exec.SelectRawPartitionsExecSpec.dummyDispatcher

class RateFunctionsSpec extends FunSpec with Matchers with ScalaFutures with BeforeAndAfterAll{

  import ZeroCopyUTF8String._

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val rand = new Random()
  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))

  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))

  val partKeyLabelValues = Map("__name__"->"http_req_total", "job"->"myCoolService", "instance"->"someHost:8787")
  val partTagsUTF8 = partKeyLabelValues.map { case (k, v) => (k.utf8, v.utf8) }
  implicit val execTimeout = 5.seconds


  val counterSamples = Seq(  8072000L->4419.00,
                      8082100L->4511.00,
                      8092196L->4614.00,
                      8102215L->4724.00,
                      8112223L->4909.00,
                      8122388L->4948.00,
                      8132570L->5000.00,
                      8142822L->5095.00,
                      8152858L->5102.00,
                      8163000L->5201.00)

  val q = new IndexedArrayQueue[TransientRow]()
  counterSamples.foreach { case (t, v) =>
    val s = new TransientRow(t, v)
    q.add(s)
  }

  builder.reset()
  counterSamples.map { t => SeqRowReader(Seq(t._1, t._2, partTagsUTF8)) }.foreach(builder.addFromReader)
  val container = builder.allContainers.head

  val counterWindow = new QueueBasedWindow(q)

  val gaugeSamples = Seq(   8072000L->7419.00,
                            8082100L->5511.00,
                            8092196L->4614.00,
                            8102215L->3724.00,
                            8112223L->4909.00,
                            8122388L->4948.00,
                            8132570L->5000.00,
                            8142822L->3095.00,
                            8152858L->5102.00,
                            8163000L->8201.00)

  val q2 = new IndexedArrayQueue[TransientRow]()
  gaugeSamples.foreach { case (t, v) =>
    val s = new TransientRow(t, v)
    q2.add(s)
  }
  val gaugeWindow = new QueueBasedWindow(q2)

  val errorOk = 0.0000001

  // Basic test cases covered
  // TODO Extrapolation special cases not done

  override def beforeAll(): Unit = {
    memStore.setup(timeseriesDataset, 0, TestData.storeConf)
    memStore.ingest(timeseriesDataset.ref, 0, SomeData(container, 0))
    memStore.commitIndexForTesting(timeseriesDataset.ref)
  }

  override def afterAll(): Unit = {
    memStore.shutdown()
  }

  it ("rate should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val expected = (q.last.value - q.head.value) / (q.last.timestamp - q.head.timestamp) * 1000
    val toEmit = new TransientRow
    RateFunction.apply(startTs,endTs, counterWindow, toEmit, queryConfig)
    Math.abs(toEmit.value - expected) should be < errorOk
  }

  it ("irate should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val prevSample = q(q.size - 2)
    val expected = (q.last.value - prevSample.value) / (q.last.timestamp - prevSample.timestamp) * 1000
    val toEmit = new TransientRow
    IRateFunction.apply(startTs, endTs, counterWindow, toEmit, queryConfig)
    Math.abs(toEmit.value - expected) should be < errorOk
  }

  it ("resets should work with empty windows and no resets data") {
    val startTs = 8071950L
    val endTs =   8163070L
    val toEmit = new TransientRow
    val q3 = new IndexedArrayQueue[TransientRow]()
    val gaugeWindowForReset = new QueueBasedWindow(q3)
    val resetsFunction = new ResetsFunction

    val counterSamples = Seq(   8072000L->1419.00,
      8082100L->2511.00,
      8092196L->3614.00,
      8102215L->4724.00,
      8112223L->5909.00,
      8122388L->6948.00,
      8132570L->7000.00,
      8142822L->8095.00,
      8152858L->9102.00,
      8163000L->9201.00)

    resetsFunction.apply(startTs, endTs, gaugeWindowForReset, toEmit, queryConfig)
    assert(toEmit.value.isNaN) // Empty window should return NaN

    counterSamples.foreach { case (t, v) =>
      val s = new TransientRow(t, v)
      q3.add(s)
      resetsFunction.addedToWindow(s, gaugeWindowForReset)
    }
    resetsFunction.apply(startTs, endTs, gaugeWindowForReset, toEmit, queryConfig)
    toEmit.value shouldEqual 0
  }


  it ("resets should return NaNs for empty (No Data) time windows") {
    import ZeroCopyUTF8String._

    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    // read from an interval of 100000ms, resulting in 11 samples
    val startTime = 8071950L
    val endTime =   8163070L

    val lp = new PeriodicSeriesWithWindowing(new RawSeries(new IntervalSelector(startTime, endTime), filters,
      List()), startTime, 60, startTime + 120, 60, Resets, List())
    val execPlan = SelectRawPartitionsExec("someQueryId", startTime, 10, dummyDispatcher,
      timeseriesDataset.ref, 0, filters, AllChunkScan, Seq(0, 1))
    execPlan.addRangeVectorTransformer(PeriodicSamplesMapper(lp.start, lp.step,
      lp.end, Some(lp.window), Some(lp.function), lp.functionArgs))
    val resp = execPlan.execute(memStore, timeseriesDataset, queryConfig).runAsync.futureValue
    val result = resp.asInstanceOf[QueryResult]
    result.result.size shouldEqual 1
    val partKeyRead = result.result(0).key.labelValues.map(lv => (lv._1.asNewString, lv._2.asNewString))
    partKeyRead shouldEqual partKeyLabelValues
    val dataRead = result.result(0).rows.map(r=>(r.getLong(0), r.getDouble(1))).toList
    dataRead.head._2.isNaN shouldEqual true
    dataRead.last._2.isNaN shouldEqual true

  }

  it ("resets should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val expected = 4.0
    val toEmit = new TransientRow
    val q3 = new IndexedArrayQueue[TransientRow]()
    val gaugeWindowForReset = new QueueBasedWindow(q3)
    val resetsFunction = new ResetsFunction

    gaugeSamples.foreach { case (t, v) =>
      val s = new TransientRow(t, v)
      q3.add(s)
      resetsFunction.addedToWindow(s, gaugeWindowForReset)
    }

    resetsFunction.apply(startTs, endTs, gaugeWindowForReset, toEmit, queryConfig)
    Math.abs(toEmit.value - expected) should be < errorOk

    // Window sliding case
    val expected2 = 1
    var toEmit2 = new TransientRow

    // 3 resets at the beginning - so resets count should drop only by 3 (4 - 3 = 1) even though we are removing 5 items
    for (i <- 0 until 5) {
      toEmit2 = q3.remove
      resetsFunction.removedFromWindow(toEmit2, gaugeWindowForReset)// old items being evicted for new window items
    }
    resetsFunction.apply(startTs, endTs, gaugeWindow, toEmit2, queryConfig)
    Math.abs(toEmit2.value - expected2) should be < errorOk


    // Empty window case - resets should be NaN
    val expected3 = true
    var toEmit3 = new TransientRow

    //Remove all the elements from the window
    for (i <- 0 until 5) {
      toEmit3 = q3.remove
      resetsFunction.removedFromWindow(toEmit3, gaugeWindowForReset)// old items being evicted
    }
    resetsFunction.apply(startTs, endTs, gaugeWindow, toEmit3, queryConfig)
    toEmit3.value.isNaN shouldEqual expected3
  }

  it ("deriv should work when start and end are outside window") {
    val gaugeSamples = Seq(
      8072000L->4419.00,
      8082100L->4419.00,
      8092196L->4419.00,
      8102215L->4724.00,
      8112223L->4724.00,
      8122388L->4724.00,
      8132570L->5000.00,
      8142822L->5000.00,
      8152858L->5000.00,
      8163000L->5201.00)

    val expectedSamples = Seq(
      8092196L->0.00,
      8102215L->15.143392157475684,
      8112223L->15.232227023719313,
      8122388L->0.0,
      8132570L->13.568427882659712,
      8142822L->13.4914241262328,
      8152858L->0.0,
      8163000L->9.978695375995517
    )
    for (i <- 0 to gaugeSamples.size - 3) {
      val startTs = gaugeSamples(i)._1
      val endTs =   gaugeSamples(i + 2)._1
      val qDeriv = new IndexedArrayQueue[TransientRow]()
      for (j <- i until i + 3) {
        val s = new TransientRow(gaugeSamples(j)._1.toLong, gaugeSamples(j)._2)
        qDeriv.add(s)
      }

      val gaugeWindow = new QueueBasedWindow(qDeriv)

      val toEmit = new TransientRow
      DerivFunction.apply(startTs, endTs, gaugeWindow, toEmit, queryConfig)
      Math.abs(toEmit.value - expectedSamples(i)._2) should be < errorOk
    }
  }

  it ("increase should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val expected = (q.last.value - q.head.value) / (q.last.timestamp - q.head.timestamp) * (endTs - startTs)
    val toEmit = new TransientRow
    IncreaseFunction.apply(startTs,endTs, counterWindow, toEmit, queryConfig)
    Math.abs(toEmit.value - expected) should be < errorOk
  }

  it ("delta should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val expected = (q2.last.value - q2.head.value) / (q2.last.timestamp - q2.head.timestamp) * (endTs - startTs)
    val toEmit = new TransientRow
    DeltaFunction.apply(startTs,endTs, gaugeWindow, toEmit, queryConfig)
    Math.abs(toEmit.value - expected) should be < errorOk
  }

  it ("idelta should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val prevSample = q2(q2.size - 2)
    //val expected = q2.last.value - prevSample.value
    val expected = q2.last.value - prevSample.value
    val toEmit = new TransientRow
    IDeltaFunction.apply(startTs,endTs, gaugeWindow, toEmit, queryConfig)
    Math.abs(toEmit.value - expected) should be < errorOk
  }

}
