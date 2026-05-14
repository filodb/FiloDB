package filodb.core.query

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


import scala.collection.mutable.ArrayBuffer

class QueryContextSpec extends AnyFunSpec with Matchers {

  it("should produce correct log line") {
    val pp = PlannerParams(
      applicationId = "fdb",
      queryTimeoutMillis = 10,
      enforcedLimits = PerQueryLimits(
        groupByCardinality = 123,
        joinQueryCardinality = 124,
        execPlanResultBytes = 125,
        execPlanSamples = 126,
        timeSeriesSamplesScannedBytes = 127,
        timeSeriesScanned = 200),
      warnLimits = PerQueryLimits(
        groupByCardinality = 128,
        joinQueryCardinality = 129,
        execPlanResultBytes = 130,
        execPlanSamples = 131,
        timeSeriesSamplesScannedBytes = 132,
        rawScannedBytes = 133),
      queryOrigin = Option("rr"),
      queryOriginId = Option("rr_id"),
      timeSplitEnabled = true,
      minTimeRangeForSplitMs = 10,
      splitSizeMs = 10,
      skipAggregatePresent = true,
      processMultiPartition = true,
      allowPartialResults = true
    )
    val queryContext = QueryContext(
      PromQlQueryParams(promQl= "myQuery", 1, 2, 3),
      plannerParams = pp
    )
    val logLine = queryContext.getQueryLogLine("My log message")
    logLine should equal (
      s"My log message promQL = -=# myQuery #=- queryOrigin = Some(rr) queryPrincipal = None " +
        s"queryOriginId = Some(rr_id) queryId = ${queryContext.queryId}"
    )
  }

  it("should maintain QueryStats state correctly") {
    val stats = QueryStats()

    stats.unsafeSize() shouldEqual 0
    stats.keys() shouldEqual Nil
    stats.get(Seq("foo", "bar")) shouldEqual None

    // Test "getOrCreate" functionality.
    stats.getSamplesScannedCounter(Seq("foo", "bar"))
    stats.unsafeSize() shouldEqual 1
    stats.keys().size shouldEqual 1
    stats.keys().toSet shouldEqual Set(Seq("foo", "bar"))
    stats.get(Seq("foo", "bar")).isDefined shouldEqual true

    // Test put.
    stats.put(Seq("abc", "123"), Stat())
    stats.unsafeSize() shouldEqual 2
    stats.keys().size shouldEqual 2
    stats.keys().toSet shouldEqual Set(Seq("foo", "bar"), Seq("abc", "123"))
    stats.get(Seq("foo", "bar")).isDefined shouldEqual true
    stats.get(Seq("abc", "123")).isDefined shouldEqual true

    // Test Nil handling.
    stats.getSamplesScannedCounter(Nil)
    stats.unsafeSize() shouldEqual 3
    stats.keys().size shouldEqual 3
    stats.keys().toSet shouldEqual Set(Seq("foo", "bar"), Seq("abc", "123"), Nil)
    stats.get(Nil).isDefined shouldEqual true

    // Test add.
    val emptyStats = QueryStats()
    emptyStats.add(stats)
    emptyStats.unsafeSize() shouldEqual 3
    stats.keys().size shouldEqual 3
    emptyStats.keys().toSet shouldEqual Set(Seq("foo", "bar"), Seq("abc", "123"), Nil)
    emptyStats.get(Nil).isDefined shouldEqual true

    // Test clear.
    stats.clear()
    stats.unsafeSize() shouldEqual 0
    stats.keys() shouldEqual Nil
    stats.get(Nil).isDefined shouldEqual false
  }

  it("should correctly support per-element QueryStats operations") {
    val stats = QueryStats()

    // Populated/cleared as part of each test below.
    val keyBuffer = ArrayBuffer[Seq[String]]()

    // Test empty stats. #################################

    stats.map(entry => entry._1) shouldEqual Nil

    stats.foreach(entry => keyBuffer.addOne(entry._1))
    keyBuffer.toSeq shouldEqual Nil

    // Test nonempty stats. #################################

    stats.getSamplesScannedCounter(Nil)
    stats.getSamplesScannedCounter(Seq("foo"))
    stats.getSamplesScannedCounter(Seq("abc", "123"))

    stats.map(entry => entry._1).toSet shouldEqual Set(Nil, Seq("foo"), Seq("abc", "123"))

    stats.foreach(entry => keyBuffer.addOne(entry._1))
    keyBuffer.toSet shouldEqual Set(Nil, Seq("foo"), Seq("abc", "123"))
    keyBuffer.clear()
  }

  it("should correctly add samples-scanned to QueryStats") {
    {
      // Empty -- effectively a no-op; should not throw.
      val stats = QueryStats()
      stats.unsafeAddSamplesScanned(100)
      stats.unsafeSize() shouldEqual 0
    }

    {
      // One key; should account for all samples.
      val stats = QueryStats()
      stats.getSamplesScannedCounter(Seq("hello"))
      stats.unsafeAddSamplesScanned(100)
      stats.foreach { case (key, stat) => stat.samplesScanned.get() shouldEqual 100 }
    }

    {
      // Two keys; each should account for half of all samples.
      val stats = QueryStats()
      stats.getSamplesScannedCounter(Seq("hello"))
      stats.getSamplesScannedCounter(Seq("goodbye"))
      stats.unsafeAddSamplesScanned(100)
      stats.foreach { case (key, stat) => stat.samplesScanned.get() shouldEqual 50 }
    }

    {
      // Nil key; should account for all samples.
      val stats = QueryStats()
      stats.getSamplesScannedCounter(Nil)
      stats.unsafeAddSamplesScanned(100)
      stats.foreach { case (key, stat) => stat.samplesScanned.get() shouldEqual 100 }
    }

    {
      // Nil key with one other; non-nil should account for all samples.
      val stats = QueryStats()
      stats.getSamplesScannedCounter(Nil)
      stats.getSamplesScannedCounter(Seq("hello"))
      stats.unsafeAddSamplesScanned(100)
      stats.foreach { case (key, stat) =>
        if (key.isEmpty) {
          stat.samplesScanned.get() shouldEqual 0
        } else {
          stat.samplesScanned.get() shouldEqual 100
        }
      }
    }

    {
      // Nil key with two others; non-nils should each account for half of all samples.
      val stats = QueryStats()
      stats.getSamplesScannedCounter(Nil)
      stats.getSamplesScannedCounter(Seq("hello"))
      stats.getSamplesScannedCounter(Seq("goodbye"))
      stats.unsafeAddSamplesScanned(100)
      stats.foreach { case (key, stat) =>
        if (key.isEmpty) {
          stat.samplesScanned.get() shouldEqual 0
        } else {
          stat.samplesScanned.get() shouldEqual 50
        }
      }
    }

    {
      // Only samples-scanned counters should be updated.
      val stats = QueryStats()
      stats.getSamplesScannedCounter(Nil)
      stats.getSamplesScannedCounter(Seq("hello"))
      stats.getSamplesScannedCounter(Seq("goodbye"))
      stats.unsafeAddSamplesScanned(100)
      stats.foreach { case (key, stat) =>
        stat.timeSeriesScanned.get() shouldEqual 0
        stat.cpuNanos.get() shouldEqual 0
        stat.dataBytesScanned.get() shouldEqual 0
        stat.resultBytes.get() shouldEqual 0
      }
    }
  }
}