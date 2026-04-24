package filodb.query.exec.rangefn

import scala.util.Random

class TsOfMaxMinOverTimeSpec extends RawDataWindowingSpec {
  val rand = new Random()
  val numIterations = 10

  it("should correctly aggregate ts_of_max_over_time and ts_of_min_over_time using chunked iterators") {
    // Create data with known timestamps and values
    val data = (1 to 240).map(_.toDouble)
    val rv = timeValueRV(data)

    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(100) + 10
      val step = rand.nextInt(75) + 5
      info(s"iteration $x windowSize=$windowSize step=$step")

      // Test ts_of_max_over_time
      val tsMaxChunkedIt = chunkedWindowIt(data, rv, new TsOfMaxOverTimeChunkedFunctionD(), windowSize, step)
      val tsMaxAggregated = tsMaxChunkedIt.map(_.getDouble(1)).toBuffer

      // Expected: timestamp (in seconds) when max value occurred in each window
      val expectedTsMax = data.sliding(windowSize, step).zipWithIndex.map { case (window, windowIdx) =>
        val maxValue = window.max
        val maxIndex = window.indexOf(maxValue)
        val timestampMillis = defaultStartTS + (windowIdx * step + maxIndex) * pubFreq
        timestampMillis.toDouble / 1000.0
      }.toBuffer

      tsMaxAggregated shouldEqual expectedTsMax

      // Test ts_of_min_over_time
      val tsMinChunkedIt = chunkedWindowIt(data, rv, new TsOfMinOverTimeChunkedFunctionD(), windowSize, step)
      val tsMinAggregated = tsMinChunkedIt.map(_.getDouble(1)).toBuffer

      // Expected: timestamp (in seconds) when min value occurred in each window
      val expectedTsMin = data.sliding(windowSize, step).zipWithIndex.map { case (window, windowIdx) =>
        val minValue = window.min
        val minIndex = window.indexOf(minValue)
        val timestampMillis = defaultStartTS + (windowIdx * step + minIndex) * pubFreq
        timestampMillis.toDouble / 1000.0
      }.toBuffer

      tsMinAggregated shouldEqual expectedTsMin
    }
  }

  it("should handle NaN values correctly in ts_of_max_over_time and ts_of_min_over_time") {
    // Test data with NaN values
    val dataWithNaN = Seq(1.0, 2.0, Double.NaN, 4.0, 5.0, Double.NaN, 3.0, 6.0)
    val rv = timeValueRV(dataWithNaN)

    val windowSize = 4
    val step = 2

    // Test ts_of_max_over_time - should ignore NaN values
    val tsMaxIt = chunkedWindowIt(dataWithNaN, rv, new TsOfMaxOverTimeChunkedFunctionD(), windowSize, step)
    val tsMaxResults = tsMaxIt.map(_.getDouble(1)).toBuffer

    // Window 1: [1.0, 2.0, NaN, 4.0] -> max is 4.0 at index 3
    // Window 2: [Double.NaN, 4.0, 5.0, Double.NaN] -> max is 5.0 at index 4
    // Window 3: [5.0, Double.NaN, 3.0, 6.0] -> max is 6.0 at index 7

    tsMaxResults.size shouldEqual 3
    tsMaxResults.foreach { ts => ts.isNaN shouldBe false }

    // Test ts_of_min_over_time - should ignore NaN values
    val tsMinIt = chunkedWindowIt(dataWithNaN, rv, new TsOfMinOverTimeChunkedFunctionD(), windowSize, step)
    val tsMinResults = tsMinIt.map(_.getDouble(1)).toBuffer

    tsMinResults.size shouldEqual 3
    tsMinResults.foreach { ts => ts.isNaN shouldBe false }
  }

  it("should return NaN timestamp when all values in window are NaN") {
    // Test data with all NaN values
    val allNaN = Seq(Double.NaN, Double.NaN, Double.NaN, Double.NaN)
    val rv = timeValueRV(allNaN)

    val windowSize = 4
    val step = 1

    // Test ts_of_max_over_time
    val tsMaxIt = chunkedWindowIt(allNaN, rv, new TsOfMaxOverTimeChunkedFunctionD(), windowSize, step)
    val tsMaxResult = tsMaxIt.next().getDouble(1)
    tsMaxResult.isNaN shouldEqual true

    // Test ts_of_min_over_time
    val tsMinIt = chunkedWindowIt(allNaN, rv, new TsOfMinOverTimeChunkedFunctionD(), windowSize, step)
    val tsMinResult = tsMinIt.next().getDouble(1)
    tsMinResult.isNaN shouldEqual true
  }

  it("should correctly identify timestamp of first occurrence when multiple max/min values exist") {
    // Test data where max/min values appear multiple times
    val dataWithDuplicates = Seq(1.0, 5.0, 3.0, 5.0, 2.0, 1.0, 4.0, 1.0)
    val rv = timeValueRV(dataWithDuplicates)

    val windowSize = 8
    val step = 1

    // Test ts_of_max_over_time - should return timestamp of FIRST max (5.0 at index 1)
    val tsMaxIt = chunkedWindowIt(dataWithDuplicates, rv, new TsOfMaxOverTimeChunkedFunctionD(), windowSize, step)
    val tsMaxResult = tsMaxIt.next().getDouble(1)
    val expectedMaxTs = (defaultStartTS + 1 * pubFreq).toDouble / 1000.0  // Index 1
    tsMaxResult shouldEqual expectedMaxTs

    // Test ts_of_min_over_time - should return timestamp of FIRST min (1.0 at index 0)
    val tsMinIt = chunkedWindowIt(dataWithDuplicates, rv, new TsOfMinOverTimeChunkedFunctionD(), windowSize, step)
    val tsMinResult = tsMinIt.next().getDouble(1)
    val expectedMinTs = (defaultStartTS + 0 * pubFreq).toDouble / 1000.0  // Index 0
    tsMinResult shouldEqual expectedMinTs
  }

  it("should work correctly with single value windows") {
    val data = Seq(42.0)
    val rv = timeValueRV(data)

    val windowSize = 1
    val step = 1

    // Test ts_of_max_over_time
    val tsMaxIt = chunkedWindowIt(data, rv, new TsOfMaxOverTimeChunkedFunctionD(), windowSize, step)
    val tsMaxResult = tsMaxIt.next().getDouble(1)
    val expectedTs = defaultStartTS.toDouble / 1000.0
    tsMaxResult shouldEqual expectedTs

    // Test ts_of_min_over_time
    val tsMinIt = chunkedWindowIt(data, rv, new TsOfMinOverTimeChunkedFunctionD(), windowSize, step)
    val tsMinResult = tsMinIt.next().getDouble(1)
    tsMinResult shouldEqual expectedTs
  }

  it("should handle negative values correctly") {
    val dataWithNegatives = Seq(-5.0, -2.0, 3.0, -8.0, 1.0, -1.0)
    val rv = timeValueRV(dataWithNegatives)

    val windowSize = 6
    val step = 1

    // Test ts_of_max_over_time - max is 3.0 at index 2
    val tsMaxIt = chunkedWindowIt(dataWithNegatives, rv, new TsOfMaxOverTimeChunkedFunctionD(), windowSize, step)
    val tsMaxResult = tsMaxIt.next().getDouble(1)
    val expectedMaxTs = (defaultStartTS + 2 * pubFreq).toDouble / 1000.0
    tsMaxResult shouldEqual expectedMaxTs

    // Test ts_of_min_over_time - min is -8.0 at index 3
    val tsMinIt = chunkedWindowIt(dataWithNegatives, rv, new TsOfMinOverTimeChunkedFunctionD(), windowSize, step)
    val tsMinResult = tsMinIt.next().getDouble(1)
    val expectedMinTs = (defaultStartTS + 3 * pubFreq).toDouble / 1000.0
    tsMinResult shouldEqual expectedMinTs
  }

  it("should work correctly across multiple chunks") {
    // Create data that will span multiple chunks
    val data = (1 to 500).map(_.toDouble)
    val rv = timeValueRV(data)

    val windowSize = 50
    val step = 25

    // Test ts_of_max_over_time
    val tsMaxIt = chunkedWindowIt(data, rv, new TsOfMaxOverTimeChunkedFunctionD(), windowSize, step)
    val tsMaxResults = tsMaxIt.map(_.getDouble(1)).toBuffer

    // Verify we got results for all windows
    val expectedWindows = numWindows(data, windowSize, step)
    tsMaxResults.size shouldEqual expectedWindows

    // All timestamps should be valid (not NaN)
    tsMaxResults.foreach { ts => ts.isNaN shouldBe false }

    // Test ts_of_min_over_time
    val tsMinIt = chunkedWindowIt(data, rv, new TsOfMinOverTimeChunkedFunctionD(), windowSize, step)
    val tsMinResults = tsMinIt.map(_.getDouble(1)).toBuffer

    tsMinResults.size shouldEqual expectedWindows
    tsMinResults.foreach { ts => ts.isNaN shouldBe false }
  }

  it("should correctly aggregate ts_of_last_over_time using chunked iterators") {
    // Create data with known timestamps and values
    val data = (1 to 240).map(_.toDouble)
    val rv = timeValueRV(data)

    (0 until numIterations).foreach { x =>
      val windowSize = rand.nextInt(100) + 10
      val step = rand.nextInt(75) + 5
      info(s"iteration $x windowSize=$windowSize step=$step")

      // Test ts_of_last_over_time
      val tsLastChunkedIt = chunkedWindowIt(data, rv, new TsOfLastOverTimeChunkedFunctionD(), windowSize, step)
      val tsLastAggregated = tsLastChunkedIt.map(_.getDouble(1)).toBuffer

      // Expected: timestamp (in seconds) when last (most recent) value occurred in each window
      val expectedTsLast = data.sliding(windowSize, step).zipWithIndex.map { case (window, windowIdx) =>
        // Last value is always at the end of the window (highest index)
        val lastIndex = window.size - 1
        val timestampMillis = defaultStartTS + (windowIdx * step + lastIndex) * pubFreq
        timestampMillis.toDouble / 1000.0
      }.toBuffer

      tsLastAggregated shouldEqual expectedTsLast
    }
  }

  it("should handle NaN values correctly in ts_of_last_over_time") {
    // Test data with NaN values
    val dataWithNaN = Seq(1.0, 2.0, Double.NaN, 4.0, 5.0, Double.NaN, 3.0, 6.0)
    val rv = timeValueRV(dataWithNaN)

    val windowSize = 4
    val step = 2

    // Test ts_of_last_over_time - should ignore NaN values and find last non-NaN
    val tsLastIt = chunkedWindowIt(dataWithNaN, rv, new TsOfLastOverTimeChunkedFunctionD(), windowSize, step)
    val tsLastResults = tsLastIt.map(_.getDouble(1)).toBuffer

    // Window 1: [1.0, 2.0, NaN, 4.0] -> last non-NaN is 4.0 at index 3
    // Window 2: [Double.NaN, 4.0, 5.0, Double.NaN] -> last non-NaN is 5.0 at index 4
    // Window 3: [5.0, Double.NaN, 3.0, 6.0] -> last non-NaN is 6.0 at index 7

    tsLastResults.size shouldEqual 3
    tsLastResults.foreach { ts => ts.isNaN shouldBe false }
  }

  it("should return NaN timestamp when all values in window are NaN for ts_of_last_over_time") {
    // Test data with all NaN values
    val allNaN = Seq(Double.NaN, Double.NaN, Double.NaN, Double.NaN)
    val rv = timeValueRV(allNaN)

    val windowSize = 4
    val step = 1

    // Test ts_of_last_over_time
    val tsLastIt = chunkedWindowIt(allNaN, rv, new TsOfLastOverTimeChunkedFunctionD(), windowSize, step)
    val tsLastResult = tsLastIt.next().getDouble(1)
    tsLastResult.isNaN shouldEqual true
  }

  it("should correctly identify timestamp of last occurrence in ts_of_last_over_time") {
    // Test data where we want to verify the last timestamp
    val data = Seq(1.0, 5.0, 3.0, 5.0, 2.0, 1.0, 4.0, 1.0)
    val rv = timeValueRV(data)

    val windowSize = 8
    val step = 1

    // Test ts_of_last_over_time - should return timestamp of LAST value (1.0 at index 7)
    val tsLastIt = chunkedWindowIt(data, rv, new TsOfLastOverTimeChunkedFunctionD(), windowSize, step)
    val tsLastResult = tsLastIt.next().getDouble(1)
    val expectedLastTs = (defaultStartTS + 7 * pubFreq).toDouble / 1000.0  // Index 7 (last)
    tsLastResult shouldEqual expectedLastTs
  }

  it("should work correctly with single value windows for ts_of_last_over_time") {
    val data = Seq(42.0)
    val rv = timeValueRV(data)

    val windowSize = 1
    val step = 1

    // Test ts_of_last_over_time
    val tsLastIt = chunkedWindowIt(data, rv, new TsOfLastOverTimeChunkedFunctionD(), windowSize, step)
    val tsLastResult = tsLastIt.next().getDouble(1)
    val expectedTs = defaultStartTS.toDouble / 1000.0
    tsLastResult shouldEqual expectedTs
  }

  it("should handle negative values correctly in ts_of_last_over_time") {
    val dataWithNegatives = Seq(-5.0, -2.0, 3.0, -8.0, 1.0, -1.0)
    val rv = timeValueRV(dataWithNegatives)

    val windowSize = 6
    val step = 1

    // Test ts_of_last_over_time - last value is -1.0 at index 5
    val tsLastIt = chunkedWindowIt(dataWithNegatives, rv, new TsOfLastOverTimeChunkedFunctionD(), windowSize, step)
    val tsLastResult = tsLastIt.next().getDouble(1)
    val expectedLastTs = (defaultStartTS + 5 * pubFreq).toDouble / 1000.0
    tsLastResult shouldEqual expectedLastTs
  }

  it("should work correctly across multiple chunks for ts_of_last_over_time") {
    // Create data that will span multiple chunks
    val data = (1 to 500).map(_.toDouble)
    val rv = timeValueRV(data)

    val windowSize = 50
    val step = 25

    // Test ts_of_last_over_time
    val tsLastIt = chunkedWindowIt(data, rv, new TsOfLastOverTimeChunkedFunctionD(), windowSize, step)
    val tsLastResults = tsLastIt.map(_.getDouble(1)).toBuffer

    // Verify we got results for all windows
    val expectedWindows = numWindows(data, windowSize, step)
    tsLastResults.size shouldEqual expectedWindows

    // All timestamps should be valid (not NaN)
    tsLastResults.foreach { ts => ts.isNaN shouldBe false }
  }
}
