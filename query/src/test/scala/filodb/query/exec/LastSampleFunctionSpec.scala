package filodb.query.exec

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.duration._
import scala.util.Random

import filodb.query.exec.rangefn.{LastSampleChunkedFunctionD, LastSampleFunction, RawDataWindowingSpec}

class LastSampleFunctionSpec extends RawDataWindowingSpec {

  val rand = new Random()
  val now = System.currentTimeMillis()
  // stddev higher than mean to simulate skipped samples
  val samples = generateRandomRawCounterSeries(2000, 20000, 25000, now)
  val rv = timeValueRV(samples)
  val w = 5.minutes.toMillis     // window size = lookback time

  val chunkedLSFunc = new LastSampleChunkedFunctionD

  it ("should work for various start times") {
    val step = 2000
    (-20000 to 20000).by(2500).foreach{ diff =>
      val start = now + diff
      val end = start + 100000L

      val lastSamplesIter = new SlidingWindowIterator(rv.rows, start, step, end, 0, LastSampleFunction, queryConfig)
      validateLastSamples(samples, lastSamplesIter, start, end, step)

      val chunkedIter = new ChunkedWindowIteratorD(rv, start, step, end, w, chunkedLSFunc, queryConfig)
      validateLastSamples(samples, chunkedIter, start, end, step)
    }
  }

  it ("test") {
//    val step = 2000
////    (-20000 to 20000).by(2500).foreach{ diff =>
//      val start = now + 1000
//      val end = start + 100000L
//
//      println(s" rv:")
////      val lastSamplesIter = new SlidingWindowItera
////      tor(rv.rows, start, step, end, 0, LastSampleFunction, queryConfig)
////      validateLastSamples(samples, lastSamplesIter, start, end, step)
//      val chunkedIter = new ChunkedWindowIteratorD(rv, start, step, end, w, chunkedLSFunc, queryConfig)
var data = Seq(1.5, 2.5, 3.5, 4.5, 5.5)
    val rv = timeValueRV(data)
    val list = rv.rows.map(x => (x.getLong(0), x.getDouble(1))).toList
    println("input list:" + list)


    val windowSize = 100
    val step = 20

    val chunkedIt = new ChunkedWindowIteratorD(rv, 100000, 5000, 120000, 30000,
      new LastSampleChunkedFunctionD, queryConfig)

    val aggregated = chunkedIt.map(x => (x.getLong(0), x.getDouble(1))).toList
    println("aggregated:" + aggregated)

      //validateLastSamples(samples, chunkedIter, start, end, step)
//    }
  }

//  it("should correctly do changes") {
//    var data = Seq(1.5, 2.5, 3.5, 4.5, 5.5)
//    val rv = timeValueRV(data)
//    val list = rv.rows.map(x => (x.getLong(0), x.getDouble(1))).toList
//
//    val windowSize = 100
////    val step = 20
//
//    val chunkedIt = new ChunkedWindowIteratorD(rv, 100000, 20000, 150000, 30000,
//      new ChangesChunkedFunctionD(), queryConfig)
//    val aggregated = chunkedIt.map(x => (x.getLong(0), x.getDouble(1))).toList
//    aggregated shouldEqual List((100000, 0.0), (120000, 2.0), (140000, 2.0))
//  }

  it ("should work for various steps") {
    val start = now
    val end = start + 100000L
    (5000 to 100000).by(5000).foreach { step =>
      val lastSamplesIter = new SlidingWindowIterator(rv.rows, start, step, end, 0, LastSampleFunction, queryConfig)
      validateLastSamples(samples, lastSamplesIter, start, end, step)

      val chunkedIter = new ChunkedWindowIteratorD(rv, start, step, end, w, chunkedLSFunc, queryConfig)
      validateLastSamples(samples, chunkedIter, start, end, step)
    }
  }

  it ("should emit single sample for start==end") {
    (1 to 10).foreach { _ =>
      val start = now + ThreadLocalRandom.current().nextLong(80000)
      val end = start
      val step = 1
      val lastSamplesIter = new SlidingWindowIterator(rv.rows, start, step, end, 0, LastSampleFunction, queryConfig)
      validateLastSamples(samples, lastSamplesIter, start, end, step)

      val chunkedIter = new ChunkedWindowIteratorD(rv, start, step, end, w, chunkedLSFunc, queryConfig)
      validateLastSamples(samples, chunkedIter, start, end, step)
    }
  }

  it ("should return NaN when no reported samples for more than 5 minutes - static samples") {
    // note std dev for interval between reported samples is 5 mins
    val samplesWithLongGap = Seq((59725569L,1.524759725569E12), (60038121L,1.524760038121E12),
                (60370409L,1.524760370409E12), (60679268L,1.524760679268E12), (60988895L,1.524760988895E12))
    val rvWithLongGap = timeValueRV(samplesWithLongGap)
    val start = 60330762L
    val end = 63030762L
    val step = 60000
    val lastSamplesIter = new SlidingWindowIterator(rvWithLongGap.rows,
                                                    start, step, end, 0, LastSampleFunction, queryConfig)
    validateLastSamples(samplesWithLongGap, lastSamplesIter, start, end, step)

    val chunkedIter = new ChunkedWindowIteratorD(rvWithLongGap, start, step, end, w, chunkedLSFunc, queryConfig)
    validateLastSamples(samplesWithLongGap, chunkedIter, start, end, step)
  }

  it ("should return NaN when no reported samples for more than 5 minutes - test case 2 dynamic samples ") {
    // note std dev for interval between reported samples is 5 mins
    val samplesWithLongGap = generateRandomRawCounterSeries(5, 300.seconds.toMillis, 50000, now)
    val rvWithLongGap = timeValueRV(samplesWithLongGap)
    val start = now + 300.seconds.toMillis
    val end = now + 300.seconds.toMillis * 10
    val step = 60000
    val lastSamplesIter = new SlidingWindowIterator(rvWithLongGap.rows,
      start, step, end, 0, LastSampleFunction, queryConfig)
    validateLastSamples(samplesWithLongGap, lastSamplesIter, start, end, step)

    val chunkedIter = new ChunkedWindowIteratorD(rvWithLongGap, start, step, end, w, chunkedLSFunc, queryConfig)
    validateLastSamples(samplesWithLongGap, chunkedIter, start, end, step)
  }

  it ("should return NaN when no reported samples for more than 5 minutes - test case 3 with more samples") {
    // note std dev for interval between reported samples is 5 mins
    val samplesWithLongGap = generateRandomRawCounterSeries(5000, 300.seconds.toMillis, 50000, now)
    val rvWithLongGap = timeValueRV(samplesWithLongGap)
    val start = now + 300.seconds.toMillis
    val end = now + 300.seconds.toMillis * 10
    val step = 60000
    val lastSamplesIter = new SlidingWindowIterator(rvWithLongGap.rows,
      start, step, end, 0, LastSampleFunction, queryConfig)
    validateLastSamples(samplesWithLongGap, lastSamplesIter, start, end, step)

    val chunkedIter = new ChunkedWindowIteratorD(rvWithLongGap, start, step, end, w, chunkedLSFunc, queryConfig)
    validateLastSamples(samplesWithLongGap, chunkedIter, start, end, step)
  }

  def generateRandomRawCounterSeries(numSamples: Int,
                                     intervalMean: Long,
                                     intervalStdDev: Long,
                                     start: Long = System.currentTimeMillis()): Seq[(Long, Double)] = {
    var time = start
    (0 until numSamples).map { _ =>
      var randomness = Math.abs(rand.nextGaussian * intervalStdDev).toLong
      // randomness should not be < -intervalMean otherwise time will reduce
      if (randomness < -intervalMean) randomness = intervalMean
      time  = time + intervalMean + randomness
      (time, time.toDouble)
    }
  }

  def iteratorOfMutableRowReaders(data: Seq[(Long, Double)]): Iterator[TransientRow] = {
    new Iterator[TransientRow] {
      var row = 0
      val sample = new TransientRow()
      override def hasNext: Boolean = row < data.size
      override def next(): TransientRow = {
        sample.setValues(data(row)._1, data(row)._2)
        row += 1
        sample
      }
    }
  }

  def validateLastSamples(input: Seq[(Long, Double)],
                          output: Iterator[TransientRow],
                          start: Long,
                          end: Long,
                          step: Int): Unit = {
    val validationMap = new java.util.TreeMap[Long, Double]()
    input.foreach(s => validationMap.put(s._1, s._2))
    var cur = start
    while (cur <= end) {
      val observed = output.next()
      observed.timestamp shouldEqual cur
      val expected = validationMap.floorEntry(cur)
      if (expected == null || cur - expected.getKey > 5.minutes.toMillis) {
        observed.value.isNaN shouldEqual true
      } else {
        observed.value shouldEqual expected.getValue
      }
      cur = cur + step
    }
    output.hasNext shouldEqual false
  }
}
