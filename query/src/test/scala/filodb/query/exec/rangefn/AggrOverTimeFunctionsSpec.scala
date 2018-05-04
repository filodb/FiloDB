package filodb.query.exec.rangefn

import scala.collection.mutable
import scala.util.Random

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}

import filodb.query.{QueryConfig, RangeFunctionId}
import filodb.query.exec.{MutableSample, QueueBasedWindow}
import filodb.query.util.IndexedArrayQueue

class AggrOverTimeFunctionsSpec extends FunSpec with Matchers {

  val rand = new Random()
  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))

  it ("aggregation functions should work correctly on a sliding window") {

    val sum = RangeFunction(Some(RangeFunctionId.SumOverTime))
    val count = RangeFunction(Some(RangeFunctionId.CountOverTime))
    val avg = RangeFunction(Some(RangeFunctionId.AvgOverTime))
    val min = RangeFunction(Some(RangeFunctionId.MinOverTime))
    val max = RangeFunction(Some(RangeFunctionId.MaxOverTime))

    val fns = Array(sum, count, avg, min, max)

    val samples = Array.fill(1000) { rand.nextInt(1000) }
    val validationQueue = new mutable.Queue[Int]()
    var added = 0
    var removed = 0
    val dummyWindow = new QueueBasedWindow(new IndexedArrayQueue[MutableSample]())
    val toEmit = new MutableSample()

    while (removed < samples.size) {
      val addTimes = rand.nextInt(10)
      for { i <- 0 until addTimes } {
        if (added < samples.size) {
          validationQueue.enqueue(samples(added))
          fns.foreach(_.addToWindow(new MutableSample(added, samples(added))))
          added += 1
        }
      }

      if (validationQueue.nonEmpty) {
        sum.apply(0, 0, dummyWindow, toEmit, queryConfig)
        toEmit.value shouldEqual validationQueue.sum

        min.apply(0, 0, dummyWindow, toEmit, queryConfig)
        toEmit.value shouldEqual validationQueue.min

        max.apply(0, 0, dummyWindow, toEmit, queryConfig)
        toEmit.value shouldEqual validationQueue.max

        count.apply(0, 0, dummyWindow, toEmit, queryConfig)
        toEmit.value shouldEqual validationQueue.size

        avg.apply(0, 0, dummyWindow, toEmit, queryConfig)
        toEmit.value shouldEqual (validationQueue.sum.toDouble / validationQueue.size)
      }

      val removeTimes = rand.nextInt(validationQueue.size + 1)
      for { i <- 0 until removeTimes } {
        if (removed < samples.size) {
          validationQueue.dequeue()
          fns.foreach(_.removeFromWindow(new MutableSample(removed, samples(removed))))
          removed += 1
        }
      }
    }

  }

}
