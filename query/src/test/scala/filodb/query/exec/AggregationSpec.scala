package filodb.query.exec

import scala.annotation.tailrec
import scala.util.Random

import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.core.query.{CustomRangeVectorKey, LabelValue, RangeVector, RangeVectorKey}
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.query.{AggregationOperator, QueryConfig}

class AggregationSpec extends FunSpec with Matchers with ScalaFutures {

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val rand = new Random()
  val error = 0.00000001d

  it ("sum aggregation should work without by or without clauses") {
    val ignoreKey = CustomRangeVectorKey(
      Seq(LabelValue(ZeroCopyUTF8String("ignore"), ZeroCopyUTF8String("ignore"))))

    val samples: Array[RangeVector] = Array.fill(100)(new RangeVector {
      val data = Stream.from(0).map { n=>
        new TransientRow(Array(n.toLong, rand.nextDouble()))
      }.take(20)
      override def key: RangeVectorKey = ignoreKey
      override def rows: Iterator[RowReader] = data.iterator
    })

    val resultObs = RangeVectorAggregator.mapReduce(AggregationOperator.Sum,
      Nil, false, Observable.fromIterable(samples), rvk=>rvk)
    val result = resultObs.toListL.runAsync.futureValue
    result.size shouldEqual 1
    result(0).key shouldEqual ignoreKey
    val readyToAggr = samples.toList.map(_.rows.toList).transpose
    compareIter(result(0).rows.map(_.getDouble(1)), readyToAggr.map(_.map(_.getDouble(1)).sum).iterator)

    val resultObs2 = RangeVectorAggregator.mapReduce(AggregationOperator.Min,
      Nil, false, Observable.fromIterable(samples), rvk=>rvk)
    val result2 = resultObs2.toListL.runAsync.futureValue
    result2.size shouldEqual 1
    result2(0).key shouldEqual ignoreKey
    val readyToAggr2 = samples.toList.map(_.rows.toList).transpose
    compareIter(result2(0).rows.map(_.getDouble(1)), readyToAggr2.map(_.map(_.getDouble(1)).min).iterator)

    val resultObs3a = RangeVectorAggregator.mapReduce(AggregationOperator.Count,
      Nil, false, Observable.fromIterable(samples), rvk=>rvk)
    val resultObs3 = RangeVectorAggregator.mapReduce(AggregationOperator.Count,
      Nil, true, resultObs3a, rvk=>rvk)
    val result3 = resultObs3.toListL.runAsync.futureValue
    result3.size shouldEqual 1
    result3(0).key shouldEqual ignoreKey
    val readyToAggr3 = samples.toList.map(_.rows.toList).transpose
    compareIter(result3(0).rows.map(_.getDouble(1)), readyToAggr3.map(_.map(_.getDouble(1)).size.toDouble).iterator)

    val resultObs4a = RangeVectorAggregator.mapReduce(AggregationOperator.Avg,
      Nil, false, Observable.fromIterable(samples), rvk=>rvk)
    val resultObs4 = RangeVectorAggregator.mapReduce(AggregationOperator.Avg,
      Nil, true, resultObs4a, rvk=>rvk)
    val result4 = resultObs4.toListL.runAsync.futureValue
    result4.size shouldEqual 1
    result4(0).key shouldEqual ignoreKey
    val readyToAggr4 = samples.toList.map(_.rows.toList).transpose
    compareIter(result4(0).rows.map(_.getDouble(1)), readyToAggr4.map { v =>
      v.map(_.getDouble(1)).sum / v.map(_.getDouble(1)).size
    }.iterator)

  }

  @tailrec
  final private def compareIter(it1: Iterator[Double], it2: Iterator[Double]) : Unit = {
    (it1.hasNext, it2.hasNext) match{
      case (true, true) =>
        val v1 = it1.next()
        val v2 = it2.next()
        Math.abs(v1-v2) should be < error
        compareIter(it1, it2)
      case (false, false) => Unit
      case _ => fail("Unequal lengths")
    }
  }

}
