package filodb.core.query

import enumeratum.EnumEntry.Snakecase
import enumeratum.{Enum, EnumEntry}
import monix.eval.Task
import monix.reactive.Observable
import org.scalactic._
import scalaxy.loops._

import filodb.core.metadata._
import filodb.core.SingleKeyTypes._

/**
 * A Combiner folds over a stream of per-partition aggregates to produce a combined aggregate.
 * The most simple case would output the same type of aggregate using the original aggregator, like sum.
 * More interesting cases include histograms, topK, bottomK, grouping functions, etc.
 */
trait Combiner {
  def fold(aggregateStream: Observable[Aggregate[_]]): Task[Aggregate[_]]
}

/**
 * The SimpleCombiner just performs the same aggregation as the original aggregate.
 */
class SimpleCombiner(val aggregator: Aggregator) extends Combiner {
  def fold(aggregateStream: Observable[Aggregate[_]]): Task[Aggregate[_]] =
    aggregateStream.foldLeftL(aggregator.emptyAggregate) { case (acc, newItem) =>
      aggregator.combine(acc, newItem.asInstanceOf[aggregator.A])
    }
}

final case class HistogramBucket(max: Double, count: Int) {
  override def toString: String = s"$max | count=$count"
}

// A histogram storing counts for values in configurable Double-based buckets
class HistogramAggregate(buckets: Array[Double]) extends Aggregate[HistogramBucket] {
  val bucketHash = buckets.hashCode
  val counts = new Array[Int](buckets.size)
  var noBucketCount = 0     // number of values that don't fall in any bucket

  def result: Array[HistogramBucket] = buckets.zip(counts).map { case (b, c) => HistogramBucket(b, c) }

  // Increments the count in the bucket to which value belongs
  def increment(value: Double): Unit = {
    val bucket = buckets.indexWhere(_ >= value)
    if (bucket >= 0) { counts(bucket) += 1 }
    else             { noBucketCount += 1 }
  }

  def merge(other: HistogramAggregate): Unit = {
    require(other.bucketHash == bucketHash, s"Buckets of $other not equal to mine")
    for { i <- 0 until buckets.size optimized } {
      counts(i) += other.counts(i)
    }
  }
}

object HistogramAggregate {
  /**
   * Creates a HistogramAggregate using a geometric series of steps, each step a linear increase in
   * exponent until the maxValue is reached.
   */
  def apply(maxValue: Double, numBuckets: Int = 10): HistogramAggregate =
    new HistogramAggregate(geometricBuckets(maxValue, numBuckets))

  def geometricBuckets(maxValue: Double, numBuckets: Int = 10): Array[Double] =
    (1 to numBuckets).map(n => Math.pow(maxValue, n.toDouble / numBuckets)).toArray
}

/**
 * The HistogramCombiner computes a histogram using primitive aggregate values from individual partitions.
 */
class HistogramCombiner(buckets: Array[Double]) extends Combiner {
  def fold(aggregateStream: Observable[Aggregate[_]]): Task[Aggregate[_]] = {
    val histo = new HistogramAggregate(buckets)
    aggregateStream.foldLeftL(histo) { case (histo, newAgg: NumericAggregate) =>
      histo.increment(newAgg.doubleValue)
      histo
    }
  }
}

final case class InvalidAggregator(combFunc: CombinerFunction, aggregator: Aggregator)
extends InvalidFunctionSpec {
  override def toString: String = s"Combiner $combFunc does not support aggregator $aggregator"
}

/**
 * CombinerFunctions produce combiner instances given input arguments.
 * NOTE: CombinerFunction.validate can rule out some aggregators if it can only work with a subset.
 */
sealed trait CombinerFunction extends FunctionValidationHelpers with EnumEntry with Snakecase {
  def validate(aggregator: Aggregator, args: Seq[String]): Combiner Or InvalidFunctionSpec
}

object CombinerFunction extends Enum[CombinerFunction] {
  val values = findValues
  val default = Simple.entryName

  case object Simple extends CombinerFunction {
    def validate(aggregator: Aggregator, arg: Seq[String]): Combiner Or InvalidFunctionSpec =
      Good(new SimpleCombiner(aggregator))
  }

  /**
   * histogram string argument:
   *   "<maxValue>"   - a single double value specifying the maximum value, with default 10 buckets
   *   "<maxValue> <numBuckets>" - maxValue: double, followed by int numBuckets
   */
  case object Histogram extends CombinerFunction {
    def getBuckets(args: Seq[String]): Array[Double] Or InvalidFunctionSpec =
      if (args.length < 1 || args.length > 2) {
        Bad(WrongNumberArguments(args.length, 1))
      } else {
        for { maxValue <- parseParam(DoubleKeyType, args(0))
              numBuckets <- parseParam(IntKeyType, args.applyOrElse(1, { n: Int => "10" })) }
        yield { HistogramAggregate.geometricBuckets(maxValue, numBuckets) }
      }

    def validate(aggregator: Aggregator, args: Seq[String]): Combiner Or InvalidFunctionSpec =
      aggregator.emptyAggregate match {
        case n: NumericAggregate => getBuckets(args).map(buckets => new HistogramCombiner(buckets))
        case other: Aggregate[_] => Bad(InvalidAggregator(this, aggregator))
      }
  }
}