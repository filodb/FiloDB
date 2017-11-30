package filodb.core.query

import scala.language.postfixOps

import scalaxy.loops._

import filodb.memory.format.{BinaryVector, FiloVector}

/**
 * An Aggregator for a single vector.
 * @param vectorPos the position of the vector within the input projection.  This tells the ChunkSetReader
 *                  which vector to pull.  The resulting ChunkSetReader passed into add() will have 1 vect
 */
abstract class OneVectorAggregator[@specialized(Int, Long, Double) T](vectorPos: Int)
extends ChunkAggregator {
  val positions = Array(vectorPos)

  final def add(orig: A, reader: ChunkSetReader): A = {
    reader.vectors(0) match {
      case bv: BinaryVector[T] if !bv.maybeNAs =>
        aggregateNoNAs(orig, bv)
      case f: FiloVector[T] =>
        aggregate(orig, f)
    }
  }

  def aggregateNoNAs(orig: A, v: BinaryVector[T]): A
  def aggregate(orig: A, v: FiloVector[T]): A
}

/**
 * Just an example of aggregation by summing doubles
 * TODO: while this should be fast, try SIMD  :D :D :D
 */
class SumDoublesAggregator(vectorPos: Int = 0) extends OneVectorAggregator[Double](vectorPos) {
  type A = DoubleAggregate
  val emptyAggregate = DoubleAggregate(0.0)

  final def aggregateNoNAs(orig: DoubleAggregate, v: BinaryVector[Double]): DoubleAggregate = {
    var localSum: Double = orig.value
    for { i <- 0 until v.length optimized } {
      localSum += v(i)
    }
    DoubleAggregate(localSum)
  }

  final def aggregate(orig: DoubleAggregate, v: FiloVector[Double]): DoubleAggregate = {
    var localSum: Double = orig.value
    for { i <- 0 until v.length optimized } {
      if (v.isAvailable(i)) localSum += v(i)
    }
    DoubleAggregate(localSum)
  }

  def combine(first: DoubleAggregate, second: DoubleAggregate): DoubleAggregate =
    DoubleAggregate(first.value + second.value)
}

/**
 * Counts the number of defined elements.  This should be super fast if the vector does not have NAs.
 */
class CountingAggregator(vectorPos: Int = 0) extends OneVectorAggregator[Any](vectorPos) {
  type A = IntAggregate
  val emptyAggregate = IntAggregate(0)

  final def aggregateNoNAs(orig: IntAggregate, v: BinaryVector[Any]): IntAggregate =
    IntAggregate(orig.value + v.length)

  final def aggregate(orig: IntAggregate, v: FiloVector[Any]): IntAggregate = {
    // TODO: Replace this with a much more efficient, NA bit counting method inlined into FiloVector itself
    var localCount = orig.value
    for { i <- 0 until v.length optimized } if (v.isAvailable(i)) localCount += 1
    IntAggregate(localCount)
  }

  def combine(first: IntAggregate, second: IntAggregate): IntAggregate =
    IntAggregate(first.value + second.value)
}