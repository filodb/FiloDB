package filodb.core.query

import org.velvia.filo.{FiloVector, BinaryVector}
import scala.language.postfixOps
import scala.reflect.ClassTag
import scalaxy.loops._

abstract class OneVectorAggregate[@specialized(Int, Long, Double) T, R: ClassTag](vectorPos: Int)
extends Aggregate[R] {
  final def add(reader: ChunkSetReader): Aggregate[R] = {
    reader.vectors(vectorPos) match {
      case bv: BinaryVector[T] if !bv.maybeNAs =>
        aggregateNoNAs(bv, reader.info.numRows)
      case f: FiloVector[T] =>
        aggregate(f, reader.info.numRows)
    }
    this
  }

  def aggregateNoNAs(v: BinaryVector[T], length: Int): Unit
  def aggregate(v: FiloVector[T], length: Int): Unit
}

/**
 * Just an example of aggregation by summing doubles
 * TODO: while this should be fast, try SIMD  :D :D :D
 */
class SumDoublesAggregate(vectorPos: Int = 0) extends OneVectorAggregate[Double, Double](vectorPos) {
  var sum = 0.0
  final def aggregateNoNAs(v: BinaryVector[Double], length: Int): Unit = {
    var localSum = 0.0
    for { i <- 0 until length optimized } {
      localSum += v(i)
    }
    sum += localSum
  }

  final def aggregate(v: FiloVector[Double], length: Int): Unit =
    for { i <- 0 until length optimized } {
      if (v.isAvailable(i)) sum += v(i)
    }

  def result: Array[Double] = Array(sum)
}

/**
 * Counts the number of defined elements.  This should be super fast if the vector does not have NAs.
 */
class CountingAggregate(vectorPos: Int = 0) extends OneVectorAggregate[Any, Int](vectorPos) {
  var count = 0
  final def aggregateNoNAs(v: BinaryVector[Any], length: Int): Unit =
    count += length

  final def aggregate(v: FiloVector[Any], length: Int): Unit =
    for { i <- 0 until length optimized } {
      if (v.isAvailable(i)) count += 1
    }

  def result: Array[Int] = Array(count)
}