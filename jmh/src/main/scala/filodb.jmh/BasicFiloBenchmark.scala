package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.language.postfixOps

import org.openjdk.jmh.annotations.{Mode, Scope, State}
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.OutputTimeUnit
import scalaxy.loops._

import filodb.memory.format._
import filodb.memory.NativeMemoryManager

/**
 * Measures basic read benchmark with no NAs for an IntColumn.
 * Just raw read speed basically.
 * Measures read speed of different encodings (int, byte, diff) as well as
 * different read methods.
 *
 * For a description of the JMH measurement modes, see
 * https://github.com/ktoso/sbt-jmh/blob/master/src/sbt-test/sbt-jmh/
 *   jmh-run/src/main/scala/org/openjdk/jmh/samples/JMHSample_02_BenchmarkModes.scala
 */
@State(Scope.Thread)
class BasicFiloBenchmark {
  import VectorReader._
  import vectors.IntBinaryVector

  // Ok, create an IntColumn and benchmark it.
  val numValues = 10000
  val memFactory = new NativeMemoryManager(10 * 1024 * 1024)

  val randomInts = (0 until numValues).map(i => util.Random.nextInt)
  val randomIntsAray = randomInts.toArray

  val ivbuilder = IntBinaryVector.appendingVectorNoNA(memFactory, numValues)
  randomInts.foreach(ivbuilder.addData)
  val iv = FiloVector[Int](ivbuilder.toFiloBuffer)

  val byteIVBuilder = IntBinaryVector.appendingVectorNoNA(memFactory, numValues)
  randomInts.map(_ % 128).foreach(byteIVBuilder.addData)
  val byteVect = FiloVector[Int](byteIVBuilder.toFiloBuffer)

  val diffBuilder = IntBinaryVector.appendingVectorNoNA(memFactory, numValues)
  randomInts.map(10000 + _ % 128).foreach(diffBuilder.addData)
  val diffVect = FiloVector[Int](diffBuilder.toFiloBuffer)

  // According to @ktosopl, be sure to return some value if possible so that JVM won't
  // optimize out the method body.  However JMH is apparently very good at avoiding this.
  // fastest loop possible using FiloVectorApply method
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumAllIntsBinaryVectApply(): Int = {
    var total = 0
    for { i <- 0 until numValues optimized } {
      total += iv(i)
    }
    total
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumAllIntsFiloByteApply(): Int = {
    var total = 0
    for { i <- 0 until numValues optimized } {
      total += byteVect(i)
    }
    total
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumAllIntsFiloDiffApply(): Int = {
    var total = 0
    for { i <- 0 until numValues optimized } {
      total += diffVect(i)
    }
    total
  }
}
