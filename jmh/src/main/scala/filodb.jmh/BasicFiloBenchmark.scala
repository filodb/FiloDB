package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.language.postfixOps

import org.openjdk.jmh.annotations._
import scalaxy.loops._

import filodb.memory.format.vectors._
import filodb.memory.NativeMemoryManager

/**
 * Measures basic read benchmark with no NAs for a time series LongColumn.
 * Just raw read speed basically.
 * Roughly randomly increasing, constant increasing, probably using DDV to encode.
 *
 * For a description of the JMH measurement modes, see
 * https://github.com/ktoso/sbt-jmh/blob/master/src/sbt-test/sbt-jmh/
 *   jmh-run/src/main/scala/org/openjdk/jmh/samples/JMHSample_02_BenchmarkModes.scala
 */
@State(Scope.Thread)
class BasicFiloBenchmark {

  // Ok, create a LongColumn and benchmark it.
  val numValues = 1000
  val memFactory = new NativeMemoryManager(10 * 1024 * 1024)

  val randomLongs = (0 until numValues).map(i => util.Random.nextInt.toLong)

  val ivbuilder = LongBinaryVector.appendingVectorNoNA(memFactory, numValues)
  randomLongs.foreach(ivbuilder.addData)
  val iv = ivbuilder.optimize(memFactory)
  val ivReader = LongBinaryVector(iv)

  val dblBuilder = DoubleVector.appendingVectorNoNA(memFactory, numValues)
  randomLongs.map(_.toDouble).foreach(dblBuilder.addData)
  val dblReader= dblBuilder.reader.asDoubleReader

  val intBuilder = IntBinaryVector.appendingVectorNoNA(memFactory, numValues)
  randomLongs.map(n => (n / 256).toInt).foreach(intBuilder.addData)
  val intReader = intBuilder.reader.asIntReader

  val byteIVBuilder = LongBinaryVector.appendingVectorNoNA(memFactory, numValues)
  randomLongs.zipWithIndex.map { case (rl, i) => i * 10000 + (rl % 128) }.foreach(byteIVBuilder.addData)
  val byteVect = byteIVBuilder.optimize(memFactory)
  val byteReader = LongBinaryVector(byteVect)

  @TearDown
  def shutdown(): Unit = {
    memFactory.shutdown()
  }

  // According to @ktosopl, be sure to return some value if possible so that JVM won't
  // optimize out the method body.  However JMH is apparently very good at avoiding this.
  // fastest loop possible using FiloVectorApply method
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumAllLongsApply(): Long = {
    var total = 0L
    for { i <- 0 until numValues optimized } {
      total += ivReader(iv, i)
    }
    total
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumAllLongsIterate(): Long = {
    var total = 0L
    val it = ivReader.iterate(iv)
    for { i <- 0 until numValues optimized } {
      total += it.next
    }
    total
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumAllLongsSumMethod(): Double = {
    ivReader.sum(iv, 0, numValues - 1)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumDoublesSumMethod(): Double = {
    dblReader.sum(dblBuilder.addr, 0, numValues - 1)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumAllIntsSumMethod(): Long = {
    intReader.sum(intBuilder.addr, 0, numValues - 1)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumTimeSeriesBytesApply(): Long = {
    var total = 0L
    for { i <- 0 until numValues optimized } {
      total += byteReader(byteVect, i)
    }
    total
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumTimeSeriesBytesIterate(): Long = {
    var total = 0L
    val it = byteReader.iterate(byteVect)
    for { i <- 0 until numValues optimized } {
      total += it.next
    }
    total
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumTimeSeriesBytesSum(): Double = {
    byteReader.sum(byteVect, 0, numValues - 1)
  }
}
