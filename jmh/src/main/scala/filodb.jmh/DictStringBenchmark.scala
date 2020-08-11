package filodb.jmh

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._
import spire.syntax.cfor._

import filodb.memory.NativeMemoryManager
import filodb.memory.format._
import filodb.memory.format.MemoryReader._

/**
 * Measures read speed for a dictionary-encoded string Filo column.
 * Has tests for both no-NA and some-NA read speed.
 * Since real world datasets tend to contain lots of string data, this is
 * probably a much more realistic speed benchmark than Ints.
 * Simulate a somewhat-realistic by varying string length and using alphanum chars
 *
 * TODO: compare against Seq[String] encoding in MessagePack, etc.
 */
@State(Scope.Thread)
class DictStringBenchmark {
  import scala.util.Random.{alphanumeric, nextInt, nextFloat}
  import ZeroCopyUTF8String._
  import vectors.UTF8Vector

  val numValues = 10000
  val memFactory = new NativeMemoryManager(10 * 1024 * 1024)
  val acc = nativePtrReader
  // NOTE: results show that time spent is heavily influenced by ratio of unique strings...
  val numUniqueStrings = 500
  val maxStringLength = 15
  val minStringLength = 5
  val naChance = 0.05    //5% of values will be NA

  def randString(len: Int): String = alphanumeric.take(len).mkString

  val uniqueStrings = (0 until numUniqueStrings).map { i =>
    randString(minStringLength + nextInt(maxStringLength - minStringLength))
  }
  val randomStrings = (0 until numValues).map(i => uniqueStrings(nextInt(numUniqueStrings)))
  val scNoNA = UTF8Vector(memFactory, randomStrings.map(_.utf8))

  def shouldNA: Boolean = nextFloat < naChance

  val scNA = UTF8Vector(memFactory, randomStrings.map(str => if (shouldNA) ZeroCopyUTF8String.empty else str.utf8))
  val scNAPtr = scNA.optimize(memFactory)

  @TearDown
  def shutdown(): Unit = {
    memFactory.shutdown()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def rawStringLengthTotal(): Int = {
    var totalLen = 0
    cforRange { 0 until numValues } { i =>
      totalLen += scNoNA(i).length
    }
    totalLen
  }

  // TODO: also a benchmark for the foreach/fold of a column with no NA's?

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  // Measures iterate() and NA read speed
  def withNAlengthTotal(): Unit = {
    var totalLen = 0

    val reader = UTF8Vector(acc, scNAPtr)
    val it = reader.iterate(acc, scNAPtr)
    cforRange { 0 until numValues } { i =>
      totalLen += it.next.length
    }
    totalLen
  }
}
