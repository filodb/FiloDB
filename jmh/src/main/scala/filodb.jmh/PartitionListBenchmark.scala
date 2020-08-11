package filodb.jmh

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._
import spire.syntax.cfor._

/**
 * Microbenchmark involving comparison of data structures for holding partition lists in a shard.
 * Requirements:
 *  extremely fast, O(1) integer key lookup, is memory efficient, and
 *  allows me to iterate through keys/values in ascending integer key order also.  Also needs to be mutable.
 *
 * TODO: profile a faster one based on array of arrays, and using unsafe perhaps.
 */
@State(Scope.Thread)
class PartitionListBenchmark {
  val lhm = new collection.mutable.LinkedHashMap[Int, String]
  cforRange { 0 until 1000000 } { i => lhm(i) = "shoo" }

  val jlhm = new java.util.LinkedHashMap[Int, String]
  cforRange { 0 until 1000000 } { i => jlhm.put(i, "shoo") }

  val abuf = collection.mutable.ArrayBuffer.fill(1000000)("shoo")



  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def scalaLinkedMapLookup(): String = {
    lhm(5132)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def javaLinkedMapLookup(): String = {
    jlhm.get(5132)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def arrayBufferLookup(): String = {
    abuf(5132)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def javaLinkedMapRemove100(): Unit = {
    cforRange { 5100 to 5199 } { i =>
      jlhm.remove(i)
    }
  }
}
