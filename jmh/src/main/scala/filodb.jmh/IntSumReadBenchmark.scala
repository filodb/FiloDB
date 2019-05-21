package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.language.postfixOps

import ch.qos.logback.classic.{Level, Logger}
import org.openjdk.jmh.annotations._
import scalaxy.loops._

import filodb.core.{NamesTestData, TestData}
import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.core.store.ChunkSet
import filodb.memory.format.{vectors => bv, TupleRowReader, UnsafeUtils}

object IntSumReadBenchmark {
  val dataset = Dataset("dataset", Seq("part:int"), Seq("int:int", "rownum:long"), "rownum",
    DatasetOptions.DefaultOptions)
  val rowIt = Iterator.from(0).map { row => (Some(scala.util.Random.nextInt), Some(row.toLong), Some(0)) }
  val partKey = NamesTestData.defaultPartKey
  val rowColumns = Seq("int", "rownum", "part")

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)
}

/**
 * Microbenchmark of simple integer summing of Filo BinaryVector chunks,
 * mostly to see what the theoretical peak output of scanning speeds can be.
 */
@State(Scope.Thread)
class IntSumReadBenchmark {
  import IntSumReadBenchmark._
  val NumRows = 10000

  val chunkSet = ChunkSet(dataset, partKey, rowIt.map(TupleRowReader).take(NumRows).toSeq, TestData.nativeMem)
  val intVectAddr = UnsafeUtils.addressFromDirectBuffer(chunkSet.chunks(0))
  val intReader   = bv.IntBinaryVector(intVectAddr)

  @TearDown
  def shutdown(): Unit = {
    TestData.nativeMem.shutdown()
  }

  /**
   * Random-access apply() method for reading each element
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def applyVectorScan(): Int = {
    var total = 0
    for { i <- 0 until NumRows optimized } {
      total += intReader(intVectAddr, i)
    }
    total
  }

  /**
   * Use iterate() method to read each element
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def iterateScan(): Int = {
    val it = intReader.iterate(intVectAddr, 0)
    var sum = 0
    for { i <- 0 until NumRows optimized } {
      sum += it.next
    }
    sum
  }
}