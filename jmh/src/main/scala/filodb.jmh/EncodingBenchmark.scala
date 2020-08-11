package filodb.jmh

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.{Mode, Scope, State}
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.OutputTimeUnit
import spire.syntax.cfor._

import filodb.memory.NativeMemoryManager
import filodb.memory.format._

/**
 * Measures the speed of encoding different types of data,
 * including just Filo vector encoding and encoding from RowReaders.
 *
 * For a description of the JMH measurement modes, see
 * https://github.com/ktoso/sbt-jmh/blob/master/src/sbt-test/sbt-jmh/jmh-run/
 *   src/main/scala/org/openjdk/jmh/samples/JMHSample_02_BenchmarkModes.scala
 */
@State(Scope.Thread)
class EncodingBenchmark {
  import scala.util.Random.{alphanumeric, nextInt}
  import vectors._

  // Ok, create an IntColumn and benchmark it.
  val numValues = 10000
  val memFactory = new NativeMemoryManager(100 * 1024 * 1024)

  val randomInts = (0 until numValues).map(i => util.Random.nextInt)
  val randomLongs = randomInts.map(_.toLong)

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

  val intArray = randomInts.toArray

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def newIntVectorEncoding(): Unit = {
    val cb = IntBinaryVector.appendingVector(memFactory, numValues)
    cforRange { 0 until numValues } { i =>
      cb.addData(intArray(i))
    }
    cb.optimize(memFactory)
    cb.dispose()
  }

  val cbAdder = IntBinaryVector.appendingVector(memFactory, numValues)

  // TODO: post method to free up space

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def growableIntVectorAddData(): Unit = {
    cbAdder.reset()
    cforRange { 0 until numValues } { i =>
      cbAdder.addData(intArray(i))
    }
  }

  val noNAAdder = IntBinaryVector.appendingVectorNoNA(memFactory, numValues)

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def noNAIntVectorAddData(): Unit = {
    noNAAdder.reset()
    cforRange { 0 until numValues } { i =>
      noNAAdder.addData(intArray(i))
    }
  }

  val utf8strings = randomStrings.map(ZeroCopyUTF8String.apply).toArray

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def newUtf8VectorEncoding(): Unit = {
    val cb = UTF8Vector.appendingVector(memFactory, numValues, maxStringLength * numUniqueStrings)
    cforRange { 0 until numValues } { i =>
      cb.addData(utf8strings(i))
    }
    cb.optimize(memFactory)
    cb.dispose()
  }
  // TODO: RowReader based vector building

  val utf8cb = UTF8Vector.appendingVector(memFactory, numValues, maxStringLength * numUniqueStrings)
  cforRange { 0 until numValues } { i =>
    utf8cb.addData(utf8strings(i))
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def newUtf8AddVector(): Unit = {
    val cb = UTF8Vector.appendingVector(memFactory, numValues, maxStringLength * numUniqueStrings)
    cb.addVector(utf8cb)
    cb.dispose()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def newDictUtf8VectorEncoding(): Unit = {
    val hint = Encodings.AutoDictString(samplingRate = 0.5)
    UTF8Vector(memFactory, utf8strings).optimize(memFactory, hint)
  }
}
