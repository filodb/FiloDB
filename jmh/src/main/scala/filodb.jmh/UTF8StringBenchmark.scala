package filodb.jmh

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.{Mode, Scope, State}
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.OutputTimeUnit

import filodb.memory._
import filodb.memory.format._

/**
 * Measures the speed of common string operations vs standard Java strings
 *
 * For a description of the JMH measurement modes, see
 * https://github.com/ktoso/sbt-jmh/blob/master/src/sbt-test/sbt-jmh/jmh-run/
 *    src/main/scala/org/openjdk/jmh/samples/JMHSample_02_BenchmarkModes.scala
 */
@State(Scope.Thread)
class UTF8StringBenchmark {
  import UTF8StringMedium._

  val str = "xylophonemania"
  val str2 = "xylophonemaniac"
  val zcStr = ZeroCopyUTF8String(str)
  val zcStr2 = ZeroCopyUTF8String(str2)

  val (base1, off1) = UTF8StringMedium(str)
  val (base2, off2) = UTF8StringMedium(str2)

  val nativeMem = new NativeMemoryManager(1024 * 1024)

  val native = str.utf8(nativeMem)
  val native2 = str2.utf8(nativeMem)

  // According to @ktosopl, be sure to return some value if possible so that JVM won't
  // optimize out the method body.  However JMH is apparently very good at avoiding this.
  // fastest loop possible using FiloVectorApply method
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def zeroCopyUtf8StrCompare(): Int = {
    zcStr.compare(zcStr2)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def nativeStrCompare(): Int = {
    str.compare(str2)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def onHeapUtf8StrCompare(): Int = {
    UTF8StringMedium.compare(base1, off1, base2, off2)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def offHeapUtf8StrCompare(): Int = {
    native.compare(native2)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def zeroCopyUtf8Substring(): ZeroCopyUTF8String = {
    zcStr.substring(2, 6)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def nativeSubstring(): String = {
    str.substring(2, 6)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def zeroCopyUtf8hash(): Int = {
    zcStr.hashCode
  }
}