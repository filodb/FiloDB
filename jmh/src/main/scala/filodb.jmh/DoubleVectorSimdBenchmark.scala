package filodb.jmh

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import filodb.memory.NativeMemoryManager
import filodb.memory.format.BinaryAppendableVector
import filodb.memory.format.MemoryReader._
import filodb.memory.format.vectors._

/**
 * JMH benchmark comparing Scala DoubleVector sum/count vs SIMD-accelerated Rust JNI implementations.
 *
 * Run with:
 *   sbt -Drust.optimize=true "jmh/jmh:run -rf json -i 5 -wi 3 -f 2 \
 *     -jvmArgsAppend -Xmx4g filodb.jmh.DoubleVectorSimdBenchmark"
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(2)
class DoubleVectorSimdBenchmark {

  @Param(Array("100", "1000", "10000", "100000"))
  var numValues: Int = _

  val memFactory = new NativeMemoryManager(100 * 1024 * 1024)
  val acc = nativePtrReader

  // Initialized in setup; values filled with fixed-seed random doubles
  var dblBuilder: BinaryAppendableVector[Double] = _
  var dblReader: DoubleVectorDataReader = _
  var vectorAddr: Long = _   // base vector address (for Scala reader calls)
  var dataAddr: Long = _     // vector + OffsetData (for native calls)

  @Setup(Level.Trial)
  def setup(): Unit = {
    val rng = new java.util.Random(42L) // fixed seed for reproducibility
    dblBuilder = DoubleVector.appendingVectorNoNA(memFactory, numValues)
    var i = 0
    while (i < numValues) {
      dblBuilder.addData(rng.nextDouble() * 1000.0)
      i += 1
    }
    dblReader = dblBuilder.reader.asDoubleReader
    vectorAddr = dblBuilder.addr
    dataAddr = vectorAddr + 8 // PrimitiveVector.OffsetData = 8
  }

  @TearDown(Level.Trial)
  def teardown(): Unit = {
    memFactory.shutdown()
  }

  // ── Sum with NaN ignore (the default Scala path) ──

  @Benchmark
  def scalaSumIgnoreNaN(): Double = {
    dblReader.sum(acc, vectorAddr, 0, numValues - 1, ignoreNaN = true)
  }

  @Benchmark
  def simdSumIgnoreNaN(): Double = {
    SimdNativeMethods.simdSumDouble(dataAddr, 0, numValues - 1, ignoreNaN = true)
  }

  // ── Sum without NaN checking ──

  @Benchmark
  def scalaSumNoNaN(): Double = {
    dblReader.sum(acc, vectorAddr, 0, numValues - 1, ignoreNaN = false)
  }

  @Benchmark
  def simdSumNoNaN(): Double = {
    SimdNativeMethods.simdSumDouble(dataAddr, 0, numValues - 1, ignoreNaN = false)
  }

  // ── Count non-NaN values ──

  @Benchmark
  def scalaCount(): Int = {
    dblReader.count(acc, vectorAddr, 0, numValues - 1)
  }

  @Benchmark
  def simdCount(): Int = {
    SimdNativeMethods.simdCountDouble(dataAddr, 0, numValues - 1)
  }
}
