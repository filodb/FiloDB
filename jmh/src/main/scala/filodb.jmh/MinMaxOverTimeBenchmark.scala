package filodb.jmh

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import filodb.memory.NativeMemoryManager
import filodb.memory.format._
import filodb.memory.format.vectors._
import filodb.query.exec.rangefn.{MaxOverTimeChunkedFunctionD, MinOverTimeChunkedFunctionD}

/**
 * Microbenchmark for min_over_time / max_over_time over one chunk of doubles, in windows of windowRows rows that
 * do not overlap (lookback == step). One op = one chunk: min and max of every window.
 *
 * encoding: double64 = non-integral values; deltaDelta = integral values; deltaDeltaConst = integral values on a
 * line (eg a histogram max/min column that does not change).
 */
@State(Scope.Thread)
class MinMaxOverTimeBenchmark {
  @Param(Array("double64", "deltaDelta", "deltaDeltaConst"))
  var encoding: String = _

  @Param(Array("6", "30"))
  var windowRows: Int = _

  final val numRows = 360   // eg 1 hour chunk at 10s interval

  val memFactory = new NativeMemoryManager(10 * 1024 * 1024)
  var tsVect: BinaryVector.BinaryVectorPtr = _
  var tsReader: LongVectorDataReader = _
  var valueVect: BinaryVector.BinaryVectorPtr = _
  var valueReader: DoubleVectorDataReader = _
  val minFunc = new MinOverTimeChunkedFunctionD
  val maxFunc = new MaxOverTimeChunkedFunctionD
  val acc = MemoryAccessor.nativePtrAccessor

  @Setup
  def setup(): Unit = {
    val rnd = new scala.util.Random(0)
    val tsAppender = LongBinaryVector.appendingVectorNoNA(memFactory, numRows)
    (0 until numRows).foreach(i => tsAppender.addData(1700000000000L + i * 10000L))
    tsVect = tsAppender.optimize(memFactory)
    tsReader = LongBinaryVector(acc, tsVect)

    val appender = DoubleVector.appendingVectorNoNA(memFactory, numRows)
    (0 until numRows).foreach { i =>
      appender.addData(encoding match {
        case "double64"        => rnd.nextDouble() * 1000
        case "deltaDelta"      => (rnd.nextInt(1000) + i).toDouble
        case "deltaDeltaConst" => 250.0
      })
    }
    valueVect = appender.optimize(memFactory)
    valueReader = DoubleVector(acc, valueVect)
  }

  @TearDown
  def shutdown(): Unit = memFactory.shutdown()

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def minMaxOverTime(blackhole: Blackhole): Unit = {
    var start = 0
    while (start < numRows) {
      val end = Math.min(start + windowRows - 1, numRows - 1)
      minFunc.reset()
      maxFunc.reset()
      minFunc.addTimeDoubleChunksWithTimestamp(acc, tsVect, tsReader, acc, valueVect, valueReader, start, end)
      maxFunc.addTimeDoubleChunksWithTimestamp(acc, tsVect, tsReader, acc, valueVect, valueReader, start, end)
      blackhole.consume(minFunc.min)
      blackhole.consume(maxFunc.max)
      start += windowRows
    }
  }
}
