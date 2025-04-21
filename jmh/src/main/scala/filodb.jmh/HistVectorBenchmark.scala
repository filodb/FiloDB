package filodb.jmh

import java.util.concurrent.TimeUnit

import org.agrona.concurrent.UnsafeBuffer
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import filodb.memory.NativeMemoryManager
import filodb.memory.format._

@State(Scope.Thread)
class HistVectorBenchmark {
  import vectors._

  val memFactory = new NativeMemoryManager(100 * 1024 * 1024)
  final val numDataPoints = 60
  val bucketScheme = Base2ExpHistogramBuckets(3, -78, 126)
  val counts = Array.fill(bucketScheme.numBuckets)(5L)
  val buffer = new UnsafeBuffer(new Array[Byte](4096))

  val appender = HistogramVector.appending(memFactory, 15000) // 15k bytes is default blob size
  val appender2 = HistogramVector.appending(memFactory, 15000) // 15k bytes is default blob size
  (0 until numDataPoints).foreach { i =>
    val hist = LongHistogram(bucketScheme, counts)
    hist.serialize(Some(buffer))
    if (appender2.addData(buffer) != Ack) {
      throw new RuntimeException(s"Failed to add histogram $i")
    }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(numDataPoints)
  def ingestion(): Unit = {
    (0 until numDataPoints).foreach { i =>
      val hist = LongHistogram(bucketScheme, counts)
      hist.serialize(Some(buffer))
      if (appender.addData(buffer) != Ack) {
        throw new RuntimeException(s"Failed to add histogram $i")
      }
    }
    appender.reset()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def queries(blackhole: Blackhole): Unit = {
    val r2 = appender2.reader.asHistReader
    val len = r2.length(MemoryAccessor.nativePtrAccessor, appender.addr)
    (0 until len).foreach { i =>
      val h = r2(i)
      blackhole.consume(h)
    }
  }

  val appenderExp = HistogramVector.appendingExp(memFactory, 15000) // 15k bytes is default blob size
  val appenderExp2 = HistogramVector.appendingExp(memFactory, 15000) // 15k bytes is default blob size
  (0 until numDataPoints).foreach { i =>
    val hist = LongHistogram(bucketScheme, counts)
    hist.serialize(Some(buffer))
    if (appenderExp2.addData(buffer) != Ack) {
      throw new RuntimeException(s"Failed to add histogram $i")
    }
  }


  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(numDataPoints)
  def ingestionExpVector(): Unit = {
    (0 until 60).foreach { i =>
      val hist = LongHistogram(bucketScheme, counts)
      hist.serialize(Some(buffer))
      if (appenderExp.addData(buffer) != Ack) {
        throw new RuntimeException(s"Failed to add histogram $i")
      }
    }
    appenderExp.reset()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def queriesExpVector(blackhole: Blackhole): Unit = {
    val r2 = appenderExp2.reader.asHistReader
    val len = r2.length(MemoryAccessor.nativePtrAccessor, appenderExp2.addr)
    (0 until len).foreach { i =>
      val h = r2(i)
      blackhole.consume(h)
    }
  }

}
