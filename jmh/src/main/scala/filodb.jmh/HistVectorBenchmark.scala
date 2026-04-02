package filodb.jmh

import java.util.concurrent.TimeUnit

import org.agrona.concurrent.UnsafeBuffer
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import filodb.memory.NativeMemoryManager
import filodb.memory.format._
import filodb.memory.format.vectors.BinaryHistogram

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

  // ── sum() benchmark: simulates rate(delta_hist[5m]) hot path ───────
  // 20-bucket geometric histograms, 30 samples (5m window @ 10s interval)
  val scheme20 = GeometricBuckets(1.0, 2.0, 20)
  final val numSamplesInWindow = 30
  val sumAppender20: BinaryAppendableVector[org.agrona.DirectBuffer] = {
    val app = HistogramVector.appending(memFactory, 15000)
    val rng = new java.util.Random(42)
    (0 until numSamplesInWindow).foreach { _ =>
      val raw = new Array[Long](20)
      (0 until 20).foreach { i => raw(i) = rng.nextInt(1000).toLong }
      (1 until 20).foreach { i => raw(i) += raw(i - 1) }
      BinaryHistogram.writeDelta(scheme20, raw, buffer)
      if (app.addData(buffer) != Ack) {
        throw new RuntimeException("Failed to add 20-bucket histogram")
      }
    }
    app
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput, Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumDeltaHist20Buckets(blackhole: Blackhole): Unit = {
    val r = sumAppender20.reader.asHistReader
    blackhole.consume(r.sum(0, numSamplesInWindow - 1))
  }

  // 127-bucket OTel exp histograms, 30 samples
  val sumAppender127: BinaryAppendableVector[org.agrona.DirectBuffer] = {
    val app = HistogramVector.appending(memFactory, 15000)
    (0 until numSamplesInWindow).foreach { _ =>
      val hist = LongHistogram(bucketScheme, counts)
      hist.serialize(Some(buffer))
      if (app.addData(buffer) != Ack) {
        throw new RuntimeException("Failed to add 127-bucket histogram")
      }
    }
    app
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput, Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def sumDeltaHist127Buckets(blackhole: Blackhole): Unit = {
    val r = sumAppender127.reader.asHistReader
    blackhole.consume(r.sum(0, numSamplesInWindow - 1))
  }

}
