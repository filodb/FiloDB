package filodb.jmh

import java.util.concurrent.TimeUnit

import org.agrona.{DirectBuffer, ExpandableArrayBuffer}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import filodb.core.memstore.aggregation.{HistogramAggregator, SumAggregator}
import filodb.memory.format.vectors.{BinaryHistogram, GeometricBuckets, LongHistogram}

// Will be extended in Tier 2 with pooling and bucket-state benchmarks.
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
class AggregationHotPathBenchmark {

  private val numBuckets = 64
  private val numSamples = 100
  private val buckets = GeometricBuckets(1.0, 2.0, numBuckets)

  private var samples: Array[DirectBuffer] = _
  private var scalarValues: Array[Double] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    samples = (0 until numSamples).map { i =>
      val values = (0 until numBuckets).map(b => (i + 1).toLong * 10L + b.toLong * 5L).toArray
      val buf = new ExpandableArrayBuffer(4096)
      BinaryHistogram.writeDelta(buckets, values, buf)
      buf: DirectBuffer
    }.toArray

    scalarValues = (0 until numSamples).map(i => i.toDouble * 1.5).toArray
  }

  @Benchmark
  def histogramAggregatorSustainedAdd(bh: Blackhole): Unit = {
    val agg = new HistogramAggregator
    var i = 0
    while (i < numSamples) {
      agg.add(samples(i))
      i += 1
    }
    bh.consume(agg.result())
  }

  @Benchmark
  def scalarAggregatorSustainedAdd(bh: Blackhole): Unit = {
    val agg = new SumAggregator
    var i = 0
    while (i < numSamples) {
      agg.addDouble(scalarValues(i))
      i += 1
    }
    bh.consume(agg.result())
  }
}
