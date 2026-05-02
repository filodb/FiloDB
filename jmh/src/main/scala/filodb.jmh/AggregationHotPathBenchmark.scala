package filodb.jmh

import java.util.concurrent.TimeUnit

import org.agrona.{DirectBuffer, ExpandableArrayBuffer}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import filodb.core.memstore.aggregation.{
  AggregationConfig,
  AggregationType,
  BucketAggregationState,
  HistogramAggregator,
  SumAggregator
}
import filodb.memory.format.vectors.{BinaryHistogram, GeometricBuckets, LongHistogram}

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

  // Tier 2 benchmark state — parallel array insert/remove/lookup
  private val tier2IntervalMs = 60000L
  private val tier2ToleranceMs = 180000L
  private val tier2MaxBuckets = 5
  private val tier2Config: Array[Option[AggregationConfig]] =
    Array(Some(AggregationConfig(0, AggregationType.Sum, tier2IntervalMs, tier2ToleranceMs)))

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

  // Tier 2: measures insert + finalize cycle on the sorted parallel arrays.
  // Target: near-zero gc.alloc.rate.norm after warm-up (no boxing, no Entry alloc).
  @Benchmark
  def bucketStateInsertRemoveCycle(bh: Blackhole): Unit = {
    val state = new BucketAggregationState(tier2Config, 1)
    var round = 0
    while (round < 10) {
      var b = 0
      while (b < tier2MaxBuckets) {
        val sampleTs = (round * tier2MaxBuckets + b).toLong * tier2IntervalMs + 1L
        state.aggregate(sampleTs, sampleTs, Array(1.0: Any))
        b += 1
      }
      val toFinalize = state.getBucketsToFinalize(Long.MaxValue)
      var f = 0
      while (f < toFinalize.size) {
        state.markFinalized(toFinalize(f))
        f += 1
      }
      round += 1
    }
    bh.consume(state.stats)
  }

  // Tier 2: measures random lookup latency on the sorted parallel arrays.
  @Benchmark
  def bucketStateLookup(bh: Blackhole): Unit = {
    val state = new BucketAggregationState(tier2Config, 1)
    var b = 0
    while (b < tier2MaxBuckets) {
      val sampleTs = b.toLong * tier2IntervalMs + 1L
      state.aggregate(sampleTs, sampleTs, Array(b.toDouble: Any))
      b += 1
    }
    b = 0
    while (b < tier2MaxBuckets) {
      val bucketTs = (b + 1).toLong * tier2IntervalMs
      bh.consume(state.getBucketValues(bucketTs))
      bh.consume(state.isActive(bucketTs))
      b += 1
    }
  }
}
