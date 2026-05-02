# Aggregation Buffer Heap Optimization — Tier 1 & Tier 2

**Date:** 2026-04-30
**Status:** Tier 1 shipped; Tier 2 rejected after benchmarking
**Related:** Fix 1 (watermark-based Kafka offset hold) — commit 788af068d on branch `out-of-orderness`

## Background

After Fix 1 landed (watermark-based offset hold), the aggregating ingestion path has known heap overhead:

- **Per-sample (Tier 1 target):** `HistogramAggregator.add(DirectBuffer)` decodes the ingested histogram into a freshly-allocated `HistogramWithBuckets` every sample. At high sample rates this dominates GC pressure (~1 KB per sample × millions of samples/sec).
- **Steady-state (Tier 2 target):** `BucketAggregationState` uses `java.util.TreeMap[java.lang.Long, _]` and `java.util.HashSet[java.lang.Long]`. Each bucket incurs ~100+ B of boxing and Entry overhead. Three active buckets per partition × hundreds of thousands of partitions = hundreds of MB resident + steady GC churn on insert/remove.

Both optimizations leave the on/off-heap architecture unchanged. They're localized, individually shippable, and land as separate commits.

## Status summary

- **Tier 1:** ✅ Implemented and merged (commit `be7404cbb`). Keep.
- **Tier 2:** ❌ Rejected after benchmarking (commit `43dc2b2e1` implemented, `228651020` reverted). The measured improvement did not justify the added complexity of sorted parallel-array bookkeeping + overflow/grow logic vs. the original TreeMap. Keeping the simpler `java.util.TreeMap` implementation.

## Tier 1 — Eliminate per-sample histogram allocation

### Current hot path

`core/src/main/scala/filodb.core/memstore/aggregation/Aggregator.scala:348-383`

```scala
class HistogramAggregator extends Aggregator {
  private var accumulator: Option[MutableHistogram] = None

  def add(value: Any): Unit = value match {
    case buf: DirectBuffer =>
      val binHist = BinaryHistogram.BinHistogram(buf)
      val hist = binHist.toHistogram     // ← allocates HistogramBuckets + Array[Long/Double]
      accumulator match {
        case Some(acc) => acc.add(hist)
        case None =>
          accumulator = Some(MutableHistogram(hist))
          initialized = true
      }
  }
}
```

`binHist.toHistogram` routes to `LongHistogram.fromPacked` or `MutableHistogram.fromPacked`, each allocating new arrays. Every sample pays this.

### Design

**Two-path add:**
- **First sample for a bucket** (accumulator empty): continue with `toHistogram` to materialize bucket schema. One-time cost, unavoidable.
- **Subsequent samples**: skip materialization. Decode packed values directly from `DirectBuffer` and add into `accumulator.values[]` bucket-by-bucket.

**New API on `BinaryHistogram`:**

```scala
object BinaryHistogram {
  /** Decodes histogram bucket values from `buf` and adds each to `target(i)`.
   *  Returns true if downstream monotonic correction is required. */
  def addValuesTo(buf: DirectBuffer, target: Array[Double]): Boolean
}
```

**Updated `HistogramAggregator`:**

```scala
private var needsMonotonicCorrection: Boolean = false

def add(value: Any): Unit = value match {
  case buf: DirectBuffer =>
    accumulator match {
      case Some(acc) =>
        val delta = BinaryHistogram.addValuesTo(buf, acc.values)
        if (delta) needsMonotonicCorrection = true
      case None =>
        val binHist = BinaryHistogram.BinHistogram(buf)
        accumulator = Some(MutableHistogram(binHist.toHistogram))
        initialized = true
    }
}

override def result(): Any = accumulator match {
  case Some(hist) =>
    if (needsMonotonicCorrection) hist.makeMonotonic()
    hist.serialize()
  case None => Histogram.empty.serialize()
}
```

### Edge cases

1. **Mixed formats within a bucket** — `addValuesTo` dispatches per-sample by format code.
2. **Schema mismatch** — `addValuesTo` verifies `buf.numBuckets == target.length`, throws `IllegalArgumentException` otherwise (preserves current behavior).
3. **Deferred `makeMonotonic`** — runs once at `result()` instead of per-add. Semantically equivalent; tests must verify.

### Scope

- **New:** `addValuesTo` + format decoders in `HistogramVector.scala` (~60 LOC)
- **Modified:** `HistogramAggregator.add`, `result`
- **Unchanged:** Public APIs, wire format, query path, schemas

### Tests

1. Correctness parity across all histogram formats (Delta/XOR variants)
2. Deferred monotonic correction produces monotonic final result
3. Schema mismatch rejection
4. First-sample cold path initializes accumulator correctly
5. JMH benchmark: ≥10× reduction in `gc.alloc.rate.norm` for sustained aggregation

## Tier 2 — Primitive-typed bucket state collections

### Current state

`BucketAggregationState` uses:
- `JTreeMap[java.lang.Long, BucketState]` for activeBuckets
- `HashSet[java.lang.Long]` for finalizedBuckets
- `JTreeMap[java.lang.Long, java.lang.Long]` for bucketMinOffset
- `JTreeMap[java.lang.Long, java.lang.Long]` for bucketLastIngestTime

**Per-bucket collection overhead:** ~272 B (entries + boxing).

### Design

Active bucket count per partition is bounded by `ceil(tolerance/interval) + 1`. For default config (2m tolerance, 1m interval), max ≈ 3. Sorted parallel primitive arrays beat TreeMap at this size.

**Replacement structure:**

```scala
class BucketAggregationState(...) {
  private val maxActive: Int = {
    val theoretical = ((primaryOooToleranceMs + primaryIntervalMs - 1) / primaryIntervalMs).toInt + 1
    math.max(theoretical * 2, 4)
  }

  // Parallel arrays, sorted ascending by timestamp
  private val bucketTs           = new Array[Long](maxActive)
  private val bucketStates       = new Array[BucketState](maxActive)
  private val bucketMinOffsetArr = new Array[Long](maxActive)
  private val bucketLastIngest   = new Array[Long](maxActive)
  private var numActive: Int = 0

  private val finalizedBuckets = debox.Set.empty[Long]
}
```

Core operations (findActiveIndex, insertActive, removeActive) are linear-scan over ~4 elements — cache-friendly, no boxing, no Entry allocation.

### Overflow handling

Inserts beyond `maxActive` trigger `growArrays()` (doubles capacity) and increment `shardStats.aggBucketOverflow`. Normal operation never hits this.

### Memory win

- Before: ~272 B/bucket of collection overhead
- After: ~40 B/bucket of collection overhead
- **~85% reduction on collection layer, ~230 B/bucket saved**
- 500k partitions × 3 buckets ≈ **350 MB heap saved**

### Scope

- **Modified:** `BucketAggregationState.scala` internals only — all public method signatures preserved
- **Unchanged:** `BucketState`, `AggregatingTimeSeriesPartition`, aggregators

### Tests

Existing `BucketAggregationStateSpec` must still pass (drop-in replacement).

New:
1. Sorted invariant under out-of-order inserts
2. Capacity growth on overflow + stat increment
3. `earliestActiveOffset` min across buckets
4. Stale bucket detection
5. JMH: zero allocation per insert/remove cycle; ≥2× speedup on lookup vs TreeMap

### Outcome (rejected)

Implemented and benchmarked on branch `agg-heap-opt-tier2` (commit `43dc2b2e1`). Post-benchmark review: the throughput/allocation delta over the TreeMap implementation was marginal and did not warrant the additional complexity (sorted-array invariant maintenance, grow-on-overflow path, new overflow stat). Reverted in commit `228651020`. The TreeMap-based `BucketAggregationState` remains in place; this section is preserved for historical context on what was tried and why it was set aside.

## Implementation order

Sequential, not parallel:

1. **Tier 1 first** — higher production impact (peak-ingest GC pressure); smaller surface; creates JMH benchmark harness at `jmh/src/main/scala/filodb.jmh/AggregationHotPathBenchmark.scala`.
2. **Tier 2 second** — extends the benchmark harness from Tier 1.

Separate PRs to `out-of-orderness` so bisection can isolate regressions.

## Benchmark harness

New `jmh/src/main/scala/filodb.jmh/AggregationHotPathBenchmark.scala` with scenarios:

- Sustained histogram ingestion: 100 samples/bucket × 1000 buckets
- Scalar-only ingestion (no histogram path)
- Mixed workload

Reports throughput (ops/s), allocation rate (B/op), P50/P99 latency.

**Done criteria per tier:** benchmark confirms claimed improvement vs baseline + no test regression + code review approved.

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Tier 1: behavioral diff from deferred `makeMonotonic` | Low | Format-parity tests |
| Tier 1: format decoder bug in `addValuesTo` | Medium | Parity tests per format |
| Tier 2: sorted-array invariant bugs | Low–Medium | Property-based insert/remove tests |
| Tier 2: maxActive miscalibration | Medium | Overflow stat + grow-on-demand |

## Non-goals

- No on/off-heap architecture change
- No schema / wire format / query path changes
- No new public APIs beyond `BinaryHistogram.addValuesTo`
- No pooling beyond what already exists
