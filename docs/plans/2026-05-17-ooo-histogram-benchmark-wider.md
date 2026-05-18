# OOO histogram-aware benchmark (wider, 64 buckets) — 2026-05-17

**Branch:** `out-of-orderness` @ `86488aa75` (benchmark adjustment commit)
**Run params:** 1 fork × 2 warmup (3s) × 3 measurement (3s), `-prof gc -prof stack:lines=12`
**Histogram:** 64 buckets, `GeometricBuckets(1.0, 2.0, 64)`
**Histogram dataset config:** `aggregationIntervalMs=10000`, `aggregationOooToleranceMs=120000`
**Active buckets per query:** **13** (verified via dev-only sanity print of `queryHistAggPart.activeBucketTimestamps.size` during setup)
**Raw output:** `/tmp/ooo-hist-bench-wider.txt`
**JVM:** OpenJDK 17.0.4, `-Xmx2G`, Compiler Blackholes
**Wall clock:** ~33 s pure JMH (after warm sbt)

## Numbers

| Benchmark | Throughput | gc.alloc.rate.norm (B/op) | gc.alloc.rate (MB/s) |
|---|---|---|---|
| ingestAggregatingHistogramInOrder | 41.29M ops/s (± 9.88M, n=3) | 24.33 | 955.2 |
| queryAggregatingHistogram         | 263.55K ops/s (± 50.10K, n=3) | 11696.13 | 2933.2 |

99.9% CIs are wide on throughput (n=3 measurement iterations). The `alloc.rate.norm` figures are tight (stdev ~1.27 B/op for query, ~0.008 B/op for ingest) and trustworthy.

## Δ vs prior 16-bucket measurement

| Metric | 16 buckets / ~1.5 active (prior) | 64 buckets / 13 active (now) | Δ |
|---|---|---|---|
| queryAggregatingHistogram alloc/op | 1288 B | **11696 B** | **+10408 B (~9.1×)** |
| queryAggregatingHistogram throughput | 915K ops/s | 263.55K ops/s | −651K (~3.5× slower) |
| queryAggregatingHistogram alloc rate | 1122.8 MB/s | 2933.2 MB/s | +1810 MB/s (~2.6×) |
| ingestAggregatingHistogramInOrder alloc/op | 24.10 B | 24.33 B | +0.23 B (essentially unchanged) |
| ingestAggregatingHistogramInOrder throughput | 19.56M ops/s | 41.29M ops/s | +21.7M (variance — see below) |
| Active buckets per query | ~1.5 | 13 | +11.5 |

The ingest-throughput jump is most plausibly measurement noise — both runs use n=3 measurement iterations and the 99.9% CIs are wide (±15.58M prior, ±9.88M now). The `alloc/op` numbers — which are tight — show ingest allocations are unchanged at ~24 B/op regardless of bucket count. Ingest cost is dominated by NibblePack decode + per-bucket aggregation compute, which scales with bucket count but does not allocate per-op.

## Snapshot path verification

Stack frames confirming `AggregatingRangeVector.snapshotBuckets` histogram branch fires (RUNNABLE-only profile, `% of all samples / % of RUNNABLE`):

- `MutableHistogram.makeMonotonic` ← `AggregatingRangeVector$.$anonfun$snapshotBuckets$1` — **7.4% / 11.1%** (called from `MergingRangeVectorCursor.findNextBucketRow`)
- `AggregatingRangeVector$.$anonfun$snapshotBuckets$1` direct frame — **1.9% / 2.8%**
- Lambda apply through Iterator.next — **1.3% / 2.0%**
- `MutableHistogram.makeMonotonic` ← `snapshotBuckets$1` ← `AggregatingRangeVector.rows` (init pre-pass) — **0.8% / 1.3%**

**Total snapshot path:** ~11.4% of all samples / ~17.2% of RUNNABLE. The inlined inner frames (`makeMonotonic`, the `[D].clone()` inside the lambda) now surface clearly at line=12 sampling depth — at 16 buckets / 1.5 active they were below the noise floor, only the lambda parent showed.

## Allocation attribution

Total query alloc/op: **11696 B**
Snapshot path delta vs the 16-bucket run: **+10408 B** (was 264 B at ~1.5 active buckets).

Estimated breakdown (per query call, 13 active buckets, 64-bucket histograms):
- Per-bucket snapshot (clone of 64-element `Array[Double]` + `MutableHistogram` object): 64 × 8 B + 16 B array header + ~32 B MH header ≈ **~560 B per active bucket**
- 13 active buckets × ~560 B ≈ **~7280 B from cloning**
- Per-row TransientRow / cursor merge overhead for the 12 extra emitted rows ≈ ~1000–1500 B
- Shared cursor/reader/logger/schema-lookup overhead (the same ~1024 B that the 16-bucket scalar+histogram runs both showed): ~1024 B
- Approximate total: ~9300–9800 B accounted for; remaining ~1900–2400 B is not separately attributed in the line=12 stack profile (likely Tuple2 boxing in `bucketValuesIteratorInRange`, MergingRangeVectorCursor row-merge bookkeeping, and per-cursor lambda captures).

`makeMonotonic` itself runs in-place on the cloned array — the 7.4% CPU it consumes is pure compute over 64 buckets, not allocation.

## Verdict

**C. Tier 3 worth considering for production-realistic histograms.**

The data has shifted decisively from the 16-bucket / ~1.5-active scenario:

| Cost dimension | 16 bkt / 1.5 active | 64 bkt / 13 active |
|---|---|---|
| Snapshot path share of query alloc | 264 / 1288 ≈ **21%** | ~9000+ / 11696 ≈ **>75%** |
| Snapshot path share of query CPU | ~4% RUNNABLE | **~17% RUNNABLE** |
| Absolute alloc per query | 264 B | **~10000 B** |
| Allocation rate from snapshot | ~230 MB/s of 1123 MB/s | **~2200 MB/s of 2933 MB/s** |

At 64 buckets and 13 active buckets — both numbers production-realistic — the per-query clone+monotonic path is now the **dominant** query allocation source, not a marginal slice. Eliminating it (Tier 3, or a less-invasive scratch-array pool inside `snapshotBuckets`) would:

- Cut `queryAggregatingHistogram` alloc/op by an estimated 70–80% (from ~11.7 KB to ~2–3 KB).
- Cut allocation-rate pressure by ~2 GB/s in this benchmark.
- Reclaim ~10–15% of query RUNNABLE CPU.

These are large enough wins to consider the engineering investment.

### Threshold at which the prior "status quo" verdict flips

Based on the two data points:

| Active buckets × bucket count | Snapshot alloc share | Verdict |
|---|---|---|
| 1.5 × 16 = 24 bucket-clones | ~21% | A. Status quo (prior) |
| 13 × 64 = 832 bucket-clones | ~75–80% | C. Tier 3 worth considering (now) |

A reasonable cutoff: **the verdict flips when `(active_buckets × bucket_count) ≳ 100`**, i.e. once the per-query cumulative bucket-clone work exceeds ~100 buckets-worth. Production tenants with 64-bucket histograms and even 2–3 active buckets per query (≈128–192) would be over the threshold; a tenant with 16 buckets and 1 active bucket (16) would still be safely under.

### Caveats

- n=3 measurement iterations — throughput numbers carry wide CIs. The `alloc.rate.norm` figures are tight and the verdict-relevant numbers.
- `GeometricBuckets(1.0, 2.0, 64)` is a synthetic shape; real production buckets vary (some workloads use `Base2ExpHistogramBuckets`). The clone path scales linearly with bucket count regardless of bucketing scheme.
- The benchmark exercises a single partition with all 13 active buckets queried back-to-back. Real query mixes spread across many partitions where a per-cursor scratch buffer might amortize differently.
- Scalar (non-histogram) datasets are unaffected — `snapshotBuckets` for `dSum` does no clone.

### Recommendation

Re-open the Tier 3 (or scratch-array pool) decision under the production-realistic 64-bucket scenario. The least-invasive option remains:
- A per-cursor reusable `Array[Double]` and `MutableHistogram` shell, reset at each `snapshotBuckets` call. This zeroes the ~7–10 KB / op cloning cost without touching memory layout.

The full Tier 3 (off-heap histogram values) is a larger investment but additionally addresses the per-bucket aggregation compute (`addValuesTo`, `BucketAggregationState.aggregate`), which was not the bottleneck at 16 buckets but becomes more material at 64.
