# OOO histogram benchmark — after per-cursor scratch pool — 2026-05-17

**Branch:** `ooo-query-scratch-pool` @ `cc55fdc2b` (perf commit on top of `out-of-orderness` @ `25f04df5b`)
**Run params:** 1 fork × 2 warmup (3s) × 3 measurement (3s), `-prof gc -prof stack:lines=12` — identical to wider (pre-pool) baseline
**Histogram:** 64 buckets, `GeometricBuckets(1.0, 2.0, 64)`
**Histogram dataset config:** `aggregationIntervalMs=10000`, `aggregationOooToleranceMs=120000`
**Active buckets per query:** 13 (unchanged from baseline)
**Raw output:** `/tmp/ooo-hist-bench-pooled.txt`
**JVM:** OpenJDK 17.0.4, `-Xmx2G`, Compiler Blackholes
**Wall clock:** ~112 s pure JMH (after warm sbt)

## Numbers

| Benchmark | Throughput | gc.alloc.rate.norm (B/op) | gc.alloc.rate (MB/s) |
|---|---|---|---|
| ingestAggregatingHistogramInOrder | 40.41M ops/s (± 11.51M, n=3) | 24.33 | 936.8 |
| queryAggregatingHistogram         | 227.62K ops/s (± 571.46K, n=3) | **4600.16** | 997.8 |

99.9% CIs on throughput are wide (n=3); query throughput CI in particular is wider than the mean and not useful in isolation. The `alloc.rate.norm` figures are tight (stdev ≤ 2.1 B/op for query, 0.007 B/op for ingest) and are the trustworthy comparison axis.

## Δ vs wider (pre-pool) baseline

| Metric | Before pool (wider) | After pool | Δ |
|---|---|---|---|
| queryAggregatingHistogram alloc/op | 11696.13 B | **4600.16 B** | **−7095.97 B (−60.7%)** |
| queryAggregatingHistogram alloc rate | 2933.2 MB/s | 997.8 MB/s | −1935.4 MB/s (−66.0%) |
| queryAggregatingHistogram throughput | 263.55K ops/s | 227.62K ops/s | −35.93K (−13.6%) — see caveats |
| ingestAggregatingHistogramInOrder alloc/op | 24.33 B | 24.33 B | 0 (unchanged, as expected) |
| ingestAggregatingHistogramInOrder throughput | 41.29M ops/s | 40.41M ops/s | within CI |

**Allocation:** the per-query allocation dropped by ~7.1 KB/op, fully consistent with the wider doc's estimate of ~7–9 KB/op coming from `MutableHistogram.values.clone()` × ~13 active buckets at 64 buckets each. The pool eliminates the clone; the remaining 4.6 KB/op now sits in `BucketRowData` + `Array[Any] values` allocations per row (deferred per plan non-goals) and shared cursor/reader overhead.

**Throughput:** the headline drop is inside the CI for both runs (pre-pool ±50K, post-pool ±571K — essentially noise at n=3) and not statistically meaningful. The combined alloc-rate drop (−1.9 GB/s) and CPU-share data below suggest no real CPU regression. A higher-fork/higher-iteration run would be needed to call throughput cleanly, but the alloc story is the load-bearing finding.

## Snapshot path verification

Stack frames confirming `AggregatingRangeVector.snapshotBucketsPooled` fires (RUNNABLE-only profile, `% of all samples / % of RUNNABLE`):

- `MutableHistogram.makeMonotonic` ← `AggregatingRangeVector$.$anonfun$snapshotBucketsPooled$1` ← `MergingRangeVectorCursor.findNextBucketRow` — **7.6% / 11.5%**
- `MutableHistogram.makeMonotonic` ← `snapshotBucketsPooled$1` ← `MergingRangeVectorCursor.<init>` (init pre-pass) — **0.8% / 1.2%**
- `MergingRangeVectorCursor.next` (now includes pooled path) — **5.2% / 7.9%**

**Snapshot path total (makeMonotonic frames):** ~8.4% of all samples / ~12.7% of RUNNABLE — vs ~8.2% / ~12.4% pre-pool (sum of the wider doc's two `makeMonotonic` lines, 7.4% + 0.8%). The path still fires at the same CPU share, confirming we have not silently skipped it. The clone (`[D.clone`) frame that was implicit in the pre-pool profile is now absent — replaced by an `arraycopy` inside the pooled lambda which the line=12 sampler does not surface as a hot frame.

## Verdict

- **Did the fix deliver the projected ~70–80% reduction in query alloc?** Close: **−60.7%** observed against a projected 70–80%. The remainder lives in BucketRowData and per-row Array[Any] allocations, both explicitly deferred per the plan's non-goals.
- **What dominates the remaining ~4.6 KB/op?** The plan's wider doc attributes ~1.0–1.5 KB to per-row Tuple2 / Array[Any] allocations in the `MergingRangeVectorCursor.next` path, plus the shared ~1 KB cursor/reader/schema-lookup overhead. The pooled run shows the residual is consistent with that breakdown.
- **Recommendation:** the pool is a high-ROI, low-risk fix that removes the dominant query allocation source. Next-tier wins (pooling `BucketRowData` itself and the per-row `Array[Any]`) would reclaim another ~1–2 KB/op but require minor cursor-protocol changes. The Tier 3 (off-heap histogram values) path remains relevant only if the per-bucket aggregation compute (`addValuesTo`, `BucketAggregationState.aggregate`) becomes the next bottleneck — current data still shows aggregation as second-tier (~10% RUNNABLE in `AggregatingTimeSeriesPartition.ingest` on the ingest side).

## Caveats

- n=3 measurement iterations — throughput numbers carry wide CIs. The `alloc.rate.norm` figures are tight and the verdict-relevant numbers.
- The live-reference contract introduced by pooling is documented in `snapshotBucketsPooled` and exercised by 8 new unit tests (`AggregatingRangeVectorSpec` "snapshotBucketsPooled"). All 71 existing tests in the spec pass and 875 core tests pass post-change.
- This document is transport for findings; may be reverted after the parent reads it.
