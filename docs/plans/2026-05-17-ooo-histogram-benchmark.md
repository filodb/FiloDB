# OOO histogram-aware benchmark — 2026-05-17

**Branch:** `out-of-orderness` @ `66fdd8dbc`
**Run params:** 1 fork × 2 warmup (3s) × 3 measurement (3s), `-prof gc -prof stack:lines=12`
**Raw output:** `/tmp/ooo-hist-bench.txt`
**JVM:** OpenJDK 17.0.4, `-Xmx2G`, Compiler Blackholes
**Wall clock:** ~26 min (sbt + Rust deps compile dominated; pure JMH run was ~36 s)

## Numbers

| Benchmark | Throughput | gc.alloc.rate.norm (B/op) | gc.alloc.rate (MB/s) |
|---|---|---|---|
| ingestAggregatingHistogramInOrder | 19.56M ops/s (± 15.58M, n=3) | 24.10 | 447.9 |
| queryAggregatingHistogram         | 915K ops/s (± 2.70M, n=3, range 748K–1029K) | 1288.05 | 1122.8 |

Caveat: 99.9% CIs are very wide because n=3 measurement iterations were used per the project's "shorter JMH settings" policy. Take throughput as order-of-magnitude; the alloc-rate-norm figures are tight (stdev ~0.026 B/op for query, ~0.001 B/op for ingest) and trustworthy.

For reference (from `docs/plans/2026-05-17-ooo-perf-measurement.md`):
| Counterpart | Throughput | Alloc/op |
|---|---|---|
| ingestAggregatingInOrder (scalar dSum) | 52.2M ops/s | 24 B |
| queryAggregating (scalar dSum)         | 1.64M ops/s | 1024 B |

Headline ratios (histogram vs scalar counterpart):
- Ingest throughput: ~2.7× slower (52.2M → 19.56M ops/s)
- Ingest alloc/op: identical (24 B vs 24.1 B) — bytes per op unchanged; throughput drop comes from compute, not allocation
- Query throughput: ~1.8× slower (1.64M → 915K ops/s)
- Query alloc/op: +264 B (1024 B → 1288 B), ~+26%

## Snapshot path verification

Stack frames confirming `AggregatingRangeVector.snapshotBuckets` histogram branch fires:
- `filodb.core.query.AggregatingRangeVector$.$anonfun$snapshotBuckets$1` — **2.7% of all samples / 4.1% of RUNNABLE samples**
- `filodb.core.memstore.aggregation.BucketAggregationState$$anon$1.next` — 1.5% / 2.2% (the iterator that drives the snapshot lambda)

The verification criterion "A frame inside `AggregatingRangeVector$.snapshotBuckets`" is satisfied. The inner-loop frames (`MutableHistogram$.apply`, `makeMonotonic`, `[J.clone`) are not visible at line=12 sampling depth — almost certainly JIT-inlined into the snapshotBuckets lambda body, since the lambda itself shows up while its callees do not.

## queryAggregatingHistogram allocation hotspots

Top RUNNABLE frames (filtered noise excluded; "% of all samples / % of RUNNABLE" shown):

1. `OOOAggregationBenchmark.queryAggregatingHistogram` — 14.5% / 21.8% — benchmark hot loop (cursor iteration, blackhole consume)
2. `ElementChunkInfoIterator.hasNext` (via `MergingRangeVectorCursor.hasNext`) — 4.0% / 5.9% — chunk-info iteration; not histogram-specific
3. `MergingRangeVectorCursor.next` — 4.0% / 5.9% — row merging across active+finalized; not histogram-specific
4. `AggregatingRangeVector$.$anonfun$snapshotBuckets$1` — 2.7% / 4.1% — **the histogram snapshot path (clone + makeMonotonic, JIT-inlined)**
5. `com.typesafe.scalalogging.StrictLogging.$init$` (via `PartitionTimeRangeReader.<init>`) — 1.7% / 2.6% — per-query Logger field init

## Per-bucket allocation attribution

Total alloc/op for queryAggregatingHistogram: **1288 B/op**
Scalar query counterpart (queryAggregating): **1024 B/op**
Histogram-specific delta: **~264 B/op**

Estimated breakdown of the 264 B histogram-specific overhead:
- `MutableHistogram` object (16 B header + 2 refs ≈ 32 B): ~32 B per snapshot
- `Array[Double]` clone for 16-bucket values (16 B header + 16 × 8 B = 144 B): ~144 B per snapshot
- Per-active-bucket snapshot total: ~176 B
- Observed 264 B/op ⇒ **~1.5 active buckets snapshotted per query call on average**, consistent with the test setup (500 samples × 10 s step over 60 s buckets ⇒ 1–2 active "head" buckets when the query window touches the latest).

`makeMonotonic` runs in-place on the snapshotted array → zero additional alloc.

The remaining 1024 B/op (shared with the scalar path) is dominated by per-rows() invocation setup: `MergingRangeVectorCursor` construction, `PartitionTimeRangeReader` construction (with the Logger field init at 2.6% of CPU), `CountingChunkInfoIterator` boxing, and List-based schema lookups (`StrictOptimizedLinearSeqOps.drop` at 0.8%).

## Verdict

**A. Status quo** — snapshot+monotonic cost is small relative to other allocation.

Specifics:
- The histogram snapshot path (`snapshotBuckets$1` lambda containing the `values.clone()` and `makeMonotonic`) accounts for **~4.1% of RUNNABLE CPU samples** and **~264 of 1288 B/op (~21%) of allocation**.
- The remaining ~75% of allocation is shared with the scalar query path: cursor/reader construction, logger field init, schema lookups via List operations.
- Tier 3 (off-heap histogram values) would target the 264 B/op slice. Even eliminating it entirely would reduce per-op alloc from 1288 B to ~1024 B (a ~20% improvement) and reduce CPU by ~4% — modest gains for a complex memory-layout change.
- The `StrictLogging.$init$` frame at 2.6% of CPU and the chunk-info iterator boxing are larger non-histogram opportunities; addressing those (cache the logger; specialize `ChunkInfoIterator`) would deliver comparable wins with less complexity.
- The ingest path shows **identical 24 B/op for both scalar and histogram** — the 2.7× ingest throughput drop is pure compute (NibblePack decode in `BinaryHistogram.addValuesTo` shows up at 0.5% of samples, plus `BucketAggregationState.aggregate`). Tier 3 does not address compute.

If a fix is desired anyway, the most-localized option (without adopting Tier 3) is to **pool a per-cursor scratch `Array[Double]` for `makeMonotonic` and reuse the `MutableHistogram` object across snapshots within a single rows() call**, which would zero out the ~264 B/op delta without touching memory layout. But the data does not justify even this — the cost is small in absolute terms.

**Recommendation:** Do not pursue Tier 3 based on this benchmark. Re-measure with (a) wider histograms (64+ buckets), (b) higher active-bucket counts (multi-window queries), and (c) more measurement iterations before reconsidering. The current setup (16 buckets, ~1.5 active per query) puts the snapshot cost well below other shared overheads.
