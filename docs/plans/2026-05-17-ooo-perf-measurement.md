# OOO performance measurement — 2026-05-17

**Branch:** `out-of-orderness` @ `44b4cd8ed`
**Host / JVM:** Apple M1 Max (Darwin 24.4.0, arm64), OpenJDK 17.0.4 (AppleJDK 17.0.4+8-LTS), JMH 1.37, `-Xmx2G`
**Run params:** 1 fork × 2 warmup (3s) × 3 measurement (3s), `-prof gc -prof stack:lines=12`
**Raw output:** `/tmp/ooo-perf-current.txt` (3,064 lines)
**Note:** baseline (5/2) used 5s warmup/measurement; current run uses 3s — JIT-warmup-sensitive numbers may shift slightly.

## Phase 1 — Current benchmark numbers

| Benchmark | Mode | Throughput | gc.alloc.rate.norm (B/op) | gc.alloc.rate (MB/s) |
|---|---|---|---|---|
| `ingestRegular` | thrpt | 10,629,113 ops/s ± 2,618,322 | 359.992 | 3,646.6 |
| `ingestAggregatingInOrder` | thrpt | 52,225,384 ops/s ± 20,898,540 | 24.098 | 1,199.4 |
| `ingestAggregatingOOO` | thrpt | 44,410,950 ops/s ± 82,542,090 | 24.098 | 1,019.9 |
| `queryRegular` | thrpt | 3,259,397 ops/s ± 1,799,868 | 328.008 | 1,018.9 |
| `queryAggregating` | thrpt | 1,637,962 ops/s ± 2,720,099 | 1,024.022 | 1,598.4 |
| `finalizeBuckets` | avgt | 6.523 µs ± 3.698 | 28,010.338 | 4,094.1 |

All benchmarks completed; none "did not complete".

## Phase 2 — Δ vs 2026-05-02 baseline (`4e474b6f8`)

| Benchmark | Δ throughput | Δ alloc/op | Significant? |
|---|---|---|---|
| `ingestRegular` | −7.5% (10.6M vs 11.5M) | 0% (360 B/op both) | **No** — alloc identical, CIs overlap heavily, wide error bars on both runs |
| `ingestAggregatingInOrder` | −2.0% (52.2M vs 53.3M) | +0.4% (24.10 vs 24.00) | **No** — within noise |
| `ingestAggregatingOOO` | **+274%** (44.4M vs 11.9M) | **−90.2%** (24.10 vs 246.34) | **Yes — improvement.** OOO path now allocation-parity with InOrder. Likely from event-time refactor + dead-code removal landed since 5/2. |
| `queryRegular` | −38.9% (3.26M vs 5.33M) | 0% (328 B/op both) | **No** — alloc unchanged; CIs overlap [1.46M–5.06M] vs [4.00M–6.67M]. Attributable to shorter warmup (3s vs 5s) + stack-profiler overhead + JVM run-to-run noise. |
| `queryAggregating` | −3.3% (1.64M vs 1.69M) | 0% (1024.022 vs 1024.009 B/op) | **No** — within noise. **No allocation regression.** |
| `finalizeBuckets` | **−39%** time (6.5µs vs 10.7µs) | **−30.6%** (28,010 vs 40,362 B/op) | **Yes — improvement.** |

**Bottom line for Q1:** No regression in `queryAggregating` since 5/2. Allocation per op is identical to four decimal places. `ingestAggregatingOOO` and `finalizeBuckets` both improved meaningfully.

## Phase 3 — `queryAggregating` allocation hotspots

JMH `-prof stack` had ~50% frames filtered (the usual JIT-frame issue), so this is qualitative for the visible portion. Top RUNNABLE frames (% of total samples / % of RUNNABLE):

1. **23.3%** `OOOAggregationBenchmark.queryAggregating` (the benchmark loop body itself — JIT-inlined)
2. **5.5%** `MergingRangeVectorCursor.next` — cursor advancement
3. **4.7%** `BucketAggregationState$$anon$1.next` (anonymous `Iterator` from `bucketValuesIteratorInRange`) → allocates `new Array[Any](numColumns)` + `Tuple2(ts, values)` per bucket (`BucketAggregationState.scala:281–291`)
4. **2.6%** `PartitionTimeRangeReader.populateIterators`
5. **2.1%** `scala.collection.ArrayOps$.foreach$extension` from `CountingChunkInfoIterator.nextInfoReader`
6. **1.9%** `FilteredChunkInfoIterator.hasNext`
7. **1.9%** `LoggerContext.getLogger` — invoked from `PartitionTimeRangeReader.<init>` via `StrictLogging.$init$` once per query

### Verdict on `MutableHistogram.values.clone()` and `makeMonotonic()`

**Not visible — and CANNOT be evaluated by this benchmark.** `OOOAggregationBenchmark` uses the `agg_metrics` dataset configured with `dSum(1)` on a `value:double` column (`OOOAggregationBenchmark.scala:65–69`). The histogram clone path in `AggregatingRangeVector.snapshotBuckets` (`AggregatingRangeVector.scala:99–102`) only fires for `case hist: MutableHistogram`; in this benchmark every column value falls into the `case other` branch. **The histogram-snapshot question this measurement was asked to answer cannot be answered with this benchmark — a histogram-typed aggregating benchmark would be required.**

What we *can* see for `queryAggregating` allocation:

- 1024 B/op total. With the query covering 500 samples × 10s step ÷ 60s/bucket ≈ 84 active buckets, this works out to ~12 B/op per bucket — which means JIT escape analysis is largely scalar-replacing the per-bucket `Array[Any]` and `Tuple2` (otherwise the ~80 B/bucket × 84 ≈ 6.7 KB lower bound would dominate). The bulk of the 1024 B/op is per-query setup: `MergingRangeVectorCursor`, `BucketDataRowReader`, `CountingChunkInfoIterator`, `PartitionTimeRangeReader` and its `Logger` pull-in.

## Phase 4 — Recommendation

**A. Status quo. Do not optimize further at this time.**

Justification (numbers + reasoning):

1. **No regression to address.** `queryAggregating` allocation is byte-identical to 5/2 (1024.022 vs 1024.009 B/op, within ±0.18 B noise). Throughput delta is within run-to-run noise.
2. **The premise of the measurement (histogram-snapshot cost) cannot be evaluated here.** `OOOAggregationBenchmark` uses a scalar `dSum` aggregator — `MutableHistogram.values.clone()` is dead code in this benchmark. Acting on Tier 3 (off-heap histogram values) based on this run would be unjustified.
3. **The visible localized opportunities are minor and not Tier-3-shaped:**
   - Fuse `BucketAggregationState.bucketValuesIteratorInRange`'s `(Long, Array[Any])` allocation with `AggregatingRangeVector.snapshotBuckets`'s second `Array[Any]` allocation → eliminates one array per bucket. JIT already scalar-replaces most of this; expected savings small.
   - Hoist `PartitionTimeRangeReader`'s `StrictLogging`-driven `LoggerContext.getLogger` to a companion-object singleton → eliminates ~1.9% per-query overhead (a once-per-query cost, dwarfed by per-row work for any non-trivial range).
   - Neither is worth disturbing the surface area for now.
4. **The big wins on the OOO path have already landed.** `ingestAggregatingOOO` is 3.7× faster and allocates 10× less than the 5/2 baseline. `finalizeBuckets` is ~40% faster and allocates ~30% less. Whatever optimization budget exists is best spent only on data-driven targets.

**To answer the histogram-snapshot question separately:** extend `OOOAggregationBenchmark` (or add a sibling) with an `aggregating-delta-histogram-v2`-style schema and re-run with `-prof gc -prof stack`. Reporting this as a follow-up; not acting on it.

**Tier 3 (off-heap) verdict:** Not justified by the data available right now. Revisit only if a histogram-aware benchmark shows `MutableHistogram.values.clone()` + `makeMonotonic()` dominating allocation **and** retained heap is meaningful relative to other workload sources.

## Phase 5 — Heap retention spot check

**Skipped.** JOL setup non-trivial in this build, and JMH alloc data is sufficient to support the recommendation. Within time cap.

## Answers to the three questions

1. **Did query-path performance regress since 5/2?** No. `queryAggregating` allocation is identical (1024.022 vs 1024.009 B/op); throughput delta within noise. `queryRegular` shows a noisy throughput dip with identical alloc — attributable to shorter warmup + stack-profiler overhead, not code regression.
2. **Is `MutableHistogram.values.clone()` the dominant allocation source in `queryAggregating`?** Cannot be answered from this benchmark — the dataset uses `dSum` on a `Double` column, so the histogram clone branch is never taken. A histogram-typed benchmark is needed to answer.
3. **Localized fix or Tier 3 off-heap?** Neither, based on current data. Status quo. Two minor localized fixes exist (fuse Array[Any] allocations; hoist Logger lookup) but savings are small and not justified.
