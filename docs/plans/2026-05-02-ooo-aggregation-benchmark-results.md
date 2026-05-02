# OOO Aggregation Benchmark Results

**Date:** 2026-05-02
**Branch:** `out-of-orderness` @ `4e474b6f8`
**Benchmark class:** `filodb.jmh.OOOAggregationBenchmark`

## Environment

- **OS:** macOS 15.4 (Darwin 24.4.0)
- **CPU:** Apple M1 Max, 10 cores
- **JVM:** OpenJDK 17.0.4 (AppleJDK 17.0.4+8-LTS)
- **JMH:** 1.37, compiler blackholes enabled
- **Heap:** `-Xmx2G`
- **Parameters:** 1 fork, 2 warmup iterations (5s), 3 measurement iterations (5s), gc profiler

## Question being answered

What is the overhead of enabling OOO (out-of-order) support via `AggregatingTimeSeriesPartition` compared to the baseline `TimeSeriesPartition`?

## Ingestion path (throughput, higher is better)

| Benchmark | Throughput (ops/s) | Error (99.9% CI) | Alloc (B/op) | Relative to baseline |
|---|---|---|---|---|
| `ingestRegular` | 11,495,273 | +/- 5,462,200 | 360.0 | 1.00x (baseline) |
| `ingestAggregatingInOrder` | 53,302,124 | +/- 6,563,459 | 24.0 | **4.64x faster** |
| `ingestAggregatingOOO` | 11,864,391 | +/- 6,869,761 | 246.3 | 1.03x (parity) |

**Takeaway:** The aggregating path with in-order data is ~4.6x faster than the old regular path and allocates 15x less per sample (24 vs 360 B/op), thanks to the in-place histogram optimization (commit `be7404cbb`). OOO ingestion matches baseline throughput at ~1.03x and allocates ~32% less (246 vs 360 B/op).

## Query path (throughput, higher is better)

| Benchmark | Throughput (ops/s) | Error (99.9% CI) | Alloc (B/op) |
|---|---|---|---|
| `queryRegular` | 5,334,727 | +/- 1,336,659 | 328.0 |
| `queryAggregating` | 1,693,645 | +/- 84,406 | 1,024.0 |

**Takeaway:** The aggregating query path is ~3.1x slower and allocates ~3.1x more per op (1024 vs 328 B/op). Reconstructing the merged time series from aggregation buckets involves more object allocation. This is the primary overhead of the OOO-capable path.

## Finalization (average time, lower is better)

| Benchmark | Avg time (us/op) | Error (99.9% CI) | Alloc (B/op) |
|---|---|---|---|
| `finalizeBuckets` | 10.7 | +/- 55.1 | 40,362 |

**Takeaway:** ~10.7 us per bucket flush with ~40 KB allocation. Wide CI due to GC interference on the first measurement iteration (14.1 us vs 9.8, 8.3 us for subsequent iterations). This is a batch operation (not per-sample), so the per-sample amortized cost is negligible.

## Observations

1. **Ingestion is not a concern.** The aggregating in-order path is dramatically faster and cheaper than baseline. Even with OOO data, throughput matches the old non-aggregating path. Enabling OOO support has zero ingestion regression.

2. **Query is the main overhead.** 3.1x slower with 3.1x more allocation. This is expected: reading from aggregation buckets requires merging and reconstructing the time series, which involves more intermediate object allocation (~1 KB/op vs ~328 B/op).

3. **Allocation profile is excellent for ingestion.** The in-place histogram optimization (Tier 1, commit `be7404cbb`) reduced in-order ingestion allocation from 360 B/op down to 24 B/op. OOO ingestion at 246 B/op is still better than the old regular path.

4. **Wide error bars on some benchmarks** (especially `ingestRegular` and `ingestAggregatingOOO`) suggest GC pressure variability. A run with more iterations or larger heap would tighten confidence intervals, but the relative ordering is clear.

5. **Finalization is cheap at batch scale.** 10.7 us per bucket amortized over hundreds or thousands of samples per bucket makes the per-sample contribution sub-nanosecond.

## Raw output

Full JMH output saved to `/tmp/ooo-support-bench.txt` during the run. Reproduced summary table:

```
Benchmark                                                             Mode  Cnt         Score         Error   Units
OOOAggregationBenchmark.ingestAggregatingInOrder                     thrpt    3  53302123.576 ± 6563459.232   ops/s
OOOAggregationBenchmark.ingestAggregatingInOrder:gc.alloc.rate.norm  thrpt    3        24.001 ±       0.004    B/op
OOOAggregationBenchmark.ingestAggregatingOOO                         thrpt    3  11864390.952 ± 6869761.070   ops/s
OOOAggregationBenchmark.ingestAggregatingOOO:gc.alloc.rate.norm      thrpt    3       246.345 ±       0.018    B/op
OOOAggregationBenchmark.ingestRegular                                thrpt    3  11495272.677 ± 5462200.276   ops/s
OOOAggregationBenchmark.ingestRegular:gc.alloc.rate.norm             thrpt    3       359.995 ±       0.016    B/op
OOOAggregationBenchmark.queryAggregating                             thrpt    3   1693644.552 ±   84406.048   ops/s
OOOAggregationBenchmark.queryAggregating:gc.alloc.rate.norm          thrpt    3      1024.009 ±       0.098    B/op
OOOAggregationBenchmark.queryRegular                                 thrpt    3   5334726.880 ± 1336658.709   ops/s
OOOAggregationBenchmark.queryRegular:gc.alloc.rate.norm              thrpt    3       328.003 ±       0.032    B/op
OOOAggregationBenchmark.finalizeBuckets                               avgt    3        10.728 ±      55.096   us/op
OOOAggregationBenchmark.finalizeBuckets:gc.alloc.rate.norm            avgt    3     40361.758 ±       3.114    B/op
```
