# Out-of-Order Sample Ingestion Support

| | |
|---|---|
| **Status** | Implemented on branch `out-of-orderness` |
| **Last updated** | 2026-05-14 |
| **Owner** | TBD |

## Overview

Out-of-Order (OOO) Sample Ingestion Support is a per-partition ingest path in FiloDB that accepts samples whose timestamps fall behind the latest seen timestamp by up to a configurable tolerance window. Standard `TimeSeriesPartition` rejects any sample whose timestamp is earlier than the latest chunk's `endTime`, which causes data loss whenever upstream producers emit events with skew (network delay, retries, distributed clock divergence). The OOO path replaces that hard rejection with bounded in-memory aggregation: incoming samples are aggregated into fixed-width event-time buckets, and finalized buckets are then handed to the existing chunk pipeline as ordinary rows.

Architecturally this feature lives entirely inside the memstore. `AggregatingTimeSeriesPartition` extends `TimeSeriesPartition` and is selected at partition-creation time when the dataset's ingestion config declares an `aggregation {}` block. All on-disk formats, downsampling, query planners, and Cassandra/persistence machinery are unchanged: from their perspective the partition emits rows at bucket boundaries instead of at original sample timestamps.

The mechanism in one line: samples are aggregated into event-time buckets keyed by `ceilToBucket(sampleTs, intervalMs)`; a watermark (the latest sample timestamp seen on the partition) advances forward only, and buckets whose timestamps fall more than `oooToleranceMs` behind the watermark are finalized and committed as a single row to the chunk vectors.

## Problem statement

Standard `TimeSeriesPartition.ingest` enforces strict in-order arrival: a sample whose timestamp is older than the most recent timestamp in the active chunk is dropped and counted on `outOfOrderDropped`. In production this assumption breaks for two practical reasons:

- **Producer-side skew.** Real producers retry, batch, and emit late. Even small clock differences between hosts can push samples behind the in-order watermark by seconds or minutes.
- **Delta-temporality metrics.** For delta counters, a dropped sample is not just a missing point — it permanently distorts every subsequent rate calculation that crosses the gap, because the dropped delta will never be re-summed by anyone downstream.

The cost of the hard-rejection model is dropped samples (visible on `outOfOrderDropped`), missing data in queries, and silently incorrect `rate()` and `delta()` results for delta-style histograms and counters.

## Goals

- Accept samples whose event-time falls within a configurable OOO tolerance window of the partition's high-water mark.
- Aggregate accepted samples into fixed-width event-time buckets in memory until a watermark advances past them.
- Persist finalized buckets as ordinary chunk rows via the existing `TimeSeriesPartition.ingest` pipeline — no new on-disk format.
- Make in-flight (active, not-yet-finalized) buckets queryable so freshly ingested data is visible immediately.
- Hold Kafka commit offsets back so that crash recovery never loses samples that were aggregated into in-memory buckets but not yet flushed.
- Use event-time semantics throughout — no wall-clock dependency. Replay is deterministic.

## Non-goals

- **Per-column tolerance windows.** Tolerance and interval are schema-wide.
- **Cross-partition aggregation.** Aggregation is strictly per-partition; samples for different partitions never combine.
- **Late-arriving data beyond the tolerance window.** Samples older than `latestSampleTimestamp - tolerance` are rejected and counted via `outOfOrderDropped`.
- **Re-aggregating already-finalized buckets.** Once a bucket is finalized and written to a chunk, additional samples for that bucket timestamp are silently dropped — they would otherwise produce duplicates.

## User-facing configuration

Aggregation is configured **per-dataset**, in an `aggregation {}` block inside the dataset's ingestion
source config (`sourceconfig`), mirroring how the `store {}` block maps to `StoreConfig`. It is *not*
part of the schema, so a single schema can be shared by aggregated and non-aggregated datasets. Three
keys go inside the block:

| Key | Meaning | Validation |
|---|---|---|
| `aggregators` | List of `name(colId)` strings, one per aggregating column; `colId` references the ingestion data schema's columns | `colId > 0` (column 0 is the timestamp); `name` must match a known aggregator |
| `interval` | Bucket width (HOCON duration) | Must be `> 0` when `aggregators` is non-empty |
| `ooo-tolerance` | OOO acceptance window (HOCON duration) | Must be `>= 0` when `aggregators` is non-empty |

The block is parsed into `AggregationConfig`
(`core/src/main/scala/filodb.core/memstore/aggregation/AggregationConfig.scala`) by `IngestionConfig`,
threaded to `TimeSeriesShard` alongside `StoreConfig`, and stored on the shard as the `aggregationConfig`
field. A non-empty `aggregators` list is the trigger for selecting `AggregatingTimeSeriesPartition` over
`TimeSeriesPartition` at partition-creation time
(`core/src/main/scala/filodb.core/memstore/TimeSeriesShard.scala`, the partition-factory branch on
`aggregationConfig.nonEmpty`). Datasets without an `aggregation {}` block are unaffected.

### Canonical example: `prometheus` dataset over `delta-histogram-v2`

The schema is an ordinary schema in `core/src/main/resources/filodb-defaults.conf`:

```hocon
delta-histogram-v2 {
  columns = ["timestamp:ts",
    "sum:double:{detectDrops=false,delta=true}",
    "count:double:{detectDrops=false,delta=true}",
    "h:hist:{counter=false,delta=true}",
    "min:double:{detectDrops=false,delta=true}",
    "max:double:{detectDrops=false,delta=true}",
    "sumLast:double:{detectDrops=false,delta=true}"
  ]
  value-column = "h"
  downsamplers = ["tTime(0)", "dSum(1)", "dSum(2)", "hSum(3)", "dMin(4)", "dMax(5)", "dLast(6)"]
  downsample-schema = "delta-histogram-v2"
  downsample-period-marker = "time(0)"
}
```

Aggregation is turned on for the dataset in its source config (`conf/timeseries-dev-source-ooo.conf`),
inside `sourceconfig`:

```hocon
aggregation {
  aggregators   = ["dSum(1)", "dSum(2)", "hSum(3)", "dMin(4)", "dMax(5)", "dLast(6)"]
  interval      = 1m
  ooo-tolerance = 2m
}
```

With this configuration, samples up to two minutes behind the latest seen timestamp are accepted, aggregated into one-minute event-time buckets, and finalized once the watermark advances 2 minutes past the bucket boundary.

### Supported aggregators

Names are parsed by `ColumnAggregator.parse` (`core/src/main/scala/filodb.core/memstore/aggregation/ColumnAggregator.scala`); the full name → `AggregationType` mapping lives in the private `nameToAggType` table inside that object.

| Name   | Aggregation type        | Behavior                                                                  |
|--------|-------------------------|---------------------------------------------------------------------------|
| `dSum`   | `Sum`                 | Sums numeric deltas in the bucket. NaN/Infinity ignored.                  |
| `dMin`   | `Min`                 | Smallest numeric value in the bucket.                                     |
| `dMax`   | `Max`                 | Largest numeric value in the bucket.                                      |
| `dLast`  | `Last`                | Numeric value with the latest sample timestamp.                           |
| `dFirst` | `First`               | Numeric value with the earliest sample timestamp.                         |
| `dCount` | `Count`               | Sample count in the bucket. Returned as `Long`.                           |
| `hSum`   | `HistogramSum`        | Element-wise sum of histogram bucket values. For delta-temporality histograms. |
| `hLast`  | `HistogramLast`       | Histogram with the latest sample timestamp. For cumulative-temporality histograms. |

Parsing: `ColumnAggregator.parse` splits on `[(@)]` (matching the existing `ChunkDownsampler.downsampler` syntax), takes the literal name and integer column id, and rejects unknown names with `IllegalArgumentException`.

## Behavior and semantics

### Event-time tolerance

A sample is accepted if and only if:

```
latestSampleTimestamp == Long.MinValue          // no samples yet
  || sampleTs >= latestSampleTimestamp - oooToleranceMs
```

`latestSampleTimestamp` is a per-partition high-water mark that advances monotonically with the largest sample timestamp seen so far. **There is no wall-clock dependency.** Replaying the same Kafka log will always produce the same accept/reject decisions and the same finalized bucket contents, regardless of when the replay runs (commit `80b8b35d5`, `BucketAggregationState.isWithinTolerance`).

Out-of-tolerance samples are counted on `shardInfo.stats.outOfOrderDropped`.

### Bucket assignment

Each accepted sample is assigned to a bucket whose timestamp is the ceiling of the sample timestamp on the configured interval:

```scala
def ceilToBucket(ts: Long, intervalMs: Long): Long =
  ((ts + intervalMs - 1) / intervalMs) * intervalMs
```

The bucket timestamp represents the **end** of a half-open interval `(bucketTs - intervalMs, bucketTs]`. A sample with `ts == bucketTs` (exact boundary) belongs to the lower bucket — `ceilToBucket` is a no-op at boundaries.

Examples with `intervalMs = 30_000`:

| Sample timestamp | Bucket timestamp |
|------------------|------------------|
| 12:00:05         | 12:00:30         |
| 12:00:25         | 12:00:30         |
| 12:00:30         | 12:00:30         |
| 12:00:31         | 12:01:00         |

### Bucket lifecycle: active → finalized

A bucket is **active** while it is held in `BucketAggregationState.activeBuckets`. While active, samples within tolerance accumulate into the bucket's per-column `Aggregator` instances.

A bucket becomes a candidate for **finalization** when, on a subsequent ingest, the threshold

```
thresholdTs = ceilToBucket(latestSampleTimestamp - oooToleranceMs, intervalMs)
```

advances past its bucket timestamp. `AggregatingTimeSeriesPartition.finalizeOldBuckets` then:

1. Gets the keys strictly less than `thresholdTs` via `TreeMap.headMap` (`BucketAggregationState.getBucketsToFinalize`). The bound is exclusive, so a bucket whose timestamp equals `thresholdTs` exactly remains active.
2. For each such bucket, materializes a `CompleteAggregatedRow` (a `RowReader` whose timestamp is the bucket timestamp and whose column values are the aggregator results) and calls `super.ingest(...)` — the standard chunk-write path.
3. Removes the bucket from `activeBuckets` and records its timestamp in `finalizedBuckets`, ensuring late samples for that bucket are rejected.

Finalization is driven by ingestion. There is no background thread and no wall-clock timer.

### Query visibility

Active buckets are queryable immediately. There is **no tolerance filter on the query path**: every bucket present in `BucketAggregationState` for the requested time range is returned. This is intentional — it lets clients see freshly aggregated data the moment a sample lands.

`ChunkSource.createRangeVector` dispatches `AggregatingTimeSeriesPartition` instances with active buckets through `AggregatingRangeVector`, which:

1. Gets the base cursor over finalized chunk rows (`partition.timeRangeRows`).
2. Snapshots active bucket values within the query range (cloning histogram value arrays and applying `makeMonotonic()` so query reads are isolated from concurrent ingest).
3. Returns a `MergingRangeVectorCursor` that drains the base cursor first, captures the timestamp of the last finalized row, and then yields bucket rows whose timestamps strictly exceed it.

Partitions whose `super.hasChunks` returns false but whose active-bucket map intersects the query range are still routed through `filterPartitions`, because `AggregatingTimeSeriesPartition.hasChunks` includes the active-bucket check (commit `47bbe01ec`).

### Idle partitions

Finalization is event-driven. A partition that stops ingesting will not finalize its trailing buckets — the watermark cannot advance without new samples. Memory cost is bounded by `tolerance/interval + 1` buckets per partition (the largest set that can simultaneously satisfy the tolerance check). This is by design: under the event-time model, a future sample is the only signal that closes a bucket. Wall-clock-driven flushing would re-introduce the wall-clock dependency the design specifically avoids.

### Histogram aggregation

`HistogramAggregator` accumulates incoming `BinaryHistogram` `DirectBuffer`s in place into a `MutableHistogram`'s `values[]` array (commit `be7404cbb`). When delta-decoded packed-bucket histograms are added, the aggregator records that monotonic correction is required; the correction is deferred to the serialization point in `result()` so query-path snapshots can apply `makeMonotonic()` independently.

`AggregatingRangeVector.snapshotBuckets` clones the `MutableHistogram.values` array and calls `makeMonotonic()` on the clone — the in-flight accumulator is never mutated by the query path, and concurrent ingest of further samples cannot corrupt an in-progress query result.

### Dataset constraint

Only datasets whose source config has a non-empty `aggregation {}` block create `AggregatingTimeSeriesPartition`. Datasets without one continue to use `TimeSeriesPartition` and remain bit-identical in behavior. The two paths are selected at `addPartition` time by branching on `aggregationConfig.nonEmpty`.

### Worked example

Given the canonical schema above (`intervalMs = 60_000`, `oooToleranceMs = 120_000`), the following sequence of samples illustrates accept, reject, and finalization decisions on a single partition:

| Step | Sample ts (`mm:ss`) | Watermark before | Bucket ts | Active buckets after step              | Watermark after |
|------|--------------------|------------------|-----------|----------------------------------------|-----------------|
| 1    | 00:30              | `Long.MinValue`  | 01:00     | `{01:00}` (first sample; no tolerance check applies) | 00:30           |
| 2    | 00:45              | 00:30            | 01:00     | `{01:00}` (within tolerance, second sample joins existing bucket) | 00:45           |
| 3    | 02:10              | 00:45            | 03:00     | `{01:00, 03:00}` (threshold after update = `ceil(00:10, 1m) = 01:00`; bucket 01:00 < 01:00 is false → no finalization, exclusive bound) | 02:10           |
| 4    | 03:30              | 02:10            | 04:00     | `{03:00, 04:00}` (threshold = `ceil(01:30, 1m) = 02:00`; bucket 01:00 < 02:00 → **finalize 01:00 to chunk**) | 03:30           |
| 5    | 01:20              | 03:30            | —         | unchanged — sample rejected; `01:20 < 03:30 - 02:00 = 01:30`. `outOfOrderDropped++`. | 03:30           |
| 6    | 02:30              | 03:30            | 03:00     | `{03:00, 04:00}` (sample within tolerance, joins active bucket; watermark unchanged) | 03:30           |
| 7    | 06:00              | 03:30            | 06:00     | `{04:00, 06:00}` (threshold = `ceil(04:00, 1m) = 04:00`; bucket 03:00 < 04:00 → **finalize 03:00 to chunk**; bucket 04:00 = 04:00 stays active, exclusive bound) | 06:00           |

Step 5 is the rejection case: with the watermark at 03:30, the tolerance window opens at 01:30, and a sample at 01:20 is too late. Step 7 illustrates that the exclusive `headMap` bound at the threshold matters — bucket 04:00 has timestamp equal to the finalization threshold and is therefore retained, not closed.

## Architecture

### Components

| Component | Path | Role |
|---|---|---|
| `AggregatingTimeSeriesPartition` | `core/src/main/scala/filodb.core/memstore/AggregatingTimeSeriesPartition.scala` | Subclass of `TimeSeriesPartition`. Overrides `ingest` and `hasChunks`. |
| `BucketAggregationState` | `core/src/main/scala/filodb.core/memstore/aggregation/BucketAggregationState.scala` | Per-partition map of active buckets and finalization bookkeeping. |
| `BucketState` | same file | Per-bucket struct holding aggregator instances and raw values for non-aggregated columns. |
| `Aggregator` trait + concrete classes | `core/src/main/scala/filodb.core/memstore/aggregation/Aggregator.scala` | `SumAggregator`, `MinAggregator`, `MaxAggregator`, `LastAggregator`, `FirstAggregator`, `CountAggregator`, `HistogramAggregator`, `HistogramLastAggregator`. |
| `ColumnAggregator` | `core/src/main/scala/filodb.core/memstore/aggregation/ColumnAggregator.scala` | Parser for the `name(colId)` string syntax. |
| `TimeBucket.ceilToBucket` | `core/src/main/scala/filodb.core/memstore/aggregation/TimeBucket.scala` | Bucket-timestamp ceiling function. |
| `AggregatingRangeVector` | `core/src/main/scala/filodb.core/query/AggregatingRangeVector.scala` | Query-side wrapper; merges chunk rows with active-bucket snapshot. |
| `MergingRangeVectorCursor` | same file | Drains base cursor, then yields bucket rows past the last finalized timestamp. |
| `BucketDataRowReader` | same file | `RowReader` over a snapshotted bucket. |
| `TimeSeriesShard.prepareFlushGroup` | `core/src/main/scala/filodb.core/memstore/TimeSeriesShard.scala` | Computes Kafka commit hold-back offset across all aggregating partitions in the group. |
| `ChunkSource.createRangeVector` | `core/src/main/scala/filodb.core/store/ChunkSource.scala` | Dispatches between `AggregatingRangeVector` and `RawDataRangeVector`. |
| `ChunkSource.filterPartitions` | same file | Includes partitions whose only data is in active buckets via `hasChunks`. |

### Ingest data flow

```
Kafka record
   │
   ▼
IngestConsumer  ─────────────────────────────┐
   │                                          │
   ▼                                          │
TimeSeriesShard.ingest                        │
   │  agg.currentIngestOffset = ingestOffset  │  (offset side-channel,
   ▼                                          │   per Fix 1 / 788af068d)
AggregatingTimeSeriesPartition.ingest         │
   │                                          │
   ├── BucketAggregationState.aggregate       │
   │     ├── reject if outside tolerance      │
   │     │   (outOfOrderDropped++)            │
   │     ├── reject if bucket finalized       │
   │     ├── per-column aggregator.add[*]     │
   │     └── update latestSampleTimestamp     │
   │                                          │
   └── finalizeOldBuckets                     │
         ├── thresholdTs = ceilToBucket(      │
         │       latestSampleTimestamp -      │
         │       tolerance, interval)         │
         ├── for each bucket < thresholdTs:   │
         │     CompleteAggregatedRow → super.ingest() ─► chunk pipeline
         └── mark bucket finalized
```

### Query data flow

```
ChunkSource.rangeVectors
   │
   ▼
filterPartitions (uses hasChunks, which includes active-bucket presence)
   │
   ▼
createRangeVector
   │
   ├── AggregatingTimeSeriesPartition with active buckets ──► AggregatingRangeVector
   │                                                           │
   │                                                           ▼
   │                                                         rows()
   │                                                           │
   │                                                ┌──────────┴───────────┐
   │                                                ▼                      ▼
   │                                          baseCursor over          snapshotBuckets
   │                                          finalized chunks         (clone + makeMonotonic)
   │                                                ▼                      ▼
   │                                                └────► MergingRangeVectorCursor ◄──┘
   │
   └── any other partition ──► RawDataRangeVector
```

`MergingRangeVectorCursor` drains the base cursor first; it captures `lastFinalizedTimestamp` from the last row read, then advances the prefetched bucket iterator past any bucket whose timestamp is `<=` that watermark before yielding remaining bucket rows.

### Crash recovery: Kafka offset hold-back

Aggregated samples live in heap memory only. To guarantee no loss across restart, Kafka commit offsets are held back to the earliest offset referenced by any active bucket (commit `788af068d`):

1. Each `BucketAggregationState.aggregate` records the minimum Kafka offset seen for its bucket in `bucketMinOffset`.
2. `AggregatingTimeSeriesPartition.earliestActiveBucketOffset` (delegating to `BucketAggregationState.earliestActiveOffset`) returns the minimum over all active buckets (`Long.MaxValue` if none).
3. `TimeSeriesShard.prepareFlushGroup` walks every partition in the flush group, takes the minimum `earliestActiveBucketOffset` across `AggregatingTimeSeriesPartition` instances, and computes:

   ```scala
   val flushOffset =
     if (holdOffset == Long.MaxValue) latestOffset
     else math.min(latestOffset, holdOffset - 1)
   ```

   The committed offset is one less than the earliest active-bucket offset, ensuring crash recovery re-reads every Kafka record contributing to an in-memory bucket. Because aggregation is deterministic under event-time semantics, replay reproduces identical bucket contents.

The offset is plumbed through a side channel (`AggregatingTimeSeriesPartition.currentIngestOffset`, set by `TimeSeriesShard`'s `IngestConsumer.onNext` path inside `getOrAddPartitionAndIngest` — see `core/src/main/scala/filodb.core/memstore/TimeSeriesShard.scala` around the `agg.currentIngestOffset = ingestConsumer.ingestOffset` assignment) so the existing `ingest(ingestionTime, row, ...)` signature is unchanged.

## Performance characteristics

Source: [`docs/plans/2026-05-02-ooo-aggregation-benchmark-results.md`](../plans/2026-05-02-ooo-aggregation-benchmark-results.md). Apple M1 Max, JDK 17, JMH 1.37, `-Xmx2G`.

### Ingestion (throughput, higher is better)

| Benchmark                     | Throughput (ops/s)        | Alloc (B/op) | Relative to baseline |
|-------------------------------|---------------------------|--------------|----------------------|
| `ingestRegular` (baseline)    | 11,495,273 ± 5,462,200    | 360.0        | 1.00× (baseline)     |
| `ingestAggregatingInOrder`    | 53,302,124 ± 6,563,459    | 24.0         | **4.64× faster**     |
| `ingestAggregatingOOO`        | 11,864,391 ± 6,869,761    | 246.3        | 1.03× (parity)       |

In-order ingest on the aggregating path is ~4.6× faster than the baseline `TimeSeriesPartition` and allocates ~15× less per sample (24 B vs 360 B). OOO ingest matches baseline throughput within noise and still allocates ~32% less than baseline.

### Query (throughput, higher is better)

| Benchmark            | Throughput (ops/s)     | Alloc (B/op) |
|----------------------|------------------------|--------------|
| `queryRegular`       | 5,334,727 ± 1,336,659  | 328.0        |
| `queryAggregating`   | 1,693,645 ± 84,406     | 1,024.0      |

The aggregating query path is ~3.1× slower with ~3.1× more allocation per op. The overhead is concentrated in `MergingRangeVectorCursor` plumbing and the per-bucket snapshot/clone in `AggregatingRangeVector.snapshotBuckets`.

### Finalization (average time, lower is better)

| Benchmark           | Avg time (us/op)      | Alloc (B/op) |
|---------------------|-----------------------|--------------|
| `finalizeBuckets`   | 10.7 ± 55.1           | 40,362       |

~10.7 µs per bucket flush, batched across hundreds-to-thousands of samples per bucket. Per-sample amortized cost is sub-nanosecond.

### Optimization history

- **Tier 1 — in-place histogram aggregation (commit `be7404cbb`).** Eliminated per-sample allocation of `HistogramWithBuckets` by adding `BinaryHistogram.addValuesTo(buf, target)`, which decodes packed values directly into the existing accumulator's `values[]` array. This is the dominant ingest-side win (24 B/op in-order vs. ~1 KB/op pre-Tier-1).
- **Tier 2 — primitive parallel-array bucket map (commit `43dc2b2e1`, reverted in `228651020`; rejection recorded in `4e474b6f8`).** Replacing `java.util.TreeMap[java.lang.Long, BucketState]` with sorted parallel `long[]/Object[]` arrays. The measured allocation/throughput improvement did not justify the added complexity of overflow/grow logic and bucket-removal compaction. **Rejected after benchmarking.**

## Operability

### Metrics

The OOO path exposes its observability through one existing counter and one programmatic accessor; there are no new exported metric names.

| Signal                                  | Source                                                              | Meaning                                                                                                  |
|-----------------------------------------|---------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `memstore-out-of-order-samples` counter | `TimeSeriesShardStats.outOfOrderDropped` (incremented in `AggregatingTimeSeriesPartition.ingest:112`) | Samples rejected because they are outside the tolerance window or target an already-finalized bucket. A sustained nonzero rate means producers are emitting samples beyond `ooo-tolerance` and the configured tolerance is too tight (or upstream skew is genuinely too large). |
| `BucketAggregationStats.activeBucketCount` | `AggregatingTimeSeriesPartition.bucketAggregationStats` (per partition) | Number of active (unfinalized) buckets currently held in memory for the partition. Bounded by `tolerance/interval + 1`. |
| `BucketAggregationStats.finalizedBucketCount` | same                                                                | Number of finalized bucket timestamps still tracked for late-sample rejection. Periodically pruned by `cleanupOldFinalizedTracking` to `2 × tolerance` behind the threshold. |
| `BucketAggregationStats.latestSampleTimestamp` | same                                                                | Per-partition event-time watermark. Useful for diagnosing why a producer's late sample was dropped: compare against the rejected sample's timestamp. |

### Tuning guidance

- **`interval` (bucket width).** Smaller intervals preserve more time resolution in queries but increase per-partition `activeBucketCount`. The chunked write path emits one chunk row per finalized bucket, so very small intervals proportionally inflate chunk volume.
- **`ooo-tolerance`.** Sets the allowed event-time skew. Setting tolerance to a value smaller than producer-side skew will permanently drop the late samples; setting it much larger increases per-partition memory (`tolerance/interval + 1` buckets) and lengthens the window during which a finalized chunk row is "young" — i.e. a query immediately after a sample arrives may need to merge active and finalized data via `MergingRangeVectorCursor`.
- **Dataset selection.** A dataset that does not need OOO tolerance should not declare an `aggregation {}` block. The aggregating path imposes per-bucket merge overhead on the query side (~3.1× slower than `RawDataRangeVector` per the benchmarks), and the in-place histogram fast path is not relevant when there is at most one sample per bucket.
- **Validation at first partition.** `AggregationConfig.validate` (invoked in `TimeSeriesShard.createNewPartition` on the first aggregating partition, against the ingestion schema's data columns) rejects a config whose `interval` is non-positive, whose `ooo-tolerance` is negative, or whose `aggregators` reference column id 0 (the timestamp) or an out-of-range column. A mis-configured `aggregation {}` block fails fast — it will not silently fall back to standard ingestion.

### Diagnosis recipes

- **"Sample drops appeared after enabling OOO."** If `outOfOrderDropped` rises but ingest looks healthy otherwise, the most common cause is producers emitting samples whose event-time falls outside the tolerance window. Compare `BucketAggregationStats.latestSampleTimestamp` to the rejected sample's timestamp (if you have it logged). Either widen `ooo-tolerance` or fix the producer.
- **"Memory grows on idle partitions."** This is the trailing-bucket retention behavior described in *Limitations*. It is bounded — a single idle partition holds at most `tolerance/interval + 1` buckets — but a fleet of idle partitions can be visible. There is no mitigation today: the event-time model deliberately does not flush on wall-clock.
- **"Crash recovery shows duplicate samples in active buckets."** Aggregators are deterministic, so re-aggregating the same Kafka records produces identical results. Duplicate values can only appear if the offset hold-back was bypassed. Check that `prepareFlushGroup` actually saw the partition (it must be a real `AggregatingTimeSeriesPartition` instance, not a fallback `TimeSeriesPartition`) and that `earliestActiveBucketOffset` returned a value other than `Long.MaxValue` at flush time.

## Limitations and known gaps

- **Bucket-timestamp boundary semantics.** Aggregated rows carry the bucket-end timestamp, not the original sample timestamp. PromQL operators that assume raw samples (e.g. `irate`, `delta` over very short windows) may produce subtly different results than they would over un-aggregated rows. Per-sample timestamp fidelity is intentionally traded for OOO tolerance.

- **PromQL semantics differ across aggregator types.** `rate()` over a `dSum`-aggregated delta column is approximately equivalent to `rate()` over the raw deltas (sum is associative). `rate()` over a `dLast`-aggregated column is **not** equivalent — it operates on a single per-bucket sample, which is rarely what a user wants for a rate. Choose aggregators based on the semantics of downstream queries, not just the data type.

- **`MergingRangeVectorCursor` ordering assumption.** The cursor assumes that the timestamp of the first active bucket is strictly greater than the last finalized chunk row's timestamp. Under normal forward-only ingestion this holds — `finalizeOldBuckets` writes chunks for buckets whose timestamps are below the watermark and only retains buckets above it. A recovery race in which an in-memory active bucket has the same timestamp as a freshly-finalized chunk row could produce an active-bucket row that is filtered out. This is a known correctness gap and is currently deferred.

- **Idle partitions hold trailing buckets indefinitely.** A partition with no further ingestion will never close its last `tolerance/interval + 1` buckets — the event-time watermark cannot advance without new samples. The memory cost is bounded but non-zero.

- **No re-aggregation of finalized buckets.** Late samples whose bucket timestamp is in `finalizedBuckets` are silently dropped (counted on `outOfOrderDropped`). There is no "patch a closed bucket" path.

- **Non-aggregating columns take the first value seen for the bucket.** Columns not listed in `aggregators` are passed through using the first non-null value observed for the bucket (`BucketState.setValue` only writes when no value is present — see `BucketAggregationState.aggregate`). Subsequent samples cannot overwrite that value.

## Acceptance criteria

Each criterion is phrased as a testable invariant. The `*Spec.scala` references locate the test that exercises the invariant. All paths are relative to repo root.

| # | Invariant | Verified by |
|---|-----------|-------------|
| 1 | A sample with `sampleTs >= latestSampleTimestamp - tolerance` is accepted; a sample older than that is rejected and counted on `outOfOrderDropped`. | `core/src/test/scala/filodb.core/memstore/aggregation/AggregatorSpec.scala`, `core/src/test/scala/filodb.core/memstore/aggregation/BucketAggregationStateSpec.scala` |
| 2 | The tolerance check uses only `latestSampleTimestamp` and `sampleTs`. No wall-clock call is made on the ingest path. Replay produces identical accept/reject decisions. | `core/src/test/scala/filodb.core/memstore/aggregation/EventTimeWatermarkSpec.scala` |
| 3 | A sample whose bucket has not yet been finalized is queryable in the same range-vector emit cycle that follows the ingest. | `core/src/test/scala/filodb.core/query/AggregatingRangeVectorSpec.scala` |
| 4 | Future-dated samples (timestamps beyond any wall-clock reference) are queryable; their visibility does not depend on wall-clock advancement. | `core/src/test/scala/filodb.core/memstore/aggregation/EventTimeWatermarkSpec.scala` |
| 5 | Histogram values returned by query are monotonic; concurrent ingest of further samples for the same bucket cannot mutate the value array seen by the query. | `core/src/test/scala/filodb.core/query/AggregatingRangeVectorSpec.scala` |
| 6 | After a crash + restart with Kafka replay, all in-memory bucket data is reconstructed. The committed Kafka offset is held to `min(latestOffset, earliestActiveBucketOffset - 1)`. | Plumbing exercised in `core/src/main/scala/filodb.core/memstore/TimeSeriesShard.scala::prepareFlushGroup`; partition-level offset bookkeeping in `BucketAggregationStateSpec.scala`. |
| 7 | A partition whose only data lives in active buckets passes `hasChunks` for a covering `TimeRangeChunkScan` and produces an `AggregatingRangeVector`. | `core/src/test/scala/filodb.core/memstore/AggregatingTimeSeriesPartitionSpec.scala` |
| 8 | In-place histogram aggregation produces output byte-identical to the pre-Tier-1 path for any sequence of `BinaryHistogram` samples within a bucket. | `core/src/test/scala/filodb.core/memstore/aggregation/AggregatorSpec.scala` |

## References

- ADR: [`doc/adr-aggregating-buffers.md`](../../doc/adr-aggregating-buffers.md) — design decisions and rationale.
- Heap optimization design: [`docs/plans/2026-04-30-agg-buffer-heap-optimization-tier1-tier2-design.md`](../plans/2026-04-30-agg-buffer-heap-optimization-tier1-tier2-design.md).
- Benchmark results: [`docs/plans/2026-05-02-ooo-aggregation-benchmark-results.md`](../plans/2026-05-02-ooo-aggregation-benchmark-results.md).
- PR #2154 — initial implementation.

### Key commits on `out-of-orderness`

| SHA          | Subject                                                                  |
|--------------|--------------------------------------------------------------------------|
| `788af068d`  | Fix 1: hold Kafka commit watermark for in-memory aggregation buckets     |
| `be7404cbb`  | Tier 1: in-place histogram aggregation (eliminate per-sample allocation) |
| `80b8b35d5`  | Event-time watermark refactor — remove wall-clock from OOO path          |
| `47bbe01ec`  | Include active-bucket-only partitions in query results (`hasChunks` fix) |
| `8edbf5771`  | Collapse `AggregationConfig` into per-column type + schema-level interval |
| `aa1cc1f45`  | Add `Aggregator.queryValue` trait method, remove downcasts in `getValueForQuery` |
