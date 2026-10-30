# Binary Join Queries with Arrow Flight vs. Akka Query Dispatch — Performance Comparison

**Date:** 2026-10-29

## Summary

At the same request rate, dispatching query execution over Arrow Flight instead of Akka remoting cuts
client-observed latency by **~2.6x** while also using **~30% less CPU**, allocating **~3.5x less heap** and
spending **~56% less time in GC pauses**. As in the previous report, the latency win is not paid for with
extra resources. The resource savings are the cause of it: Akka spends most of its extra work serializing,
compressing and deserializing results.

| Metric | Akka (baseline) | Arrow Flight | Improvement |
|---|---|---|---|
| **P50 latency** | 275 ms | 101 ms | **2.7x faster** |
| **P99 latency** | 510 ms | 194 ms | **2.6x faster** |
| **Mean latency** | 287 ms | 111 ms | **2.6x faster** |
| **JVM CPU (18 cores)** | 18.5% (3.33 cores) | 12.9% (2.32 cores) | **30% lower** |
| **Young-gen allocation rate** | 2,441 MB/s | 700 MB/s | **3.5x lower** |
| **Total GC pause time** | 3.74 s/min (6.1%) | 1.64 s/min (2.6%) | **56% lower** |
| **Max committed heap** | 2,202 MB | 912 MB | **2.4x lower** |

When the previous version of the report was written, Gatling runs were broken since we were in
the middle of the Scala/Java upgrade. Load tests were run and latencies were measured using a Python script using half the throughput.
Since then, Gatling simulation tests have been fixed, the Flight path has gained server backpressure for slow clients, runs its
server pipeline on the query scheduler, and no longer allocates per-RV in Arrow result serialization.
Using Gatling runs for performance comparison yields a more accurate picture of the gap.

---

## Latency (client-observed)

| Percentile | Akka | Flight | Improvement |
|---|---|---|---|
| Min | 215 ms | 83 ms | 2.6x |
| P50 | 275 ms | 101 ms | 2.7x |
| P75 | 294 ms | 121 ms | 2.4x |
| P95 | 384 ms | 146 ms | 2.6x |
| P99 | 510 ms | 194 ms | 2.6x |
| Max | 744 ms | 458 ms | 1.6x |
| Std dev | 51 ms | 25 ms | 2.0x tighter |

**Why:** The improvement is roughly the same at every percentile from min to P99, which points to a fixed
per-request cost being removed (result serialization and transport) rather than fewer rare outliers.
Flight's distribution is also about twice as tight.

## CPU

| | Akka | Flight | Improvement |
|---|---|---|---|
| JVM CPU (user + system) | 18.5% (3.33 cores) | 12.9% (2.32 cores) | 30% |
| CPU per query | ~335 ms | ~233 ms | 30% |
| Whole-machine CPU (includes Gatling) | 47.8% | 37.8% | 21% |

Where busy (on-CPU) execution samples go:

| Category | Akka | Flight |
|---|---|---|
| Leaf scan + range functions (the actual query work) | 24% | 40% |
| Reading serialized results back row by row | **24%** | 2% |
| Serializing results (`SerializedRangeVector` / Arrow VSR population) | 11% | 11% |
| Transport (Akka remoting + Kryo + LZ4 / gRPC + Netty) | 9% | **18%** |
| GC threads | **12%** | 5% |
| Binary join + aggregation | 15% | 16% |

**Why:** Both paths spend about the same absolute CPU on the real query work (leaf scans, range functions,
joins). Akka adds a large cost on the receiving side: iterating `SerializedRangeVector` / `RecordContainer`
rows with boxing. On top of that come LZ4 compression, Kryo and Akka remoting, plus three times the GC CPU.
Flight's main overhead is the gRPC/Netty event loop moving Arrow buffers. That costs more than Akka's
transport layer alone, but it removes the much larger cost of reading results back.

## Heap Memory & Allocation

| | Akka | Flight | Improvement |
|---|---|---|---|
| Young-gen allocation rate | 2,441 MB/s | 700 MB/s | 3.5x |
| Allocation per query | ~245 MB | ~70 MB | 3.5x |
| Heap after GC (p50 / max) | 894 / 1,463 MB | 515 / 803 MB | 1.7x / 1.8x |
| Old gen after GC | 643–1,036 MB (swinging) | 453–457 MB (flat) | |
| Survivor space after young GC (avg) | 64 MB | 13 MB | 5x |
| Max heap used | 2,042 MB | 859 MB | 2.4x |
| Max committed heap | 2,202 MB | 912 MB | 2.4x |

**Why:** About half of Akka's sampled allocation is byte arrays, from Kryo `readBytes`/`toBytes`, protobuf
`ByteString.copyFrom` and Netty 3 `HeapChannelBuffer`. Another 12% is `java.lang.Integer` boxing in
`SerializedRangeVector.estimateSerializedRowBytes`, and the Akka remote dispatcher thread alone accounts for
about a third of all allocation. Akka also keeps more in-flight result data alive across young GCs (5x
the survivor occupancy). That data gets promoted, so old gen swings and concurrent marking has more work.
Flight's old gen stays flat for the whole run.

The largest remaining allocation site on Flight is a new `ChunkSetInfoOffHeap` per chunk in
`ChunkInfoIterator.nextInfoReader` (25–32% of Flight's sampled allocation). Akka allocates a similar
absolute volume there, a smaller share of its much larger total. It is a candidate for a follow-up that
would benefit both paths.

## Garbage Collection

| | Akka | Flight | Improvement |
|---|---|---|---|
| Young GCs | 3.9/s (avg eden 624 MB) | 1.8/s (avg eden 396 MB) | 2.2x fewer |
| Total GC pause time | 3.74 s/min (6.1% of wall time) | 1.64 s/min (2.6%) | 56% |
| Pause p50 / p99 / max | 5.7 / 36.5 / 41.9 ms | 4.2 / 31.0 / 31.2 ms | |
| GC thread CPU (busy samples) | 1,981 | 549 | 3.6x |

**Why:** GC frequency follows the allocation rate. Individual pauses are similar in length, but Akka
pauses more than twice as often and its GC threads use more than three times the CPU.

## Off-Heap Memory

Flight keeps result batches in off-heap Arrow buffers (`ArrowBuf`, pooled Netty allocator), so the heap
figures above understate Flight's total memory use. JFR does not track Arrow allocator memory. A
byte-accurate number is available from the `flight-used-memory` metric (and the new peak memory metric),
but those were not exported in these runs. Turning on the metrics reporter for the next run would fill
this gap.

## Performance Test Setup

* 2 FiloDB processes, 4 GB heap max, using `conf/promperf-filodb-server.conf` config
* 1 Query facade process that does the query planning and routes execution plans to FiloDB nodes
* FiloDB cluster has 16 shards, 8 shards for each FiloDB process
* Single Partition Binary Join query, issued via the query facade
* Both runs use the query scheduler
* JDK 21, with G1GC
* MacBook Pro M5 Max, 18 cores

### Methodology notes

- Load generated with Gatling `BinaryJoinQueryRangeSimulation`. It injects 10 users/s for 30 s, then 10 users/s
  for 3 min, for 2,100 `query_range` requests per run (3 h range, 60 s step). Same query, dataset and shard
  layout for each dispatch path. 0 errors in every run.
- Server-side metrics come from JFR recordings of about 60 s, taken during the load (async-profiler based,
  via IntelliJ):
- They were analyzed with a custom JFR event parser covering CPU load, GC heap summaries, G1 before/after-GC
  summaries, execution samples and allocation samples:
  - **Allocation rate** is derived from eden occupancy before each young GC. Sampled allocation weights were
    used only to rank allocation sites, because their totals varied too much between recordings (average
    sample weight 5.3–26 MB) to compare across runs.
  - **GC pause durations** are timed from the G1 before/after heap-summary events, because GC pause events
    were not enabled in this recording profile.
  - **Busy CPU breakdowns** exclude idle and parked samples (`__psynch_cvwait`, `kevent`, etc.).
- The Flight baseline ran on a freshly started JVM (3x more JIT compiler CPU than the other runs), so it was
  slightly disadvantaged.
- All figures are single-run measurements, not averages across repeated trials. Treat them as
  directionally strong: the Akka vs. Flight gaps are large compared with run-to-run noise. The
  differences between Flight runs are not.
- Profiling should start before the load; attaching mid-run stalled requests for about a second in one run.

## Detailed Latency Statistics

Query: `count(heap_usage0{_ns_="App-0",_ws_="demo"} + heap_usage1{_ns_="App-0",_ws_="demo"})`

### Flight RPC

```
---- Global Information -------------------------------------------------------------|---Total---|-----OK----|----KO----
> request count                                                                      |     2,100 |     2,100 |         -
> min response time (ms)                                                             |        83 |        83 |         -
> max response time (ms)                                                             |       458 |       458 |         -
> mean response time (ms)                                                            |       111 |       111 |         -
> response time std deviation (ms)                                                   |        25 |        25 |         -
> response time 50th percentile (ms)                                                 |       101 |       101 |         -
> response time 75th percentile (ms)                                                 |       121 |       121 |         -
> response time 95th percentile (ms)                                                 |       146 |       146 |         -
> response time 99th percentile (ms)                                                 |       194 |       194 |         -
> mean throughput (rps)                                                              |      9.95 |      9.95 |         -
```

### Akka RPC

```
---- Global Information -------------------------------------------------------------|---Total---|-----OK----|----KO----
> request count                                                                      |     2,100 |     2,100 |         -
> min response time (ms)                                                             |       215 |       215 |         -
> max response time (ms)                                                             |       744 |       744 |         -
> mean response time (ms)                                                            |       287 |       287 |         -
> response time std deviation (ms)                                                   |        51 |        51 |         -
> response time 50th percentile (ms)                                                 |       275 |       275 |         -
> response time 75th percentile (ms)                                                 |       294 |       294 |         -
> response time 95th percentile (ms)                                                 |       384 |       383 |         -
> response time 99th percentile (ms)                                                 |       510 |       499 |         -
> mean throughput (rps)                                                              |      9.95 |      9.95 |         -
```

## Bottom line

The Akka path pays a cost on every query result: encode it into binary records, serialize it with Kryo,
compress it with LZ4, ship it, then decompress, deserialize and iterate it row by row on the receiving
side. That cost dominates the CPU beyond the query work itself and is the largest source of heap garbage,
which in turn drives GC frequency, survivor occupancy and old-gen growth. Arrow Flight removes most of it by
keeping results in Arrow's columnar, off-heap format end to end. Its remaining overhead is mostly the
gRPC/Netty transport.

Queries that export a lot of data out of the storage nodes (binary joins, raw time series export, logical
operators, etc.) benefit the most from Flight RPC. With more remote shards, as is typical in production,
the gap should widen further, because Akka's serialization cost is paid per remote shard and concentrates
on the node that gathers the results.
