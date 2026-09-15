# Binary Join Queries with Arrow Flight vs. Akka Query Dispatch — Performance Comparison

**Date:** 2026-07-14

## Summary

Switching query result dispatch from Akka remoting to Arrow Flight *for Binary Join query*
cuts client-observed latency by **~4.2x** while simultaneously *reducing* server-side CPU,
memory churn, and GC activity — a rare case where a latency win doesn't come at the cost
of additional resource consumption. It's the opposite: the resource savings are the root
cause of the latency win.

| Metric | Akka (baseline) | Arrow Flight | Improvement |
|---|---|---|---|
| **P50 latency** | 2,243.6 ms | 519.2 ms | **4.3x faster** |
| **P99 latency** | 2,939.6 ms | 705.2 ms | **4.2x faster** |
| **Mean latency** | 2,199.2 ms | 519.9 ms | **4.2x faster** |
| **Avg CPU (JVM user time)** | 42.1% | 18.3% | **2.3x lower** |
| **Avg CPU (whole machine)** | 92.2% | 56.3% | **1.6x lower** |
| **Avg heap in use** | 3,396.7 MB | 808.8 MB | **4.2x lower** |
| **Heap allocation rate** | 4,260 MB/s | 450 MB/s | **9.5x lower** |
| **GC cycles per test run** | 306 | 24 | **12.8x fewer** |
| **Off-heap (Arrow) buffer usage** | negligible | active, bounded pool | *(architectural — see note)* |

*Off-heap row is a qualitative finding, not a byte count — see "Off-Heap Memory" section below for why, and how
to get exact bytes in a follow-up run.*

---

## Latency (client-observed)

| Percentile | Akka | Flight | Improvement |
|---|---|---|---|
| P50 | 2,243.6 ms | 519.2 ms | 4.3x |
| P90 | 2,642.0 ms | 575.6 ms | 4.6x |
| P99 | 2,939.6 ms | 705.2 ms | 4.2x |
| Max | 3,150.1 ms | 746.2 ms | 4.2x |

**Why:** Flight's tail (P99) tracks its median (P50) closely — both are ~4x better than Akka at every percentile,
not just on average. That consistency across percentiles indicates the improvement comes from removing a fixed
per-request cost (serialization), not from reducing rare outliers.

## CPU

| | Akka | Flight | Improvement |
|---|---|---|---|
| Avg JVM user time | 42.1% | 18.3% | 2.3x |
| Avg whole-machine CPU | 92.2% | 56.3% | 1.6x |

**Why:** Profiling shows Akka's hottest code paths are range-vector *serialization and compression* (binary-record
encoding, LZ4 compression) — work that exists purely to move query results across the wire. Flight's hottest paths
are the *query computation itself* (joins, aggregation) — the serialization step is largely eliminated because
Arrow's columnar buffers are streamed directly instead of being re-encoded into Java objects first. Additionally,
because Akka takes ~4x longer per request, more requests are in flight concurrently at any moment under the same
request rate, compounding the CPU load — slow responses cause work to pile up, which shows up as sustained high CPU.

## Heap Memory & Allocation

| | Akka | Flight | Improvement |
|---|---|---|---|
| Avg heap used | 3,396.7 MB | 808.8 MB | 4.2x |
| Peak heap used | 4,279.4 MB (at the 4 GB configured max) | 3,006.9 MB | 1.4x |
| Allocation rate | 4,260 MB/s | 450 MB/s | 9.5x |
| Allocated bytes per query (est.) | ~874 MB | ~92 MB | 9.4x |

**Why:** Under Akka, every query result is serialized into on-heap byte arrays (byte-array allocation alone
accounts for ~2/3 of all sampled heap allocation) and then LZ4-compressed for transport. That's the same work
identified as the CPU hotspot in Section 2 — it's also the single largest source of garbage. Flight avoids
materializing results as heap byte arrays in the first place, so there's roughly an order of magnitude less
garbage generated per query.

With Flight, much less heap memory can be allocated than what was assigned for the perf test because of the
low allocation rate.

## Garbage Collection

| | Akka | Flight | Improvement |
|---|---|---|---|
| GC cycles observed | 306 | 24 | 12.8x |

**Why:** GC frequency is a direct consequence of allocation rate (Section 3) — allocate ~9.5x faster, and
the collector has to run proportionally more often to reclaim it. Under Akka, heap occupancy also runs right
up against the configured 4 GB ceiling for most of the test, which typically means more expensive, less
efficient collections; Flight has comfortable headroom throughout.

## Off-Heap Memory

Arrow Flight is designed around off-heap columnar buffers (`ArrowBuf`, backed by a pooled Netty allocator)
instead of on-heap Java objects, and FiloDB confirms this: the profiler's allocation samples show extensive
touching of Arrow/Netty off-heap buffer classes on the Flight path (`ArrowBuf`, `DirectByteBuffer`, Netty
buffer/allocation-manager classes) and almost none on the Akka path.

**What we can't yet say precisely:** we do not have a reliable *byte count* for off-heap usage from this test.
The profiler's allocation-weight numbers are a statistical extrapolation tuned for on-heap objects; applied
to small buffer-wrapper objects, they wildly overstate the actual bytes (individual samples showing >6 GB for
a single ~50-byte wrapper object). That's a sampling artifact, not a real number, so it's intentionally left
out of the summary table above.

**How to get an exact number:** FiloDB already instruments this precisely — `flight-used-memory` and
`flight-allocated-memory` metrics (tagged by allocator) report real off-heap bytes reserved by Arrow's
allocator, sourced directly from the allocator's own accounting rather than sampling. They aren't currently
being exported in this test environment (the metrics reporter is disabled by default). Turning it on for
the next run would give a byte-accurate off-heap figure to complete this table with the same rigor as the other rows.

## Performance Test Setup

* 2 FiloDB processes, 4GB heap max, 6GB off-heap max, using `conf/promperf-filodb-server.conf` config
* Filodb codebase at commit `07c400d0bbf62617781b2be1fbe0231db3b9890a` (2026-07-14)
* 1 Query facade process that does the query planning and routes execution plan to FiloDB nodes
* FiloDB cluster has 16 shards , 8 shards for each FiloDB
* Single Partition Binary Join Query for 5000 time series issued via Query Service
* JDK21, with G1GC
* Macbook Pro M5 Max, 18 cores

### Methodology notes

- Load generated via `scripts/http_load_test.py` at 5 rps for 60s (300 requests) against each dispatch path,
  same query/dataset/shard layout (the Gatling tests in the repo could not be used since the Gatling upgrade
  since Scala 2.13 brought in Akka 2.6 which conflicts with FiloDB which still uses Akka 2.5. Gatling runs need
  to get fixed). This is a poor man's load generator applied for both Akka & Flight and works for small QPS.
- Server-side metrics from JFR recordings captured over the same window (`FiloServer_2026_07_14_100729 - Flight.jfr`,
  `FiloServer_2026_07_14_100429 - Akka.jfr`), analyzed with a custom JFR event parser covering CPU load, GC heap 
  summaries, G1 before/after-GC deltas (used for the allocation-rate figures, cross-checked against sampled allocation
  weights), and wall-clock execution samples.
- GC pause *durations* were not captured in this recording profile (`jdk.GCPhasePause` was not enabled) — GC cycle
  *counts* and heap occupancy are used as the proxy for GC pressure instead.
- All figures are single-run measurements, not averaged across repeated trials; treat as directionally strong
  (the gaps are large relative to run-to-run noise) rather than statistically rigorous.

## Detailed Latency Statistics

Query: `count(heap_usage0{ns="App-0",ws="demo"} + heap_usage0{ns="App-0",ws="demo"})`

### Flight RPC
```
> python3 scripts/http_load_test.py --url $URL --rps 5 --duration 60 # flight
Target RPS : 5.0
Duration   : 60.0s

========== RESULTS ==========
Requests : 300
Success  : 300
Failed   : 0
Mean : 519.86 ms
Min  : 355.52 ms
P50  : 519.18 ms
P90  : 575.64 ms
P99  : 705.21 ms
Max  : 746.15 ms
```


### Akka RPC

```
> python3 scripts/http_load_test.py --url $URL --rps 5 --duration 60 # akka
Target RPS : 5.0
Duration   : 60.0s

========== RESULTS ==========
Requests : 300
Success  : 300
Failed   : 0
Mean : 2199.17 ms
Min  : 1113.35 ms
P50  : 2243.62 ms
P90  : 2642.02 ms
P99  : 2939.56 ms
Max  : 3150.06 ms
```

## Bottom line

The Akka path pays a large, tax on every query result: encode it into Java objects, serialize it, compress it,
then ship it — and that tax dominates CPU time and is the single biggest source of heap garbage, which in
turn drives GC frequency and heap pressure toward the configured limit. This tax is also pronounced at the receiver end,
which accumulates data from all shards. Arrow Flight removes that tax by keeping results in Arrow's columnar,
off-heap format end-to-end. The ~4.2x latency improvement isn't a
tuning win — it's the direct, expected result of eliminating a serialization step that was previously the
dominant cost of every binary join query.

Those queries that export a lot of data out of the storage node (binary join queries, raw time series export,
logical operators etc.) will get a significant performance boost from Flight RPC

If more remote shards are added to the cluster which is typically the case in production configurations,
the performance gap will widen further because Akka's serialization tax is per-remote-shard.
