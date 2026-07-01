# Active Series Redis Snapshot — Design

**Date:** 2026-07-01
**Status:** Ready for review
**Branch:** reject-series-on-quota-breach
**Supersedes:** [2026-06-22-active-series-redis-mirror-design.md](./2026-06-22-active-series-redis-mirror-design.md) (delta/SET-based approach abandoned — see "Why not delta" below)

## Goal

Publish FiloDB's per-shard view of active-series cardinality into Redis every 60 seconds, at two granularities:

- Per `(workspace, namespace)` — for namespace-level quota enforcement.
- Per `(workspace, namespace, metric_name)` — so consumers can identify top-cardinality metrics inside a breaching namespace.

FiloDB's `CardinalityTracker` remains the source of truth; Redis is a periodically-refreshed derived view.

## Non-goals (explicitly deferred)

- **Gateway-side read path and enforcement.** The gateway consumes what this pushes; that's a separate design.
- **Compactor / ZUNIONSTORE aggregation** across per-shard ZSETs. Out of scope here — this doc covers only FiloDB → Redis writes.
- **Metrics/dashboards/alerts** for the sink itself.
- **Reconciliation service.** Snapshot semantics are self-healing every cycle; no explicit repair job needed.
- **Delta writes** (`ZINCRBY`/`INCRBY` per activate/deactivate). See "Why not delta."
- **Reads from Redis on FiloDB's own query path.**
- **Redis Cluster.** Single Redis node is sufficient at current sizing; Cluster is a future concern.

## Why snapshot, not delta

Delta writes are cheaper per event but drift on any missed event (Kafka replay after restart, retry, queue overflow, network hiccup). Drift cannot be detected from Redis alone, so a separate reconciliation service is required to keep counts correct — which is more code than a snapshot loop.

Snapshot writes push the current authoritative count on every cycle. Missed cycles produce staleness (bounded by the snapshot interval) but never drift. There is no reconciliation to build; there is no Kafka-replay awareness to code. Every 60 seconds, everything is correct.

For quota semantics measured in TS1H (active series over the last hour), 60 seconds of staleness is a rounding error. This trade favors the operationally simple choice.

## How FiloDB tracks active-series cardinality today

`CardinalityTracker` (`core/src/main/scala/filodb.core/memstore/ratelimit/CardinalityTracker.scala`) maintains a trie of shard-key prefixes. Each node stores a `CardinalityValue`:

- `tsCount` — total time series under the prefix.
- `activeTsCount` — currently-active time series under the prefix.
- `billableTsCount`, `childrenCount`, `childrenQuota` — not used here.

For a shard-key `Seq(ws, ns, metric)`:

- Depth 2 node (prefix `Seq(ws, ns)`) holds this shard's `activeTsCount` for the namespace.
- Depth 3 nodes (prefix `Seq(ws, ns, metric)`) hold this shard's `activeTsCount` per metric.

`CardinalityTracker.scan(prefix, depth)` returns all records at the given depth under the prefix. This is the read API used by the snapshot loop; no new tracker method is required.

## Data model

### Redis keys

Two keys per `(ws, ns)` per shard:

| Key | Type | Meaning | Written by |
|---|---|---|---|
| `ns_total:{ws}:{ns}` | HASH, field=`shard-{N}` | this shard's `activeTsCount` at depth 2 | every shard that has data for `(ws, ns)` |
| `zset:shard-{N}:{ws}:{ns}` | ZSET, member=metric_name, score=`activeTsCount` | this shard's per-metric counts | shard `N` only |

`{ws}`, `{ns}` — workspace / namespace strings from the shard key. Colons in these are not expected (FiloDB shard-key values do not contain them today), but the writer must fail loudly if it encounters one rather than silently produce an ambiguous key.

`{N}` — the shard number (`0..totalShards-1`).

### Why HASH-per-namespace-with-shard-fields for totals

Multiple shards contribute to the same namespace. If we used a plain counter, shards would collide. With a HASH, each shard writes to its own field (`HSET ns_total:ws:ns shard-7 <count>`). Overwrites are idempotent — no coordination needed. Consumers compute the real total as the sum of HASH fields.

### Why per-shard ZSETs (not one shared ZSET)

If all shards `ZADD`'d to a shared ZSET with metric_name as member, they'd overwrite each other's scores (shards see different slices of the same metric). Options were:

- **Delta writes** (`ZINCRBY`) — rejected upstream; drifts on any missed event.
- **One shared ZSET, coordinated** — requires distributed lock or Lua CAS. Too much machinery.
- **Per-shard ZSETs, compact server-side** — chosen. Each shard owns its own ZSET; a separate compactor (out of scope for this doc) can `ZUNIONSTORE` them for consumers.

## Snapshot loop

Runs inside every `TimeSeriesShard`, once per 60 seconds. Jittered by shard number so 35 k shards don't all fire on the same wall-clock second.

Pseudocode:

```scala
// pseudocode, not final code
def snapshot(): Unit = {
  // 1. Scan cardTracker for every (ws, ns) this shard has data for.
  val nsRecords = cardTracker.scan(Seq(), depth = 2)

  val touchedThisCycle = mutable.Set[(String, String)]()

  for (nsRec <- nsRecords) {
    val ws = nsRec.prefix(0)
    val ns = nsRec.prefix(1)
    touchedThisCycle += ((ws, ns))

    // 2. Overwrite this shard's slice of the namespace total.
    redis.hset(s"ns_total:$ws:$ns", s"shard-$shardNum", nsRec.value.activeTsCount)

    // 3. Rewrite this shard's per-metric ZSET for this namespace.
    val metricRecs = cardTracker.scan(Seq(ws, ns), depth = 3)
    val zsetKey = s"zset:shard-$shardNum:$ws:$ns"
    redis.del(zsetKey)
    if (metricRecs.nonEmpty) {
      val zaddArgs: Seq[(Double, String)] = metricRecs.map { r =>
        (r.value.activeTsCount.toDouble, r.prefix(2))
      }
      redis.zadd(zsetKey, zaddArgs)
    }
  }

  // 4. Clean up namespaces I wrote last cycle but no longer have data for.
  val stale = lastCycleTouched -- touchedThisCycle
  for ((ws, ns) <- stale) {
    redis.hdel(s"ns_total:$ws:$ns", s"shard-$shardNum")
    redis.del(s"zset:shard-$shardNum:$ws:$ns")
  }
  lastCycleTouched = touchedThisCycle
}
```

Notes:

- **Namespace total is written directly from the depth-2 `CardinalityRecord`** rather than summed from depth-3 metrics. Both should be equal (invariant of the tracker), but reading depth 2 is one scan cheaper.
- **`DEL` + `ZADD` semantics.** We overwrite the whole ZSET each cycle. This is the simplest idempotent operation; the alternative (diffing against last cycle) has no correctness benefit here and adds state. Bandwidth cost is bounded: a shard has ~50-500 metrics per namespace × ~10-50 namespaces ≈ a few thousand ZADD members per snapshot cycle. Fine.
- **Stale-cleanup loop (step 4)** matters. Without it, when a namespace's last series expires on a shard, the shard's contribution stays in `ns_total:ws:ns` forever, inflating the sum.

## Volume and pacing

Estimated from production numbers (138 partitions × ~256 shards ≈ 35 k shards; 16.7 k namespaces; ~100 metrics/ns average):

| Metric | Value |
|---|---|
| Redis writes per snapshot cycle per shard | ~10 namespaces × 2 ops ≈ 20 |
| Fleet-wide writes per 60 s | ~35 k × 20 ≈ 700 k |
| **Fleet-wide writes/sec (averaged over 60 s window)** | **~12 k ops/sec** |
| Total ZSET entries fleet-wide | ~100 metrics × 16.7 k ns ≈ 1.7 M |
| Total Redis memory (rough) | ~200 MB |

Single Redis node handles this trivially. Load is distributed across the 60 s window by jitter (see below).

## Jitter and connection pooling

**Jitter.** Each shard's first snapshot fires at a random offset in `[0, 60s)`, then fixed 60 s cadence. Without jitter, all 35 k shards would fire at the same wall-clock instant every minute → spike. With jitter, load is uniform.

Implementation: seed the offset from `shardNum` (deterministic across restarts), not from `System.nanoTime()` (which changes and would shift the pattern on every restart). E.g. `firstDelayMs = (shardNum * 60000L / totalShards) % 60000L`.

**Connection pooling.** 35 k persistent connections would exhaust Redis's default `maxclients` (10 k). Solution: one Lettuce `RedisClient` per FiloDB JVM process (not per shard). Shards on the same node share the connection. At ~16 shards per node → ~2 200 nodes → ~2 200 connections to Redis. Comfortable.

Lettuce's async API is inherently multiplexed — many concurrent commands over one connection is the intended usage.

## Integration points

### Existing wiring (unchanged)

`TimeSeriesShard.scala:377-384` already instantiates an `ActiveSeriesSink` from config:

```scala
private[memstore] val activeSeriesSink: ActiveSeriesSink =
  if (storeConfig.activeSeriesRedisEnabled)
    new RedisActiveSeriesSink(...)
  else
    NoOpActiveSeriesSink
```

The trait currently has `onActivate` / `onDeactivate` / `close`. Those methods are edge-triggered, matching the delta model of the superseded design. **They are not called by the new snapshot loop** and can be removed if no other consumer needs them.

### New wiring

Replace the `ActiveSeriesSink` trait with a `CardinalitySnapshotSink` trait whose surface is:

```scala
trait CardinalitySnapshotSink {
  /** Push this shard's current cardinality view to the sink.
    * Called once per snapshot interval by the shard's scheduler. */
  def publish(shardNum: Int, snapshot: Seq[CardinalityRecord]): Unit
  def close(): Unit
}
```

`snapshot` is the shard's combined depth-2 + depth-3 scan result. The sink implementation decides how to route those records into Redis structures.

### Scheduling

The snapshot task is scheduled on a per-JVM `ScheduledExecutorService` (shared across shards on the same node) rather than a per-shard timer. This keeps thread count bounded regardless of shard count on the node. Each scheduled task carries its owning shard as a parameter.

Cancellation: on `TimeSeriesShard.shutdown`, the scheduled task for that shard is cancelled; on JVM shutdown, the executor is drained and the shared Redis connection closed.

### Guard

The whole thing is behind `storeConfig.meteringEnabled` (already the gate for `cardTracker`) plus a new `store-config.active-series-redis.enabled` flag. Off by default. No behavior change for existing deployments.

## Configuration

Additions to each `store-config` block in `filodb-defaults.conf`:

```hocon
active-series-redis {
  enabled = false
  host = "localhost"
  port = 6379
  snapshot-interval-seconds = 60
  # Command timeout — Redis outage must not block ingest.
  command-timeout-ms = 500
}
```

Corresponding parsed fields on `StoreConfig`.

## Failure handling

- **Redis unreachable at snapshot time.** The snapshot task logs at `warn`, skips this cycle, and retries at the next interval. No state is corrupted; the next successful cycle is fully authoritative.
- **Redis partially fails mid-cycle** (some ops succeed, others don't). Same recovery: next cycle overwrites everything for this shard. No compensating action needed.
- **Snapshot task takes longer than the interval.** Overlapping executions are prevented by a per-shard `AtomicBoolean` guard; if the guard is held, the tick is skipped and logged.
- **Shard restart.** `lastCycleTouched` is lost. Consequence: any `(ws, ns)` that this shard had data for pre-restart but doesn't touch post-restart will have a stale `shard-N` entry in Redis. Mitigation: on shard startup, wait one full snapshot cycle, then scan `HKEYS ns_total:*` for our shard field, compare against what we've now written, and clean up leftovers. This is a bounded one-time cost at boot.
- **Ingest never blocks on Redis.** Snapshot runs on its own scheduler thread. Ingest threads never touch the sink.

## Testing strategy

**Unit tests** (`RedisSnapshotSinkSpec`):

- Fake `CardinalityRecord`s → assert the exact sequence of Redis commands issued.
- Stale-cleanup: shard emits namespace X in cycle 1, not in cycle 2 → assert `HDEL` + `DEL` for X in cycle 2.
- Empty snapshot → no-op.
- Redis client throws → sink logs and returns without propagating.

**Integration test** using an embedded Valkey (Apple licensing note: `redis` brew formula blocked; use `valkey` — RESP-compatible):

- Bring up local Valkey.
- Wire a real `TimeSeriesShard` with `meteringEnabled = true` and `active-series-redis.enabled = true`.
- Ingest N series across M `(ws, ns, metric)` triples.
- Force snapshot.
- Assert `HGETALL ns_total:ws:ns` sums equal `cardTracker.getCount(Seq(ws, ns)).activeTsCount`, per namespace.
- Assert per-metric ZSET matches `cardTracker.scan(Seq(ws, ns), depth = 3)`.

**Manual smoke test:**

1. `brew install valkey && brew services start valkey`
2. Enable in dev config, ingest known workload.
3. `valkey-cli HGETALL ns_total:demo:default` — verify shard-N fields exist.
4. Wait 60 s, verify values update.
5. Stop ingest, wait for flush + one snapshot cycle, verify counts drop.

## Rollout

- Land behind the off-by-default flag.
- Enable on one dev partition first; observe Redis load and shard CPU.
- Compare `sum(HGETALL ns_total:ws:ns)` in Redis against `mosaic get workspace <ws>` utilization for spot-check.
- Then enable per partition, watching Redis memory and latency.

Rollback: set `active-series-redis.enabled = false` and restart. Redis state persists but goes stale; can be `FLUSHDB`'d out of band.

## Open questions (to resolve before implementation)

None known. All the load-bearing design choices (snapshot vs. delta, counter type, cadence, sizing) are settled by the preceding conversation.

## Decision summary

| Decision | Choice | Reason |
|---|---|---|
| Mode | Snapshot every 60 s | Self-healing; no reconciler; no Kafka-replay reasoning |
| Counter | `activeTsCount` | Matches "active in last hour" quota semantics |
| Namespace total structure | HASH with shard-N fields | Idempotent per-shard overwrite; consumers sum |
| Per-metric structure | Per-shard ZSET | Avoids inter-shard write conflicts |
| Cleanup | Track last-cycle set; HDEL+DEL orphans | Prevents drift from disappearing namespaces |
| Interval | 60 s | Below TS1H quota window; ample staleness budget |
| Jitter | Deterministic from shardNum | Spreads load; stable across restarts |
| Connection | One Lettuce client per JVM | Bounds connection count |
| Failure mode | Log + skip + retry next cycle | Snapshot semantics self-heal; no state to corrupt |
| Toggle | Off by default | No behavior change for existing deployments |
| Compactor | Out of scope | Belongs with gateway design |
