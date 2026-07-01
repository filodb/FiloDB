# Active-Series Redis Snapshot — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace FiloDB's current per-event Redis mirror (SADD/SREM on activate/deactivate) with a periodic snapshot publisher that overwrites this shard's slice of `activeTsCount` for every `(ws, ns)` and per-metric ZSET in Redis every 60 seconds.

**Architecture:** Each `TimeSeriesShard` runs a `CardinalitySnapshotDriver` on a shared per-JVM scheduler. Every 60s (jittered from `shardNum`), the driver scans its `CardinalityTracker` at depths 2 and 3 and hands the records to a `CardinalitySnapshotSink`. The sink translates records into pipelined Redis commands (`HSET` for namespace totals, `DEL`+`ZADD` for per-metric ZSETs) against a JVM-scoped Lettuce client, keyed by `(deployment-partition, ws, ns)` since Redis is shared across all 138 FiloDB partitions. The driver tracks which `(ws, ns)` pairs it wrote each cycle and issues `HDEL`+`DEL` for stale ones.

**Tech Stack:** Scala 2.13, sbt, Lettuce 6.3 (already a dependency), ScalaTest AnyFunSpec + Matchers + BeforeAndAfter, Valkey (Apple-compliant Redis fork) for local integration testing.

**Spec:** `docs/superpowers/specs/2026-07-01-active-series-snapshot-design.md`

---

## File Structure

**Deleted (obsolete delta-model sink):**
- `core/src/main/scala/filodb.core/memstore/ActiveSeriesSink.scala`
- `core/src/main/scala/filodb.core/memstore/RedisActiveSeriesSink.scala`
- `core/src/test/scala/filodb.core/memstore/ActiveSeriesSinkSpec.scala`

**Created:**
- `core/src/main/scala/filodb.core/memstore/CardinalitySnapshotSink.scala` — trait + `NoOpCardinalitySnapshotSink`
- `core/src/main/scala/filodb.core/memstore/CardinalitySnapshotDriver.scala` — timer + tracker scans + stale-cleanup state; per shard
- `core/src/main/scala/filodb.core/memstore/RedisSnapshotClient.scala` — JVM-scoped Lettuce client singleton keyed by `(host, port)`
- `core/src/main/scala/filodb.core/memstore/RedisCardinalitySnapshotSink.scala` — `CardinalitySnapshotSink` impl, pipelines commands via Lettuce
- `core/src/test/scala/filodb.core/memstore/CardinalitySnapshotSinkSpec.scala` — trait behavior + `RecordingCardinalitySnapshotSink` fixture
- `core/src/test/scala/filodb.core/memstore/CardinalitySnapshotDriverSpec.scala` — snapshot loop unit tests using recording sink
- `core/src/test/scala/filodb.core/memstore/RedisCardinalitySnapshotSinkSpec.scala` — integration test against local Valkey, gated by `-Dfilodb.test.redis.host` sys prop

**Modified:**
- `core/src/main/scala/filodb.core/store/IngestionConfig.scala` — swap batch-size / batch-interval-ms fields for `snapshot-interval-seconds` and `command-timeout-ms`
- `core/src/main/scala/filodb.core/memstore/TimeSeriesShard.scala` — replace old sink wiring (lines 377-384) with `CardinalitySnapshotDriver` instantiation; remove `onActivate`/`onDeactivate` call sites (lines 869-870 recovery, plus three more per the previous spec)

---

## Preconditions

- Working tree is clean or contains only expected untracked files (see `git status -s`).
- Current branch: `reject-series-on-quota-breach`.
- `deployment-partition-name` config value is present in `filodbConfig` (already required, sourced at `TimeSeriesShard.scala:293`).
- Valkey is available locally for integration test: `brew install valkey && brew services start valkey`.

---

## Task 1: Sweep the old sink and adjust config

**Files:**
- Delete: `core/src/main/scala/filodb.core/memstore/ActiveSeriesSink.scala`
- Delete: `core/src/main/scala/filodb.core/memstore/RedisActiveSeriesSink.scala`
- Delete: `core/src/test/scala/filodb.core/memstore/ActiveSeriesSinkSpec.scala`
- Modify: `core/src/main/scala/filodb.core/store/IngestionConfig.scala`
- Modify: `core/src/main/scala/filodb.core/memstore/TimeSeriesShard.scala`

**Rationale for combining into one task:** the old sink is wired into `TimeSeriesShard` and `IngestionConfig`; deleting piecewise leaves the build broken. Do it as one atomic sweep.

- [ ] **Step 1: Find every old-sink reference before deleting**

Run:
```bash
grep -rn "ActiveSeriesSink\|activeSeriesRedis\|activeSeriesSink\|onActivate\|onDeactivate" \
  core/src/main/scala/ core/src/test/scala/ --include="*.scala"
```

Expected: hits in `TimeSeriesShard.scala` (~4 call sites plus construction), `IngestionConfig.scala` (5 fields + toConfig block), `ActiveSeriesSink.scala`, `RedisActiveSeriesSink.scala`, `ActiveSeriesSinkSpec.scala`. Note the exact line numbers — later steps depend on them.

- [ ] **Step 2: Delete the three obsolete files**

Run:
```bash
git rm core/src/main/scala/filodb.core/memstore/ActiveSeriesSink.scala \
       core/src/main/scala/filodb.core/memstore/RedisActiveSeriesSink.scala \
       core/src/test/scala/filodb.core/memstore/ActiveSeriesSinkSpec.scala
```

- [ ] **Step 3: Swap `activeSeriesRedis*` fields in `StoreConfig`**

In `core/src/main/scala/filodb.core/store/IngestionConfig.scala`:

Two fields change (not the whole block): `activeSeriesRedisBatchSize` and `activeSeriesRedisBatchIntervalMillis` are replaced by `activeSeriesRedisSnapshotIntervalSeconds` and `activeSeriesRedisCommandTimeoutMs`. `enabled`, `host`, and `port` stay.

Before (lines 47-51):
```scala
activeSeriesRedisEnabled: Boolean,
activeSeriesRedisHost: String,
activeSeriesRedisPort: Int,
activeSeriesRedisBatchSize: Int,
activeSeriesRedisBatchIntervalMillis: Long
```

After:
```scala
activeSeriesRedisEnabled: Boolean,
activeSeriesRedisHost: String,
activeSeriesRedisPort: Int,
activeSeriesRedisSnapshotIntervalSeconds: Int,
activeSeriesRedisCommandTimeoutMs: Long
```

In the same file's `toConfig` method (lines 77-82), replace the last two map entries:
```scala
// before
"active-series-redis.batch-size" -> activeSeriesRedisBatchSize,
"active-series-redis.batch-interval-ms" -> activeSeriesRedisBatchIntervalMillis
```
with:
```scala
"active-series-redis.snapshot-interval-seconds" -> activeSeriesRedisSnapshotIntervalSeconds,
"active-series-redis.command-timeout-ms" -> activeSeriesRedisCommandTimeoutMs
```

In `StoreConfig.defaults` (lines 121-127), replace:
```
|active-series-redis {
|  enabled = false
|  host = "localhost"
|  port = 6379
|  batch-size = 100
|  batch-interval-ms = 100
|}
```
with:
```
|active-series-redis {
|  enabled = false
|  host = "localhost"
|  port = 6379
|  snapshot-interval-seconds = 60
|  command-timeout-ms = 500
|}
```

- [ ] **Step 4: Locate the `StoreConfig.apply` factory that reads these keys**

Run:
```bash
grep -n "active-series-redis\." core/src/main/scala/filodb.core/store/IngestionConfig.scala
```

The `StoreConfig.apply(config: Config)` factory (elsewhere in the same file, below `defaults`) reads each `active-series-redis.*` key. Find those lines and swap `batch-size` / `batch-interval-ms` for `snapshot-interval-seconds` / `command-timeout-ms` with types `Int` / `Long` respectively. If the factory reads keys via `config.getInt("active-series-redis.batch-size")`, the new lines read `config.getInt("active-series-redis.snapshot-interval-seconds")` and `config.getLong("active-series-redis.command-timeout-ms")`.

- [ ] **Step 5: Update reference config**

Run:
```bash
grep -n "active-series-redis\|batch-size\|batch-interval-ms" core/src/main/resources/filodb-defaults.conf kafka/src/main/resources/filodb-defaults.conf
```

If either file references `active-series-redis.batch-*`, swap those lines to `snapshot-interval-seconds = 60` and `command-timeout-ms = 500`. If neither file references them (only `StoreConfig.defaults` HOCON string carries them), skip this step.

- [ ] **Step 6: Delete old wiring in `TimeSeriesShard`**

In `core/src/main/scala/filodb.core/memstore/TimeSeriesShard.scala`:

Delete the `activeSeriesSink` val at lines 377-384:
```scala
private[memstore] val activeSeriesSink: ActiveSeriesSink =
  if (storeConfig.activeSeriesRedisEnabled)
    new RedisActiveSeriesSink(storeConfig.activeSeriesRedisHost,
                              storeConfig.activeSeriesRedisPort,
                              storeConfig.activeSeriesRedisBatchSize,
                              storeConfig.activeSeriesRedisBatchIntervalMillis)
  else
    NoOpActiveSeriesSink
```

Delete the `import` for `ActiveSeriesSink` / `NoOpActiveSeriesSink` / `RedisActiveSeriesSink` if present (search the top-of-file imports).

Use the *exact* call-site line numbers captured from Step 1's grep (do not re-approximate). Every `activeSeriesSink.onActivate(...)` and `activeSeriesSink.onDeactivate(...)` must be removed, along with the surrounding try/catch that wrapped the call.

- [ ] **Step 7: Compile**

Run: `sbt "coreJVM/compile"`
Expected: BUILD SUCCESS. Any dangling reference means step 6 missed a call site.

- [ ] **Step 8: Run existing tests to confirm nothing else broke**

Run: `sbt "coreJVM/testOnly filodb.core.memstore.TimeSeriesMemStoreSpec"`
Expected: existing tests pass. No `ActiveSeriesSink`-related failures.

- [ ] **Step 9: Commit**

```bash
git add core/src/main/scala/filodb.core/store/IngestionConfig.scala \
        core/src/main/scala/filodb.core/memstore/TimeSeriesShard.scala
git commit -m "$(cat <<'EOF'
refactor(memstore): remove delta-based ActiveSeriesSink, prep config for snapshot mode

Delta-model Redis sink is superseded by the snapshot design (see
docs/superpowers/specs/2026-07-01-active-series-snapshot-design.md). This
change deletes the trait, implementation, and test, plus every hook site
in TimeSeriesShard, and swaps the StoreConfig batch-size/batch-interval
fields for snapshot-interval-seconds/command-timeout-ms.

No new behavior yet — the snapshot driver and Redis sink are added in
subsequent commits.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: `CardinalitySnapshotSink` trait, NoOp, and recording fixture

**Files:**
- Create: `core/src/main/scala/filodb.core/memstore/CardinalitySnapshotSink.scala`
- Create: `core/src/test/scala/filodb.core/memstore/CardinalitySnapshotSinkSpec.scala`

- [ ] **Step 1: Write the failing test**

`core/src/test/scala/filodb.core/memstore/CardinalitySnapshotSinkSpec.scala`:

```scala
package filodb.core.memstore

import scala.collection.mutable

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.memstore.ratelimit.{CardinalityRecord, CardinalityValue}

class CardinalitySnapshotSinkSpec extends AnyFunSpec with Matchers {

  private def rec(prefix: Seq[String], active: Long): CardinalityRecord =
    CardinalityRecord(shard = 0, prefix = prefix,
      value = CardinalityValue(tsCount = active, activeTsCount = active,
        billableTsCount = active, childrenCount = 0L, childrenQuota = 1000000L))

  describe("NoOpCardinalitySnapshotSink") {
    it("ignores publish/evict/close without throwing") {
      noException should be thrownBy {
        NoOpCardinalitySnapshotSink.publish(
          partition = "tsdb0", shardNum = 3,
          ns = Seq(rec(Seq("ws1", "ns1"), 100)),
          perMetric = Map(Seq("ws1", "ns1") -> Seq(rec(Seq("ws1", "ns1", "cpu"), 100))))
        NoOpCardinalitySnapshotSink.evict("tsdb0", 3, Set(("ws1", "ns1")))
        NoOpCardinalitySnapshotSink.close()
      }
    }
  }

  describe("RecordingCardinalitySnapshotSink (test fixture)") {
    it("captures publish and evict calls in order") {
      val sink = new RecordingCardinalitySnapshotSink

      sink.publish("tsdb0", 3,
        ns = Seq(rec(Seq("wsA", "nsA"), 100)),
        perMetric = Map(Seq("wsA", "nsA") ->
          Seq(rec(Seq("wsA", "nsA", "cpu"), 60), rec(Seq("wsA", "nsA", "mem"), 40))))
      sink.evict("tsdb0", 3, Set(("wsA", "nsOld")))

      sink.publishCalls should have size 1
      sink.publishCalls.head.partition shouldEqual "tsdb0"
      sink.publishCalls.head.shardNum shouldEqual 3
      sink.publishCalls.head.ns.map(_.prefix) shouldEqual Seq(Seq("wsA", "nsA"))
      sink.publishCalls.head.perMetric(Seq("wsA", "nsA")).map(_.prefix.last) should
        contain theSameElementsAs Seq("cpu", "mem")

      sink.evictCalls should have size 1
      sink.evictCalls.head.stale shouldEqual Set(("wsA", "nsOld"))
    }
  }
}
```

- [ ] **Step 2: Run the test — expect compile failure**

Run: `sbt "coreJVM/testOnly filodb.core.memstore.CardinalitySnapshotSinkSpec"`
Expected: compile error, `CardinalitySnapshotSink`/`NoOpCardinalitySnapshotSink`/`RecordingCardinalitySnapshotSink` not defined.

- [ ] **Step 3: Write the production trait + NoOp**

`core/src/main/scala/filodb.core/memstore/CardinalitySnapshotSink.scala`:

```scala
package filodb.core.memstore

import filodb.core.memstore.ratelimit.CardinalityRecord

/**
 * Sink for periodic cardinality snapshots from a TimeSeriesShard.
 *
 * The shard owns the schedule, the CardinalityTracker scans, and the state
 * that tracks which (ws, ns) pairs were written last cycle. The sink is
 * stateless with respect to the shard: every `publish` is a full overwrite
 * intent for the passed records, and every `evict` is a full removal intent.
 *
 * Implementations MUST be thread-safe (may be called concurrently by shards
 * on the same JVM sharing one sink instance) and MUST NOT throw — exceptions
 * are caught and logged by the caller, but should not occur in steady state.
 */
trait CardinalitySnapshotSink {

  /**
   * Publish this shard's current cardinality view.
   *
   * @param partition FiloDB deployment-partition name (e.g. "tsdb3")
   * @param shardNum  shard number within this partition
   * @param ns        depth-2 records: one per (ws, ns) this shard has data for
   * @param perMetric depth-3 records grouped by (ws, ns): metric-level counts
   */
  def publish(partition: String, shardNum: Int,
              ns: Seq[CardinalityRecord],
              perMetric: Map[Seq[String], Seq[CardinalityRecord]]): Unit

  /**
   * Remove this shard's contribution for namespaces that were written in
   * a prior cycle but are no longer present in this shard's tracker.
   */
  def evict(partition: String, shardNum: Int, stale: Set[(String, String)]): Unit

  def close(): Unit
}

object NoOpCardinalitySnapshotSink extends CardinalitySnapshotSink {
  override def publish(partition: String, shardNum: Int,
                       ns: Seq[CardinalityRecord],
                       perMetric: Map[Seq[String], Seq[CardinalityRecord]]): Unit = ()
  override def evict(partition: String, shardNum: Int, stale: Set[(String, String)]): Unit = ()
  override def close(): Unit = ()
}
```

- [ ] **Step 4: Write the recording fixture (in the test file, below the specs)**

Append to `CardinalitySnapshotSinkSpec.scala`:

```scala
/** Thread-safe in-memory sink used in unit tests. */
class RecordingCardinalitySnapshotSink extends CardinalitySnapshotSink {
  import filodb.core.memstore.RecordingCardinalitySnapshotSink._
  private val lock = new Object
  private val pubBuf = mutable.Buffer.empty[PublishCall]
  private val evictBuf = mutable.Buffer.empty[EvictCall]

  override def publish(partition: String, shardNum: Int,
                       ns: Seq[CardinalityRecord],
                       perMetric: Map[Seq[String], Seq[CardinalityRecord]]): Unit =
    lock.synchronized { pubBuf += PublishCall(partition, shardNum, ns, perMetric) }

  override def evict(partition: String, shardNum: Int,
                     stale: Set[(String, String)]): Unit =
    lock.synchronized { evictBuf += EvictCall(partition, shardNum, stale) }

  override def close(): Unit = ()

  def publishCalls: Seq[PublishCall] = lock.synchronized(pubBuf.toSeq)
  def evictCalls: Seq[EvictCall] = lock.synchronized(evictBuf.toSeq)
  def reset(): Unit = lock.synchronized { pubBuf.clear(); evictBuf.clear() }
}

object RecordingCardinalitySnapshotSink {
  final case class PublishCall(partition: String, shardNum: Int,
                                ns: Seq[CardinalityRecord],
                                perMetric: Map[Seq[String], Seq[CardinalityRecord]])
  final case class EvictCall(partition: String, shardNum: Int,
                              stale: Set[(String, String)])
}
```

- [ ] **Step 5: Run the test — expect PASS**

Run: `sbt "coreJVM/testOnly filodb.core.memstore.CardinalitySnapshotSinkSpec"`
Expected: 2 tests, all PASS.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/scala/filodb.core/memstore/CardinalitySnapshotSink.scala \
        core/src/test/scala/filodb.core/memstore/CardinalitySnapshotSinkSpec.scala
git commit -m "$(cat <<'EOF'
feat(memstore): add CardinalitySnapshotSink trait + NoOp + recording fixture

Trait surface matches the snapshot design: publish takes ns/perMetric records,
evict takes stale (ws, ns) set. RecordingCardinalitySnapshotSink is the test
fixture used by driver and downstream tests.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: `CardinalitySnapshotDriver` — the timer + scan + call-sink loop

**Files:**
- Create: `core/src/main/scala/filodb.core/memstore/CardinalitySnapshotDriver.scala`
- Create: `core/src/test/scala/filodb.core/memstore/CardinalitySnapshotDriverSpec.scala`

**Design note:** The driver holds the schedule and the `lastCycleTouched` state. It exposes `snapshotOnce()` (unit-testable, no scheduler) plus `start()` and `close()`. `TimeSeriesShard` calls `start()` at construction and `close()` at shutdown.

- [ ] **Step 1: Verify tracker and store trait signatures before writing the fixture**

The test uses `CardinalityTracker.modifyCount`, `CardinalityTracker.decrementCount`, and an in-memory `CardinalityStore`. Confirm the signatures match your local tree:

```bash
grep -n "def modifyCount\|def decrementCount\|def scan\b" \
  core/src/main/scala/filodb.core/memstore/ratelimit/CardinalityTracker.scala
grep -n "trait CardinalityStore\|  def store\|  def getOrZero\|  def remove\|  def scanChildren\|  def close" \
  core/src/main/scala/filodb.core/memstore/ratelimit/CardinalityStore.scala
```

Expected (as of writing this plan):
- `def modifyCount(shardKey: Seq[String], totalDelta: Int, activeDelta: Int, billableDelta: Int)`
- `def decrementCount(shardKey: Seq[String])`
- `def scan(shardKeyPrefix: Seq[String], depth: Int)`
- `CardinalityStore` has: `store`, `getOrZero`, `remove`, `scanChildren`, `close`.

If any signature differs (extra params, renamed methods), adjust the test code below accordingly before writing it.

- [ ] **Step 2: Write the failing test**

```scala
package filodb.core.memstore

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.DatasetRef
import filodb.core.memstore.ratelimit.{CardinalityRecord, CardinalityStore,
  CardinalityTracker, CardinalityValue}

class CardinalitySnapshotDriverSpec extends AnyFunSpec with Matchers {

  private def value(active: Long, children: Int = 0): CardinalityValue =
    CardinalityValue(tsCount = active, activeTsCount = active,
      billableTsCount = active, childrenCount = children, childrenQuota = 1000000L)

  /** In-memory CardinalityStore backing test tracker(s). */
  private def newTracker(shardNum: Int): CardinalityTracker = {
    val store = new InMemoryCardinalityStore
    new CardinalityTracker(
      ref = DatasetRef("test"),
      shard = shardNum,
      shardKeyLen = 3,
      defaultChildrenQuota = Seq(1000000L, 1000000L, 1000000L, 1000000L),
      store = store)
  }

  describe("snapshotOnce") {
    it("publishes ns records and perMetric groups for every (ws, ns) on the tracker") {
      val tracker = newTracker(shardNum = 3)
      // Two namespaces, two metrics each. modifyCount(shardKey, totalDelta=1, activeDelta=1, billableDelta=1).
      tracker.modifyCount(Seq("wsA", "ns1", "cpu"), 1, 1, 1)
      tracker.modifyCount(Seq("wsA", "ns1", "mem"), 1, 1, 1)
      tracker.modifyCount(Seq("wsA", "ns2", "cpu"), 1, 1, 1)

      val sink = new RecordingCardinalitySnapshotSink
      val driver = new CardinalitySnapshotDriver(
        partition = "tsdb0", shardNum = 3, cardTracker = tracker, sink = sink)

      driver.snapshotOnce()

      sink.publishCalls should have size 1
      val call = sink.publishCalls.head
      call.partition shouldEqual "tsdb0"
      call.shardNum shouldEqual 3
      call.ns.map(_.prefix).toSet shouldEqual Set(Seq("wsA", "ns1"), Seq("wsA", "ns2"))
      call.perMetric(Seq("wsA", "ns1")).map(_.prefix.last).toSet shouldEqual Set("cpu", "mem")
      call.perMetric(Seq("wsA", "ns2")).map(_.prefix.last).toSet shouldEqual Set("cpu")
    }

    it("evicts (ws, ns) that was in last cycle but not this cycle") {
      val tracker = newTracker(shardNum = 0)
      tracker.modifyCount(Seq("wsA", "ns1", "cpu"), 1, 1, 1)
      tracker.modifyCount(Seq("wsA", "ns2", "cpu"), 1, 1, 1)

      val sink = new RecordingCardinalitySnapshotSink
      val driver = new CardinalitySnapshotDriver(
        partition = "tsdb0", shardNum = 0, cardTracker = tracker, sink = sink)

      driver.snapshotOnce()          // both ns1 and ns2 touched
      tracker.decrementCount(Seq("wsA", "ns2", "cpu")) // ns2 goes away
      sink.reset()
      driver.snapshotOnce()

      sink.publishCalls should have size 1
      sink.publishCalls.head.ns.map(_.prefix) shouldEqual Seq(Seq("wsA", "ns1"))
      sink.evictCalls should have size 1
      sink.evictCalls.head.stale shouldEqual Set(("wsA", "ns2"))
    }

    it("issues no evict on the first cycle") {
      val tracker = newTracker(shardNum = 0)
      tracker.modifyCount(Seq("wsA", "ns1", "cpu"), 1, 1, 1)

      val sink = new RecordingCardinalitySnapshotSink
      val driver = new CardinalitySnapshotDriver(
        partition = "tsdb0", shardNum = 0, cardTracker = tracker, sink = sink)

      driver.snapshotOnce()

      sink.evictCalls shouldBe empty
    }

    it("swallows sink exceptions and does not update lastCycleTouched") {
      val tracker = newTracker(shardNum = 0)
      tracker.modifyCount(Seq("wsA", "ns1", "cpu"), 1, 1, 1)

      val throwingSink = new CardinalitySnapshotSink {
        override def publish(p: String, s: Int, ns: Seq[CardinalityRecord],
                             perMetric: Map[Seq[String], Seq[CardinalityRecord]]): Unit =
          throw new RuntimeException("simulated")
        override def evict(p: String, s: Int, stale: Set[(String, String)]): Unit = ()
        override def close(): Unit = ()
      }
      val driver = new CardinalitySnapshotDriver(
        partition = "tsdb0", shardNum = 0, cardTracker = tracker, sink = throwingSink)

      noException should be thrownBy driver.snapshotOnce()
      // On next cycle with same state, still no evict — first successful cycle
      // is treated as first cycle. (Test with a recording sink swap-in.)
    }
  }
}
```

Note the test uses a helper `InMemoryCardinalityStore`. Check whether one already exists:

Run:
```bash
grep -rn "class.*CardinalityStore\|extends CardinalityStore" \
  core/src/test/scala/ core/src/main/scala/ --include="*.scala"
```

If a suitable one exists (e.g. `MockCardinalityStore` in tests), use it. Otherwise, create a minimal one in the test file:

```scala
class InMemoryCardinalityStore extends filodb.core.memstore.ratelimit.CardinalityStore {
  import filodb.core.memstore.ratelimit.CardinalityRecord
  private val m = scala.collection.mutable.Map.empty[Seq[String], CardinalityRecord]
  override def store(rec: CardinalityRecord): Unit = m.put(rec.prefix, rec)
  override def getOrZero(prefix: Seq[String], zero: CardinalityRecord): CardinalityRecord =
    m.getOrElse(prefix, zero)
  override def remove(prefix: Seq[String]): Unit = { m.remove(prefix); () }
  override def scanChildren(prefix: Seq[String], depth: Int): Seq[CardinalityRecord] =
    m.values.filter(r => r.prefix.length == depth && r.prefix.startsWith(prefix)).toSeq
  override def close(): Unit = ()
}
```

- [ ] **Step 3: Run the test — expect compile failure**

Run: `sbt "coreJVM/testOnly filodb.core.memstore.CardinalitySnapshotDriverSpec"`
Expected: compile error, `CardinalitySnapshotDriver` not defined.

- [ ] **Step 4: Write `CardinalitySnapshotDriver`**

`core/src/main/scala/filodb.core/memstore/CardinalitySnapshotDriver.scala`:

```scala
package filodb.core.memstore

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.util.control.NonFatal

import com.typesafe.scalalogging.StrictLogging

import filodb.core.memstore.ratelimit.{CardinalityRecord, CardinalityTracker}

/**
 * Drives periodic snapshots of a shard's CardinalityTracker into a
 * CardinalitySnapshotSink. Owned by TimeSeriesShard.
 *
 * The tracker is scanned at depth 2 (ns) and depth 3 (per-metric under each
 * ns) once per interval; the resulting records are handed to the sink.
 * The driver tracks which (ws, ns) pairs were written last cycle so that
 * disappearing namespaces can be evicted from the sink's downstream store.
 *
 * `snapshotOnce()` is the unit of work — visible so tests can call it
 * directly. `start(scheduler, intervalSeconds)` schedules it on a shared
 * executor with a per-shard jittered first-delay.
 */
class CardinalitySnapshotDriver(partition: String,
                                shardNum: Int,
                                cardTracker: CardinalityTracker,
                                sink: CardinalitySnapshotSink)
    extends StrictLogging {

  private val running = new AtomicBoolean(false)
  @volatile private var scheduled: Option[ScheduledFuture[_]] = None
  @volatile private var lastCycleTouched: Set[(String, String)] = Set.empty

  /**
   * Perform one snapshot: scan tracker, publish, evict disappearing (ws, ns).
   * Safe to call directly from tests. Never throws — sink exceptions are
   * caught and logged; on failure, `lastCycleTouched` is not updated so the
   * next cycle re-publishes.
   */
  def snapshotOnce(): Unit = {
    if (!running.compareAndSet(false, true)) {
      logger.warn(s"Snapshot for partition=$partition shard=$shardNum still " +
        s"running; skipping this tick")
      return
    }
    try {
      val nsRecs = cardTracker.scan(Seq.empty, depth = 2)
      val touched = mutable.Set.empty[(String, String)]
      val perMetric = mutable.Map.empty[Seq[String], Seq[CardinalityRecord]]

      nsRecs.foreach { nsRec =>
        if (nsRec.prefix.length == 2) {
          val key = (nsRec.prefix(0), nsRec.prefix(1))
          touched += key
          perMetric.put(nsRec.prefix,
            cardTracker.scan(nsRec.prefix, depth = 3))
        }
      }

      try sink.publish(partition, shardNum, nsRecs.filter(_.prefix.length == 2),
                        perMetric.toMap)
      catch { case NonFatal(t) =>
        logger.warn(s"sink.publish threw for partition=$partition shard=$shardNum", t)
        return // do NOT update lastCycleTouched
      }

      val touchedSet = touched.toSet
      val stale = lastCycleTouched -- touchedSet
      if (stale.nonEmpty) {
        try sink.evict(partition, shardNum, stale)
        catch { case NonFatal(t) =>
          logger.warn(s"sink.evict threw for partition=$partition shard=$shardNum", t)
          // still update lastCycleTouched — otherwise stale accumulates forever
        }
      }
      lastCycleTouched = touchedSet
    } finally running.set(false)
  }

  /**
   * Schedule periodic snapshots on the given executor. First fire is jittered
   * deterministically by shardNum so 35 k shards don't stampede on second 0.
   */
  def start(scheduler: ScheduledExecutorService, intervalSeconds: Int,
            totalShardsHint: Int): Unit = {
    require(scheduled.isEmpty, "already started")
    val jitteredFirstDelayMs =
      ((shardNum.toLong * (intervalSeconds * 1000L)) /
        math.max(totalShardsHint, 1)) % (intervalSeconds * 1000L)
    scheduled = Some(scheduler.scheduleAtFixedRate(
      () => snapshotOnce(),
      jitteredFirstDelayMs, intervalSeconds * 1000L, TimeUnit.MILLISECONDS))
    logger.info(s"CardinalitySnapshotDriver started partition=$partition " +
      s"shard=$shardNum interval=${intervalSeconds}s " +
      s"firstDelayMs=$jitteredFirstDelayMs")
  }

  def close(): Unit = {
    scheduled.foreach(_.cancel(false))
    scheduled = None
  }
}
```

- [ ] **Step 5: Run the tests — expect PASS**

Run: `sbt "coreJVM/testOnly filodb.core.memstore.CardinalitySnapshotDriverSpec"`
Expected: all 4 tests PASS.

If the last "swallow exceptions" test fails because the throwing sink can't be swapped out mid-driver, split the test into two: one with throwing sink asserting `noException`, one with fresh driver + recording sink asserting behavior. Do NOT weaken the driver — its correctness is what the test proves.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/scala/filodb.core/memstore/CardinalitySnapshotDriver.scala \
        core/src/test/scala/filodb.core/memstore/CardinalitySnapshotDriverSpec.scala
git commit -m "$(cat <<'EOF'
feat(memstore): add CardinalitySnapshotDriver

Drives the periodic snapshot loop for one shard: scans CardinalityTracker at
depths 2 and 3, hands records to the sink, tracks lastCycleTouched to evict
disappearing (ws, ns). snapshotOnce() is unit-testable; start(scheduler,
intervalSeconds, totalShardsHint) schedules with deterministic jitter.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: `RedisSnapshotClient` — JVM-scoped Lettuce singleton

**Files:**
- Create: `core/src/main/scala/filodb.core/memstore/RedisSnapshotClient.scala`

- [ ] **Step 1: Write the failing test**

Append to `CardinalitySnapshotSinkSpec.scala` (or new file — same package):

```scala
class RedisSnapshotClientSpec extends AnyFunSpec with Matchers {
  describe("RedisSnapshotClient.acquire") {
    it("returns the same client for the same (host, port)") {
      val a = RedisSnapshotClient.acquire("localhost", 6379, commandTimeoutMs = 500)
      val b = RedisSnapshotClient.acquire("localhost", 6379, commandTimeoutMs = 500)
      (a eq b) shouldBe true
      RedisSnapshotClient.releaseAllForTest()
    }
    it("returns a different client for a different (host, port)") {
      val a = RedisSnapshotClient.acquire("localhost", 6379, commandTimeoutMs = 500)
      val b = RedisSnapshotClient.acquire("localhost", 6380, commandTimeoutMs = 500)
      (a eq b) shouldBe false
      RedisSnapshotClient.releaseAllForTest()
    }
  }
}
```

Note: this test does not connect to Redis. `RedisClient.create(uri)` is lazy — it only opens a socket on `.connect()`. So the test works without a running Redis.

- [ ] **Step 2: Run — expect compile failure**

Run: `sbt "coreJVM/testOnly filodb.core.memstore.RedisSnapshotClientSpec"`
Expected: compile error.

- [ ] **Step 3: Implement `RedisSnapshotClient`**

```scala
package filodb.core.memstore

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

import io.lettuce.core.{RedisClient, RedisURI}

/**
 * JVM-scoped Lettuce client cache. One `RedisClient` per (host, port).
 * Shared across all shards on this JVM to bound the connection count.
 *
 * The client is not closed until JVM shutdown. This is intentional — shard
 * shutdown is not a signal to tear down the shared Redis client.
 */
object RedisSnapshotClient {
  private val clients = new ConcurrentHashMap[(String, Int), RedisClient]()

  def acquire(host: String, port: Int, commandTimeoutMs: Long): RedisClient =
    clients.computeIfAbsent((host, port), _ => {
      val uri = RedisURI.Builder.redis(host, port)
        .withTimeout(Duration.ofMillis(commandTimeoutMs))
        .build()
      RedisClient.create(uri)
    })

  /** For tests only. Never call in production code. */
  private[memstore] def releaseAllForTest(): Unit = {
    clients.values().forEach { c => try c.shutdown() catch { case _: Throwable => } }
    clients.clear()
  }
}
```

- [ ] **Step 4: Run — expect PASS**

Run: `sbt "coreJVM/testOnly filodb.core.memstore.RedisSnapshotClientSpec"`
Expected: 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add core/src/main/scala/filodb.core/memstore/RedisSnapshotClient.scala \
        core/src/test/scala/filodb.core/memstore/CardinalitySnapshotSinkSpec.scala
git commit -m "$(cat <<'EOF'
feat(memstore): add RedisSnapshotClient JVM-scoped Lettuce cache

One RedisClient per (host, port) shared across all shards on this JVM. Bounds
connection count regardless of shard count. Client lifetime is JVM-scoped;
shard shutdown does not tear it down.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: `RedisCardinalitySnapshotSink` — Lettuce sink with pipelining

**Files:**
- Create: `core/src/main/scala/filodb.core/memstore/RedisCardinalitySnapshotSink.scala`
- Create: `core/src/test/scala/filodb.core/memstore/RedisCardinalitySnapshotSinkSpec.scala`

**Design note:** the integration test is gated by `-Dfilodb.test.redis.host=localhost -Dfilodb.test.redis.port=6379`. When absent, tests are ignored (not failed) via `assume(...)`. This keeps CI green on machines without Redis.

- [ ] **Step 1: Write the failing integration test**

`core/src/test/scala/filodb.core/memstore/RedisCardinalitySnapshotSinkSpec.scala`:

```scala
package filodb.core.memstore

import scala.jdk.CollectionConverters._

import io.lettuce.core.RedisClient
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.memstore.ratelimit.{CardinalityRecord, CardinalityValue}

/**
 * Integration test against a local Redis/Valkey. Gated by system properties:
 *   sbt -Dfilodb.test.redis.host=localhost -Dfilodb.test.redis.port=6379 test
 * Without those, every test is skipped via assume(...).
 */
class RedisCardinalitySnapshotSinkSpec
    extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  private lazy val host = Option(System.getProperty("filodb.test.redis.host"))
  private lazy val port = Option(System.getProperty("filodb.test.redis.port"))
                            .map(_.toInt)
  private lazy val enabled = host.isDefined && port.isDefined

  private lazy val sink: RedisCardinalitySnapshotSink =
    new RedisCardinalitySnapshotSink(host.get, port.get, commandTimeoutMs = 500L)

  override def afterAll(): Unit = {
    if (enabled) sink.close()
    RedisSnapshotClient.releaseAllForTest()
  }

  private def value(active: Long): CardinalityValue =
    CardinalityValue(active, active, active, 0, 1000000L)

  private def rec(prefix: Seq[String], active: Long): CardinalityRecord =
    CardinalityRecord(shard = 0, prefix = prefix, value = value(active))

  private def flushDb(): Unit = {
    val c = RedisClient.create(s"redis://${host.get}:${port.get}")
    try {
      val conn = c.connect(); try conn.sync().flushdb() finally conn.close()
    } finally c.shutdown()
  }

  describe("publish") {
    it("writes ns_total HASH field and per-shard ZSET for each (ws, ns)") {
      assume(enabled, "Redis not configured; set -Dfilodb.test.redis.host and .port")
      flushDb()

      sink.publish(partition = "tsdb0", shardNum = 3,
        ns = Seq(rec(Seq("wsA", "ns1"), 100), rec(Seq("wsA", "ns2"), 200)),
        perMetric = Map(
          Seq("wsA", "ns1") -> Seq(rec(Seq("wsA", "ns1", "cpu"), 60),
                                    rec(Seq("wsA", "ns1", "mem"), 40)),
          Seq("wsA", "ns2") -> Seq(rec(Seq("wsA", "ns2", "cpu"), 200))))

      val c = RedisClient.create(s"redis://${host.get}:${port.get}")
      try {
        val conn = c.connect()
        try {
          val sync = conn.sync()
          sync.hget("ns_total:tsdb0:wsA:ns1", "shard-3") shouldEqual "100"
          sync.hget("ns_total:tsdb0:wsA:ns2", "shard-3") shouldEqual "200"
          sync.zscore("zset:tsdb0:shard-3:wsA:ns1", "cpu") shouldEqual 60.0
          sync.zscore("zset:tsdb0:shard-3:wsA:ns1", "mem") shouldEqual 40.0
          sync.zscore("zset:tsdb0:shard-3:wsA:ns2", "cpu") shouldEqual 200.0
        } finally conn.close()
      } finally c.shutdown()
    }

    it("overwrites the previous ZSET rather than merging") {
      assume(enabled)
      flushDb()
      sink.publish("tsdb0", 3,
        ns = Seq(rec(Seq("wsA", "ns1"), 100)),
        perMetric = Map(Seq("wsA", "ns1") ->
          Seq(rec(Seq("wsA", "ns1", "cpu"), 60), rec(Seq("wsA", "ns1", "mem"), 40))))
      // Second cycle: cpu drops, mem gone, disk added
      sink.publish("tsdb0", 3,
        ns = Seq(rec(Seq("wsA", "ns1"), 100)),
        perMetric = Map(Seq("wsA", "ns1") ->
          Seq(rec(Seq("wsA", "ns1", "cpu"), 30), rec(Seq("wsA", "ns1", "disk"), 5))))

      val c = RedisClient.create(s"redis://${host.get}:${port.get}")
      try {
        val conn = c.connect()
        try {
          val sync = conn.sync()
          sync.zscore("zset:tsdb0:shard-3:wsA:ns1", "cpu") shouldEqual 30.0
          sync.zscore("zset:tsdb0:shard-3:wsA:ns1", "mem") shouldBe null // removed
          sync.zscore("zset:tsdb0:shard-3:wsA:ns1", "disk") shouldEqual 5.0
        } finally conn.close()
      } finally c.shutdown()
    }
  }

  describe("evict") {
    it("removes ns_total field and deletes ZSET for stale (ws, ns)") {
      assume(enabled)
      flushDb()
      sink.publish("tsdb0", 3,
        ns = Seq(rec(Seq("wsA", "ns1"), 100)),
        perMetric = Map(Seq("wsA", "ns1") -> Seq(rec(Seq("wsA", "ns1", "cpu"), 100))))
      sink.evict("tsdb0", 3, Set(("wsA", "ns1")))

      val c = RedisClient.create(s"redis://${host.get}:${port.get}")
      try {
        val conn = c.connect()
        try {
          val sync = conn.sync()
          sync.hget("ns_total:tsdb0:wsA:ns1", "shard-3") shouldBe null
          sync.exists("zset:tsdb0:shard-3:wsA:ns1") shouldEqual 0L
        } finally conn.close()
      } finally c.shutdown()
    }
  }

  describe("colon safety") {
    it("throws IllegalArgumentException when ws or ns contains a colon") {
      assume(enabled)
      an[IllegalArgumentException] should be thrownBy
        sink.publish("tsdb0", 3,
          ns = Seq(rec(Seq("ws:bad", "ns1"), 100)),
          perMetric = Map(Seq("ws:bad", "ns1") ->
            Seq(rec(Seq("ws:bad", "ns1", "cpu"), 100))))
    }
  }
}
```

- [ ] **Step 2: Run — expect compile failure**

Run: `sbt "coreJVM/testOnly filodb.core.memstore.RedisCardinalitySnapshotSinkSpec"`
Expected: compile error.

- [ ] **Step 3: Implement `RedisCardinalitySnapshotSink`**

```scala
package filodb.core.memstore

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.typesafe.scalalogging.StrictLogging
import io.lettuce.core.{RedisFuture, ScoredValue}
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.StringCodec

import filodb.core.memstore.ratelimit.CardinalityRecord

/**
 * CardinalitySnapshotSink backed by Redis via Lettuce. All commands within a
 * single publish() or evict() call are pipelined (auto-flush disabled around
 * the batch) — at ~2 000 commands per shard cycle, un-pipelined ~2 seconds vs.
 * pipelined ~milliseconds.
 *
 * Thread-safe: the underlying Lettuce connection is multiplexed and safe
 * for concurrent commands.
 */
class RedisCardinalitySnapshotSink(host: String, port: Int,
                                    commandTimeoutMs: Long)
    extends CardinalitySnapshotSink with StrictLogging {

  private val client = RedisSnapshotClient.acquire(host, port, commandTimeoutMs)
  private val connection: StatefulRedisConnection[String, String] =
    client.connect(StringCodec.UTF8)
  private val async = connection.async()

  logger.info(s"RedisCardinalitySnapshotSink connected host=$host port=$port " +
    s"commandTimeoutMs=$commandTimeoutMs")

  private def requireNoColon(s: String, field: String): Unit =
    require(!s.contains(':'), s"$field must not contain colon: '$s'")

  override def publish(partition: String, shardNum: Int,
                       ns: Seq[CardinalityRecord],
                       perMetric: Map[Seq[String], Seq[CardinalityRecord]]): Unit = {
    requireNoColon(partition, "partition")
    val shardField = s"shard-$shardNum"

    async.setAutoFlushCommands(false)
    try {
      val pending = scala.collection.mutable.ArrayBuffer.empty[RedisFuture[_]]

      ns.foreach { nsRec =>
        val ws = nsRec.prefix(0)
        val nsName = nsRec.prefix(1)
        requireNoColon(ws, "ws")
        requireNoColon(nsName, "ns")
        val hkey = s"ns_total:$partition:$ws:$nsName"
        pending += async.hset(hkey, shardField, nsRec.value.activeTsCount.toString)
      }

      perMetric.foreach { case (nsPrefix, metrics) =>
        val ws = nsPrefix(0)
        val nsName = nsPrefix(1)
        val zkey = s"zset:$partition:shard-$shardNum:$ws:$nsName"
        pending += async.del(zkey)
        if (metrics.nonEmpty) {
          val members: Array[ScoredValue[String]] = metrics.map { r =>
            ScoredValue.just(r.value.activeTsCount.toDouble, r.prefix(2))
          }.toArray
          pending += async.zadd(zkey, members: _*)
        }
      }

      async.flushCommands()
      // Wait for all commands to complete within commandTimeoutMs each — Lettuce
      // returns per-command futures, and their timeout is set on the RedisURI.
      pending.foreach { f =>
        try f.get()
        catch { case NonFatal(t) =>
          logger.warn(s"redis command failed partition=$partition shard=$shardNum: " +
            t.getMessage)
        }
      }
    } finally async.setAutoFlushCommands(true)
  }

  override def evict(partition: String, shardNum: Int,
                     stale: Set[(String, String)]): Unit = {
    if (stale.isEmpty) return
    requireNoColon(partition, "partition")
    val shardField = s"shard-$shardNum"

    async.setAutoFlushCommands(false)
    try {
      val pending = scala.collection.mutable.ArrayBuffer.empty[RedisFuture[_]]
      stale.foreach { case (ws, nsName) =>
        requireNoColon(ws, "ws")
        requireNoColon(nsName, "ns")
        pending += async.hdel(s"ns_total:$partition:$ws:$nsName", shardField)
        pending += async.del(s"zset:$partition:shard-$shardNum:$ws:$nsName")
      }
      async.flushCommands()
      pending.foreach { f =>
        try f.get()
        catch { case NonFatal(t) =>
          logger.warn(s"redis evict failed partition=$partition shard=$shardNum: " +
            t.getMessage)
        }
      }
    } finally async.setAutoFlushCommands(true)
  }

  override def close(): Unit = {
    try connection.close() catch { case _: Throwable => }
    // Do NOT shut down the JVM-scoped client here.
    logger.info(s"RedisCardinalitySnapshotSink closed host=$host port=$port")
  }
}
```

- [ ] **Step 4: Run the integration test with Valkey**

Start Valkey if not already: `brew services start valkey`, verify with `valkey-cli ping` → `PONG`.

Run:
```bash
sbt -Dfilodb.test.redis.host=localhost -Dfilodb.test.redis.port=6379 \
  "coreJVM/testOnly filodb.core.memstore.RedisCardinalitySnapshotSinkSpec"
```

Expected: 4 tests PASS.

- [ ] **Step 5: Confirm the test is skipped when Redis is not configured**

Run: `sbt "coreJVM/testOnly filodb.core.memstore.RedisCardinalitySnapshotSinkSpec"` (no `-D` flags).
Expected: 4 tests PASS as ignored/canceled (via `assume(...)`).

- [ ] **Step 6: Commit**

```bash
git add core/src/main/scala/filodb.core/memstore/RedisCardinalitySnapshotSink.scala \
        core/src/test/scala/filodb.core/memstore/RedisCardinalitySnapshotSinkSpec.scala
git commit -m "$(cat <<'EOF'
feat(memstore): add RedisCardinalitySnapshotSink

Lettuce-backed CardinalitySnapshotSink. Commands within one publish/evict
call are pipelined (setAutoFlushCommands(false) around the batch, single
flushCommands() at the end). Keys are partition-prefixed to prevent
cross-partition collision in the shared Redis. Fails fast if ws/ns/partition
contains a colon (reserved delimiter).

Integration test gated by -Dfilodb.test.redis.host / .port sys props.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Wire `CardinalitySnapshotDriver` into `TimeSeriesShard`

**Files:**
- Modify: `core/src/main/scala/filodb.core/memstore/TimeSeriesShard.scala`

**Design note:** the JVM-shared `ScheduledExecutorService` for the snapshot driver lives in a small singleton (also created in this task). Every driver on the JVM uses it. Thread count is bounded (default 4). Shard-level `close()` cancels the shard's `ScheduledFuture`; JVM shutdown drains the pool.

- [ ] **Step 1: Create the shared scheduler singleton**

Create `core/src/main/scala/filodb.core/memstore/CardinalitySnapshotScheduler.scala`:

```scala
package filodb.core.memstore

import java.util.concurrent.{Executors, ScheduledExecutorService}

/**
 * Shared ScheduledExecutorService used by every CardinalitySnapshotDriver
 * on this JVM. Sized to a small fixed pool — tasks are I/O bound (Redis
 * calls) and short-lived, so a few threads are enough for thousands of
 * shards' worth of scheduled work.
 */
object CardinalitySnapshotScheduler {
  private lazy val scheduler: ScheduledExecutorService =
    Executors.newScheduledThreadPool(4, (r: Runnable) => {
      val t = new Thread(r, "cardinality-snapshot")
      t.setDaemon(true)
      t
    })
  def get: ScheduledExecutorService = scheduler
}
```

- [ ] **Step 2: Modify `TimeSeriesShard`**

Two identifiers used below are already in scope on `TimeSeriesShard`:

- `deploymentPartitionName` — `private val` declared at `TimeSeriesShard.scala:293` (`filodbConfig.getString("deployment-partition-name")`).
- `numShards` — constructor parameter at `TimeSeriesShard.scala:276`.

No new field or fallback is required. If either identifier is out of scope in your local tree, stop and reconcile with the current source before continuing.

**Zero-pointer note:** when `meteringEnabled = false`, `cardTracker` is `UnsafeUtils.ZeroPointer.asInstanceOf[CardinalityTracker]` (a null; see `TimeSeriesShard.scala:761`). The driver constructor merely holds the reference — no dereference — so construction is safe. `snapshotOnce()` DOES dereference, so it must never run against a null tracker. That's guaranteed by the `if (storeConfig.meteringEnabled && storeConfig.activeSeriesRedisEnabled)` guard around `start()` below.

In `core/src/main/scala/filodb.core/memstore/TimeSeriesShard.scala`:

At the top of the file, add imports if missing:
```scala
// (near other filodb.core.memstore imports)
```

Below the existing `cardTracker` val (~line 375) and where `activeSeriesSink` used to be (~lines 377-384), add:

```scala
private[memstore] val cardinalitySnapshotSink: CardinalitySnapshotSink =
  if (storeConfig.activeSeriesRedisEnabled)
    new RedisCardinalitySnapshotSink(
      host = storeConfig.activeSeriesRedisHost,
      port = storeConfig.activeSeriesRedisPort,
      commandTimeoutMs = storeConfig.activeSeriesRedisCommandTimeoutMs)
  else
    NoOpCardinalitySnapshotSink

private[memstore] val cardinalitySnapshotDriver: CardinalitySnapshotDriver =
  new CardinalitySnapshotDriver(
    partition = deploymentPartitionName,
    shardNum = shardNum,
    cardTracker = cardTracker,
    sink = cardinalitySnapshotSink)

if (storeConfig.meteringEnabled && storeConfig.activeSeriesRedisEnabled) {
  cardinalitySnapshotDriver.start(
    scheduler = CardinalitySnapshotScheduler.get,
    intervalSeconds = storeConfig.activeSeriesRedisSnapshotIntervalSeconds,
    totalShardsHint = numShards)
}
```

- [ ] **Step 3: Add shutdown hook**

Find the existing `shutdown()` or `close()` method on `TimeSeriesShard`. Add these two lines before other close calls:

```scala
try cardinalitySnapshotDriver.close() catch { case _: Throwable => }
try cardinalitySnapshotSink.close() catch { case _: Throwable => }
```

- [ ] **Step 4: Compile**

Run: `sbt "coreJVM/compile"`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Run all core tests to ensure nothing regressed**

Run: `sbt "coreJVM/test"`
Expected: all tests pass. If existing `TimeSeriesShard`-based tests fail because they don't provide `deployment-partition-name` or the new config keys, add them to the test config helpers.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/scala/filodb.core/memstore/CardinalitySnapshotScheduler.scala \
        core/src/main/scala/filodb.core/memstore/TimeSeriesShard.scala
git commit -m "$(cat <<'EOF'
feat(memstore): wire CardinalitySnapshotDriver into TimeSeriesShard

Each shard constructs a driver (partition, shardNum, cardTracker, sink) at
init and, when metering + active-series-redis are enabled, starts it on the
shared per-JVM CardinalitySnapshotScheduler. Shard shutdown cancels the
driver's ScheduledFuture and closes its connection; the JVM-scoped Lettuce
client persists.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Manual smoke test + final push

**Rationale:** unit tests cover the driver (Task 3) and Redis sink (Task 5) in isolation; Task 6 wires them into `TimeSeriesShard` and its compile+existing-tests gate confirms no regression. A full shard-level automated integration test would require substantial fixture wiring around `TimeSeriesShard`'s many collaborators — value is low relative to a hands-on smoke test with real Valkey. So we do the manual smoke test here and defer any automated shard-level end-to-end to future work.

- [ ] **Step 1: Start Valkey locally**

```bash
brew install valkey 2>/dev/null || true
brew services start valkey
valkey-cli ping   # → PONG
```

- [ ] **Step 2: Enable in a dev config, boot FiloDB with a small ingest workload**

Point a dev FiloDB at Valkey by setting:
```hocon
filodb {
  memstore {
    store-config {
      active-series-redis {
        enabled = true
        host = "localhost"
        port = 6379
        snapshot-interval-seconds = 10   # short for smoke test
        command-timeout-ms = 500
      }
    }
  }
  deployment-partition-name = "tsdb-dev"
}
```

Run the standard local FiloDB dev startup command (per project README) and let it ingest a known-shape workload.

- [ ] **Step 3: Verify Redis state after 10-20s**

```bash
valkey-cli --scan --pattern "ns_total:tsdb-dev:*" | head -5
valkey-cli --scan --pattern "zset:tsdb-dev:*"    | head -5
valkey-cli hgetall "ns_total:tsdb-dev:<ws>:<ns>"
valkey-cli zrevrange "zset:tsdb-dev:shard-0:<ws>:<ns>" 0 4 WITHSCORES
```
Expected: HASH fields named `shard-{N}` with numeric activeTsCount values, ZSET members are metric names with numeric scores.

- [ ] **Step 4: Stop ingest, wait ≥ 1 snapshot cycle, verify eviction**

Stop ingest and let flush run. After the next snapshot cycle, evicted `(ws, ns)` should have their shard-N field removed from `ns_total:...` and their ZSET deleted.

- [ ] **Step 5: Cross-check against `mosaic get workspace`**

```bash
mosaic get workspace <ws>          # note TS1H utilization
# sum HASH fields across all 138 partitions for the same ws+ns:
for p in tsdb0 tsdb3 tsdb6 ...; do
  valkey-cli hvals "ns_total:$p:<ws>:<ns>"
done | awk '{s+=$1} END {print s}'
```
Expected: sum of HGETALL values across all partitions ≈ `mosaic` TS1H (within snapshot-cycle staleness).

- [ ] **Step 6: Push branch**

```bash
git push origin reject-series-on-quota-breach
```

---

## Rollout notes

- Land behind `active-series-redis.enabled = false` (default). No behavior change for existing deployments.
- Enable on one dev partition first; watch shard CPU and Redis load.
- Cross-check `HGETALL ns_total:{partition}:{ws}:{ns}` sum against `mosaic get workspace {ws}` utilization for spot verification.
- Roll out partition by partition.

## Rollback

Set `active-series-redis.enabled = false` and restart. Redis state persists but is no longer updated. Can be flushed out of band: `valkey-cli --scan --pattern "ns_total:{partition}:*" | xargs valkey-cli del` and same for `zset:{partition}:*`.

## Followup work (out of scope for this plan)

- Compactor that runs `ZUNIONSTORE` across per-shard ZSETs to produce `zset:{partition}:{ws}:{ns}` (or globally across partitions) for gateway consumers.
- Gateway-side reader + cache + block-list logic (separate spec).
- FiloDB metrics for snapshot cycle duration, Redis command latency, failures.
- Cross-partition namespace-total aggregation (client-side sum across all 138 partition HASHes).
