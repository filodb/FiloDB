package filodb.coordinator.v2

import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core.{DatasetRef, TestData}
import filodb.core.MetricsTestData._
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SomeData, TimeSeriesMemStore}
import filodb.core.memstore.ratelimit.{CardinalityRecord, CardinalityValue}
import filodb.core.metadata.Schemas
import filodb.core.query.{QueryConfig, QueryContext, QuerySession}
import filodb.core.store.{ChunkSource, InMemoryMetaStore, NullColumnStore}
import filodb.memory.format.SeqRowReader
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query.{QueryResult, StreamQueryResponse}
import filodb.query.exec._
import filodb.query.exec.TsCardExec.{CardCounts, RowData}

/**
 * Covers the direct (non-QueryActor) cardinality path:
 *   - ClusterCardinalities.merge, the cross-node/cross-shard fold
 *   - equivalence with the existing TsCardExec/TsCardReduceExec ExecPlan path
 *
 * The two paths read the same per-shard cardinality stores, so for the same prefix/depth they
 * must produce identical counts. That equivalence is the point of this spec: the HTTP endpoints
 * /timeseries and /timeseries/execplan are expected to be interchangeable.
 */
class ClusterCardinalitiesSpec extends AnyFunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  private val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  private val queryConfig = QueryConfig(config.getConfig("query"))
  private val querySession = QuerySession(QueryContext(), queryConfig)

  private val policy = new FixedMaxPartitionsEvictionPolicy(20)
  private val memStore = new TimeSeriesMemStore(
    config, new NullColumnStore, new NullColumnStore, new InMemoryMetaStore(), Some(policy))

  private val dsRef = timeseriesDatasetMultipleShardKeys.ref
  private val now = System.currentTimeMillis()
  private val tuples = (100 until 0).by(-1).map { n => (now - n * 10000, n.toDouble) }

  // 2 shards, with demo/App-0 series deliberately split across both so the merge has to sum
  private val shardPartKeyLabelValues = Seq(
    Seq(  // shard 0
      ("http_req_total", Map("instance" -> "h1", "_ws_" -> "demo", "_ns_" -> "App-0")),
      ("http_foo_total", Map("instance" -> "h1", "_ws_" -> "demo", "_ns_" -> "App-0"))
    ),
    Seq(  // shard 1
      ("http_req_total", Map("instance" -> "h2", "_ws_" -> "demo", "_ns_" -> "App-0")),
      ("http_bar_total", Map("instance" -> "h1", "_ws_" -> "demo", "_ns_" -> "App-0")),
      ("http_req_total-A", Map("instance" -> "h2", "_ws_" -> "demo-A", "_ns_" -> "App-A"))
    )
  )
  private val numShards = shardPartKeyLabelValues.size

  private def initShard(partKeyLabelValues: Seq[(String, Map[String, String])], ishard: Int): Unit = {
    val partTagsUTF8s = partKeyLabelValues.map { case (m, t) => (m, t.map { case (k, v) => (k.utf8, v.utf8) }) }
    builder.reset()
    partTagsUTF8s.foreach { case (metric, partTagsUTF8) =>
      tuples.map { t => SeqRowReader(Seq(t._1, t._2, metric, partTagsUTF8)) }
        .foreach(builder.addFromReader(_, Schemas.promCounter))
    }
    memStore.setup(dsRef, Schemas(Schemas.promCounter), ishard, TestData.storeConf, 1)
    memStore.ingest(dsRef, ishard, SomeData(builder.allContainers.head, 0))
  }

  override def beforeAll(): Unit = {
    for (ishard <- 0 until numShards) initShard(shardPartKeyLabelValues(ishard), ishard)
    memStore.refreshIndexForTesting(dsRef)
  }

  override def afterAll(): Unit = memStore.shutdown()

  private val executeDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlanWithClientParams, source: ChunkSource)
                         (implicit sched: Scheduler): Task[filodb.query.QueryResponse] =
      plan.execPlan.execute(memStore, querySession)(sched)
    override def clusterName: String = "raw"
    override def isLocalCall: Boolean = true
    override def dispatchStreaming(plan: ExecPlanWithClientParams, source: ChunkSource)
                                  (implicit sched: Scheduler): Observable[StreamQueryResponse] = ???
  }

  private def cv(ts: Long, active: Long, billable: Long, children: Long = 0, quota: Long = 0) =
    CardinalityValue(ts, active, billable, children, quota)

  // ---------- ClusterCardinalities.merge ----------

  it("should sum counts for the same prefix across shards and across nodes") {
    val nodeA = LocalCardinalities(dsRef, Seq(0, 1), Seq(
      CardinalityRecord(0, Seq("demo", "App-0"), cv(2, 2, 2)),
      CardinalityRecord(1, Seq("demo", "App-0"), cv(3, 3, 3))))
    val nodeB = LocalCardinalities(dsRef, Seq(2, 3), Seq(
      CardinalityRecord(2, Seq("demo", "App-0"), cv(5, 4, 4)),
      CardinalityRecord(3, Seq("demo-A", "App-A"), cv(1, 1, 1))))

    val merged = ClusterCardinalities.merge(dsRef, 4, Seq(Some(nodeA), Some(nodeB)))

    merged.missingShards shouldEqual Nil
    val byPrefix = merged.cardinalities.map(r => r.prefix -> r.value).toMap
    byPrefix(Seq("demo", "App-0")) shouldEqual cv(10, 9, 9)
    byPrefix(Seq("demo-A", "App-A")) shouldEqual cv(1, 1, 1)
    // shard is meaningless post-merge
    merged.cardinalities.map(_.shard).distinct shouldEqual Seq(-1)
  }

  it("should carry the max of childrenCount/childrenQuota rather than summing them") {
    // these describe one shard's trie shape and quota; summing across shards would be nonsense
    val node = LocalCardinalities(dsRef, Seq(0, 1), Seq(
      CardinalityRecord(0, Seq("demo"), cv(2, 2, 2, children = 3, quota = 100)),
      CardinalityRecord(1, Seq("demo"), cv(2, 2, 2, children = 5, quota = 100))))

    val merged = ClusterCardinalities.merge(dsRef, 2, Seq(Some(node)))

    merged.cardinalities.head.value shouldEqual cv(4, 4, 4, children = 5, quota = 100)
  }

  it("should report shards no node scanned as missing") {
    val node = LocalCardinalities(dsRef, Seq(0, 1), Seq(
      CardinalityRecord(0, Seq("demo"), cv(1, 1, 1))))

    ClusterCardinalities.merge(dsRef, 4, Seq(Some(node))).missingShards shouldEqual Seq(2, 3)
  }

  it("should report a failed node's shards as missing without knowing which shards it owned") {
    val nodeA = LocalCardinalities(dsRef, Seq(0, 1), Seq(
      CardinalityRecord(0, Seq("demo"), cv(1, 1, 1))))
    // nodeB failed/timed out - it reports nothing, so its shards fall out of the covered set
    val merged = ClusterCardinalities.merge(dsRef, 4, Seq(Some(nodeA), None))

    merged.missingShards shouldEqual Seq(2, 3)
    // partial data is still returned; refusing it is the caller's (route's) decision
    merged.cardinalities should have size 1
  }

  it("should report every shard missing when all nodes fail") {
    val merged = ClusterCardinalities.merge(dsRef, 3, Seq(None, None))
    merged.cardinalities shouldEqual Nil
    merged.missingShards shouldEqual Seq(0, 1, 2)
  }

  it("should pass the overflow prefix through as an ordinary group") {
    import filodb.core.memstore.ratelimit.CardinalityStore.OVERFLOW_PREFIX
    val nodeA = LocalCardinalities(dsRef, Seq(0), Seq(CardinalityRecord(0, OVERFLOW_PREFIX, cv(7, 7, 7))))
    val nodeB = LocalCardinalities(dsRef, Seq(1), Seq(CardinalityRecord(1, OVERFLOW_PREFIX, cv(3, 3, 3))))

    val merged = ClusterCardinalities.merge(dsRef, 2, Seq(Some(nodeA), Some(nodeB)))

    merged.cardinalities.map(r => r.prefix -> r.value).toMap shouldEqual
      Map(OVERFLOW_PREFIX -> cv(10, 10, 10))
  }

  // ---------- equivalence with the existing ExecPlan path ----------

  /** Runs the existing path: one TsCardExec per shard, reduced by TsCardReduceExec. */
  private def viaExecPlan(prefix: Seq[String], depth: Int): Map[Seq[String], CardCounts] = {
    val leaves = (0 until numShards).map { ishard =>
      TsCardExec(QueryContext(), executeDispatcher, dsRef, ishard, prefix, depth, "raw")
    }
    val resp = TsCardReduceExec(QueryContext(), executeDispatcher, leaves)
      .execute(memStore, querySession).runToFuture.futureValue
    (resp: @unchecked) match {
      case QueryResult(_, _, rvs, _, _, _, _) =>
        rvs.flatMap(_.rows().map(RowData.fromRowReader).toSeq).map { d =>
          // group is "ws,ns,...,dataset" - drop the trailing dataset to get the prefix
          d.group.toString.split(TsCardExec.PREFIX_DELIM).dropRight(1).toSeq -> d.counts
        }.toMap
    }
  }

  /** Runs the direct path: scan each node's shards, then merge. */
  private def viaDirectPath(prefix: Seq[String],
                            depth: Int,
                            shardsPerNode: Seq[Seq[Int]]): Map[Seq[String], CardCounts] = {
    val perNode = shardsPerNode.map { shards =>
      Some(LocalCardinalities(dsRef, shards,
        memStore.scanTsCardinalities(QueryContext(), dsRef, shards, prefix, depth)))
    }
    val merged = ClusterCardinalities.merge(dsRef, numShards, perNode)
    merged.missingShards shouldEqual Nil
    merged.cardinalities.map { r =>
      r.prefix -> CardCounts(r.value.activeTsCount, r.value.billableTsCount, r.value.tsCount)
    }.toMap
  }

  private val equivalenceCases = Seq(
    (Nil, 1),
    (Nil, 2),
    (Seq("demo"), 1),
    (Seq("demo"), 2),
    (Seq("demo"), 3),
    (Seq("demo", "App-0"), 2),
    (Seq("demo", "App-0"), 3),
    (Seq("demo", "App-0", "http_req_total"), 3)
  )

  it("should produce the same counts as the ExecPlan path, for one shard per node") {
    equivalenceCases.foreach { case (prefix, depth) =>
      withClue(s"prefix=$prefix depth=$depth: ") {
        viaDirectPath(prefix, depth, Seq(Seq(0), Seq(1))) shouldEqual viaExecPlan(prefix, depth)
      }
    }
  }

  it("should produce the same counts as the ExecPlan path when one node owns several shards") {
    // the direct path collapses a node's shards into a single scan, so this is the case where a
    // per-node multi-shard scan must still equal the per-shard fan-out
    equivalenceCases.foreach { case (prefix, depth) =>
      withClue(s"prefix=$prefix depth=$depth: ") {
        viaDirectPath(prefix, depth, Seq(Seq(0, 1))) shouldEqual viaExecPlan(prefix, depth)
      }
    }
  }

  it("should produce non-trivial counts, so the equivalence checks are not comparing empty maps") {
    // guards against both paths silently returning nothing and the comparison passing anyway
    val direct = viaDirectPath(Nil, 2, Seq(Seq(0), Seq(1)))
    direct.keySet shouldEqual Set(Seq("demo", "App-0"), Seq("demo-A", "App-A"))
    // 4 series under demo/App-0: req(shard0), foo(shard0), req(shard1), bar(shard1)
    direct(Seq("demo", "App-0")).shortTerm shouldEqual 4
    direct(Seq("demo-A", "App-A")).shortTerm shouldEqual 1
  }

  it("should scan all shards when the requested shard list is empty") {
    // TimeSeriesMemStore treats an empty shard list as `all local shards`, which is what the
    // scatter relies on
    val all = memStore.scanTsCardinalities(QueryContext(), dsRef, Nil, Nil, 2)
    val explicit = memStore.scanTsCardinalities(QueryContext(), dsRef, Seq(0, 1), Nil, 2)
    all.toSet shouldEqual explicit.toSet
  }

  it("should NOT be able to express 'scan nothing' via an empty shard list") {
    // Pins the hazard the handler guards against: an empty shard list means ALL shards, not none,
    // so a node whose owned-shard intersection comes out empty must short circuit instead of
    // calling scanTsCardinalities. If this ever starts returning empty, that guard can be dropped.
    memStore.scanTsCardinalities(QueryContext(), dsRef, Nil, Nil, 2) should not be empty
  }

  it("should report a node that scanned nothing as covering no shards") {
    // shape returned by the handler's empty-intersection short circuit
    val merged = ClusterCardinalities.merge(dsRef, numShards,
      Seq(Some(LocalCardinalities(dsRef, Nil, Nil))))
    merged.cardinalities shouldEqual Nil
    merged.missingShards shouldEqual Seq(0, 1)
  }

  it("should treat a node that scanned fewer shards than it owns as partial") {
    // if the memstore has not instantiated an owned shard, the node reports only what it scanned,
    // so the gap must surface rather than being inferred as complete
    val partial = LocalCardinalities(dsRef, Seq(0), Seq(CardinalityRecord(0, Seq("demo"), cv(1, 1, 1))))
    ClusterCardinalities.merge(dsRef, numShards, Seq(Some(partial))).missingShards shouldEqual Seq(1)
  }
}
