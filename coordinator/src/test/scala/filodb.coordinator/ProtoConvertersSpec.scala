package filodb.coordinator

import filodb.core.DatasetRef
import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.core.query.{ColumnFilter, QueryConfig, QueryContext}
import filodb.core.store.AllChunkScan
import filodb.query.exec.{InProcessPlanDispatcher, MultiSchemaPartitionsExec}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import filodb.coordinator.ProtoConverters._

import java.util.concurrent.TimeUnit

class ProtoConvertersSpec extends AnyFunSpec with Matchers {

  val qContext = QueryContext()
  val now = System.currentTimeMillis()
  val timeout = akka.util.Timeout(10L, TimeUnit.SECONDS);
//  val f = ActorSystemHolder.system.actorSelection("mypath").resolveOne()(timeout);
//  val ar: akka.actor.ActorRef = Await.result(f, Duration(60L, TimeUnit.SECONDS))
//  val actorDispatcher = ActorPlanDispatcher(
//    ar, "clusterName"
//  )
  val filters = Seq(ColumnFilter("_ws_", filodb.core.query.Filter.Equals("demo")),
    ColumnFilter("_ns_", filodb.core.query.Filter.Equals("App-0")),
    ColumnFilter("_metric_", filodb.core.query.Filter.Equals("http_req_total")),
  )


  val inProcessDispatcher = InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig)

  it("should convert LabelCardinalityExec to proto and back") {

    val shardPartKeyLabelValues = Seq(
      Seq( // shard 0
        ("http_req_total", Map("instance" -> "someHost:8787", "job" -> "myCoolService",
          "unicode_tag" -> "uni\u03C0tag", "_ws_" -> "demo", "_ns_" -> "App-0")),
        ("http_foo_total", Map("instance" -> "someHost:8787", "job" -> "myCoolService",
          "unicode_tag" -> "uni\u03BCtag", "_ws_" -> "demo", "_ns_" -> "App-0"))
      ),
      Seq( // shard 1
        ("http_req_total", Map("instance" -> "someHost:9090", "job" -> "myCoolService",
          "unicode_tag" -> "uni\u03C0tag", "_ws_" -> "demo", "_ns_" -> "App-0")),
        ("http_bar_total", Map("instance" -> "someHost:8787", "job" -> "myCoolService",
          "unicode_tag" -> "uni\u03C0tag", "_ws_" -> "demo", "_ns_" -> "App-0")),
        ("http_req_total-A", Map("instance" -> "someHost:9090", "job" -> "myCoolService",
          "unicode_tag" -> "uni\u03C0tag", "_ws_" -> "demo-A", "_ns_" -> "App-A")),
      )
    )

    val timeseriesDatasetMultipleShardKeys = Dataset.make("timeseries",
      Seq("tags:map"),
      Seq("timestamp:ts", "value:double:detectDrops=true"),
      Seq.empty,
      None,
      DatasetOptions(Seq("_metric_", "_ws_", "_ns_"), "_metric_")).get



    val leaves = (0 until shardPartKeyLabelValues.size).map { ishard =>
      filodb.query.exec.LabelCardinalityExec(qContext, inProcessDispatcher,
        timeseriesDatasetMultipleShardKeys.ref, ishard, filters, now - 5000, now)
    }

    val execPlan = filodb.query.exec.LabelCardinalityReduceExec(qContext, inProcessDispatcher, leaves)
    execPlan.toProto.fromProto shouldEqual execPlan
  }

  it("should convert MultiSchemaPartitionsExec to proto and back") {
    val dsRef = DatasetRef("raw-metrics")

    val execPlan = MultiSchemaPartitionsExec(QueryContext(), inProcessDispatcher,
      dsRef, 0, filters, AllChunkScan, "_metric_")
    execPlan.toProto.fromProto shouldEqual execPlan
  }
}
