package filodb.coordinator

import akka.actor.Props
import com.typesafe.config.ConfigFactory
import filodb.core.{DatasetRef, GdeltTestData, TestData}
import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.core.query.{ColumnFilter, QueryConfig, QueryContext}
import filodb.core.store.AllChunkScan
import filodb.query.exec.{InProcessPlanDispatcher, MultiSchemaPartitionsExec}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import filodb.coordinator.ProtoConverters._
import filodb.core.binaryrecord2.RecordBuilder

import java.util.concurrent.TimeUnit

class ProtoConvertersSpec extends AnyFunSpec with Matchers {


  val qContext = QueryContext()
  val now = System.currentTimeMillis()
  val timeout = akka.util.Timeout(10L, TimeUnit.SECONDS);
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

  it("should convert PartKeyLuceneIndexRecord to proto and back") {
    val dataset6 = filodb.core.GdeltTestData.dataset6
    val partBuilder = new RecordBuilder(TestData.nativeMem)

    // Add the first ten keys and row numbers
    val pkrs = GdeltTestData.partKeyFromRecords(dataset6, GdeltTestData.records(dataset6, GdeltTestData.readers.take(10)), Some(partBuilder))
      .zipWithIndex.map { case (addr, i) =>
      val pk = dataset6.partKeySchema.asByteArray(filodb.memory.format.UnsafeUtils.ZeroPointer, addr)
      val pklir = filodb.core.memstore.PartKeyLuceneIndexRecord(pk, i, i + 10)
      val pklir2 = pklir.toProto.fromProto
      pklir.partKey shouldEqual pklir2.partKey
      pklir.startTime shouldEqual pklir2.startTime
      pklir.endTime shouldEqual pklir2.endTime
    }
  }

  object DummyActor {
    def props: Props =
      Props(new DummyActor)
  }

  class DummyActor extends akka.actor.Actor {
    override def receive: Receive = {
      case "" => Unit
    }
  }

  it("should convert ActorPlanDispatcher to proto and back") {
    val config = ConfigFactory.load()
    val ownActorSystem = ActorSystemHolder.system == null
    val system = if (ownActorSystem)
      ActorSystemHolder.createActorSystem("testActorSystem", config)
    else
      ActorSystemHolder.system
    try {
      val actorRef = system.actorOf(DummyActor.props)
      val dispatcher = ActorPlanDispatcher(actorRef, "testCluster")
      dispatcher shouldEqual dispatcher.toProto.fromProto
    } finally {
      if (ownActorSystem) {
        ActorSystemHolder.terminateActorSystem()
      }
    }
  }
}
