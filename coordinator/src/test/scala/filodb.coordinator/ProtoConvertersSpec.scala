package filodb.coordinator

import akka.actor.Props
import com.typesafe.config.ConfigFactory

import filodb.core.{DatasetRef, GdeltTestData, TestData}
import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.core.query.{ColumnFilter, ColumnInfo, CustomRangeVectorKey, NoCloseCursor, QueryConfig, QueryContext, QueryStats, RangeVector, RangeVectorCursor, RangeVectorKey, ResultSchema, RvRange, SerializedRangeVector, TransientRow}
import filodb.core.store.AllChunkScan
import filodb.query.exec.{InProcessPlanDispatcher, MultiSchemaPartitionsExec}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.coordinator.ProtoConverters._
import filodb.core.binaryrecord2.{BinaryRecordRowReader, RecordBuilder}
import java.util.concurrent.TimeUnit

import org.apache.arrow.memory.RootAllocator

import filodb.coordinator.flight.ArrowSerializedRangeVectorOps
import filodb.core.binaryrecord2.RecordContainer.BRIterator
import filodb.core.metadata.Column.ColumnType
import filodb.memory.format.{ZeroCopyUTF8String => UTF8Str}
import filodb.query.ProtoConverters.{SerializableRangeVectorFromProtoConverter, SerializableRangeVectorToProtoConverter}

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
    execPlan.toProto.fromProto(qContext) shouldEqual execPlan
  }

  // Helper to create mock RangeVector with double values
  private def toRv(samples: Seq[(Long, Double)],
                   rangeVectorKey: RangeVectorKey,
                   rvPeriod: RvRange): RangeVector = {
    new RangeVector {
      override def key: RangeVectorKey = rangeVectorKey
      override def rows(): RangeVectorCursor = NoCloseCursor.NoCloseCursor(samples.map(r => new TransientRow(r._1, r._2)).iterator)
      override def outputRange: Option[RvRange] = Some(rvPeriod)
    }
  }

  it("should convert the ArrowSerializedRangeVector to proto and back") {

    System.setProperty("arrow.memory.debug.allocator", "true") // allows debugging of memory leaks - look into logs
    val allocator = new RootAllocator(10000000)

    try {
      val keysMap = Map(UTF8Str("metric") -> UTF8Str("temperature"),
        UTF8Str("host") -> UTF8Str("server1"))
      val key = CustomRangeVectorKey(keysMap)
      val resSchema = new ResultSchema(Seq(
        ColumnInfo("time", ColumnType.TimestampColumn),
        ColumnInfo("value", ColumnType.DoubleColumn)
      ), 1)
      val recSchema = resSchema.toRecordSchema
      val rb = SerializedRangeVector.newBuilder()

      val outputRange = Some(RvRange(1000, 1000, 5000))
      val rv = toRv(
        Seq((1000, 10.0), (2000, 20.0), (3000, 30.0), (4000, 40.0), (5000, 50.0)),
        key,
        outputRange.get
      )

      val queryStats = QueryStats()
      val vsrs = ArrowSerializedRangeVectorOps.VsrPopulationState()
      val brIterator = new BRIterator(new BinaryRecordRowReader(recSchema))

      // Populate VSR
      ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
        rv, recSchema, "testExecPlan", rb, queryStats, allocator, vsrs, brIterator
      )

      // Convert to ArrowSerializedRangeVector2 instances
      val allVsrs = vsrs.finishedVsrs ++ Seq(vsrs.currentVsr)
      val asrvs = ArrowSerializedRangeVectorOps.convertVsrsIntoArrowSrvs(allVsrs, resSchema)
      asrvs.foreach { a =>
        val roundTrip = a.toProto.fromProto
        roundTrip.rows().map(r => (r.getLong(0) , r.getDouble(1))).toList shouldEqual a.rows().map(r => (r.getLong(0) , r.getDouble(1))).toList
        roundTrip.key shouldEqual a.key
        roundTrip.outputRange shouldEqual a.outputRange
      }
      allVsrs.foreach(_.close())
    } finally {
     allocator.close()
    }
  }

  it("should convert MultiSchemaPartitionsExec to proto and back") {
    val dsRef = DatasetRef("raw-metrics")
    val qContext = QueryContext()
    val execPlan = MultiSchemaPartitionsExec(qContext, inProcessDispatcher,
      dsRef, 0, filters, AllChunkScan, "_metric_")
    execPlan.toProto.fromProto(qContext) shouldEqual execPlan
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
