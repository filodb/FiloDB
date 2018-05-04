package filodb.coordinator.client

import akka.actor.ActorRef
import akka.serialization.SerializationExtension
import akka.testkit.TestProbe
import org.scalatest.concurrent.ScalaFutures

import filodb.coordinator.{ActorSpecConfig, ActorTest, NodeClusterActor, ShardMapper}
import filodb.coordinator.queryengine.Utils
import filodb.coordinator.queryengine2.QueryEngine
import filodb.core.{MachineMetricsData, MetricsTestData, NamesTestData, TestData}
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.memstore._
import filodb.core.metadata.Column.ColumnType
import filodb.core.store._
import filodb.memory._
import filodb.memory.format.{RowReader, SeqRowReader}
import filodb.query.{QueryResult => QueryResult2, _}

object SerializationSpecConfig extends ActorSpecConfig {
  override val defaultConfig = """
                      |akka.loggers = ["akka.testkit.TestEventListener"]
                      |akka.actor.serialize-messages = on
                      |akka.actor.kryo.buffer-size = 2048
                      """.stripMargin
}

/**
 * Tests Akka serialization of various messages by enabling serialization of all messages, including that
 * Akka config is set properly for serialization.
 * You probably want to play around with config in filodb-defaults.conf
 */
class SerializationSpec extends ActorTest(SerializationSpecConfig.getNewSystem) with ScalaFutures {
  import IngestionCommands._
  import NodeClusterActor._
  import LogicalPlan._
  import QueryCommands._
  import NamesTestData._

  val serialization = SerializationExtension(system)

  private def roundTrip(thing: AnyRef): AnyRef = {
    val serializer = serialization.findSerializerFor(thing)
    serializer.fromBinary(serializer.toBinary(thing))
  }

  it("should be able to serialize different IngestionCommands messages") {
    val setupMsg = DatasetSetup(dataset.asCompactString, TestData.storeConf)
    Seq(setupMsg,
        IngestionCommands.UnknownDataset,
        BadSchema("no match foo blah"),
        Ack(123L)).foreach { thing => roundTrip(thing) shouldEqual thing }
  }

  it("should be able to serialize IngestionConfig, SetupDataset, DatasetResourceSpec, IngestionSource") {
    val source1 = s"""
                       |dataset = ${dataset.name}
                       |num-shards = 128
                       |min-num-nodes = 32
                       |sourceconfig {
                       |  filo-topic-name = "org.example.app.topic1"
                       |  bootstrap.servers = "host:port"
                       |  filo-record-converter = "org.example.app.SomeRecordConverter"
                       |  value.deserializer=com.apple.pie.filodb.timeseries.TimeSeriesDeserializer
                       |  group.id = "org.example.app.consumer.group1"
                       |  my.custom.key = "custom.value"
                       |  store {
                       |    flush-interval = 5m
                       |    shard-memory-mb = 500
                       |  }
                       |}
                     """.stripMargin

    val source2 = """
                    |dataset = "gdelt"
                    |num-shards = 32
                    |min-num-nodes = 10
                    |sourceconfig {
                    |  store {
                    |    flush-interval = 5m
                    |    shard-memory-mb = 500
                    |  }
                    |}
                  """.stripMargin

    val source3 = """
                    |dataset = "a.b.c"
                    |num-shards = 32
                    |min-num-nodes = 10
                    |sourceconfig {
                    |  store {
                    |    flush-interval = 5m
                    |    shard-memory-mb = 500
                    |  }
                    |}
                  """.stripMargin

    val source4 = """
                    |dataset = "a-b-c"
                    |num-shards = 32
                    |min-num-nodes=10
                    |sourceconfig {
                    |  store {
                    |    flush-interval = 5m
                    |    shard-memory-mb = 500
                    |  }
                    |}
                  """.stripMargin

    val command1 = SetupDataset(IngestionConfig(source1, "a.backup").get)
    val command2 = SetupDataset(IngestionConfig(source2, "a.backup").get)
    val command3 = SetupDataset(IngestionConfig(source2, "a.backup").get)
    val command4 = SetupDataset(IngestionConfig(source4, "a.backup").get)
    Set(command1, command2, command3, command4) forall(cmd => roundTrip(cmd) === cmd) shouldEqual true
  }

  it("should be able to serialize a ShardMapper") {
    val emptyRef = ActorRef.noSender
    val mapper = new ShardMapper(16)
    mapper.registerNode(Seq(4, 7, 8), emptyRef)

    roundTrip(mapper) shouldEqual mapper
  }

  it("should be able to serialize and deserialize IngestRows with BinaryRecords") {
    import Serializer._
    import filodb.core.NamesTestData._

    putPartitionSchema(dataset.partitionBinSchema)
    putDataSchema(dataset.dataBinSchema)
    val routing = IngestRouting(dataset, Seq("first", "last", "age", "seg"))
    val records = mapper(names).zipWithIndex.map { case (r, idx) =>
      val record = IngestRecord(routing, r, idx)
      record.copy(partition = dataset.partKey(record.partition),
                  data = BinaryRecord(dataset.dataBinSchema, record.data))
    }
    val cmd = IngestRows(dataset.ref, 1, records)
    roundTrip(cmd) shouldEqual cmd
  }

  import filodb.core.query._

  it("should be able to serialize different Aggregates") {
    roundTrip(DoubleAggregate(99.9)) shouldEqual (DoubleAggregate(99.9))

    val arrayAgg = new ArrayAggregate(10, 0.0)
    val deserArrayAgg = roundTrip(arrayAgg).asInstanceOf[ArrayAggregate[Double]]
    deserArrayAgg.result should === (arrayAgg.result)

    val pointAgg = new PrimitiveSimpleAggregate(DoubleSeriesValues(3, "foo",
                                                                   Seq(DoubleSeriesPoint(100000L, 1.2))))
    val deserPointAgg = roundTrip(pointAgg).asInstanceOf[PrimitiveSimpleAggregate[DoubleSeriesValues]]
    deserPointAgg.data should equal (pointAgg.data)
  }

  it("should be able to serialize BinaryRecords larger than buffer size") {
    import MachineMetricsData._

    for { recSize <- Seq(1000, 1020, 2040, 2050, 4086, 4096, 4106) } {
      val record = dataset1.partKey(" " * recSize)
      val tupleResult = TupleResult(ResultSchema(dataset1.infosFromIDs(0 to 0), 1),
                                    Tuple(Some(PartitionInfo(record, 0)), record))
      roundTrip(tupleResult) shouldEqual tupleResult
    }
  }

  val timeScan = KeyRangeQuery(Seq(110000L), Seq(130000L))

  import monix.execution.Scheduler.Implicits.global

  it("should be able to serialize different Result types") {
    import MachineMetricsData._

    // 2 chunks of 10 samples each, (100000-109000), (110000-119000)
    val data = records(linearMultiSeries().take(20))
    val chunksets = TestData.toChunkSetStream(dataset1, defaultPartKey, data.map(_.data)).toListL
    val readers = chunksets.runAsync.futureValue.map(ChunkSetReader(_, dataset1, 0 to 2))
    val partVector = PartitionVector(Some(PartitionInfo(defaultPartKey, 0)), readers)

    // VectorListResult is the most complex, it has BinaryRecords, ColumnTypes, and ChunkSetReaders
    val chunkMethod = Utils.validateDataQuery(dataset1, timeScan).get.asInstanceOf[RowKeyChunkScan]
    val vectResult = VectorListResult(Some((chunkMethod.startkey, chunkMethod.endkey)),
                                      ResultSchema(dataset1.infosFromIDs(0 to 3), 1),
                                      Seq(partVector))

    val readVectResult = roundTrip(vectResult).asInstanceOf[VectorListResult]
    readVectResult.keyRange shouldEqual vectResult.keyRange
    readVectResult.schema shouldEqual vectResult.schema
    readVectResult.vectorList should have length (vectResult.vectorList.length)
    val vect = readVectResult.vectorList.head
    vect.info shouldEqual vectResult.vectorList.head.info
    vect.readers.head.info shouldEqual readers.head.info
    for { n <- 0 until vect.readers.head.vectors.size } {
      vect.readers.head.vectors(n).getClass shouldEqual readers.head.vectors(n).getClass
      vect.readers.head.vectors(n).toBuffer shouldEqual readers.head.vectors(n).toBuffer
    }

    // Also test BinaryRecord/Tuple serialization
    val tupleResult = TupleResult(ResultSchema(dataset1.infosFromIDs(0 to 0), 1),
                                  Tuple(Some(PartitionInfo(defaultPartKey, 0)), chunkMethod.startkey))
    roundTrip(tupleResult) shouldEqual tupleResult
  }


  val colStore: ColumnStore = new NullColumnStore()
  val memFactory = new NativeMemoryManager(10 * 1024 * 1024)

  val reclaimer = new ReclaimListener {
    def onReclaim(metaAddr: Long, numBytes: Int): Unit = {}
  }

  it("should be able to serialize writable buffers as part of VectorListResult") {
    import MachineMetricsData._
    val bufferPool = new WriteBufferPool(memFactory, dataset1, 100, 50)
    val chunkRetentionHours = 10
    val blockStore = new PageAlignedBlockManager(100 * 1024 * 1024,
      new MemoryStats(Map("test"-> "test")), reclaimer, 1, chunkRetentionHours)
    val pagedChunkStore = new DemandPagedChunkStore(dataset1, blockStore, 12, chunkRetentionHours, 1)
    val ingestBlockHolder = new BlockMemFactory(blockStore, None, 12, true)
    val part = new TimeSeriesPartition(0, dataset1, defaultPartKey, 0, colStore, bufferPool, false,
          pagedChunkStore,  new TimeSeriesShardStats(dataset1.ref, 0))
    val data = singleSeriesReaders().take(10)
    data.zipWithIndex.foreach { case (r, i) => part.ingest(r, 1000L + i, ingestBlockHolder) }

    val readers = part.readers(LastSampleChunkScan, Array(0, 1)).toSeq
    val partVector = PartitionVector(Some(PartitionInfo(defaultPartKey, 0)), readers)
    val vectResult = VectorListResult(None, ResultSchema(dataset1.infosFromIDs(0 to 3), 1), Seq(partVector))

    val readVectResult = roundTrip(vectResult).asInstanceOf[VectorListResult]
    readVectResult.keyRange shouldEqual vectResult.keyRange
    readVectResult.schema shouldEqual vectResult.schema
    readVectResult.vectorList should have length (vectResult.vectorList.length)
    val vect = readVectResult.vectorList.head
    vect.info shouldEqual vectResult.vectorList.head.info
    vect.readers.head.info shouldEqual readers.head.info
    for { n <- 0 until vect.readers.head.vectors.size } {
      // Can't compare classes since GrowableVector is restored as a Masked*BinaryVector
      vect.readers.head.vectors(n).toBuffer shouldEqual readers.head.vectors(n).toBuffer
    }
  }

  it("should be able to serialize LogicalPlans") {
    val series = (1 to 3).map(n => Seq(s"Series $n"))
    val q1 = LogicalPlanQuery(dataset.ref, simpleAgg("time_group_avg", Seq("2"), childPlan=
               PartitionsRange(MultiPartitionQuery(series), timeScan, Seq("min"))))
    roundTrip(q1) shouldEqual q1

    val series2 = (2 to 4).map(n => s"Series $n")
    val multiFilter = Seq(ColumnFilter("series", Filter.In(series2.toSet.asInstanceOf[Set[Any]])))
    val q2 = LogicalPlanQuery(dataset.ref, PartitionsInstant(FilteredPartitionQuery(multiFilter), Seq("min")))
    roundTrip(q2) shouldEqual q2
  }

  it("should be able to serialize ExecPlans") {
    import MachineMetricsData._
    val keys = (2 to 4).map(n => dataset1.partKey(s"Series $n"))
    val plan = ExecPlan.streamLastTuplePlan(dataset1, Seq(0, 1), MultiPartitionScan(keys))
    val query = ExecPlanQuery(dataset.ref, plan, 500)

    val readQuery = roundTrip(query).asInstanceOf[ExecPlanQuery]
    readQuery.dataset shouldEqual query.dataset
    readQuery.execPlan.getClass shouldEqual query.execPlan.getClass
    readQuery.execPlan.children.head.getClass shouldEqual query.execPlan.children.head.getClass
  }

  it("should be able to create, serialize and read from QueryResult") {

    import MachineMetricsData._

    val now = System.currentTimeMillis()
    val numRawSamples = 1000
    val limit = 900
    val reportingInterval = 10000
    val tuples = (numRawSamples until 0).by(-1).map(n => (now - n * reportingInterval, n.toDouble))

    val rvKey = new RangeVectorKey {
      def labelValues: Seq[LabelValue] = Seq()
      def sourceShards: Seq[Int] = Seq(0)
    }

    val rowsIterator = tuples.map { t =>
      new SeqRowReader(Seq[Any](t._1, t._2))
    }.iterator

    val rv = new RangeVector {
      override val rows: Iterator[RowReader] = rowsIterator
      override val key: RangeVectorKey = rvKey
    }

    val cols = Array(new ColumnInfo("timestamp", ColumnType.LongColumn),
      new ColumnInfo("value", ColumnType.DoubleColumn))
    val srv = SerializableRangeVector(rv, cols, limit)
    val observedTs = srv.rows.toSeq.map(_.getLong(0))
    val observedVal = srv.rows.toSeq.map(_.getDouble(1))
    observedTs shouldEqual tuples.map(_._1).take(limit)
    observedVal shouldEqual tuples.map(_._2).take(limit)

    // now we should also be able to create SerializableRangeVector using fast filo row iterator
    // since srv iterator is based on FFRR, try that
    val srv2 = SerializableRangeVector(srv, cols, limit)
    val observedTs2 = srv2.rows.toSeq.map(_.getLong(0))
    val observedVal2 = srv2.rows.toSeq.map(_.getDouble(1))
    observedTs2 shouldEqual tuples.map(_._1).take(limit)
    observedVal2 shouldEqual tuples.map(_._2).take(limit)

    val schema = ResultSchema(dataset1.infosFromIDs(0 to 0), 1)

    val result = QueryResult2("someId", schema, Seq(srv, srv2))
    val roundTripResult = roundTrip(result).asInstanceOf[QueryResult2]

    result.resultSchema shouldEqual roundTripResult.resultSchema
    result.id shouldEqual roundTripResult.id
    for {i <- 0 until roundTripResult.result.size } {
      // BinaryVector deserializes to different impl, so cannot compare top levle object
      roundTripResult.result(i).parsers.head.toBuffer shouldEqual result.result(i).parsers.head.toBuffer
      roundTripResult.result(i).key.labelValues shouldEqual result.result(i).key.labelValues
      roundTripResult.result(i).key.sourceShards shouldEqual result.result(i).key.sourceShards
    }
  }

  it ("should serialize and deserialize ExecPlan2") {
    val node0 = TestProbe().ref
    val mapper = new ShardMapper(1)
    mapper.registerNode(Seq(0), node0)
    def mapperRef: ShardMapper = mapper
    val dataset = MetricsTestData.timeseriesDataset
    val engine = new QueryEngine(dataset, mapperRef)
    val f1 = Seq(ColumnFilter("__name__", Filter.Equals("http_request_duration_seconds_bucket")),
      ColumnFilter("job", Filter.Equals("myService")),
      ColumnFilter("le", Filter.Equals("0.3")))

    val to = System.currentTimeMillis()
    val from = to - 50000

    val intervalSelector = IntervalSelector(Seq(from), Seq(to))

    val raw1 = RawSeries(rangeSelector = intervalSelector, filters= f1, columns = Seq("value"))
    val windowed1 = PeriodicSeriesWithWindowing(raw1, from, 1000, to, 5000, RangeFunctionId.Rate)
    val summed1 = Aggregate(AggregationOperator.Sum, windowed1, Nil, Seq("job"))

    val f2 = Seq(ColumnFilter("__name__", Filter.Equals("http_request_duration_seconds_count")),
      ColumnFilter("job", Filter.Equals("myService")))
    val raw2 = RawSeries(rangeSelector = intervalSelector, filters= f2, columns = Seq("value"))
    val windowed2 = PeriodicSeriesWithWindowing(raw2, from, 1000, to, 5000, RangeFunctionId.Rate)
    val summed2 = Aggregate(AggregationOperator.Sum, windowed2, Nil, Seq("job"))
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, summed2)
    val execPlan = engine.materialize(logicalPlan, QueryOptions(shardKeySpread = 0))
    roundTrip(execPlan) shouldEqual execPlan
  }

}