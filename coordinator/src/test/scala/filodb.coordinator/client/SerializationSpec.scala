package filodb.coordinator.client

import scala.collection.mutable.Set

import akka.actor.ActorRef
import akka.serialization.SerializationExtension
import akka.testkit.TestProbe
import org.scalatest.concurrent.ScalaFutures

import filodb.coordinator.{ActorSpecConfig, ActorTest, NodeClusterActor, ShardMapper}
import filodb.coordinator.queryengine2.QueryEngine
import filodb.core.{MachineMetricsData, MetricsTestData, NamesTestData, TestData}
import filodb.core.metadata.Column.ColumnType
import filodb.core.store._
import filodb.memory.format.{RowReader, SeqRowReader, ZCUTF8IteratorRowReader, ZeroCopyUTF8String => UTF8Str}
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
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
  import NamesTestData._
  import NodeClusterActor._
  import QueryCommands._

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
                       |    shard-mem-size = 256MB
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
                    |    shard-mem-size = 256MB
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
                    |    shard-mem-size = 256MB
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
                    |    shard-mem-size = 256MB
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

  import filodb.core.query._

  it("should be able to create, serialize and read from QueryResult") {
    import MachineMetricsData._

    val now = System.currentTimeMillis()
    val numRawSamples = 1000
    val limit = 900
    val reportingInterval = 10000
    val tuples = (numRawSamples until 0).by(-1).map(n => (now - n * reportingInterval, n.toDouble))

    // scalastyle:off null
    val rvKey = new PartitionRangeVectorKey(null, defaultPartKey, dataset1.partKeySchema,
                                            Seq(ColumnInfo("string", ColumnType.StringColumn)), 1, 5)

    val rowbuf = tuples.map { t =>
      new SeqRowReader(Seq[Any](t._1, t._2))
    }.toBuffer


    val cols = Array(new ColumnInfo("timestamp", ColumnType.LongColumn),
      new ColumnInfo("value", ColumnType.DoubleColumn))
    val srvs = for { i <- 0 to 9 } yield {
      val rv = new RangeVector {
        override val rows: Iterator[RowReader] = rowbuf.iterator
        override val key: RangeVectorKey = rvKey
      }
      val srv = SerializableRangeVector(rv, cols, limit)
      val observedTs = srv.rows.toSeq.map(_.getLong(0))
      val observedVal = srv.rows.toSeq.map(_.getDouble(1))
      observedTs shouldEqual tuples.map(_._1).take(limit)
      observedVal shouldEqual tuples.map(_._2).take(limit)
      srv
    }

    val schema = ResultSchema(dataset1.infosFromIDs(0 to 0), 1)

    val result = QueryResult2("someId", schema, srvs)
    val roundTripResult = roundTrip(result).asInstanceOf[QueryResult2]

    result.resultSchema shouldEqual roundTripResult.resultSchema
    result.id shouldEqual roundTripResult.id
    for { i <- 0 until roundTripResult.result.size } {
      // BinaryVector deserializes to different impl, so cannot compare top level object
      roundTripResult.result(i).schema shouldEqual result.result(i).schema
      roundTripResult.result(i).rows.map(_.getDouble(1)).toSeq shouldEqual
        result.result(i).rows.map(_.getDouble(1)).toSeq
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
    val logicalPlan = BinaryJoin(summed1, BinaryOperator.DIV, Cardinality.OneToOne, summed2)
    val execPlan = engine.materialize(logicalPlan, QueryOptions(0, 100))
    roundTrip(execPlan) shouldEqual execPlan
  }

  it ("should serialize and deserialize ExecPlan2 involving complicated queries") {
    val node0 = TestProbe().ref
    val mapper = new ShardMapper(1)
    def mapperRef: ShardMapper = mapper
    mapper.registerNode(Seq(0), node0)
    val to = System.currentTimeMillis() / 1000
    val from = to - 50
    val qParams = TimeStepParams(from, 10, to)
    val dataset = MetricsTestData.timeseriesDataset
    val engine = new QueryEngine(dataset, mapperRef)

    val logicalPlan1 = Parser.queryRangeToLogicalPlan(
      "sum(rate(http_request_duration_seconds_bucket{job=\"prometheus\"}[20s])) by (handler)",
      qParams)
    val execPlan1 = engine.materialize(logicalPlan1, QueryOptions(0, 100))
    roundTrip(execPlan1) shouldEqual execPlan1

    // scalastyle:off
    val logicalPlan2 = Parser.queryRangeToLogicalPlan(
      "sum(rate(http_request_duration_microseconds_sum{job=\"prometheus\"}[5m])) by (handler) / sum(rate(http_request_duration_microseconds_count{job=\"prometheus\"}[5m])) by (handler)",
      qParams)
    // scalastyle:on
    val execPlan2 = engine.materialize(logicalPlan2, QueryOptions(0, 100))
    roundTrip(execPlan2) shouldEqual execPlan2

  }

  it ("should serialize and deserialize QueryError") {
    val err = QueryError("xdf", new IllegalStateException("Some message"))
    val deser1 = roundTrip(err).asInstanceOf[QueryError]
    val deser2 = roundTrip(deser1).asInstanceOf[QueryError]
    deser2.id shouldEqual err.id
    deser2.t.getMessage shouldEqual err.t.getMessage
    deser2.t.getCause shouldEqual err.t.getCause
  }

  it ("should serialize and deserialize result involving CustomRangeVectorKey") {

    val keysMap = Map(UTF8Str("key1") -> UTF8Str("val1"),
                      UTF8Str("key2") -> UTF8Str("val2"))
    val key = CustomRangeVectorKey(keysMap)
    val cols = Seq(ColumnInfo("value", ColumnType.DoubleColumn))
    val ser = SerializableRangeVector(IteratorBackedRangeVector(key, Iterator.empty), cols, 10)

    val schema = ResultSchema(MachineMetricsData.dataset1.infosFromIDs(0 to 0), 1)

    val result = QueryResult2("someId", schema, Seq(ser))
    val roundTripResult = roundTrip(result).asInstanceOf[QueryResult2]

    roundTripResult.result.head.key.labelValues shouldEqual keysMap

  }

  it ("should serialize and deserialize result involving Metadata") {

    val expected = List(UTF8Str("App-0"), UTF8Str("App-1"))
    val schema = new ResultSchema(Seq(new ColumnInfo("app", ColumnType.StringColumn)), 1)
    val cols = Seq(ColumnInfo("value", ColumnType.StringColumn))
    val ser = Seq(SerializableRangeVector(IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
      new ZCUTF8IteratorRowReader(expected.toIterator)), cols, 10))

    val result = QueryResult2("someId", schema, ser)
    val roundTripResult = roundTrip(result).asInstanceOf[QueryResult2]
    roundTripResult.result.size shouldEqual 1

    val srv = roundTripResult.result(0)
    srv.rows.size shouldEqual 2
    val actual = srv.rows.map(record => {
      record.filoUTF8String(0)
    })
    actual.toList shouldEqual expected
  }

}
