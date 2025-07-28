package filodb.coordinator

import com.typesafe.config.ConfigFactory
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.coordinator.queryplanner.SingleClusterPlanner
import filodb.core.{MachineMetricsData, SpreadChange, TestData}
import filodb.core.memstore.TimeSeriesMemStore
import filodb.core.query.{PlannerParams, PromQlQueryParams, QueryConfig, QueryContext}
import filodb.core.store.{InMemoryMetaStore, NullColumnStore}
import filodb.memory.format.ZeroCopyUTF8String
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.prometheus.parse.Parser.Antlr
import filodb.query.QueryResult
import kamon.Kamon
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class TimeSeriesMemstoreQuerySpec extends AnyFunSpec with Matchers with BeforeAndAfter with ScalaFutures{

  implicit val s = monix.execution.Scheduler.Implicits.global

  import MachineMetricsData._
  import ZeroCopyUTF8String._
  import scala.concurrent.duration._

  val config = ConfigFactory.parseString("""
                                           |filodb.memstore.max-partitions-on-heap-per-shard = 1100
                                           |filodb.memstore.ensure-block-memory-headroom-percent = 10
                                           |filodb.memstore.ensure-tsp-count-headroom-percent = 10
                                           |filodb.memstore.ensure-native-memory-headroom-percent = 10
                                           |filodb.memstore.index-updates-publishing-enabled = true
                                           |  """.stripMargin)
    .withFallback(ConfigFactory.load("application_test.conf"))

  it("should ingest ooo data and respond correctly to PromQL aggregation queries with overlapping chunks") {
    val config2 = ConfigFactory.parseString("""
                                              |memstore.ooo-data-points-enabled = true
                                              |""".stripMargin)
      .withFallback(config.getConfig("filodb"))

    val memStore1 = new TimeSeriesMemStore(config2, new NullColumnStore, new InMemoryMetaStore())
    memStore1.setup(datasetOoo2_1.ref, schemasOoo2_1, 0, TestData.storeConf, 1)
    val series = withMap(linearOooMultiSeries(numSeries = 1).take(100), 1, Map("_ws_".utf8 -> "myWs".utf8, "_ns_".utf8 -> "myNs".utf8))
    val data = records(datasetOoo2_1, series)
    memStore1.ingest(datasetOoo2_1.ref, 0, data)

    memStore1.refreshIndexForTesting(datasetOoo2_1.ref)

    val shard = memStore1.getShard(datasetOoo2_1.ref, 0).get
    val parts = shard.partitions
    parts.size shouldEqual 3
    parts.get(0).stringPartition shouldEqual "b2[schema=schemaID:61840  _o_=0,_metric_=Series 0,tags={_ns_: myNs, _ws_: myWs, n: 0}]"
    parts.get(1).stringPartition shouldEqual "b2[schema=schemaID:61840  _o_=1,_metric_=Series 0,tags={_ns_: myNs, _ws_: myWs, n: 0}]"
    parts.get(2).stringPartition shouldEqual "b2[schema=schemaID:61840  _o_=2,_metric_=Series 0,tags={_ns_: myNs, _ws_: myWs, n: 0}]"

    val startSeconds = 0
    val endSeconds = 300L
    val step = 20
    val queryParams = PromQlQueryParams("notUsedQuery", startSeconds, step, endSeconds)
    val mapper = new ShardMapper(1)
    val system = ActorSystemHolder.createActorSystem("testActorSystem", config)
    val qActor = system.actorOf(QueryActor.props(memStore = memStore1, dataset = datasetOoo2_1,
      schemas = schemasOoo2_1, shardMapFunc = mapper, earliestRawTimestampFn = 0))
    mapper.registerNode(Seq(0), qActor)
    val queryConfig = QueryConfig(config.getConfig("filodb.query")).copy(plannerSelector = Some("plannerSelector"))
    val planner = new SingleClusterPlanner(datasetOoo2_1, schemasOoo2_1, mapper,
      earliestRetainedTimestampFn = 0, queryConfig, "raw",
      spreadProvider = StaticSpreadProvider(SpreadChange(0, 0))
    )
    val lp = Parser.queryRangeToLogicalPlan(
      """sum(sum_over_time({__name__="Series 0", _ws_="myWs",_ns_="myNs"}[1i]))""",
      TimeStepParams(startSeconds, step, endSeconds), Antlr)
    val execPlan = planner.materialize(lp, QueryContext(
      origQueryParams = queryParams,
      plannerParams = PlannerParams(maxShardKeyRegexFanoutBatchSize = 1)))  // set the batch size == 1

    implicit val queryAskTimeout = FiniteDuration(10, SECONDS)
    val response1 = planner.dispatchExecPlan(execPlan, Kamon.currentSpan()).runToFuture.futureValue
    val qr1 = response1.asInstanceOf[QueryResult]
    qr1.result.size shouldEqual 1
    println(qr1.result(0).prettyPrint())
    qr1.result(0).rows().map(_.getDouble(1)).filter(!_.isNaN).sum shouldEqual 14075.0


    // Switch buffers, encode and release/return buffers for all partitions
    val blockFactory = shard.blockFactoryPool.checkoutForOverflow(0)
    for { n <- 0 until parts.size() } {
      val part = shard.partitions.get(n)
      part.switchBuffers(blockFactory, encode = true)
    }

    // ingest same data again - it should be out of order, and should result in overlapping chunks
    val series2 = withMap(linearOooMultiSeries(numSeries = 1).take(200), 1, Map("_ws_".utf8 -> "myWs".utf8, "_ns_".utf8 -> "myNs".utf8))
    val data2 = records(datasetOoo2_1, series2)
    memStore1.ingest(datasetOoo2_1.ref, 0, data2)

    memStore1.refreshIndexForTesting(datasetOoo2_1.ref)
    parts.size shouldEqual 6

    val response2 = planner.dispatchExecPlan(execPlan, Kamon.currentSpan()).runToFuture.futureValue
    val qr2 = response2.asInstanceOf[QueryResult]
    qr2.result.size shouldEqual 1
    println(qr2.result(0).prettyPrint())
    qr2.result(0).rows().map(_.getDouble(1)).filter(!_.isNaN).sum shouldEqual 28150.0

  }

}
