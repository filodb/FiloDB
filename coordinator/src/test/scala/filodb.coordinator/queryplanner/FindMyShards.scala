package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.core.{MetricsTestData, SpreadChange}
import filodb.core.metadata.Schemas
import filodb.core.query.{QueryConfig, QueryContext}
import filodb.prometheus.parse.Parser

object FindMyShards extends StrictLogging {

  // scalastyle:off line.size.limit
  def main(args: Array[String]): Unit = {

    val numShard = 256
    val spread = 3
    val queries = Seq(
      """(histogram_quantile(0.9995, sum(rate(http_request_duration_seconds_hist{_ws_="swe-sd", _ns_="saks", env="prod"}[4h])))/1000000)""",
      """histogram_quantile(0.993, sum without(host,success)(rate(images_e2e_latency_seconds{_ws_="swe-sd", _ns_="sd-krane", host=~".*-prod-.*"}[2h])))>= 7200""")

    val engine = initEngine(numShard, spread)
    queries.foreach(query => {
      val lp = Parser.queryToLogicalPlan(query, 100, 1)
      val shardRange = engine.getShardSpanFromLp(lp, QueryContext())
      logger.info(s">>> Shard$shardRange >>> $query")
    })

    System.exit(1)
  }
  // scalastyle:on line.size.limit

  def initEngine(numShard: Int, spread: Int): SingleClusterPlanner = {

    implicit val system: akka.actor.ActorSystem = ActorSystem ()
    val node = TestProbe ().ref

    val mapper = new ShardMapper (numShard)
    for {i <- 0 until numShard} mapper.registerNode (Seq (i), node)

    def mapperRef = mapper

    val dataset = MetricsTestData.timeseriesDatasetMultipleShardKeys
    val dsRef = dataset.ref
    val schemas = Schemas (dataset.schema)

    val config = ConfigFactory.load ("application_test.conf").resolve()
    val queryConfig = QueryConfig (config.getConfig ("filodb.query") )

    new SingleClusterPlanner(dataset, schemas, mapperRef, earliestRetainedTimestampFn = 0,
      queryConfig, "raw", StaticSpreadProvider(SpreadChange(0, spread)))

  }

}
