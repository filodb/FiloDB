package filodb.standalone

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import com.softwaremill.sttp._
import com.softwaremill.sttp.akkahttp.AkkaHttpBackend
import com.softwaremill.sttp.circe._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.xerial.snappy.Snappy
import remote.RemoteStorage.{LabelMatcher, Query, ReadRequest, ReadResponse}

import filodb.coordinator._
import filodb.coordinator.NodeClusterActor.{DatasetResourceSpec, IngestionSource}
import filodb.coordinator.client.LocalClient
import filodb.core.{DatasetRef, ErrorResponse}
import filodb.core.store.StoreConfig
import filodb.http.PromCirceSupport
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.prometheus.query.PrometheusModel.SuccessResponse
import filodb.query.{QueryError, QueryResult => QueryResult2}

/**
 * A trait used for MultiJVM tests based on starting the standalone FiloServer using timeseries-dev config
 * (ie pretty much the same as deployed setup)
 */
abstract class StandaloneMultiJvmSpec(config: MultiNodeConfig) extends MultiNodeSpec(config)
  with Suite with StrictLogging
  with ScalaFutures with FlatSpecLike with ImplicitSender
  with Matchers with BeforeAndAfterAll {
  override def initialParticipants: Int = roles.size

  import akka.testkit._

  override implicit val patienceConfig = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  lazy val watcher = TestProbe()

  val duration = 10.seconds.dilated
  val longDuration = 60.seconds
  val removedDuration = longDuration * 8

  // Ingestion Source section
  val source = ConfigFactory.parseFile(new java.io.File("conf/timeseries-standalonetest-source.conf"))
  val dataset = DatasetRef(source.as[String]("dataset"))
  val numShards = source.as[Int]("num-shards")
  val resourceSpec = DatasetResourceSpec(numShards, source.as[Int]("min-num-nodes"))
  val sourceconfig = source.getConfig("sourceconfig")
  val storeConf = StoreConfig(sourceconfig.getConfig("store"))
  val ingestionSource = source.as[Option[String]]("sourcefactory").map { factory =>
    IngestionSource(factory, sourceconfig)
  }.get
  val chunkDuration = storeConf.flushInterval
  val numGroupsPerShard = storeConf.groupsPerShard

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()

  override def afterAll(): Unit = multiNodeSpecAfterAll()

  /** Execute within a `runOn`. */
  def awaitNodeUp(server: FiloServer, within: FiniteDuration = duration): Unit = {
    server.start()
    awaitCond(server.cluster.isInitialized, within)
  }

  /** Execute within a `runOn`. */
  def awaitNodeDown(server: FiloServer, within: FiniteDuration = longDuration * 2): Unit = {
    server.shutdown()
    awaitCond(server.cluster.isTerminated, within)
  }

  def waitAllShardsIngestionActive(): Unit = {
    var activeShards = 0
    while (activeShards < numShards) {
      expectMsgPF(duration) {
        case CurrentShardSnapshot(ref, map) =>
          activeShards = map.activeShards(0 until map.numShards).length
      }
    }
  }

  /**
    * @param shards use when some are up and some down, to test different shard status
    */
  def validateShardStatus(client: LocalClient,
                          coordinator: Option[ActorRef] = None,
                          shards: Seq[Int] = Seq.empty)
                         (statusValidator: ShardStatus => Boolean): Unit = {
    client.getShardMapper(dataset) match {
      case Some(map) =>
        info(s"Shard map:  $map")
        info(s"Shard map nodes: ${map.allNodes}")
        if (coordinator.nonEmpty) coordinator forall (c => map.allNodes contains c) shouldEqual true
        map.allNodes.size shouldEqual 2 // only two nodes assigned
        map.shardValues.size shouldBe numShards
        shards match {
          case Seq() =>
           map.shardValues.forall { case (_, status) => statusValidator(status) } shouldEqual true
          case _ =>
            shards forall(shard => statusValidator(map.statusForShard(shard))) shouldEqual true
        }

      case _ =>
        fail(s"Unable to obtain status for dataset $dataset")
    }
  }

  def validateShardAssignments(client: LocalClient,
                               nodeCount: Int,
                               assignments: Seq[Int],
                               coordinator: akka.actor.ActorRef): Unit =
    client.getShardMapper(dataset) match {
      case Some(mapper) =>
        mapper.allNodes.size shouldEqual nodeCount
        mapper.assignedShards shouldEqual Seq(0, 1, 2, 3)
        mapper.unassignedShards shouldEqual Seq.empty
        val shards = mapper.shardsForCoord(coordinator)
        shards shouldEqual assignments
        for {
          shard <- shards
        } info(s"shard($shard) ${mapper.statusForShard(shard)} $coordinator")

      case _ =>

    }

  def setupDataset(client: LocalClient): Unit = {
    client.setupDataset(dataset, resourceSpec, ingestionSource, storeConf).foreach {
      e: ErrorResponse => fail(s"Errors setting up dataset $dataset: $e")
    }
  }

  def topValuesInShards(client: LocalClient, tagKey: String, shards: Seq[Int]): Unit = {
    shards.foreach { shard =>
      val values = client.getIndexValues(dataset, tagKey, shard)
      info(s"Top values for shard=$shard tag=$tagKey is: $values")
    }
  }

  val query = "heap_usage{dc=\"DC0\",app=\"App-2\"}[1m]"
  val query1 = "heap_usage{dc=\"DC0\",app=\"App-2\"}"

  // queryTimestamp is in millis
  def runCliQuery(client: LocalClient, queryTimestamp: Long): Double = {
    val logicalPlan = Parser.queryToLogicalPlan(query, queryTimestamp/1000)

    val curTime = System.currentTimeMillis
    val result = client.logicalPlan2Query(dataset, logicalPlan) match {
      case r: QueryResult2 =>
        val vals = r.result.flatMap(_.rows.map { r => (r.getLong(0) - curTime, r.getDouble(1)) })
        // info(s"result values were $vals")
        vals.length should be > 0
        vals.map(_._2).sum
      case e: QueryError => fail(e.t)
    }
    info(s"CLI Query Result for $query at $queryTimestamp was $result")
    result
  }

  // Get a point for every minute of an interval for multiple time series for comparing missing data
  // TODO: maybe do a sum_over_time() as current windowing just gets last data point
  def runRangeQuery(client: LocalClient, startTime: Long, endTime: Long): Map[String, Array[Double]] = {
    val logicalPlan = Parser.queryRangeToLogicalPlan(query1, TimeStepParams(startTime/1000, 60, endTime/1000))

    var totalSamples = 0
    client.logicalPlan2Query(dataset, logicalPlan) match {
      case r: QueryResult2 =>
        // Transform range query vectors
        val map = r.result.map { rv =>
          val sampleArray = rv.rows.map(_.getDouble(1)).toArray
          totalSamples += sampleArray.size
          rv.key.toString -> sampleArray
        }.toMap
        info(s"Range query result for interval [$startTime, $endTime]: ${map.size} rows, $totalSamples samples")
        map
      case e: QueryError => fail(e.t)
    }
  }

  def compareRangeResults(map1: Map[String, Array[Double]], map2: Map[String, Array[Double]]): Unit = {
    map1.keySet shouldEqual map2.keySet
    map1.foreach { case (key, samples) =>
      samples.toList shouldEqual map2(key).toList
    }
  }

  def printChunkMeta(client: LocalClient): Unit = {
    val chunkMetaQuery = "_filodb_chunkmeta_all(heap_usage{dc=\"DC0\",app=\"App-2\"})"
    val logicalPlan = Parser.queryRangeToLogicalPlan(chunkMetaQuery, TimeStepParams(0, 60, Int.MaxValue))
    client.logicalPlan2Query(dataset, logicalPlan) match {
      case QueryResult2(_, schema, result) => result.foreach(rv => println(rv.prettyPrint()))
      case e: QueryError => fail(e.t)
    }
  }

  def runHttpQuery(queryTimestamp: Long): Double = {

    import io.circe.generic.auto._
    import PromCirceSupport._

    implicit val sttpBackend = AkkaHttpBackend()
    val url = uri"http://localhost:8080/promql/prometheus/api/v1/query?query=$query&time=${queryTimestamp/1000}"
    info(s"Querying: $url")
    val result1 = sttp.get(url).response(asJson[SuccessResponse]).send().futureValue.unsafeBody.right.get.data.result
    val result = result1.flatMap(_.values.map { d => (d.timestamp, d.value) })
    info(s"result values were $result")
    result.length should be > 0
    val sum = result.map(_._2).sum
    info(s"HTTP Query Result for $query at $queryTimestamp was $sum")
    sum
  }

  def runRemoteReadQuery(queryTimestamp: Long): Double = {

    implicit val sttpBackend = AkkaHttpBackend()
    val start = queryTimestamp / 1000 * 1000 - 1.minutes.toMillis // needed to make it equivalent to http/cli queries
    val end = queryTimestamp / 1000 * 1000
    val nameMatcher = LabelMatcher.newBuilder().setName("__name__").setValue("heap_usage")
    val dcMatcher = LabelMatcher.newBuilder().setName("dc").setValue("DC0")
    val jobMatcher = LabelMatcher.newBuilder().setName("app").setValue("App-2")
    val query = Query.newBuilder().addMatchers(nameMatcher)
                                  .addMatchers(dcMatcher)
                                  .addMatchers(jobMatcher)
                                  .setStartTimestampMs(start)
                                  .setEndTimestampMs(queryTimestamp / 1000 * 1000)
    val rr = Snappy.compress(ReadRequest.newBuilder().addQueries(query).build().toByteArray())
    val url = uri"http://localhost:8080/promql/prometheus/api/v1/read"
    info(s"Querying: $url")
    val result1 = sttp.post(url).body(rr).response(asByteArray).send().futureValue.unsafeBody
    val result = ReadResponse.parseFrom(Snappy.uncompress(result1))
    val values = result.getResultsList().asScala
           .flatMap(_.getTimeseriesList.asScala).flatMap(_.getSamplesList().asScala.map(_.getValue()))
    info(s"result values were $values")
    info(s"Remote Read Query Result for $query at $queryTimestamp was ${values.sum}")
    values.sum
  }

}

