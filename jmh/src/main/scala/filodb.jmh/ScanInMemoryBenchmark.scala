package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import akka.actor.ActorSystem
import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.reactive.Observable
import org.openjdk.jmh.annotations._

import filodb.core.binaryrecord2.RecordContainer
import filodb.core.memstore.{SomeData, TimeSeriesMemStore}
import filodb.core.store.StoreConfig
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.{QueryError => QError, QueryResult => QueryResult2}
import filodb.timeseries.TestTimeseriesProducer

//scalastyle:off regex
/**
 * A macrobenchmark (IT-test level) for QueryEngine2 aggregations, in-memory only (no on-demand paging)
 * No ingestion occurs while query test is running -- this is intentional to allow for query-focused CPU
 * and memory allocation analysis.
 * Ingests a fixed amount of tsgenerator data in Prometheus schema (time, value, tags) and runs various queries.
 */
@State(Scope.Thread)
class ScanInMemoryBenchmark extends StrictLogging {
  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.WARN)

  import filodb.coordinator._
  import client.Client.{actorAsk, asyncAsk}
  import client.QueryCommands._
  import NodeClusterActor._

  val numShards = 1
  val numSeries = 500
  val samplesDuration = 4.hours
  val numSamples = numSeries * (samplesDuration / 10.seconds).toInt
  val ingestionStartTime = System.currentTimeMillis - samplesDuration.toMillis
  val spread = 0

  // TODO: move setup and ingestion to another trait
  val system = ActorSystem("test", ConfigFactory.load("filodb-defaults.conf"))
  private val cluster = FilodbCluster(system)
  cluster.join()

  private val coordinator = cluster.coordinatorActor
  private val clusterActor = cluster.clusterSingleton(ClusterRole.Server, None)

  // Set up Prometheus dataset in cluster, initialize in metastore
  private val dataset = TestTimeseriesProducer.dataset
  private val shardMapper = new ShardMapper(numShards)
  (0 until numShards).foreach { s =>
    shardMapper.updateFromEvent(IngestionStarted(dataset.ref, s, coordinator))
  }

  Await.result(cluster.metaStore.initialize(), 3.seconds)
  Await.result(cluster.metaStore.newDataset(dataset), 5.seconds)

  val storeConf = StoreConfig(ConfigFactory.parseString("""
                  | flush-interval = 1h
                  | shard-mem-size = 512MB
                  | ingestion-buffer-mem-size = 50MB
                  | groups-per-shard = 4
                  | demand-paging-enabled = false
                  """.stripMargin))
  val command = SetupDataset(dataset.ref, DatasetResourceSpec(numShards, 1), noOpSource, storeConf)
  actorAsk(clusterActor, command) { case DatasetVerified => println(s"dataset setup") }

  import monix.execution.Scheduler.Implicits.global

  // Manually pump in data ourselves so we know when it's done.
  Thread sleep 2000    // Give setup command some time to set up dataset shards etc.
  val (producingFut, containerStream) = TestTimeseriesProducer.metricsToContainerStream(ingestionStartTime,
                                            numShards, numSeries, numSamples * numSeries, dataset, shardMapper, spread)
  val ingestTask = containerStream.groupBy(_._1)
                    // Asynchronously subcribe and ingest each shard
                    .mapAsync(numShards) { groupedStream =>
                      val shard = groupedStream.key
                      println(s"Starting ingest on shard $shard...")
                      val shardStream = groupedStream.zipWithIndex.flatMap { case ((_, bytes), idx) =>
                        // println(s"   XXX: -->  Got ${bytes.map(_.size).sum} bytes in shard $shard")
                        val data = bytes.map { array => SomeData(RecordContainer(array), idx) }
                        Observable.fromIterable(data)
                      }
                      Task.fromFuture(cluster.memStore.ingestStream(dataset.ref, shard, shardStream, global))
                    }.countL.runAsync
  Await.result(producingFut, 30.seconds)
  Thread sleep 2000
  cluster.memStore.asInstanceOf[TimeSeriesMemStore].commitIndexForTesting(dataset.ref) // commit lucene index
  println(s"Ingestion ended")

  // Stuff for directly executing queries ourselves
  import filodb.coordinator.queryengine2.QueryEngine
  val engine = new QueryEngine(dataset, shardMapper)

  val numQueries = 500       // Please make sure this number matches the OperationsPerInvocation below
  val queryIntervalSec = samplesDuration.toSeconds  // # minutes between start and stop
  val queryStep = 150        // # of seconds between each query sample "step"
  val sumRateQuery = """sum(rate(heap_usage{_ns="App-2"}[5m]))"""
  val queryStartTime = ingestionStartTime + 5.minutes.toMillis  // 5 minutes from start until 60 minutes from start
  val qParams = TimeStepParams(queryStartTime/1000, queryStep, queryStartTime/1000 + queryIntervalSec)
  val execPlan = engine.materialize(Parser.queryRangeToLogicalPlan(sumRateQuery, qParams), QueryOptions(1, 20000))
  val scanExecPlan = execPlan.children(0)

  @TearDown
  def shutdownFiloActors(): Unit = {
    cluster.shutdown()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(500)
  def scanSubQuery(): Unit = {
    val futures = (0 until numQueries).map { n =>
      val f = asyncAsk(coordinator, scanExecPlan)
      f.onSuccess {
        case q: QueryResult2 =>
          if (q.result.head.numRows == 0) throw new RuntimeException("Empty query result")
        case e: QError => throw new RuntimeException(s"Query error $e")
      }
      f
    }
    Await.result(Future.sequence(futures), 60.seconds)
  }
}