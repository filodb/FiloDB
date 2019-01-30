package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.ActorSystem
import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler
import monix.reactive.Observable
import org.openjdk.jmh.annotations._

import filodb.coordinator.client.QueryCommands._
import filodb.coordinator.queryengine2.QueryEngine
import filodb.coordinator.{FilodbCluster, IngestionStarted, ShardMapper}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore._
import filodb.core.store._
import filodb.core.{MachineMetricsData, MetricsTestData, TestData}
import filodb.memory.format.SeqRowReader
import filodb.memory.MemFactory
import filodb.prometheus.parse.Parser
import filodb.query.QueryConfig

//scalastyle:off regex
/**
 * Benchmark measuring query performance of various HistogramColumn schemes
 * (plus versus traditional Prom schema).
 * All samples include tags with five tags.
 * Write buffer queries only - since most queries will be to write buffers only.
 */
@State(Scope.Thread)
class HistogramQueryBenchmark {
  import MachineMetricsData._

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.WARN)

  import monix.execution.Scheduler.Implicits.global

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val policy = new FixedMaxPartitionsEvictionPolicy(1000)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))
  val ingestConf = TestData.storeConf.copy(shardMemSize = 512 * 1024 * 1024, maxChunksSize = 200)

  // HistogramColumn data: 10 series, 180 samples per series = 1800 total
  println("Ingesting containers of histogram schema data....")
  val histSchemaData = linearHistSeries(numBuckets = 64).map(SeqRowReader)
  val histSchemaBuilder = new RecordBuilder(MemFactory.onHeapFactory, histDataset.ingestionSchema, 230000)
  histSchemaData.take(10 * 180).foreach(histSchemaBuilder.addFromReader)

  memStore.setup(histDataset, 0, ingestConf)
  val hShard = memStore.getShardE(histDataset.ref, 0)
  histSchemaBuilder.allContainers.foreach { c => hShard.ingest(c, 0) }
  memStore.commitIndexForTesting(histDataset.ref) // commit lucene index

  // Prometheus hist data: 10 series * 66 = 660 series * 180 samples
  println("Ingesting containers of prometheus schema data....")
  val promDataset = MetricsTestData.timeseriesDataset
  val promData = MetricsTestData.promHistSeries(numBuckets = 64).map(SeqRowReader)
  val promBuilder = new RecordBuilder(MemFactory.onHeapFactory, promDataset.ingestionSchema, 4200000)
  promData.take(10*66*180).foreach(promBuilder.addFromReader)

  memStore.setup(promDataset, 0, ingestConf)
  val pShard = memStore.getShardE(promDataset.ref, 0)
  promBuilder.allContainers.foreach { c => pShard.ingest(c, 0) }
  memStore.commitIndexForTesting(promDataset.ref) // commit lucene index

  val system = ActorSystem("test", ConfigFactory.load("filodb-defaults.conf"))
  private val cluster = FilodbCluster(system)
  cluster.join()

  private val coordinator = cluster.coordinatorActor
  private val shardMapper = new ShardMapper(1)
  shardMapper.updateFromEvent(IngestionStarted(histDataset.ref, 0, coordinator))

  // Query configuration
  val hEngine = new QueryEngine(histDataset, shardMapper)
  val pEngine = new QueryEngine(promDataset, shardMapper)
  val startTime = 100000L + 100*1000  // 100 samples in.  Look back 30 samples, which normally would be 5min

  val histQuery = """histogram_quantile(0.9, sum_over_time(http_requests_total{job="prometheus",__col__="h"}[30s]))"""
  val promQuery = """histogram_quantile(0.9, sum_over_time(http_requests_total_bucket{job="prometheus"}[30s]))"""

  // Single-threaded query test
  val numQueries = 500
  val qOptions = QueryOptions(1, 100).copy(shardOverrides = Some(Seq(0)))
  val hLogicalPlan = Parser.queryToLogicalPlan(histQuery, startTime/1000)
  val hExecPlan = hEngine.materialize(hLogicalPlan, qOptions)
  val querySched = Scheduler.singleThread(s"benchmark-query")
  val queryConfig = new QueryConfig(config.getConfig("query"))

  val pLogicalPlan = Parser.queryToLogicalPlan(promQuery, startTime/1000)
  val pExecPlan = pEngine.materialize(pLogicalPlan, qOptions)

  @TearDown
  def shutdownFiloActors(): Unit = {
    cluster.shutdown()
  }

  // These are boh single threaded queries
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(500)
  def histSchemaQuantileQuery(): Long = {
    val f = Observable.fromIterable(0 until numQueries).mapAsync(1) { n =>
      hExecPlan.execute(memStore, histDataset, queryConfig)(querySched, 60.seconds)
    }.executeOn(querySched)
     .countL.runAsync
    Await.result(f, 60.seconds)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(500)
  def promSchemaQuantileQuery(): Long = {
    val f = Observable.fromIterable(0 until numQueries).mapAsync(1) { n =>
      pExecPlan.execute(memStore, promDataset, queryConfig)(querySched, 60.seconds)
    }.executeOn(querySched)
     .countL.runAsync
    Await.result(f, 60.seconds)
  }
}