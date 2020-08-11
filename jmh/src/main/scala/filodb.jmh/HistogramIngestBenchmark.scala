package filodb.jmh

import java.util.concurrent.TimeUnit

import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.config.ConfigFactory
import org.agrona.{ExpandableArrayBuffer, ExpandableDirectByteBuffer}
import org.agrona.concurrent.UnsafeBuffer
import org.openjdk.jmh.annotations.{Level => JMHLevel, _}
import spire.syntax.cfor._

import filodb.core.{MachineMetricsData, MetricsTestData, TestData}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore._
import filodb.core.metadata.Schemas
import filodb.core.store._
import filodb.memory.MemFactory
import filodb.memory.format.{NibblePack, SeqRowReader}

//scalastyle:off regex
/**
 * Benchmark measuring ingestion/encoding performance of various HistogramColumn schemes
 * (plus versus traditional Prom schema).
 * All benchmarks measure histograms per second ingested.
 * Encoding/compression is included - multiple chunks are built and compressed.
 * All samples include tags with five tags.
 */
@State(Scope.Thread)
class HistogramIngestBenchmark {
  import MachineMetricsData._

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.WARN)

  // HistogramColumn schema, test data, builder
  println("Be patient, generating lots of containers of histogram schema data....")
  val histSchemaData = linearHistSeries(numBuckets = 64).map(SeqRowReader)
  // sized just big enough for ~300 entries per container 700 * 300
  val histSchemaBuilder = new RecordBuilder(MemFactory.onHeapFactory, 230000)
  histSchemaData.take(300*100).grouped(300).foreach { rows =>
    rows.foreach(histSchemaBuilder.addFromReader(_, histDataset.schema))
    println(s"We have ${histSchemaBuilder.allContainers.length} containers, " +
            s"remaining = ${histSchemaBuilder.containerRemaining}")
    histSchemaBuilder.newContainer()   // Force switching to new container
  }
  val histContainers = histSchemaBuilder.allContainers.toArray

  // Prometheus schema, test data, builder
  println("Be patient, generating lots of containers of prometheus schema data....")
  val promDataset = MetricsTestData.timeseriesDataset
  val promData = MetricsTestData.promHistSeries(numBuckets = 64).map(SeqRowReader)
  val promBuilder = new RecordBuilder(MemFactory.onHeapFactory, 4200000)
  promData.take(300*66*100).grouped(300*66).foreach { rows =>
    rows.foreach(promBuilder.addFromReader(_, promDataset.schema))
    println(s"We have ${promBuilder.allContainers.length} containers, " +
            s"remaining = ${promBuilder.containerRemaining}")
    promBuilder.newContainer()   // Force switching to new container
  }
  val promContainers = promBuilder.allContainers.toArray

  println(s"DONE generating.\nHistSchema container size=${histContainers.head.numBytes} " +
          s"# records=${histContainers.head.countRecords}")
  println(s"Prom schema container size=${promContainers.head.numBytes} " +
          s"# records=${promContainers.head.countRecords}")

  import monix.execution.Scheduler.Implicits.global

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val policy = new FixedMaxPartitionsEvictionPolicy(1000)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))
  val ingestConf = TestData.storeConf.copy(shardMemSize = 512 * 1024 * 1024, maxChunksSize = 100)
  memStore.setup(histDataset.ref, Schemas(histDataset.schema), 0, ingestConf)
  memStore.setup(promDataset.ref, Schemas(promDataset.schema), 0, ingestConf)

  val hShard = memStore.getShardE(histDataset.ref, 0)
  val pShard = memStore.getShardE(promDataset.ref, 0)

  var containerNo = 0

  // Setup per iteration to clean shard state and make sure ingestion is repeatable.
  // NOTE: need to use per-iteration, not invocation, or else the setup costs affect the benchmark results
  @Setup(JMHLevel.Iteration)
  def cleanIngest(): Unit = {
    println(s"hShard #partitions=${hShard.numActivePartitions}  pShard #partitions=${pShard.numActivePartitions}")
    hShard.reset()
    pShard.reset()
    containerNo = 0
  }

  /**
   * Ingest 300 histograms every invocation; 30 per partition.  Every roughly 3 invocations there is encoding.
   * Each iteration = 100 invocations, thus there is plenty of encoding cycles.
   * Note that adding partitions is only done at the start of each iteration, not invocation, since the setup
   * to clean the shard state is only done at the beginning of each iteration.
   *
   * Time reported is time to ingest 30k histograms.  To get throughput, divide 30k by the time in seconds
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(batchSize = 50)
  @Measurement(batchSize = 100)
  def ingestHistColumn1(): Unit = {
    hShard.ingest(histContainers(containerNo), 0)
    containerNo += 1
  }

  /**
   * Ingest 300 histograms every invocation; 30 per partition.  Every roughly 3 invocations there is encoding.
   * Each iteration = 100 invocations, thus there is plenty of encoding cycles.
   * Note that adding partitions is only done at the start of each iteration, not invocation, since the setup
   * to clean the shard state is only done at the beginning of each iteration.
   *
   * Time reported is time to ingest 30k histograms.  To get throughput, divide 30k by the time in seconds
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(batchSize = 50)
  @Measurement(batchSize = 100)
  def ingestPromHistograms(): Unit = {
    pShard.ingest(promContainers(containerNo), 0)
    containerNo += 1
  }

  def nonzeroLongInputs(numNonZeroes: Int): Array[Long] = {
    val longs = new Array[Long](64)
    (1 to numNonZeroes).foreach { i =>
      longs(i) = (Math.sin(i * Math.PI / numNonZeroes) * 1000.0).toLong
    }
    longs
  }

  def increasingNonzeroes(numNonZeroes: Int): Array[Long] = {
    val longs = nonzeroLongInputs(numNonZeroes)
    for { i <- 1 until 64 } {
      longs(i) = longs(i - 1) + longs(i)
    }
    longs
  }

  val inputs = increasingNonzeroes(16)
  val buf = new ExpandableArrayBuffer()

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def nibblePackDelta64(): Int = {
    NibblePack.packDelta(inputs, buf, 0)
  }

  val bytesWritten = NibblePack.packDelta(inputs, buf, 0)
  val sink = NibblePack.DeltaSink(new Array[Long](inputs.size))
  val bufSlice = new UnsafeBuffer(buf, 0, bytesWritten)

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def nibbleUnpackDelta64(): Unit = {
    bufSlice.wrap(buf, 0, bytesWritten)
    sink.reset()
    val res = NibblePack.unpackToSink(bufSlice, sink, inputs.size)
    require(res == NibblePack.Ok)
  }

  // Add additional inputs for 2D Delta benchmark
  val numInputs = 100
  val increasingBuf = new ExpandableDirectByteBuffer()
  var lastPos = 0
  val increasingHistPos = (0 until numInputs).map { i =>
    val longs = inputs.zipWithIndex.map { case (a, j) => a + i + j }
    lastPos = NibblePack.packDelta(longs, increasingBuf, lastPos)
    lastPos
  }.toArray

  // Now, use DeltaDiffPackSink to recompress to deltas from initial inputs
  val outBuf = new ExpandableDirectByteBuffer()
  val ddsink = NibblePack.DeltaDiffPackSink(new Array[Long](inputs.size), outBuf)
  val ddSlice = new UnsafeBuffer(buf, 0, bytesWritten)

  // Simulates DeltaDiffPackSink for many inputs... decompressing and recompressing delta of deltas
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(100)
  def nibble2DDeltaRepack(): Unit = {
    ddsink.writePos = 0
    java.util.Arrays.fill(ddsink.lastHistDeltas, 0)
    var lastPos = 0
    cforRange { 0 until numInputs } { i =>
      ddSlice.wrap(increasingBuf, lastPos, increasingHistPos(i) - lastPos)
      val res = NibblePack.unpackToSink(ddSlice, ddsink, inputs.size)
      require(res == NibblePack.Ok)
      lastPos = increasingHistPos(i)
      ddsink.reset()
    }
  }
}
