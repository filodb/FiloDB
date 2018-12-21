package filodb.jmh

import java.util.concurrent.TimeUnit

import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.config.ConfigFactory
import org.openjdk.jmh.annotations.{Level => JMHLevel, _}

import filodb.core.{MachineMetricsData, TestData}
import filodb.core.binaryrecord2.{RecordBuilder, RecordComparator, RecordSchema}
import filodb.core.memstore._
import filodb.core.store._
import filodb.memory.{BinaryRegionConsumer, MemFactory}

/**
 * Microbenchmark measuring tasks affecting ingestion performance
 * - Deserialization
 * - Comparison of IngestRecord/SchemaRowReader to BinaryRecord/PartitionKeys
 * - maybe hashcode comparison of above
 * - copying of ingestion record to a PartitionKey (esp important during recovery)
 *
 * For realism we use the MachineMetricsData time series with a map partition key with 5 columns
 *
 * RESULTS: for compare, BRv2 was about twice as fast.  For making a single part key, BRv2 was 15-30x faster.
 */
@State(Scope.Thread)
class IngestionBenchmark {
  import MachineMetricsData._

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)

  // # of records in a container to test ingestion speed
  val dataStream = withMap(linearMultiSeries(), extraTags=extraTags)

  val schemaWithPredefKeys = RecordSchema.ingestion(dataset2,
                                                    Seq("job", "instance"))
  // sized just big enough for a 1000 entries per container
  val ingestBuilder = new RecordBuilder(MemFactory.onHeapFactory, schemaWithPredefKeys, 176064)
  val comparator = new RecordComparator(schemaWithPredefKeys)
  //scalastyle:off
  println("Be patient, generating lots of containers of raw data....")
  dataStream.take(1000*100).grouped(1000).foreach { data => addToBuilder(ingestBuilder, data) }

  val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory, comparator.partitionKeySchema)

  val consumer = new BinaryRegionConsumer {
    def onNext(base: Any, offset: Long): Unit = comparator.buildPartKeyFromIngest(base, offset, partKeyBuilder)
  }
  ingestBuilder.allContainers.head.consumeRecords(consumer)

  val (ingBr2Base, ingBr2Off) = (ingestBuilder.allContainers.head.base, ingestBuilder.allContainers.head.offset + 4)
  val (partBr2Base, partBr2Off) = (partKeyBuilder.allContainers.head.base, partKeyBuilder.allContainers.head.offset + 4)

  val containers = ingestBuilder.allContainers.toArray

  import monix.execution.Scheduler.Implicits.global

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val policy = new FixedMaxPartitionsEvictionPolicy(100)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))
  val ingestConf = TestData.storeConf.copy(shardMemSize = 512 * 1024 * 1024, maxChunksSize = 200)
  memStore.setup(dataset1, 0, ingestConf)

  val shard = memStore.getShardE(dataset1.ref, 0)

  /**
   * Equality of incoming ingest BinaryRecord2 partition fields vs a BinaryRecord2 partition key
   * Application: matching of incoming samples to the right TimeSeriesPartition
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def compareBRv2IngestPartition(): Boolean = {
    comparator.partitionMatch(ingBr2Base, ingBr2Off, partBr2Base, partBr2Off)
  }

  var containerNo = 0

  // Setup per iteration to clean shard state and make sure ingestion is repeatable.
  // NOTE: need to use per-iteration, not invocation, or else the setup costs affect the benchmark results
  @Setup(JMHLevel.Iteration)
  def cleanIngest(): Unit = {
    shard.reset()
    containerNo = 0
  }


  /**
   * Ingest 1000 records, or 100 per TSPartition, every invocation.  Note that the throughput reported is x100 so
   * it is not the containers throughput but the actual records throughput.
   * 100 per partition is just enough that each invocation should result in releasing/obtaining
   * fresh write buffers, and every other invocation results in encoding.
   * resetLastTimes() is called to enable the same records to be appended and "bypass" the out of order mechanism.
   * Note that adding partitions is only done at the start of each iteration, not invocation, since the setup
   * to clean the shard state is only done at the beginning of each iteration.
   *
   * Note2: one chunk every 200 samples.
   *
   * Time reported is time to ingest 100k samples.  To get throughput, divide 100k by the time in seconds
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(1000)
  @Warmup(batchSize=50)
  @Measurement(batchSize=100)
  def ingestEncodeRecords(): Unit = {
    shard.ingest(containers(containerNo), 0)
    containerNo += 1
  }

  // This benchmark doesn't really measure anything significant
  // @Benchmark
  // @BenchmarkMode(Array(Mode.Throughput))
  // @OutputTimeUnit(TimeUnit.SECONDS)
  // def nativeMemAlloc(): Unit = {
  //   val ptr = TestData.nativeMem.allocateOffheap(16)
  //   TestData.nativeMem.freeMemory(ptr)
  // }
}