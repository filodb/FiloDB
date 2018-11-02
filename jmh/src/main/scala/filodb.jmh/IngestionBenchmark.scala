package filodb.jmh

import java.util.concurrent.TimeUnit

import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.config.ConfigFactory
import org.openjdk.jmh.annotations._

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
  val dataStream = withMap(linearMultiSeries(), extraTags=extraTags) take 100

  val schemaWithPredefKeys = RecordSchema.ingestion(dataset2,
                                                    Seq("job", "instance"))
  val ingestBuilder = new RecordBuilder(MemFactory.onHeapFactory, schemaWithPredefKeys)
  val comparator = new RecordComparator(schemaWithPredefKeys)
  addToBuilder(ingestBuilder, dataStream)
  val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory, comparator.partitionKeySchema)

  val consumer = new BinaryRegionConsumer {
    def onNext(base: Any, offset: Long): Unit = comparator.buildPartKeyFromIngest(base, offset, partKeyBuilder)
  }
  ingestBuilder.allContainers.head.consumeRecords(consumer)

  val (ingBr2Base, ingBr2Off) = (ingestBuilder.allContainers.head.base, ingestBuilder.allContainers.head.offset + 4)
  val (partBr2Base, partBr2Off) = (partKeyBuilder.allContainers.head.base, partKeyBuilder.allContainers.head.offset + 4)

  import monix.execution.Scheduler.Implicits.global

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val policy = new FixedMaxPartitionsEvictionPolicy(100)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))
  memStore.setup(dataset1, 0, TestData.storeConf)

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

  /**
   * Ingest a single RecordContainer with 100 records in it.  Note that the throughput reported is x100 so
   * it is not the containers throughput but the actual records throughput.
   *
   * NOTE: based on -prof stack profiling, this benchmark does invoke the flushing logic plus
   * WriteBufferPool buffer recycling logic.  The time is thus heavily influenced by the chunking/encoding
   * logic, OffheapSortedIDMap stuff, and native memory allocation cost.
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  @OperationsPerInvocation(100)
  def ingest100records(): Unit = {
    shard.ingest(ingestBuilder.allContainers.head, 0)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def nativeMemAlloc(): Unit = {
    val ptr = TestData.nativeMem.allocateOffheap(16)
    TestData.nativeMem.freeMemory(ptr)
  }

}