package filodb.jmh

import java.util.concurrent.TimeUnit

import ch.qos.logback.classic.{Level, Logger}
import org.openjdk.jmh.annotations._

import filodb.core.{MachineMetricsData, TestData}
import filodb.core.binaryrecord2.{RecordBuilder, RecordComparator, RecordSchema}
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

  val dataStream = withMap(linearMultiSeries(), extraTags=extraTags) take 50

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

  // TODO: measure deserialization from ProtoBuf

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

  // Creating 50 PartKey BRv2 from 50 ingestion BRv2's
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def makePartKeyBRv2(): Int = {
    partKeyBuilder.resetCurrent()
    ingestBuilder.allContainers.head.consumeRecords(consumer)
    0
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def nativeMemAlloc(): Unit = {
    val ptr = TestData.nativeMem.allocateOffheap(16)
    TestData.nativeMem.freeMemory(ptr)
  }

}