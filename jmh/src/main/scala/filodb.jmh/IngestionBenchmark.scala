package filodb.jmh

import java.util.concurrent.TimeUnit

import ch.qos.logback.classic.{Level, Logger}
import org.openjdk.jmh.annotations._

import filodb.coordinator.client.IngestionCommands.IngestRows
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.binaryrecord2.{RecordBuilder, RecordComparator, RecordSchema}
import filodb.core.MachineMetricsData
import filodb.memory.{BinaryRegionConsumer, MemFactory}

/**
 * Microbenchmark measuring tasks affecting ingestion performance
 * - Deserialization
 * - Comparison of IngestRecord/SchemaRowReader to BinaryRecord/PartitionKeys
 * - maybe hashcode comparison of above
 * - copying of ingestion record to a PartitionKey (esp important during recovery)
 *
 * For realism we use the MachineMetricsData time series with a map partition key with 5 columns
 */
@State(Scope.Thread)
class IngestionBenchmark {
  import MachineMetricsData._

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)

  val dataStream = withMap(linearMultiSeries(), extraTags=extraTags) take 50
  val ingestRowsRegular = IngestRows(dataset2.ref, 0,
                                     records(dataStream))
  val ingestRecordBuffer = ingestRowsRegular.rows.toBuffer

  def getBinRecordRows: IngestRows = {
    ingestRowsRegular.copy(rows = ingestRowsRegular.rows.map { r =>
      r.copy(partition = dataset2.partKey(r.partition),
             data = BinaryRecord(dataset2.dataBinSchema, r.data))
    })
  }

  val incomingPartKey1 = ingestRowsRegular.rows.head.partition
  val brv1PartKey1 = getBinRecordRows.rows.head.partition

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
   * Equality of SchemaRowReader vs BinaryRecordv1 partition key (partition key search)
   * NOTE: this test does not capture the calculation of hashes in each UTF8String, which is cached, but
   * in reality has to be computed the first time no matter what.
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def compareSchemaRRAndBRv1(): Boolean = {
    incomingPartKey1.equals(brv1PartKey1)
  }

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
   * Creating 50 PartitionKey BinaryRecords from IngestRecord/SchemaRowReader
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def makePartKeyBRv1(): Seq[BinaryRecord] = {
    // NOTE: you MUST use ingestRecordBuffer otherwize the map is lazy and doesn't actually create all the BR's!
    ingestRecordBuffer.map { r => dataset2.partKey(r.partition) }
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
}