package filodb.jmh

import ch.qos.logback.classic.{Level, Logger}
import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.{Mode, State, Scope}

import filodb.core.binaryrecord.{BinaryRecord, RecordSchema}
import filodb.core.GdeltTestData
import filodb.coordinator.IngestionCommands.IngestRows
import filodb.coordinator.Serializer

/**
 * Microbenchmark involving BinaryRecord serialization / comparison vs other record/key formats
 * Serialization benchmark consists of 50 rows of GDELT mini data (8 columns)
 *
 * To make the serialization more realistic, we use the SeqRowReader where the input lines has already been
 * parsed from CSV strings.  This is much more like the Spark Row input that is most likely to be used.
 */
@State(Scope.Thread)
class BinaryRecordBenchmark {
  import GdeltTestData._

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)

  val binSchema = RecordSchema(schema)
  Serializer.putSchema(binSchema)

  val ingestRowsRegular = IngestRows(projection2.datasetRef, 0, seqReaders.take(50).toList, 100L)

  def getBinRecordRows: IngestRows = {
    val binRecords = seqReaders.take(50).map { r => BinaryRecord(binSchema, r) }
    ingestRowsRegular.copy(rows = binRecords)
  }

  private def serializeJavaRows: Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    try {
      oos.writeObject(ingestRowsRegular)
      baos.toByteArray
    } finally {
      baos.close()
    }
  }

  import Serializer._

  // scalastyle:off
  println(s"   Java Serialization IngestRows size:  ${serializeJavaRows.size} bytes")
  println(s"   BinaryRecord-based IngestRows size:  ${getBinRecordRows.toBytes.size} bytes")
  // scalastyle:on

  /**
   * Java Serialization of a non-BinaryRecord IngestRows
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def deserJava(): IngestRows = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(serializeJavaRows))
    ois.readObject.asInstanceOf[IngestRows]
  }

  /**
   * Custom BinaryRecord-based IngestRows deser, including conversion into BinaryRecord
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def deserBinRecord(): IngestRows = {
    val binRows = getBinRecordRows
    fromBinaryIngestRows(binRows.toBytes)
  }
}