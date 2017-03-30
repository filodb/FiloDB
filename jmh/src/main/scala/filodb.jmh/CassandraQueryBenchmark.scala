package filodb.jmh

import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.config.ConfigFactory
import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.{Mode, State, Scope}

import filodb.core._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.RichProjection
import filodb.core.store.{SinglePartitionScan, RowKeyChunkScan}
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.metastore.CassandraMetaStore

/**
 * Microbenchmark of single partition reads, including some range scans, from Cassandra.  No Spark.
 * Assumes data has been ingested using IngestionStress.scala -- the first entire month of NYC taxi trip
 * data, approx. 14.77 million rows;  into the filostress keyspace.
 *
 * Designed to test read performance against various Cassandra query techniques and for testing other
 * query side optimizations and changes.  Mostly measures I/O and segment/chunk read pipeline efficiency,
 * does not measure cost of query aggregations themselves.
 */
@State(Scope.Thread)
class CassandraQueryBenchmark {
  import filodb.coordinator.client.Client._

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)

  val config = ConfigFactory.load("filodb-defaults.conf").getConfig("filodb")

  // import scala.concurrent.ExecutionContext.Implicits.global
  val colStore = new CassandraColumnStore(config, context)
  val metaStore = new CassandraMetaStore(config.getConfig("cassandra"))

  val hourOfDayRef = DatasetRef("taxi_hour_of_day", Some("filostress"))
  val hodDataset = parse(metaStore.getDataset(hourOfDayRef)) { x => x }
  val hodSchema = parse(metaStore.getSchema(hourOfDayRef, 0)) { s => s }
  val hodProj = RichProjection(hodDataset, hodSchema.values.toList)

  val medallionSegRef = DatasetRef("taxi_medallion_seg", Some("filostress"))
  val medDataset = parse(metaStore.getDataset(medallionSegRef)) { x => x }
  val medSchema = parse(metaStore.getSchema(medallionSegRef, 0)) { s => s }
  val medProj = RichProjection(medDataset, medSchema.values.toList)

  @TearDown
  def shutdown(): Unit = {
    colStore.shutdown()
    metaStore.shutdown()
  }

  val hodPartScan = SinglePartitionScan(hodProj.partKey(9))   // 9am - peak time - lots of records, around 700k
  val columns = Seq("passenger_count", "pickup_datetime", "medallion")

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def wholePartitionScanHOD(): Int = {
    colStore.scanRows(hodProj, columns.map(hodSchema), 0, hodPartScan).length
  }

  // Like the previous one, but does not actually iterate through the rows... so just count the read perf
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def wholePartitionReadOnlyHOD(): Int = {
    colStore.scanChunks(hodProj, columns.map(hodSchema), 0, hodPartScan).length
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def oneColumnPartitionScanHOD(): Int = {
    colStore.scanRows(hodProj, columns.take(1).map(hodSchema), 0, hodPartScan).length
  }

  // How fast is it only to read the indices of a partition?  This is a constant cost no matter
  // the number of chunks read
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def wholePartitionIndicesOnlyHOD(): Int = {
    parse(colStore.scanPartitions(hodProj, 0, hodPartScan).countL.runAsync) { l => l.toInt }
  }

  // We no longer break down components of the read because everything is pipelined

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def wholePartitionScanMed(): Int = {
    val medPartScan = SinglePartitionScan(medProj.partKey("AA"))
    colStore.scanRows(medProj, columns.map(medSchema), 0, medPartScan).length
  }

  val hodKey1 = BinaryRecord(hodProj, Seq(Timestamp.valueOf("2013-01-02 08:00:00"), "0", "0"))
  val hodKey2 = BinaryRecord(hodProj, Seq(Timestamp.valueOf("2013-01-03 23:59:59"), "0", "0"))
  val hodKey3 = BinaryRecord(hodProj, Seq(Timestamp.valueOf("2013-01-19 08:00:00"), "0", "0"))
  val hodKey4 = BinaryRecord(hodProj, Seq(Timestamp.valueOf("2013-01-25 23:59:59"), "0", "0"))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def earlyTwoDaysRangeScanHOD(): Int = {
    val pmethod = SinglePartitionScan(hodProj.partKey(9))
    val cmethod = RowKeyChunkScan(hodKey1, hodKey2)
    colStore.scanRows(hodProj, columns.map(hodSchema), 0, pmethod, cmethod).length
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def lateTwoDaysRangeScanHOD(): Int = {
    val pmethod = SinglePartitionScan(hodProj.partKey(9))
    val cmethod = RowKeyChunkScan(hodKey3, hodKey4)
    colStore.scanRows(hodProj, columns.map(hodSchema), 0, pmethod, cmethod).length
  }

  val medKey1 = BinaryRecord(medProj, Seq("AAF", "", Timestamp.valueOf("2013-01-02 08:00:00")))
  val medKey2 = BinaryRecord(medProj, Seq("AAL", "", Timestamp.valueOf("2013-01-03 23:59:59")))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def middleMedallionRangeScan(): Int = {
    val pmethod = SinglePartitionScan(medProj.partKey("AA"))
    val cmethod = RowKeyChunkScan(medKey1, medKey2)
    colStore.scanRows(medProj, columns.map(medSchema), 0, pmethod, cmethod).length
  }
}