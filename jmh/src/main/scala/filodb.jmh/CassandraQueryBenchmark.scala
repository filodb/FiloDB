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
import filodb.core.store.{SinglePartitionScan, SinglePartitionRowKeyScan}
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

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def wholePartitionScanHOD(): Int = {
    val hodPartScan = SinglePartitionScan(9)   // 9am - peak time - lots of records, around 700k
    parse(colStore.scanRows(hodProj, Seq("passenger_count").map(hodSchema), 0, hodPartScan)) {
      rowIter => rowIter.length }
  }

  // Like the previous one, but does not actually iterate through the rows... so just count the read perf
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def wholePartitionReadOnlyHOD(): Int = {
    val hodPartScan = SinglePartitionScan(9)   // 9am - peak time - lots of records, around 700k
    parse(colStore.scanSegments(hodProj, Seq("passenger_count").map(hodSchema), 0, hodPartScan)) {
      segIter => segIter.length }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def wholePartitionScanMed(): Int = {
    val medPartScan = SinglePartitionScan("AA")
    parse(colStore.scanRows(medProj, Seq("passenger_count").map(medSchema), 0, medPartScan)) {
      rowIter => rowIter.length }
  }

  val hodKey1 = BinaryRecord(hodProj, Seq(Timestamp.valueOf("2013-01-02 08:00:00"), "0", "0"))
  val hodKey2 = BinaryRecord(hodProj, Seq(Timestamp.valueOf("2013-01-03 23:59:59"), "0", "0"))
  val hodKey3 = BinaryRecord(hodProj, Seq(Timestamp.valueOf("2013-01-19 08:00:00"), "0", "0"))
  val hodKey4 = BinaryRecord(hodProj, Seq(Timestamp.valueOf("2013-01-25 23:59:59"), "0", "0"))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def earlyTwoDaysRangeScanHOD(): Int = {
    val method = SinglePartitionRowKeyScan(9, hodKey1, hodKey2)
    parse(colStore.scanRows(hodProj, Seq("passenger_count").map(hodSchema), 0, method)) {
      rowIter => rowIter.length }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def lateTwoDaysRangeScanHOD(): Int = {
    val method = SinglePartitionRowKeyScan(9, hodKey3, hodKey4)
    parse(colStore.scanRows(hodProj, Seq("passenger_count").map(hodSchema), 0, method)) {
      rowIter => rowIter.length }
  }

  val medKey1 = BinaryRecord(medProj, Seq("AAF", "", Timestamp.valueOf("2013-01-02 08:00:00")))
  val medKey2 = BinaryRecord(medProj, Seq("AAL", "", Timestamp.valueOf("2013-01-03 23:59:59")))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def middleMedallionRangeScan(): Int = {
    val method = SinglePartitionRowKeyScan("AA", medKey1, medKey2)
    parse(colStore.scanRows(medProj, Seq("passenger_count").map(medSchema), 0, method)) {
      rowIter => rowIter.length }
  }
}