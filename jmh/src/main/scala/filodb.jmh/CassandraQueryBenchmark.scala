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

  val medallionSegRef = DatasetRef("taxi_medallion_seg", Some("filostress"))
  val medDataset = parse(metaStore.getDataset(medallionSegRef)) { x => x }

  @TearDown
  def shutdown(): Unit = {
    colStore.shutdown()
    metaStore.shutdown()
  }

  val hodPartScan = SinglePartitionScan(hodDataset.partKey(9))   // 9am - peak time - lots of records, around 700k
  val columns = Seq("passenger_count", "pickup_datetime", "medallion")

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def wholePartitionScanHOD(): Int = {
    colStore.scanRows(hodDataset, hodDataset.colIDs(columns: _*).get, hodPartScan).length
  }

  // Like the previous one, but does not actually iterate through the rows... so just count the read perf
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def wholePartitionReadOnlyHOD(): Int = {
    colStore.scanChunks(hodDataset, hodDataset.colIDs(columns: _*).get, hodPartScan).length
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def oneColumnPartitionScanHOD(): Int = {
    colStore.scanRows(hodDataset, hodDataset.colIDs(columns.take(1): _*).get, hodPartScan).length
  }

  // How fast is it only to read the indices of a partition?  This is a constant cost no matter
  // the number of chunks read
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def wholePartitionIndicesOnlyHOD(): Int = {
    parse(colStore.scanPartitions(hodDataset, hodPartScan).countL.runAsync) { l => l.toInt }
  }

  // We no longer break down components of the read because everything is pipelined

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def wholePartitionScanMed(): Int = {
    val medPartScan = SinglePartitionScan(medDataset.partKey("AA"))
    colStore.scanRows(medDataset, medDataset.colIDs(columns: _*).get, medPartScan).length
  }

  val hodKey1 = BinaryRecord(hodDataset, Seq(Timestamp.valueOf("2013-01-02 08:00:00"), "0", "0"))
  val hodKey2 = BinaryRecord(hodDataset, Seq(Timestamp.valueOf("2013-01-03 23:59:59"), "0", "0"))
  val hodKey3 = BinaryRecord(hodDataset, Seq(Timestamp.valueOf("2013-01-19 08:00:00"), "0", "0"))
  val hodKey4 = BinaryRecord(hodDataset, Seq(Timestamp.valueOf("2013-01-25 23:59:59"), "0", "0"))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def earlyTwoDaysRangeScanHOD(): Int = {
    val pmethod = SinglePartitionScan(hodDataset.partKey(9))
    val cmethod = RowKeyChunkScan(hodKey1, hodKey2)
    colStore.scanRows(hodDataset, hodDataset.colIDs(columns: _*).get, pmethod, cmethod).length
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def lateTwoDaysRangeScanHOD(): Int = {
    val pmethod = SinglePartitionScan(hodDataset.partKey(9))
    val cmethod = RowKeyChunkScan(hodKey3, hodKey4)
    colStore.scanRows(hodDataset, hodDataset.colIDs(columns: _*).get, pmethod, cmethod).length
  }

  val medKey1 = BinaryRecord(medDataset, Seq("AAF", "", Timestamp.valueOf("2013-01-02 08:00:00")))
  val medKey2 = BinaryRecord(medDataset, Seq("AAL", "", Timestamp.valueOf("2013-01-03 23:59:59")))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def middleMedallionRangeScan(): Int = {
    val pmethod = SinglePartitionScan(medDataset.partKey("AA"))
    val cmethod = RowKeyChunkScan(medKey1, medKey2)
    colStore.scanRows(medDataset, medDataset.colIDs(columns: _*).get, pmethod, cmethod).length
  }
}