package filodb.jmh

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import scalaxy.loops._
import scala.language.postfixOps
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import filodb.core.store.RowWriterSegment
import filodb.spark.{FiloSetup, FiloRelation}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkContext, SparkException, SparkConf}
import org.velvia.filo.{RowReader, TupleRowReader}

/**
 * A benchmark to compare performance of FiloRelation against different scenarios,
 * for an analytical query summing 5 million random integers from a single column of a
 * FiloDB dataset.  Description:
 * - sparkSum(): Sum 5 million integers stored using InMemoryColumnStore.
 * - sparkBaseline(): Get the first 2 records.  Just to see what the baseline latency is of a
 *   DataFrame query.
 * - inMemoryColStoreOnly(): No Spark, just reading rows from the InMemoryColumnStore
 * - sparkCassSum(): Sum 5 million integers using CassandraColumnStore.  Must have run CreateCassTestData
 *   first to populate into Cassandra.
 *   NOTE: This has been moved to SparkCassBenchmark.scala
 *
 * To get the scan speed, one needs to subtract the baseline from the total time of sparkSum/sparkCassSum.
 * For example, on my laptop, here is the JMH output:
 * {{{
 *  Benchmark                         Mode  Cnt  Score   Error  Units
 *  SparkReadBenchmark.sparkBaseline    ss   15  0.023 ± 0.002   s/op
 *  SparkReadBenchmark.sparkSum         ss   15  0.415 ± 0.034   s/op
 *  SparkCassBenchmark.sparkCassSum     ss   15  0.622 ± 0.031   s/op
 * }}}
 *
 * (The above run against Cassandra 2.1.6, 5GB heap, with jmh:run -i 3 -wi 3 -f3 filodb.jmh.SparkReadBenchmark)
 *
 * Thus:
 * - Cassandra scan speed = 5000000 / (0.622 - 0.023) =  8,347,245 ops/sec
 * - InMemory scan speed  = 5000000 / (0.415 - 0.023) = 12,755,102 ops/sec
 */
@State(Scope.Benchmark)
class SparkReadBenchmark {
  val NumRows = 5000000
  // Source of rows
  implicit val keyHelper = IntKeyHelper(10000)

  val conf = (new SparkConf).setMaster("local[4]")
                            .setAppName("test")
                            // .set("spark.sql.tungsten.enabled", "false")
                            .set("spark.filodb.store", "in-memory")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)
  // Below is to make sure that Filo actor system stuff is run before test code
  // so test code is not hit with unnecessary slowdown
  val filoConfig = FiloSetup.configFromSpark(sc)
  FiloSetup.init(filoConfig)

  val schema = Seq(Column("int", "dataset", 0, Column.ColumnType.IntColumn),
                   Column("rownum", "dataset", 0, Column.ColumnType.IntColumn))

  val dataset = Dataset("dataset", "rownum")
  val projection = RichProjection[Int](dataset, schema)

  // Initialize metastore
  import scala.concurrent.ExecutionContext.Implicits.global
  Await.result(FiloSetup.metaStore.newDataset(dataset), 3.seconds)
  val createColumns = schema.map { col => FiloSetup.metaStore.newColumn(col) }
  Await.result(Future.sequence(createColumns), 3.seconds)

  val rowStream = Iterator.from(0).map { row => (Some(util.Random.nextInt), Some(row)) }

  // Merge segments into InMemoryColumnStore
  import scala.concurrent.ExecutionContext.Implicits.global
  rowStream.take(NumRows).grouped(10000).foreach { rows =>
    val firstRowNum = rows.head._2.get
    val keyRange = KeyRange("dataset", "partition", firstRowNum, firstRowNum + 10000)
    val writerSeg = new RowWriterSegment(keyRange, schema)
    writerSeg.addRowsAsChunk(rows.toIterator.map(TupleRowReader),
                             (r: RowReader) => r.getInt(1) )
    Await.result(FiloSetup.columnStore.appendSegment(projection, writerSeg, 0), 10.seconds)
  }

  @TearDown
  def shutdownFiloActors(): Unit = {
    FiloSetup.shutdown()
    sc.stop()
  }

  val df = sql.read.format("filodb.spark").option("dataset", dataset.name).load

  // How long does it take to iterate through all the rows
  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def sparkSum(): Any = {
    df.agg(sum(df("int"))).collect().head
  }

  // Measure the speed of InMemoryColumnStore's ScanSegments over many segments
  // Including null check
  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def inMemoryColStoreOnly(): Any = {
    val it = FiloRelation.getRows(dataset.options, dataset.name, 0, schema, schema.last,
                                  (x => true), Map.empty).asInstanceOf[Iterator[InternalRow]]
    var sum = 0
    while (it.hasNext) {
      val row = it.next
      if (!row.isNullAt(0)) sum += row.getInt(0)
    }
    sum
  }

  // Baseline comparison ... see what the minimal time for a Spark task is.
  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def sparkBaseline(): Any = {
    df.select("int").limit(2).collect()
  }
}