package filodb.jmh

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.{Mode, State, Scope}
import org.openjdk.jmh.annotations.OutputTimeUnit
import scalaxy.loops._
import scala.language.postfixOps
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import filodb.core.columnstore.{InMemoryColumnStore, RowReaderSegment, RowWriterSegment}
import filodb.spark.{SparkRowReader, TypeConverters}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.sum
import org.apache.spark.{SparkContext, SparkException, SparkConf}
import org.velvia.filo.{RowReader, TupleRowReader}

object SparkReadBenchmark {
  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val keyHelper = IntKeyHelper(10000)

  def readInner(schema: Seq[Column]): Iterator[Row] = {
    val _colStore = new InMemoryColumnStore
    Await.result(_colStore.scanSegments[Int](schema, "dataset", 0), 10.seconds).flatMap { seg =>
      val readerSeg = seg.asInstanceOf[RowReaderSegment[Int]]
      readerSeg.rowIterator((bytes, clazzes) => new SparkRowReader(bytes, clazzes))
         .asInstanceOf[Iterator[Row]]
    }

  }
}

@State(Scope.Thread)
class SparkReadBenchmark {
  val NumRows = 5000000
  // Source of rows
  implicit val keyHelper = IntKeyHelper(10000)

  val schema = Seq(Column("int", "dataset", 0, Column.ColumnType.IntColumn),
                   Column("rownum", "dataset", 0, Column.ColumnType.IntColumn))

  val dataset = Dataset("dataset", "rownum")
  val projection = RichProjection[Int](dataset, schema)

  val rowStream = Iterator.from(0).map { row => (Some(util.Random.nextInt), Some(row)) }

  // Merge segments into InMemoryColumnStore
  import scala.concurrent.ExecutionContext.Implicits.global
  val colStore = new InMemoryColumnStore
  rowStream.take(NumRows).grouped(10000).foreach { rows =>
    val firstRowNum = rows.head._2.get
    val keyRange = KeyRange("dataset", "partition", firstRowNum, firstRowNum + 10000)
    val writerSeg = new RowWriterSegment(keyRange, schema)
    writerSeg.addRowsAsChunk(rows.toIterator.map(TupleRowReader),
                             (r: RowReader) => r.getInt(1) )
    Await.result(colStore.appendSegment(projection, writerSeg, 0), 10.seconds)
  }

  private def makeTestRDD() = {
    val _schema = schema
    sc.parallelize(Seq(1), 1).mapPartitions { x =>
      SparkReadBenchmark.readInner(_schema)
    }
  }

  // Now create an RDD[Row] out of it, and a Schema, -> DataFrame
  val conf = (new SparkConf).setMaster("local[4]")
                            .setAppName("test")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)
  val sqlSchema = StructType(TypeConverters.columnsToSqlFields(schema))

  // How long does it take to iterate through all the rows
  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def sparkSum(): Any = {
    val testRdd = makeTestRDD()
    val df = sql.createDataFrame(testRdd, sqlSchema)
    df.agg(sum(df("int"))).collect().head
  }

  // Baseline comparison ... see what the minimal time for a Spark task is.
  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def sparkBaseline(): Any = {
    val testRdd = makeTestRDD()
    val df = sql.createDataFrame(testRdd, sqlSchema)
    df.select("int").limit(2).collect()
  }
}