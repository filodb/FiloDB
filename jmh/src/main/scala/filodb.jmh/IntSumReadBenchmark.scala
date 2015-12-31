package filodb.jmh

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.{Mode, State, Scope}
import org.openjdk.jmh.annotations.OutputTimeUnit
import scalaxy.loops._
import scala.language.postfixOps

import filodb.core._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import filodb.core.columnstore.{RowReaderSegment, RowWriterSegment}
import org.velvia.filo.{FiloVector, FiloRowReader, RowReader, TupleRowReader}

import java.util.concurrent.TimeUnit

/**
 * Microbenchmark of simple integer summing of Filo chunks in FiloDB segments,
 * mostly to see what the theoretical peak output of scanning speeds can be.
 * Does not involve Spark (at least this one doesn't).
 */
@State(Scope.Thread)
class IntSumReadBenchmark {
  val NumRows = 10000
  implicit val keyHelper = IntKeyHelper(10000)

  val schema = Seq(Column("int", "dataset", 0, Column.ColumnType.IntColumn),
                   Column("rownum", "dataset", 0, Column.ColumnType.IntColumn))

  val dataset = Dataset("dataset", "rownum")

  val rowStream = Iterator.from(0).map { row => (Some(util.Random.nextInt), Some(row)) }

  val keyRange = KeyRange("dataset", "partition", 0, NumRows)
  val writerSeg = new RowWriterSegment(keyRange, schema)
  writerSeg.addRowsAsChunk(rowStream.map(TupleRowReader).take(NumRows),
                           (r: RowReader) => r.getInt(1) )
  val readSeg = RowReaderSegment(writerSeg, schema)

  /**
   * Simulation of a columnar query engine scanning the segment chunks columnar wise
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def columnarSegmentScan(): Int = {
    val intVector = FiloVector[Int](readSeg.chunks(0)(0))
    var total = 0
    for { i <- 0 until NumRows optimized } {
      total += intVector(i)
    }
    total
  }

  /**
   * Simulation of ideal row-wise scanning speed with no boxing (Spark 1.5+ w Tungsten?)
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def rowWiseSegmentScan(): Int = {
    val it = readSeg.rowIterator()
    var sum = 0
    while(it.hasNext) {
      sum += it.next.getInt(0)
    }
    sum
  }

  /**
   * Simulation of boxed row-wise scanning speed (Spark 1.4.x aggregations)
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def rowWiseBoxedSegmentScan(): Int = {
    val it = readSeg.rowIterator()
    var sum = 0
    while(it.hasNext) {
      sum += it.next.asInstanceOf[FiloRowReader].getAny(0).asInstanceOf[Int]
    }
    sum
  }
}