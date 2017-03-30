package filodb.jmh

import ch.qos.logback.classic.{Level, Logger}
import com.googlecode.javaewah.EWAHCompressedBitmap
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.{Mode, State, Scope}
import scala.language.postfixOps
import scala.util.Random
import scalaxy.loops._

import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset, RichProjection}
import filodb.core.query.ChunkSetReader
import filodb.core.store.{ChunkSet}
import org.velvia.filo.{FiloVector, FastFiloRowReader, RowReader, TupleRowReader}

import java.util.concurrent.TimeUnit

object IntSumReadBenchmark {
  val schema = Seq(DataColumn(0, "int", "dataset", 0, Column.ColumnType.IntColumn),
                   DataColumn(1, "rownum", "dataset", 0, Column.ColumnType.IntColumn))

  val dataset = Dataset("dataset", "rownum", ":round rownum 10000")
  val ref = DatasetRef("dataset")
  val projection = RichProjection(dataset, schema)

  val rowIt = Iterator.from(0).map { row => (Some(scala.util.Random.nextInt), Some(row)) }

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)
}

/**
 * Microbenchmark of simple integer summing of Filo chunks in FiloDB segments,
 * mostly to see what the theoretical peak output of scanning speeds can be.
 * Does not involve Spark (at least this one doesn't).
 */
@State(Scope.Thread)
class IntSumReadBenchmark {
  import IntSumReadBenchmark._
  val NumRows = 10000

  val state = new TestSegmentState(projection, schema)
  val chunkSet = ChunkSet(state, rowIt.map(TupleRowReader).take(NumRows))
  val reader = ChunkSetReader(chunkSet, schema)

  val NumSkips = 300  // 3% skips - not that much really
  val skips = (0 until NumSkips).map { i => Random.nextInt(NumRows) }.sorted.distinct
  val readerWithSkips = ChunkSetReader(chunkSet, schema, EWAHCompressedBitmap.bitmapOf(skips :_*))

  /**
   * Simulation of a columnar query engine scanning the segment chunks columnar wise
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def columnarChunkScan(): Int = {
    val intVector = reader.vectors(0).asInstanceOf[FiloVector[Int]]
    var total = 0
    for { i <- 0 until NumRows optimized } {
      total += intVector(i)
    }
    total
  }

  /**
   * Simulation of ideal row-wise scanning speed with no boxing (Spark 1.5+ w Tungsten?) and no rows to skip
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def rowWiseChunkScan(): Int = {
    val it = reader.rowIterator()
    var sum = 0
    while(it.hasNext) {
      sum += it.next.getInt(0)
    }
    sum
  }

  /**
   * Row-wise scanning with null/isAvailable check
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def rowWiseChunkScanNullCheck(): Int = {
    val it = reader.rowIterator()
    var sum = 0
    while(it.hasNext) {
      val row = it.next
      if (row.notNull(0)) sum += row.getInt(0)
    }
    sum
  }

  /**
   * Row-wise scanning with null/isAvailable check and rows to skip
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def rowWiseChunkScanNullWithSkips(): Int = {
    val it = readerWithSkips.rowIterator()
    var sum = 0
    while(it.hasNext) {
      val row = it.next
      if (row.notNull(0)) sum += row.getInt(0)
    }
    sum
  }

  /**
   * Simulation of boxed row-wise scanning speed (Spark 1.4.x aggregations)
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def rowWiseBoxedChunkScan(): Int = {
    val it = reader.rowIterator()
    var sum = 0
    while(it.hasNext) {
      sum += it.next.asInstanceOf[FastFiloRowReader].getAny(0).asInstanceOf[Int]
    }
    sum
  }
}