package filodb.jmh

import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.config.ConfigFactory
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.{Mode, State, Scope}
import org.velvia.filo.ArrayStringRowReader

import filodb.core.GdeltTestData
import filodb.core.reprojector.{DefaultReprojector, FiloMemTable, SegmentStateCache}
import filodb.core.store.{InMemoryColumnStore, Segment}

/**
 * Microbenchmark of reprojection (ingestion) pipeline.
 */
@State(Scope.Thread)
class ReprojectionBenchmark {
  import GdeltTestData._

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)

  val numRows = 2000   // 50 records per segment, so 40 segments
  val lines = (0 until numRows).map { i =>
    val parts = gdeltLines(i % gdeltLines.length).split(',')
    parts(0) = i.toString
    ArrayStringRowReader(parts)
  }

  val newSetting = "memtable.max-rows-per-table = 200000"
  val config = ConfigFactory.parseString(newSetting).withFallback(
                 ConfigFactory.load("application_test.conf")).getConfig("filodb")
  val mTable = new FiloMemTable(projection2, config, "localhost", 0)

  import monix.execution.Scheduler.Implicits.global
  val colStore = new InMemoryColumnStore(scala.concurrent.ExecutionContext.Implicits.global)
  val stateCache = new SegmentStateCache(config, colStore)
  val reprojector = new DefaultReprojector(config, colStore, stateCache)

  // Populate memtable
  mTable.ingestRows(lines)

  val partitions = mTable.partitions.toSeq

  /**
   * Simulation of a columnar query engine scanning the segment chunks columnar wise
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def toSegments(): Seq[Segment] = {
    // This is run multiple times, we are not writing to column store, so have to ensure state is reset
    stateCache.clear()
    reprojector.toSegments(mTable, partitions, 0).toList
  }
}