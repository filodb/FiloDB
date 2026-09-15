package filodb.jmh

import java.util.concurrent.TimeUnit

import ch.qos.logback.classic.{Level, Logger}
import org.apache.arrow.memory.RootAllocator
import org.openjdk.jmh.annotations.{Level => JMHLevel, _}

import filodb.coordinator.flight.ArrowSerializedRangeVectorOps
import filodb.coordinator.flight.ArrowSerializedRangeVectorOps.VsrPopulationState
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.{ZeroCopyUTF8String => UTF8Str}

/**
 * Measures ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs across four workloads:
 *
 *   smallRv        —  100 rows, single VSR (lightweight query response)
 *   mediumRv       — 1 000 rows, single VSR (typical dense time series)
 *   largeRvMultiVsr — maxNumRows + 100 rows, spills into a second VSR
 *   multiSeriesRvs  — 100 RVs × 50 rows  (multi-series query result)
 *
 * Run with:
 *   jmh/jmh:run -i 5 -wi 5 -f1 -t1 .*PopulateRvIntoVsr.*
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class PopulateRvIntoVsrBenchmark {

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)

  private val allocator  = new RootAllocator(Long.MaxValue)
  private val queryStats = QueryStats()

  private val resSchema = new ResultSchema(Seq(
    ColumnInfo("time", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn)
  ), 1)
  private val recSchema = resSchema.toRecordSchema

  private val baseKey = CustomRangeVectorKey(Map(
    UTF8Str("metric") -> UTF8Str("bench_metric"),
    UTF8Str("host")   -> UTF8Str("server1")
  ))

  private val smallRvData     = makeData(100)
  private val mediumRvData    = makeData(1_000)
  private val largeRvData     = makeData(ArrowSerializedRangeVectorOps.maxNumRows + 100)
  private val multiSeriesRvs  = (1 to 100).map { i =>
    buildRv(makeData(50), CustomRangeVectorKey(Map(UTF8Str("host") -> UTF8Str(s"server$i"))))
  }

  private val smallRv      = buildRv(smallRvData, baseKey)
  private val mediumRv     = buildRv(mediumRvData, baseKey)
  private val largeRv      = buildRv(largeRvData, baseKey)

  private def makeData(numRows: Int): IndexedSeq[(Long, Double)] =
    (1 to numRows).map(i => (i.toLong * 60_000L, i.toDouble))

  private def buildRv(samples: IndexedSeq[(Long, Double)], rvKey: RangeVectorKey): RangeVector = {
    val range = RvRange(samples.head._1, 60_000L, samples.last._1)
    new RangeVector {
      import NoCloseCursor._
      override def key: RangeVectorKey          = rvKey
      override def rows(): RangeVectorCursor    = samples.map(r => new TransientRow(r._1, r._2)).iterator
      override def outputRange: Option[RvRange] = Some(range)
    }
  }

  // VsrPopulationState is mutable and accumulates VSRs; reset per invocation.
  private var state: VsrPopulationState = _

  @Setup(JMHLevel.Invocation)
  def resetState(): Unit = state = VsrPopulationState()

  @TearDown(JMHLevel.Invocation)
  def freeVsrs(): Unit =
    (state.finishedVsrs ++ Option(state.currentVsr)).foreach(_.close())

  @TearDown(JMHLevel.Trial)
  def closeAllocator(): Unit = allocator.close()

  @Benchmark
  def smallRvVsr(): Unit =
    ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
      smallRv, recSchema, "bench", queryStats, allocator, state)

  @Benchmark
  def mediumRvVsr(): Unit =
    ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
      mediumRv, recSchema, "bench", queryStats, allocator, state)

  @Benchmark
  def largeRvMultiVsr(): Unit =
    ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
      largeRv, recSchema, "bench", queryStats, allocator, state)

  @Benchmark
  def multiSeriesRvsVsr(): Unit =
    multiSeriesRvs.foreach { rv =>
      ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(
        rv, recSchema, "bench", queryStats, allocator, state)
    }
}
