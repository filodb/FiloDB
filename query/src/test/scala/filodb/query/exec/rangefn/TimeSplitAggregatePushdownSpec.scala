package filodb.query.exec.rangefn

import java.util.concurrent.atomic.AtomicLong

import scala.util.Random

import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures

import filodb.core.MetricsTestData
import filodb.core.memstore.{TimeSeriesPartitionSpec, WriteBufferPool}
import filodb.core.metadata.Dataset
import filodb.core.query.{CustomRangeVectorKey, RawDataRangeVector, ResultSchema, RvRange, TransientRow}
import filodb.core.query.NoCloseCursor.NoCloseCursor
import filodb.core.store.AllChunkScan
import filodb.memory.format.TupleRowReader
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query.AggregationOperator
import filodb.query.exec.{AggregateMapReduce, InternalRangeFunction, PeriodicSamplesMapper, StitchOverlapMerge,
  StitchRvsExec}

/**
 * Demonstrates the results of `sum(rate(foo[5m]))` and `sum(increase(foo[5m]))` with and without the
 * time-split aggregate pushdown of MultiPartitionPlanner, for cumulative and delta counters.
 *
 * Ingestion of all series moves from partition P1 to P2 at time `splitMs`. The move is scheduled ahead of time and
 * gateways route each sample by its own timestamp, so no sample is lost or duplicated: P1 holds every sample with
 * a timestamp before the split and P2 every sample from the split onwards.
 *
 *  - Without pushdown (what the remote raw export path computes): each series' samples from both partitions are
 *    stitched into one series, then rate/increase and sum are applied.
 *  - With pushdown: the whole `sum(fn(...))` runs on each partition's samples alone over the time range the planner
 *    would give that partition, and the two partial results are added per timestamp with
 *    StitchRvsExec(overlapMerge = Sum).
 *
 * Both sides use the production code: chunked storage, PeriodicSamplesMapper (which picks ChunkedRateFunction /
 * ChunkedIncreaseFunction for cumulative schemas and RateOverDeltaChunkedFunctionD / SumOverTimeChunkedFunctionD for
 * delta schemas), AggregateMapReduce(Sum) and StitchRvsExec.merge.
 *
 * Expected: delta counters give identical results, since rate/increase over deltas is a plain sum over the window
 * which splits exactly across partitions. Cumulative counters match outside the overlap but are underestimated inside
 * it, since each partition extrapolates only over its own portion of the window.
 */
class TimeSplitAggregatePushdownSpec extends RawDataWindowingSpec with ScalaFutures {

  private val scrapeMs = 30000L
  private val lookbackMs = 300000L
  private val stepMs = 60000L
  private val uncertaintyMs = 60000L
  private val numSeries = 25
  private val splitMs = 10000000L
  private val queryStartMs = splitMs - 1800000L
  private val queryEndMs = splitMs + 1800000L

  // The ranges MultiPartitionPlanner.materializeTimeSplitAggregatePushdown gives each partition (no offset here):
  // P1 up to split + lookback + uncertainty snapped down to the step grid, P2 from split - uncertainty snapped up.
  private val p1EndMs = queryStartMs + (splitMs + lookbackMs + uncertaintyMs - queryStartMs) / stepMs * stepMs
  private val p2StartMs = queryStartMs +
    (splitMs - uncertaintyMs - queryStartMs + stepMs - 1) / stepMs * stepMs

  private val relTolerance = 1e-9

  /** One series' samples: timestamps and the increment of the counter over the interval ending at each timestamp */
  private case class Series(times: Seq[Long], increments: Seq[Double])

  /**
   * Deterministic series with random scrape phase, +/-1s scrape jitter, a random base rate per series between
   * 1/s and 10/s, and +/-20% noise on each increment.
   */
  private def generateSeries(): Seq[Series] = {
    val rnd = new Random(42)
    (0 until numSeries).map { _ =>
      val phaseMs = rnd.nextInt(scrapeMs.toInt).toLong
      val ratePerSec = 1 + rnd.nextDouble() * 9
      val firstMs = queryStartMs - 2 * lookbackMs + phaseMs
      val times = Iterator.iterate(firstMs)(_ + scrapeMs).takeWhile(_ <= queryEndMs)
        .map(t => t + rnd.nextInt(2001) - 1000).toList
      val increments = times.map(_ => ratePerSec * scrapeMs / 1000 * (0.8 + 0.4 * rnd.nextDouble()))
      Series(times, increments)
    }
  }

  /** Samples as ingested for the counter type: running total for cumulative counters, increment for delta */
  private def samples(series: Series, isDelta: Boolean, seriesNo: Int): Seq[(Long, Double)] =
    if (isDelta) series.times.zip(series.increments)
    else series.times.zip(series.increments.scanLeft(1000d * (seriesNo + 1))(_ + _).tail)

  private def rawRv(dataset: Dataset, pool: WriteBufferPool, seriesNo: Int,
                    data: Seq[(Long, Double)]): RawDataRangeVector = {
    val part = TimeSeriesPartitionSpec.makePart(0, dataset, MetricsTestData.defaultPartKey, bufferPool = pool)
    data.foreach { case (t, v) =>
      part.ingest(0, TupleRowReader((Some(t), Some(v))), ingestBlockHolder, createChunkAtFlushBoundary = false,
        flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
    }
    part.switchBuffers(ingestBlockHolder, encode = true)
    val key = CustomRangeVectorKey(Map("series".utf8 -> seriesNo.toString.utf8))
    RawDataRangeVector(key, part, AllChunkScan, Array(0, 1), new AtomicLong(), _ => {}, Long.MaxValue, "query-id")
  }

  /** sum(fn(series[lookback])) over [startMs, endMs] using the production range function and aggregation code */
  private def sumOverSeries(fn: InternalRangeFunction, rvs: Seq[RawDataRangeVector], schema: ResultSchema,
                            startMs: Long, endMs: Long): Seq[(Long, Double)] = {
    val psm = PeriodicSamplesMapper(startMs, stepMs, endMs, Some(lookbackMs), Some(fn),
      stepMultipleNotationUsed = false, Nil, None)
    val perSeries = psm.apply(Observable.fromIterable(rvs), querySession, 1000000, schema, Nil)
    val summed = AggregateMapReduce(AggregationOperator.Sum, Nil)
      .apply(perSeries, querySession, 1000000, psm.schema(schema))
    val result = summed.toListL.runToFuture.futureValue
    result.size shouldEqual 1
    result.head.rows().map(r => (r.getLong(0), r.getDouble(1))).toList
  }

  private def cursor(rows: Seq[(Long, Double)]) =
    new NoCloseCursor(rows.iterator.map { case (t, v) => new TransientRow(t, v) })

  private case class Comparison(timestampMs: Long, withoutPushdown: Double, withPushdown: Double) {
    def inOverlap: Boolean = timestampMs >= p2StartMs && timestampMs <= p1EndMs
    def relDiff: Double = (withPushdown - withoutPushdown) / withoutPushdown
    def matches: Boolean =
      (withoutPushdown.isNaN && withPushdown.isNaN) || math.abs(relDiff) <= relTolerance
  }

  private def compare(fn: InternalRangeFunction, isDelta: Boolean): Seq[Comparison] = {
    val (dataset, pool) =
      if (isDelta) (MetricsTestData.deltaTimeseriesDatasetWithMetric, deltaTsBufferPool)
      else (MetricsTestData.timeseriesDatasetWithMetric, tsBufferPool)
    val schema = ResultSchema(dataset.schema.infosFromIDs(0 to 1), 1)
    val allSamples = generateSeries().zipWithIndex.map { case (s, i) => samples(s, isDelta, i) }
    val p1Samples = allSamples.map(_.filter(_._1 < splitMs))
    val p2Samples = allSamples.map(_.filter(_._1 >= splitMs))

    // Without pushdown: each series stitched across the partitions, as the raw export path computes it
    val stitchedRvs = p1Samples.zip(p2Samples).zipWithIndex.map { case ((a, b), i) => rawRv(dataset, pool, i, a ++ b) }
    val withoutPushdown = sumOverSeries(fn, stitchedRvs, schema, queryStartMs, queryEndMs)

    // With pushdown: sum(fn) on each partition over its range, partial results added per timestamp
    val p1Rvs = p1Samples.zipWithIndex.map { case (data, i) => rawRv(dataset, pool, i, data) }
    val p2Rvs = p2Samples.zipWithIndex.map { case (data, i) => rawRv(dataset, pool, i, data) }
    val p1Result = sumOverSeries(fn, p1Rvs, schema, queryStartMs, p1EndMs)
    val p2Result = sumOverSeries(fn, p2Rvs, schema, p2StartMs, queryEndMs)
    val withPushdown = StitchRvsExec.merge(Seq(cursor(p1Result), cursor(p2Result)),
      Some(RvRange(queryStartMs, stepMs, queryEndMs)), overlapMerge = StitchOverlapMerge.Sum)
      .map(r => (r.getLong(0), r.getDouble(1))).toList

    withoutPushdown.map(_._1) shouldEqual withPushdown.map(_._1)
    withoutPushdown.zip(withPushdown).map { case ((t, without), (_, withPd)) => Comparison(t, without, withPd) }
  }

  // scalastyle:off regex
  private def printComparison(title: String, rows: Seq[Comparison]): Unit = {
    println(s"\n$title  (overlap: split${(p2StartMs - splitMs) / 1000}s .. split+${(p1EndMs - splitMs) / 1000}s)")
    println(f"${"t - split"}%10s ${"without"}%12s ${"with"}%12s ${"diff"}%9s")
    rows.filter(c => c.timestampMs >= p2StartMs - 2 * stepMs && c.timestampMs <= p1EndMs + 2 * stepMs).foreach { c =>
      val marker = if (c.inOverlap) " <- overlap" else ""
      println(f"${(c.timestampMs - splitMs) / 1000}%9ds ${c.withoutPushdown}%12.4f ${c.withPushdown}%12.4f " +
        f"${c.relDiff * 100}%8.3f%%$marker")
    }
  }
  // scalastyle:on regex

  Seq(("rate", InternalRangeFunction.Rate), ("increase", InternalRangeFunction.Increase)).foreach {
    case (fnName, fn) =>
      val scenario = s"sum($fnName(foo[5m])), ${scrapeMs / 1000}s scrapes"

      it(s"should give identical results with and without pushdown for delta counters: $scenario") {
        val rows = compare(fn, isDelta = true)
        printComparison(s"DELTA counter: $scenario", rows)
        rows.filterNot(_.matches) shouldBe empty
      }

      it(s"should underestimate only within the overlap with pushdown for cumulative counters: $scenario") {
        val rows = compare(fn, isDelta = false)
        printComparison(s"CUMULATIVE counter: $scenario", rows)
        // Outside the overlap only one partition has data in the window, so results are identical
        rows.filterNot(_.inOverlap).filterNot(_.matches) shouldBe empty
        // Within the overlap each partition extrapolates only over its own portion of the window, so the increase
        // between the last P1 sample and the first P2 sample of a series is partly lost: never higher, lower at some
        // instants. With staggered scrape phases across series the dip is small (about -0.7% here).
        val overlap = rows.filter(_.inOverlap)
        overlap.foreach(c => c.withPushdown should be <= c.withoutPushdown * (1 + relTolerance))
        overlap.map(_.relDiff).min should be < -relTolerance
      }
  }
}
