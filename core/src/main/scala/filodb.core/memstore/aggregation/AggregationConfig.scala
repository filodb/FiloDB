package filodb.core.memstore.aggregation

import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
 * Per-dataset aggregation configuration for out-of-order sample ingestion.
 *
 * This is parsed from an `aggregation {}` block inside the dataset's ingestion source config,
 * mirroring how the `store {}` block maps to [[filodb.core.store.StoreConfig]]. Aggregation used
 * to live on the schema (`DataSchema`); it is now configured per-dataset so that a single schema
 * can be shared by aggregated and non-aggregated datasets.
 *
 * The aggregators reference column IDs of the ingestion data schema; those IDs are validated
 * against the schema when the first aggregating partition is created (see TimeSeriesShard).
 *
 * @param aggregators    which columns to aggregate and how (parsed from "name(colId)" strings)
 * @param intervalMs     bucket width in millis (samples within a bucket are aggregated together)
 * @param oooToleranceMs how far behind the event-time watermark out-of-order samples are accepted
 */
final case class AggregationConfig(aggregators: Seq[ColumnAggregator] = Seq.empty,
                                   intervalMs: Long = 0L,
                                   oooToleranceMs: Long = 0L) {
  def nonEmpty: Boolean = aggregators.nonEmpty
  def isEmpty: Boolean = aggregators.isEmpty

  /**
   * Validates this config against the data columns of the schema it will aggregate.
   * Preserves the checks that previously ran at schema-parse time: a positive interval,
   * a non-negative tolerance, and column IDs that reference a real (non-timestamp) data column.
   * Throws IllegalArgumentException with a clear message on the first violation.
   *
   * @param numDataColumns number of data columns in the ingestion schema (col 0 is the timestamp)
   */
  def validate(numDataColumns: Int): Unit = {
    if (nonEmpty) {
      require(intervalMs > 0,
        s"aggregation interval must be positive when aggregators are defined, got $intervalMs")
      require(oooToleranceMs >= 0,
        s"aggregation ooo-tolerance must be non-negative when aggregators are defined, got $oooToleranceMs")
      aggregators.foreach { agg =>
        require(agg.columnId >= 1 && agg.columnId < numDataColumns,
          s"Aggregator column id ${agg.columnId} is out of range (1 to ${numDataColumns - 1})")
      }
    }
  }
}

object AggregationConfig {
  val empty = AggregationConfig()

  /**
   * Parses an AggregationConfig from the config object inside an `aggregation {}` block, e.g.:
   * {{{
   *   aggregation {
   *     aggregators   = ["dSum(1)", "hSum(3)"]
   *     interval      = 1m
   *     ooo-tolerance = 2m
   *   }
   * }}}
   * Reuses [[ColumnAggregator.parseAll]] for the aggregator specs.
   */
  def apply(aggConfig: Config): AggregationConfig = {
    val aggregatorNames = aggConfig.as[Option[Seq[String]]]("aggregators").getOrElse(Seq.empty)
    val intervalMs = aggConfig.as[Option[FiniteDuration]]("interval").map(_.toMillis).getOrElse(0L)
    val toleranceMs = aggConfig.as[Option[FiniteDuration]]("ooo-tolerance").map(_.toMillis).getOrElse(0L)
    AggregationConfig(ColumnAggregator.parseAll(aggregatorNames), intervalMs, toleranceMs)
  }

  /**
   * Parses the per-dataset AggregationConfig from a source config. If there is no `aggregation {}`
   * block, returns [[empty]] (an absent block disables aggregation).
   */
  def fromSourceConfig(sourceConfig: Config): AggregationConfig =
    if (sourceConfig.hasPath("aggregation")) apply(sourceConfig.getConfig("aggregation"))
    else empty
}
