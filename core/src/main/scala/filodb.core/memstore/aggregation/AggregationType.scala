package filodb.core.memstore.aggregation

/**
 * Sealed trait representing different types of aggregations supported for time-bucketed samples.
 * Each type determines how multiple samples within a time bucket are combined.
 */
sealed trait AggregationType

object AggregationType {
  /**
   * Sum aggregation - adds all values in the bucket
   */
  case object Sum extends AggregationType

  /**
   * Average aggregation - computes mean of all values in the bucket
   */
  case object Avg extends AggregationType

  /**
   * Minimum aggregation - finds smallest value in the bucket
   */
  case object Min extends AggregationType

  /**
   * Maximum aggregation - finds largest value in the bucket
   */
  case object Max extends AggregationType

  /**
   * Last aggregation - keeps the most recent value (by timestamp) in the bucket
   */
  case object Last extends AggregationType

  /**
   * First aggregation - keeps the earliest value (by timestamp) in the bucket
   */
  case object First extends AggregationType

  /**
   * Count aggregation - counts number of samples in the bucket
   */
  case object Count extends AggregationType

  /**
   * Histogram aggregation - accumulates histogram values within the bucket
   * Uses MutableHistogram to add bucket values from multiple samples.
   * Use for delta temporality histograms.
   */
  case object HistogramSum extends AggregationType

  /**
   * Histogram last aggregation - keeps the most recent histogram by timestamp
   * Use for cumulative temporality histograms (e.g., Prometheus histograms)
   * where values are monotonically increasing counters.
   */
  case object HistogramLast extends AggregationType

  /**
   * Parses an aggregation type from a string.
   * @param s the string to parse (case-insensitive)
   * @return Some(AggregationType) if valid, None otherwise
   */
  def parse(s: String): Option[AggregationType] = s.toLowerCase match {
    case "sum"             => Some(Sum)
    case "avg" | "average" => Some(Avg)
    case "min"             => Some(Min)
    case "max"             => Some(Max)
    case "last"            => Some(Last)
    case "first"           => Some(First)
    case "count"           => Some(Count)
    case "histogram" | "histogram_sum" => Some(HistogramSum)
    case "histogram_last"              => Some(HistogramLast)
    case _                 => None
  }

  /**
   * Returns all supported aggregation types as a string for error messages.
   */
  def supportedTypes: String = "sum, avg, min, max, last, first, count, histogram_sum, histogram_last"
}

/**
 * Configuration for aggregation on a specific column.
 *
 * @param columnIndex the index of the column in the schema
 * @param aggType the type of aggregation to perform
 * @param intervalMs the time bucket interval in milliseconds
 * @param oooToleranceMs the out-of-order tolerance window in milliseconds
 */
case class AggregationConfig(
  columnIndex: Int,
  aggType: AggregationType,
  intervalMs: Long,
  oooToleranceMs: Long
) {
  require(intervalMs > 0, s"Aggregation interval must be positive, got $intervalMs")
  require(oooToleranceMs >= 0, s"Out-of-order tolerance must be non-negative, got $oooToleranceMs")
  require(columnIndex >= 0, s"Column index must be non-negative, got $columnIndex")

  override def toString: String =
    s"AggregationConfig(col=$columnIndex, type=$aggType, interval=${intervalMs}ms, tolerance=${oooToleranceMs}ms)"
}
