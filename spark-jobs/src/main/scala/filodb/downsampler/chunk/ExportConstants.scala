package filodb.downsampler.chunk

/**
 * Iceberg Table columns derived from specific key labels from labels map
 */
object ExportConstants {
  // labels keys in timeseries
  val LABEL_NAME = "__name__"
  val LABEL_METRIC = "_metric_"
  val LABEL_LE = "le"
  // column names in iceberg table
  val COL_LABELS = "labels"
  val COL_METRIC = "metric"
  val COL_EPOCH_TIMESTAMP = "epoch_timestamp"
  val COL_TIMESTAMP = "timestamp"
  val COL_VALUE = "value"
  val COL_YEAR = "year"
  val COL_MONTH = "month"
  val COL_DAY = "day"
  val COL_HOUR = "hour"
}
