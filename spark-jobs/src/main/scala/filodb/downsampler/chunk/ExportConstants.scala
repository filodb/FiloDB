package filodb.downsampler.chunk

/**
 * Iceberg Table columns derived from specific key labels from labels map
 */
object ExportSchemaConstants {
  val WORKSPACE = "_ws_"
  val NAMESPACE = "_ns_"
  val NAME = "__name__"
  val METRIC = "_metric_"
}
