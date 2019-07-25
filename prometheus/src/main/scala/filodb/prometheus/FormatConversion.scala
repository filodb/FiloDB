package filodb.prometheus

import remote.RemoteStorage.TimeSeries

import filodb.core.metadata.{Dataset, DatasetOptions}

/**
 * Methods to aid in converting Prometheus TimeSeries protos to BinaryRecord v2 records
 */
object FormatConversion {
  // An official Prometheus-format Dataset object with a single timestamp and value
  val options = DatasetOptions(Seq("__name__", "_ns"),
                    "__name__", Map("__name__" -> Seq("_bucket", "_count", "_sum")), Seq("le"),
                    Map("exporter" -> "_ns", "job" -> "_ns"))
  val dataset = Dataset("prometheus", Seq("tags:map"), Seq("timestamp:ts", "value:double"), options)

  /**
   * Extracts a java ArrayList of labels from the TimeSeries
   */
  def getLabels(ts: TimeSeries): java.util.ArrayList[(String, String)] = {
    val list = new java.util.ArrayList[(String, String)]()
    for { i <- 0 until ts.getLabelsCount } {
      val labelPair = ts.getLabels(i)
      list.add((labelPair.getName, labelPair.getValue))
    }
    list
  }
}