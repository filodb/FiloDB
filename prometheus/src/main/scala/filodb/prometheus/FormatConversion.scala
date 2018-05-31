package filodb.prometheus

import remote.RemoteStorage.TimeSeries

import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.{Dataset, DatasetOptions}

/**
 * Methods to aid in converting Prometheus TimeSeries protos to BinaryRecord v2 records
 */
object FormatConversion {
  // An official Prometheus-format Dataset object with a single timestamp and value
  val dataset = Dataset("prometheus", Seq("tags:map"), Seq("timestamp:long", "value:double"))
                  .copy(options = DatasetOptions(Seq("__name__", "job"), "__name__", "value"))

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

  /**
   * Adds BinaryRecord v2 to the builder/containers, one per Sample per TimeSeries proto instance.
   * Prior to calling this, one should have called builder.sortAndComputeHashes().
   * @param builder a filodb.core.binaryrecord2.RecordBuilder with schema from the dataset method above
   * @param ts the Protobuf TimeSeries proto instance
   * @param sortedLabels labels from getLabels after calling sortAndComputeHashes(), which sorts them
   * @param hashes hashes from the output of sortAndComputeHashes() method
   */
  def addRecord(builder: RecordBuilder,
                ts: TimeSeries,
                sortedLabels: java.util.ArrayList[(String, String)],
                hashes: Array[Int]): Unit = {
    for { i <- 0 until ts.getSamplesCount } {
      val sample = ts.getSamples(i)
      builder.startNewRecord()
      builder.addLong(sample.getTimestampMs)
      builder.addDouble(sample.getValue)
      builder.addSortedPairsAsMap(sortedLabels, hashes)
      builder.endRecord()
    }
  }
}