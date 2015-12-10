package filodb.core.query

import java.net.InetAddress

import filodb.core.Types.ColumnId
import filodb.core.metadata.{KeyRange, Projection}

trait ScanInfo extends Serializable {

  def locationInfo: Seq[InetAddress]

  def projection: Projection

  def columns: Seq[ColumnId]
}

object ScanInfo {
  val LOCAL: Seq[InetAddress] = Seq(InetAddress.getLocalHost)
}

trait SegmentedScanInfo extends ScanInfo {
  def segmentRange: KeyRange[_]
}

case class PartitionScanInfo(projection: Projection,
                             columns: Seq[ColumnId],
                             partition: Any) extends ScanInfo {
  override def locationInfo: Seq[InetAddress] = ScanInfo.LOCAL
}

case class SegmentedPartitionScanInfo(projection: Projection,
                                      columns: Seq[ColumnId],
                                      partition: Any,
                                      segmentRange: KeyRange[_]) extends SegmentedScanInfo {
  override def locationInfo: Seq[InetAddress] = ScanInfo.LOCAL
}
