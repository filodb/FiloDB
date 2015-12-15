package filodb.core.query

import java.net.InetAddress

import filodb.core.Types.ColumnId
import filodb.core.metadata.{KeyRange, Projection}

trait ScanInfo extends Serializable {

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
                             partition: Any) extends ScanInfo

case class SegmentedPartitionScanInfo(projection: Projection,
                                      columns: Seq[ColumnId],
                                      partition: Any,
                                      segmentRange: KeyRange[_]) extends SegmentedScanInfo

case class ScanSplit(scans:Seq[ScanInfo],
                     replicas:Seq[String] = ScanInfo.LOCAL.map(_.toString))
