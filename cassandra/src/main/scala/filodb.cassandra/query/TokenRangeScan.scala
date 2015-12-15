package filodb.cassandra.query

import java.net.InetAddress

import com.datastax.driver.core.{TokenRange => DriverTokenRange}
import filodb.core.Types._
import filodb.core.metadata.{KeyRange, Projection}
import filodb.core.query.{ScanInfo, SegmentedScanInfo}

case class TokenRangeScanInfo(projection: Projection,
                              columns: Seq[ColumnId],
                              tokenRange: TokenRange) extends ScanInfo

case class SegmentedTokenRangeScanInfo(projection: Projection,
                                       columns: Seq[ColumnId],
                                       tokenRange: TokenRange,
                                       segmentRange: KeyRange[_]) extends SegmentedScanInfo


case class TokenRange(start: Long, end: Long,
                      replicas: Set[InetAddress],
                      dataSize: Long) {

  def isWrapAround: Boolean =
    start >= end

  def unwrap(): Seq[TokenRange] = {
    val minToken = Long.MinValue
    if (isWrapAround) {
      Seq(
        TokenRange(start, minToken, replicas, dataSize / 2),
        TokenRange(minToken, end, replicas, dataSize / 2))
    } else {
      Seq(this)
    }
  }
}

