package filodb.cassandra.columnstore

import com.datastax.driver.core.Session
import com.websudos.phantom.connectors.KeySpace
import filodb.cassandra.query.{Cluster, SegmentedTokenRangeScanInfo, TokenRangeScanInfo}
import filodb.core.Types._
import filodb.core.metadata.{KeyRange, Projection}
import filodb.core.query.{PartitionScanInfo, ScanInfo, SegmentedPartitionScanInfo}
import filodb.core.store.QueryApi

import scala.concurrent.{ExecutionContext, Future}

trait CassandraQueryApi extends QueryApi {

  def session: Session

  def keySpace: KeySpace

  implicit val ec: ExecutionContext

  override def getScanSplits(splitCount: Int,
                             splitSize: Long,
                             projection: Projection,
                             columns: Seq[ColumnId],
                             partition: Option[Any],
                             segmentRange: Option[KeyRange[_]]): Future[Seq[Seq[ScanInfo]]] = {
    partition match {
      case Some(pk) => segmentRange match {
        case Some(range) => Future(Seq(Seq(
          SegmentedPartitionScanInfo(projection, columns, pk, range)
        )))
        case None => Future(Seq(Seq(
          PartitionScanInfo(projection, columns, pk)
        )))
      }
      case None =>
        val groupedRanges =
          Cluster.getClusterTokenRanges(session, keySpace.name, splitCount, splitSize)

        segmentRange match {
          case Some(range) =>
            Future(groupedRanges.map(group =>
              group.map(SegmentedTokenRangeScanInfo(projection, columns, _, range))))
          case None =>
            Future(groupedRanges.map(group =>
              group.map(TokenRangeScanInfo(projection, columns, _))))
        }
    }

  }
}
