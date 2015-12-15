package filodb.cassandra.columnstore

import com.datastax.driver.core.Session
import com.websudos.phantom.connectors.KeySpace
import filodb.cassandra.cluster.{Cluster, NodeAddresses}
import filodb.cassandra.query.{SegmentedTokenRangeScanInfo, TokenRangeScanInfo}
import filodb.core.Types._
import filodb.core.metadata.{KeyRange, Projection}
import filodb.core.query.{PartitionScanInfo, ScanSplit, SegmentedPartitionScanInfo}
import filodb.core.store.QueryApi

import scala.concurrent.{ExecutionContext, Future}

trait CassandraQueryApi extends QueryApi {

  def session: Session

  def keySpace: KeySpace

  implicit val ec: ExecutionContext
  private lazy val nodeAddresses = new NodeAddresses(session)

  override def getScanSplits(splitCount: Int,
                             splitSize: Long,
                             projection: Projection,
                             columns: Seq[ColumnId],
                             partition: Option[Any],
                             segmentRange: Option[KeyRange[_]]): Future[Seq[ScanSplit]] = {
    val totalDataSize = splitCount * splitSize
    val allTokenRanges = Cluster.describeRing(keySpace.name, totalDataSize, session)
    partition match {
      case Some(pk) =>
        val replicas = allTokenRanges.flatMap(_.replicas.flatMap(nodeAddresses.hostNames))
        segmentRange match {
          case Some(range) =>
            Future(Seq(
              ScanSplit(Seq(SegmentedPartitionScanInfo(projection, columns, pk, range)), replicas)
            ))
          case None => Future(Seq(
            ScanSplit(Seq(PartitionScanInfo(projection, columns, pk)), replicas)
          ))
        }
      case None =>
        val groupedRanges =
          Cluster.getNodeGroupedTokenRanges(allTokenRanges, keySpace.name, splitSize)

        segmentRange match {
          case Some(range) =>
            Future(groupedRanges.map { group =>
              val replicas = group.map(_.replicas).reduce(_ intersect _)
                .flatMap(nodeAddresses.hostNames).toSeq
              ScanSplit(group.map(SegmentedTokenRangeScanInfo(projection, columns, _, range)), replicas)
            })
          case None =>
            Future(groupedRanges.map { group =>
              val replicas = group.map(_.replicas).reduce(_ intersect _)
                .flatMap(nodeAddresses.hostNames).toSeq
              ScanSplit(group.map(TokenRangeScanInfo(projection, columns, _)), replicas)
            })
        }
    }

  }

}
