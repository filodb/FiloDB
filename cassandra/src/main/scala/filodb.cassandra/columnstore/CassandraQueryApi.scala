package filodb.cassandra.columnstore

import com.datastax.driver.core.Session
import com.websudos.phantom.connectors.KeySpace
import filodb.cassandra.cluster.{Cluster, NodeAddresses}
import filodb.cassandra.query.{SegmentedTokenRangeScanInfo, TokenRange, TokenRangeScanInfo}
import filodb.core.Types._
import filodb.core.metadata.{KeyRange, Projection}
import filodb.core.query.{PartitionScanInfo, ScanSplit, SegmentedPartitionScanInfo}
import filodb.core.store.QueryApi
import filodb.core.util.FiloLogging

import scala.concurrent.{ExecutionContext, Future}

trait CassandraQueryApi extends QueryApi with FiloLogging {

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
    val projectionId = projection.id
    val dataSetName = projection.dataset

    val debugMessage =
      s"""Getting scan splits for
         |$splitCount splits with size $splitSize
          |for projection $projectionId of dataset $dataSetName"""
    flow.debug(debugMessage)

    val totalDataSize = splitCount * splitSize
    val allTokenRanges = Cluster.describeRing(keySpace.name, totalDataSize, session)
    partition match {
      case Some(pk) =>
        withPartition(projection, columns, segmentRange, allTokenRanges, pk)
      case None =>
        withTokenRange(splitSize, projection, columns, segmentRange, allTokenRanges)
    }

  }

  private def withTokenRange(splitSize: Long,
                     projection: Projection,
                     columns: Seq[ColumnId],
                     segmentRange: Option[KeyRange[_]],
                     allTokenRanges: Seq[TokenRange]): Future[Seq[ScanSplit]] = {
    val groupedRanges =
      Cluster.getNodeGroupedTokenRanges(allTokenRanges, keySpace.name, splitSize)

    segmentRange match {
      case Some(range) =>
        flow.debug(s"Token range scan with segment range $range")
        Future(groupedRanges.map { group =>
          val replicas = group.map(_.replicas).reduce(_ intersect _)
            .flatMap(nodeAddresses.hostNames).toSeq
          ScanSplit(group.map(SegmentedTokenRangeScanInfo(projection, columns, _, range)), replicas)
        })
      case None =>
        flow.debug(s"Full Token range scan")
        Future(groupedRanges.map { group =>
          val replicas = group.map(_.replicas).reduce(_ intersect _)
            .flatMap(nodeAddresses.hostNames).toSeq
          ScanSplit(group.map(TokenRangeScanInfo(projection, columns, _)), replicas)
        })
    }
  }

  private def withPartition(projection: Projection,
                    columns: Seq[ColumnId],
                    segmentRange: Option[KeyRange[_]],
                    allTokenRanges: Seq[TokenRange],
                    pk: Any): Future[Seq[ScanSplit]] = {
    val replicas = allTokenRanges.flatMap(_.replicas.flatMap(nodeAddresses.hostNames))
    segmentRange match {
      case Some(range) =>
        flow.debug(s"Scan with partition $pk and segment range $range")
        Future(Seq(
          ScanSplit(Seq(SegmentedPartitionScanInfo(projection, columns, pk, range)), replicas)
        ))
      case None =>
        flow.debug(s"Scan with partition $pk")
        Future(Seq(
          ScanSplit(Seq(PartitionScanInfo(projection, columns, pk)), replicas)
        ))
    }
  }
}
