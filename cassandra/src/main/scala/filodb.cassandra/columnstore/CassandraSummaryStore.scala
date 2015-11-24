package filodb.cassandra.columnstore

import com.typesafe.config.Config
import filodb.cassandra.util.TimeUUIDUtils
import filodb.core.Messages.Success
import filodb.core.metadata.{SegmentInfo, SegmentSummary}
import filodb.core.store.SummaryStore

import scala.concurrent.Future

class CassandraSummaryStore(config: Config) extends SummaryStore {
  val summaryTable = new SummaryTable(config)

  /**
   * Atomically compare and swap the new SegmentSummary for this SegmentID
   */
  override def compareAndSwapSummary(oldVersion: SegmentVersion,
                                     segmentVersion: SegmentVersion,
                                     segmentInfo: SegmentInfo,
                                     segmentSummary: SegmentSummary): Future[Boolean] = {
    val projection = segmentInfo.projection
    val pType = projection.partitionType
    val pk = pType.toBytes(segmentInfo.partition.asInstanceOf[pType.T]).toByteBuffer
    val segmentId = segmentInfo.segment.toString
    summaryTable.compareAndSetSummary(projection, pk, segmentId, oldVersion, newVersion, segmentSummary).map {
      case Success => true
      case _ => false
    }
  }

  override def readSegmentSummary(segmentInfo: SegmentInfo): Future[Option[(SegmentVersion, SegmentSummary)]] = {
    val projection = segmentInfo.projection
    val pType = projection.partitionType
    val pk = pType.toBytes(segmentInfo.partition.asInstanceOf[pType.T]).toByteBuffer
    val segmentId = segmentInfo.segment.toString
    summaryTable.readSummary(projection, pk, segmentId)
  }

  override def newVersion: SegmentVersion = TimeUUIDUtils.getUniqueTimeUUIDinMillis
}
