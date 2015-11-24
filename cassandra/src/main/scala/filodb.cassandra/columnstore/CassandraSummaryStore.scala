package filodb.cassandra.columnstore

import com.typesafe.config.Config
import filodb.cassandra.util.TimeUUIDUtils
import filodb.core.Messages.Success
import filodb.core.metadata.{Projection, SegmentSummary}
import filodb.core.store.SummaryStore

import scala.concurrent.Future

class CassandraSummaryStore(config: Config) extends SummaryStore {
  val summaryTable = new SummaryTable(config)

  /**
   * Atomically compare and swap the new SegmentSummary for this SegmentID
   */
  override def compareAndSwapSummary(projection: Projection,
                                     partition: Any,
                                     segment: Any,
                                     oldVersion: Option[SegmentVersion],
                                     segmentVersion: SegmentVersion,
                                     segmentSummary: SegmentSummary): Future[Boolean] = {
    val pType = projection.partitionType
    val pk = pType.toBytes(partition.asInstanceOf[pType.T]).toByteBuffer
    val segmentId = segment.toString
    summaryTable.compareAndSetSummary(projection, pk, segmentId,
      oldVersion, newVersion, segmentSummary).map {
      case Success => true
      case _ => false
    }
  }

  override def readSegmentSummary(projection: Projection,
                                  partition: Any,
                                  segment: Any): Future[Option[(SegmentVersion, SegmentSummary)]] = {
    val pType = projection.partitionType
    val pk = pType.toBytes(partition.asInstanceOf[pType.T]).toByteBuffer
    val segmentId = segment.toString
    summaryTable.readSummary(projection, pk, segmentId)
  }

  override def newVersion: SegmentVersion = TimeUUIDUtils.getUniqueTimeUUIDinMillis
}
