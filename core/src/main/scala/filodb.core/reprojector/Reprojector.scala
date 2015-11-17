package filodb.core.reprojector

import java.nio.ByteBuffer

import filodb.core.Types._
import filodb.core.metadata._
import org.velvia.filo.{RowReader, RowToVectorBuilder}


case class SegmentFlush[R, S](segmentInfo: SegmentInfo[R, S], rowKeys: Seq[R], columnVectors: Array[ByteBuffer])

trait Reprojector {

  def project[R, S](partitionKey: PartitionKey, projection: ProjectionInfo[R, S],
                    rows: Seq[RowReader]): Seq[SegmentFlush[R, S]]
}

object Reprojector extends Reprojector {

  override def project[R, S](partitionKey: PartitionKey, projection: ProjectionInfo[R, S],
                             rows: Seq[RowReader]): Seq[SegmentFlush[R, S]] = {

    implicit val sortOrder = projection.segmentType.ordering
    val groupedRows = rows.groupBy(projection.segmentFunction)

    groupedRows.map { case (segment, segmentRows) =>

      // For each segment grouping of rows... set up a SegmentInfo
      val segmentInfo = DefaultSegmentInfo(projection.dataset, partitionKey, segment, projection)
      // then write the rows as a chunk to the segment
      val columnVectorMap = RowToVectorBuilder.buildFromRows(segmentRows.iterator, projection.filoSchema)
      val columnVectors = new Array[ByteBuffer](projection.columns.length)
      projection.columns.zipWithIndex.foreach { case (c, i) => columnVectors(i) = columnVectorMap(c.name) }
      val rowKeys = segmentRows.map(projection.rowKeyFunction)
      SegmentFlush(segmentInfo, rowKeys, columnVectors)
    }.toSeq
  }


}
