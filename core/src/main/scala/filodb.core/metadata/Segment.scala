package filodb.core.metadata

import filodb.core.Types._
import filodb.core._


case class KeyRange[+K](start: K, end: K, endExclusive: Boolean = true)


trait SegmentInfo {

  def segment: Any

  def dataset: TableName

  def projection: Projection

  def partition: Any

  def columns: Seq[Column] = projection.schema

  override def toString: String = s"Segment($dataset : $partition / $segment) columns(${columns.mkString(",")})"
}


case class DefaultSegmentInfo(dataset: Types.TableName,
                              partition: Any,
                              segment: Any, projection: Projection) extends SegmentInfo


trait Segment {

  def projection: Projection = segmentInfo.projection

  def columns: Seq[Column] = segmentInfo.columns

  def segmentInfo: SegmentInfo

  // chunks are time ordered
  def chunks: Seq[ChunkWithMeta]

  def numRows: Int = chunks.map(_.numRows).sum

  def numChunks: Int = chunks.length

}


case class DefaultSegment(segmentInfo: SegmentInfo, chunks: Seq[ChunkWithMeta]) extends Segment
