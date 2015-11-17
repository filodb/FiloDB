package filodb.core.metadata

import filodb.core.Types._
import filodb.core._


case class KeyRange[K](start: K, end: K, endExclusive: Boolean = true)


trait SegmentInfo[R, S] {

  def segment: S

  def dataset: TableName

  def projection: ProjectionInfo[R, S]

  def partition: PartitionKey

  def columns:Seq[Column] = projection.columns

  override def toString: String = s"Segment($dataset : $partition / ${segment}) columns(${columns.mkString(",")})"
}


case class DefaultSegmentInfo[R, S](dataset: Types.TableName,
                                    partition: Types.PartitionKey,
                                    segment: S, projection: ProjectionInfo[R, S]) extends SegmentInfo[R, S]


trait Segment[R, S] {

  def projection:ProjectionInfo[R,S] = segmentInfo.projection

  def columns:Seq[Column] = segmentInfo.columns

  def segmentInfo: SegmentInfo[R,S]

  // chunks are time ordered
  def chunks: Seq[Chunk[R, S]]

  def numRows:Int = chunks.map(_.numRows).sum

  def numChunks:Int = chunks.length

}


case class DefaultSegment[R, S](segmentInfo: SegmentInfo[R,S],
                                chunks: Seq[Chunk[R, S]]) extends Segment[R, S]
