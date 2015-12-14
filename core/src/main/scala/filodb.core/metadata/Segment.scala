package filodb.core.metadata


case class KeyRange[+K](start: Option[K],
                        end: Option[K],
                        startExclusive: Boolean = false,
                        endExclusive: Boolean = false)

trait Segment {

  def projection: Projection

  def segmentId: Any

  // chunks are time ordered
  def chunks: Seq[ChunkWithMeta]

  def numRows: Int = chunks.map(_.numRows).sum

  def numChunks: Int = chunks.length

  override def toString: String = s"Segment($segmentId) chunks($numChunks) rows($numRows)"

}


case class DefaultSegment(projection: Projection,
                          partition: Any,
                          segmentId: Any,
                          chunks: Seq[ChunkWithMeta]) extends Segment
