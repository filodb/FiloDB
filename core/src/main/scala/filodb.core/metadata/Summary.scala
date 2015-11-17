package filodb.core.metadata

import filodb.core.KeyType
import filodb.core.Types._


/**
 * SegmentSummary holds summary about the chunks within a segment. It contains a ChunkSummary of each chunk
 * which is written to this segment. Each SegmentSummary has a version based on which the overrides of a new incoming
 * chunk are calculated.
 *
 * This SegmentSummaryVersion may also be used to update a SegmentSummary in a SegmentStore in a MVCC fashion
 * using Compare and Swap.
 *
 * When a segment is read, its SegmentSummary helps read the data in a cache friendly manner by allowing it to skip
 * rows in earlier chunks which have been replaced as a result of writing successive chunks.
 *
 */
trait SegmentSummary[R, S] {

  def segmentInfo: SegmentInfo[R, S]

  def nextChunkId: ChunkId = numChunks

  def numChunks: Int = chunkSummaries.fold(0)(seq => seq.length)

  def chunkSummaries: Option[Seq[(ChunkId, ChunkSummary[R])]]

  def buildOverrides(implicit helper: KeyType[R],
                     rowKeys: Seq[R], keyRange: KeyRange[R]): Option[Seq[(ChunkId, Seq[R])]] = {
    chunkSummaries map (seq => seq.filter { case (x, summary) =>
      summary.isOverlap(keyRange)
    }.map { case (it, summary) =>
      (it, rowKeys.filter(i => summary.digest.contains(i)))
    })
  }

  def withSortKeys(chunkId: ChunkId, rowKeys: Seq[R]): (ChunkSummary[R], SegmentSummary[R, S])

}


case class DefaultSegmentSummary[R, S](helper: KeyType[R], segmentInfo: SegmentInfo[R, S],
                                       chunkSummaries: Option[Seq[(ChunkId, ChunkSummary[R])]] = None)
  extends SegmentSummary[R, S] {

  override def withSortKeys(chunkId: ChunkId, rowKeys: Seq[R]): (ChunkSummary[R], SegmentSummary[R, S]) = {
    val keyDigest = BloomDigest(rowKeys, helper)
    val newChunkSummary = ChunkSummary(helper, keyDigest, rowKeys.length, KeyRange(rowKeys.head, rowKeys.last))
    val newSummary = (chunkId, newChunkSummary)
    val newSummaries = chunkSummaries match {
      case Some(summaries) => summaries :+ newSummary
      case _ => List(newSummary)
    }
    (newChunkSummary, DefaultSegmentSummary(helper, segmentInfo, Some(newSummaries)))
  }
}


/**
 * ChunkSummary is a quick summary of the number of rows, the key range(max and min keys) and the KeySetDigest
 * of a Chunk
 */
case class ChunkSummary[K](helper: KeyType[K], digest: KeySetDigest, numRows: Int, keyRange: KeyRange[K]) {

  import scala.math.Ordered._

  def isOverlap(other: KeyRange[K]): Boolean = {
    implicit val ordering: Ordering[K] = helper.ordering
    keyRange.end > other.start && keyRange.start < other.end
  }
}


