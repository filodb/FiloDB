package filodb.core.metadata

import filodb.core.KeyType
import filodb.core.Types._

import scala.collection.mutable.ArrayBuffer


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
trait SegmentSummary {

  def segmentInfo: SegmentInfo

  def nextChunkId: ChunkId = numChunks

  def numChunks: Int = chunkSummaries.fold(0)(seq => seq.length)

  def chunkSummaries: Option[Seq[(ChunkId, ChunkSummary)]]

  def possibleOverrides(rowKeys: Seq[Any]): Option[Seq[ChunkId]] = {
    chunkSummaries map (seq => seq.map { case (it, summary) =>
      (it, rowKeys.count(i => summary.digest.contains(i)))
    }.filter { case (id, l) =>
      l > 0
    }.map(_._1))
  }

  def actualOverrides(rowKeys: Seq[Any], chunks: Seq[ChunkWithId]): Seq[(ChunkId, Seq[Int])] = {

    chunks.map { chunk =>
      val positions = ArrayBuffer[Int]()
      // this chunk is likely to have one of the rowKeys
      rowKeys.foreach { key =>
        val index = chunk.keys.indexOf(key)
        if (index > -1) positions += index
      }
      (chunk.chunkId, positions.toSeq)
    }
  }

  def withKeys(chunkId: ChunkId, keys: Seq[Any], range: KeyRange[Any]): (ChunkSummary, SegmentSummary)

}


case class DefaultSegmentSummary(keyType: KeyType,
                                 segmentInfo: SegmentInfo,
                                 chunkSummaries: Option[Seq[(ChunkId, ChunkSummary)]] = None)
  extends SegmentSummary {

  override def withKeys(chunkId: ChunkId, keys: Seq[Any], range: KeyRange[Any]): (ChunkSummary, SegmentSummary) = {
    val keyDigest = BloomDigest(keys, keyType)
    val newChunkSummary = ChunkSummary(keyDigest, keys.length, range)
    val newSummary = (chunkId, newChunkSummary)
    val newSummaries = chunkSummaries match {
      case Some(summaries) => summaries :+ newSummary
      case _ => List(newSummary)
    }
    (newChunkSummary, DefaultSegmentSummary(keyType, segmentInfo, Some(newSummaries)))
  }
}


/**
 * ChunkSummary is a quick summary of the number of rows, the key range(max and min keys) and the KeySetDigest
 * of a Chunk
 */
case class ChunkSummary(digest: KeySetDigest, numRows: Int, sortKeyRange: KeyRange[Any])


