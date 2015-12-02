package filodb.core.metadata

import java.io._
import java.nio.ByteBuffer

import filodb.core.KeyType
import filodb.core.Types._
import it.unimi.dsi.io.ByteBufferInputStream
import scodec.bits.ByteVector

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

  def actualOverrides(rowKeys: Seq[Any], chunks: Seq[(ChunkId, Seq[Any])]): Seq[(ChunkId, Seq[Int])] = {

    chunks.map { chunk =>
      val positions = ArrayBuffer[Int]()
      // this chunk is likely to have one of the rowKeys
      rowKeys.foreach { key =>
        val index = chunk._2.indexOf(key)
        if (index > -1) positions += index
      }
      (chunk._1, positions.toSeq)
    }
  }

  def withKeys(chunkId: ChunkId, keys: Seq[Any]): SegmentSummary

  def toBytes: ByteBuffer = {
    val baos = new ByteArrayOutputStream()
    val os = new DataOutputStream(baos)
    chunkSummaries match {
      case Some(summaries) => {
        summaries.foreach { case (cid, summary) =>
          os.writeInt(cid)
          summary.write(os)
        }
      }
      case None => os.writeInt(0)
    }
    os.flush()
    baos.flush()
    ByteBuffer.wrap(baos.toByteArray)

  }

}

object SegmentSummary {
  def fromBytes(keyType: KeyType, bb: ByteBuffer): SegmentSummary = {
    val in = new DataInputStream(new ByteBufferInputStream(bb))
    val length = in.readInt()
    if (length > 0) {
      val chunkSummaries = (0 until length).map { i =>
        val chunkId = in.readInt()
        val chunkSummary = ChunkSummary.read(in, keyType)
        (chunkId, chunkSummary)
      }
      DefaultSegmentSummary(keyType, Some(chunkSummaries))
    } else {
      DefaultSegmentSummary(keyType, None)
    }
  }
}


case class DefaultSegmentSummary(keyType: KeyType,
                                 chunkSummaries: Option[Seq[(ChunkId, ChunkSummary)]] = None)
  extends SegmentSummary {

  override def withKeys(chunkId: ChunkId, keys: Seq[Any]): SegmentSummary = {
    val keyDigest = BloomDigest(keys, keyType)
    val newChunkSummary = ChunkSummary(keyDigest, keys.length)
    val newSummary = (chunkId, newChunkSummary)
    val newSummaries = chunkSummaries match {
      case Some(summaries) => summaries :+ newSummary
      case _ => List(newSummary)
    }
    DefaultSegmentSummary(keyType, Some(newSummaries))
  }
}


/**
 * ChunkSummary is a quick summary of the number of rows, the key range(max and min keys) and the KeySetDigest
 * of a Chunk
 */
case class ChunkSummary(digest: KeySetDigest, numRows: Int) {
  def write(out: DataOutput): Unit = {
    val bytes = digest.toBytes
    out.writeInt(bytes.length)
    out.write(bytes.toArray)
    out.writeInt(numRows)
  }
}

object ChunkSummary {
  def read(in: DataInput, keyType: KeyType): ChunkSummary = {
    val numBytes = in.readInt()
    val byteArr = new Array[Byte](numBytes)
    in.readFully(byteArr)
    val digest = BloomDigest(ByteVector(byteArr), keyType)
    val numRows = in.readInt()
    ChunkSummary(digest, numRows)
  }
}


