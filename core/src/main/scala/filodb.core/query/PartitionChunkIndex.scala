package filodb.core.query

import org.velvia.filo.RowReader
import scala.collection.immutable.TreeSet
import scala.collection.mutable.HashMap

import filodb.core.Types.{BinaryPartition, ChunkID}
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.RichProjection
import filodb.core.store.{ChunkSetInfo, ChunkRowSkipIndex}

/**
 * A trait providing facilities to search through the chunks of a partition, as well as maintain the skiplist
 * for each chunk.
 * Designed to be added to incrementally and kept in memory - this way it can lower the overhead of
 * repeated queries to the same partition.
 * NOTE: it focuses purely on the ChunkSetInfo, not on filters and other things
 */
trait PartitionChunkIndex {
  def projection: RichProjection
  def binPartition: BinaryPartition

  /**
   * Adds a ChunkSetInfo and the skips to the index
   */
  def add(info: ChunkSetInfo, skips: Seq[ChunkRowSkipIndex]): Unit

  def numChunks: Int

  /**
   * Obtains a sequence of chunks where at least some of the rowkeys inside are within (startKey, endKey)
   * The ordering of the chunks returned depends on implementation.
   */
  def rowKeyRange(startKey: BinaryRecord, endKey: BinaryRecord): Iterator[(ChunkSetInfo, Array[Int])]

  /**
   * Returns all chunks in some order which is implementation-specific
   */
  def allChunks: Iterator[(ChunkSetInfo, Array[Int])]

  /**
   * Returns the ChunkSetInfo and skips for a single chunk with startKey and id.
   * Depending on implementation, either only the id (which should be unique) or both may be used.
   * @return an iterator with a single item (ChunkSetInfo, skipArray) or empty iterator if not found
   */
  def singleChunk(startKey: BinaryRecord, id: ChunkID): Iterator[(ChunkSetInfo, Array[Int])]
}

object PartitionChunkIndex {
  val emptySkips = Array[Int]()
}

/**
 * An implementation of PartitionChunkIndex which orders things by row key - thus rowKeyRange and allChunks
 * will return chunk infos in increasing startKey order

 * TODO: improve this implementation with binary bit indices such as JavaEWAH.
 */
class RowkeyPartitionChunkIndex(val binPartition: BinaryPartition, val projection: RichProjection)
extends PartitionChunkIndex {
  import collection.JavaConverters._
  implicit val ordering = projection.rowKeyType.rowReaderOrdering

  val skipRows = new HashMap[ChunkID, TreeSet[Int]].withDefaultValue(TreeSet[Int]())
  val infos = new java.util.TreeMap[(RowReader, ChunkID), ChunkSetInfo](
                                    Ordering[(RowReader, ChunkID)])

  def add(info: ChunkSetInfo, skips: Seq[ChunkRowSkipIndex]): Unit = {
    infos.put((info.firstKey, info.id), info)
    for { skipIndex <- skips } {
      skipRows(skipIndex.id) = skipRows(skipIndex.id) ++ skipIndex.overrides
    }
  }

  def numChunks: Int = infos.size

  def rowKeyRange(startKey: BinaryRecord, endKey: BinaryRecord): Iterator[(ChunkSetInfo, Array[Int])] = {
    // Exclude chunks which start after the end search range
    infos.headMap((endKey, Long.MaxValue)).values.iterator.asScala.collect {
      case c @ ChunkSetInfo(id, _, k1, k2) if c.intersection(startKey, endKey).isDefined =>
        (c, skipRows(id).toArray)
    }
  }

  def allChunks: Iterator[(ChunkSetInfo, Array[Int])] =
    infos.values.iterator.asScala.map { info =>
      (info, skipRows(info.id).toArray)
    }

  def singleChunk(startKey: BinaryRecord, id: ChunkID): Iterator[(ChunkSetInfo, Array[Int])] =
    infos.subMap((startKey, id), true, (startKey, id), true).values.iterator.asScala.map { info =>
      (info, skipRows(info.id).toArray)
    }
}