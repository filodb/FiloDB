package filodb.core.query

import com.googlecode.javaewah.EWAHCompressedBitmap
import org.jctools.maps.NonBlockingHashMapLong
import scala.collection.immutable.TreeSet
import scala.collection.mutable.HashMap

import filodb.core.Types.{PartitionKey, ChunkID}
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.RichProjection
import filodb.core.store.{ChunkSetInfo, ChunkRowSkipIndex}
import ChunkSetInfo._

/**
 * A trait providing facilities to search through the chunks of a partition, as well as maintain the skiplist
 * for each chunk.
 * Designed to be added to incrementally and kept in memory - this way it can lower the overhead of
 * repeated queries to the same partition.
 * NOTE: it focuses purely on the ChunkSetInfo, not on filters and other things
 */
trait PartitionChunkIndex {
  def projection: RichProjection
  def binPartition: PartitionKey

  /**
   * Adds a ChunkSetInfo and the skips to the index
   */
  def add(info: ChunkSetInfo, skips: Seq[ChunkRowSkipIndex]): Unit

  def numChunks: Int

  /**
   * Obtains a sequence of chunks where at least some of the rowkeys inside are within (startKey, endKey)
   * The ordering of the chunks returned depends on implementation.
   */
  def rowKeyRange(startKey: BinaryRecord, endKey: BinaryRecord): Iterator[(ChunkSetInfo, SkipMap)]

  /**
   * Returns all chunks in some order which is implementation-specific
   */
  def allChunks: Iterator[(ChunkSetInfo, SkipMap)]

  /**
   * Returns the ChunkSetInfo and skips for a single chunk with startKey and id.
   * Depending on implementation, either only the id (which should be unique) or both may be used.
   * @return an iterator with a single item (ChunkSetInfo, skipArray) or empty iterator if not found
   */
  def singleChunk(startKey: BinaryRecord, id: ChunkID): Iterator[(ChunkSetInfo, SkipMap)]
}

object PartitionChunkIndex {
  val emptySkips = new SkipMap()

  def newSkip(key: java.lang.Long): SkipMap = new SkipMap()
}

/**
 * An implementation of PartitionChunkIndex which orders things by row key - thus rowKeyRange and allChunks
 * will return chunk infos in increasing startKey order

 * TODO: improve this implementation with binary bit indices such as JavaEWAH.
 */
class RowkeyPartitionChunkIndex(val binPartition: PartitionKey, val projection: RichProjection)
extends PartitionChunkIndex {
  import collection.JavaConverters._
  import PartitionChunkIndex._
  import filodb.core._

  val skipRows = new NonBlockingHashMapLong[SkipMap](64)
  val infos = new java.util.TreeMap[(BinaryRecord, ChunkID), ChunkSetInfo](
                                    Ordering[(BinaryRecord, ChunkID)])

  def add(info: ChunkSetInfo, skips: Seq[ChunkRowSkipIndex]): Unit = {
    infos.put((info.firstKey, info.id), info)
    for { skip <- skips } {
      skipRows.put(skip.id, skipRows.getOrElseUpdate(skip.id, newSkip).or(skip.overrides))
    }
  }

  def numChunks: Int = infos.size

  def rowKeyRange(startKey: BinaryRecord, endKey: BinaryRecord): Iterator[(ChunkSetInfo, SkipMap)] = {
    // Exclude chunks which start after the end search range
    infos.headMap((endKey, Long.MaxValue)).values.iterator.asScala.collect {
      case c @ ChunkSetInfo(id, _, k1, k2) if c.intersection(startKey, endKey).isDefined =>
        (c, skipRows.get(id))
    }
  }

  def allChunks: Iterator[(ChunkSetInfo, SkipMap)] =
    infos.values.iterator.asScala.map { info =>
      (info, skipRows.get(info.id))
    }

  def singleChunk(startKey: BinaryRecord, id: ChunkID): Iterator[(ChunkSetInfo, SkipMap)] =
    infos.subMap((startKey, id), true, (startKey, id), true).values.iterator.asScala.map { info =>
      (info, skipRows.get(info.id))
    }
}

/**
 * A PartitionChunkIndex which is ordered by increasing ChunkID
 */
class ChunkIDPartitionChunkIndex(val binPartition: PartitionKey, val projection: RichProjection)
extends PartitionChunkIndex {
  import collection.JavaConverters._
  import PartitionChunkIndex._
  import filodb.core._

  val skipRows = new NonBlockingHashMapLong[SkipMap](64)
  val infosSkips = new java.util.TreeMap[ChunkID, (ChunkSetInfo, SkipMap)]

  def add(info: ChunkSetInfo, skips: Seq[ChunkRowSkipIndex]): Unit = {
    infosSkips.put(info.id, (info, PartitionChunkIndex.emptySkips))
    for { skip <- skips } {
      val newSkips = skipRows.getOrElseUpdate(skip.id, newSkip).or(skip.overrides)
      skipRows.put(skip.id, newSkips)
      Option(infosSkips.get(skip.id)) match {
        case Some((origInfo, _)) => infosSkips.put(skip.id, (origInfo, newSkips))
        case None                =>
      }
    }
  }

  def numChunks: Int = infosSkips.size

  def rowKeyRange(startKey: BinaryRecord, endKey: BinaryRecord): Iterator[(ChunkSetInfo, SkipMap)] = {
    // Linear search through all infos to find intersections
    // TODO: use an interval tree to speed this up?
    infosSkips.values.iterator.asScala.filter {
      case (info, skips) => info.intersection(startKey, endKey).isDefined
    }
  }

  def allChunks: Iterator[(ChunkSetInfo, SkipMap)] = infosSkips.values.iterator.asScala

  def singleChunk(startKey: BinaryRecord, id: ChunkID): Iterator[(ChunkSetInfo, SkipMap)] =
    infosSkips.subMap(id, true, id, true).values.iterator.asScala
}