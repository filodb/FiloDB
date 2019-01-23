package filodb.core.query

import org.jctools.maps.NonBlockingHashMapLong

import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.Dataset
import filodb.core.store._
import filodb.core.Types.ChunkID
import filodb.core.store.ChunkSetInfo._

/**
 * An index providing facilities to search through the chunks of a partition in different ways
 * NOTE: it focuses purely on the ChunkSetInfo, not on filters and other things
 */
trait PartitionChunkIndex {
  def dataset: Dataset
  def partKeyBase: Any
  def partKeyOffset: Long

  def numChunks: Int

  /**
   * Obtains a sequence of chunks where at least some of the rowkeys inside are within (startKey, endKey)
   * The ordering of the chunks returned depends on implementation.
   */
  def rowKeyRange(startKey: BinaryRecord, endKey: BinaryRecord): InfosSkipsIt

  /**
   * Returns all chunks in some order which is implementation-specific
   */
  def allChunks: InfosSkipsIt

  /**
   * Returns the latest N chunk infos
   */
  def latestN(n: Int): InfosSkipsIt

  /**
   * Returns the ChunkSetInfo and skips for a single chunk with startKey and id.
   * Depending on implementation, either only the id (which should be unique) or both may be used.
   * @return an iterator with a single item (ChunkSetInfo, skipArray) or empty iterator if not found
   */
  def singleChunk(startKey: BinaryRecord, id: ChunkID): InfosSkipsIt

  def findByMethod(method: ChunkScanMethod): InfosSkipsIt =
    method match {
      case AllChunkScan             => allChunks
      case RowKeyChunkScan(k1, k2)  => rowKeyRange(k1.binRec, k2.binRec)
      case WriteBufferChunkScan      => latestN(1)
      case InMemoryChunkScan        => allChunks
    }
}

/**
 * A PartitionChunkIndex that can incrementally update itself with new ChunkSetInfos.
 * Designed to be added to incrementally and kept in memory - this way it can lower the overhead of
 * repeated queries to the same partition.
 */
trait MutablePartitionChunkIndex extends PartitionChunkIndex {
  /**
   * Adds a ChunkSetInfo and the skips to the index
   */
  def add(info: ChunkSetInfo, skips: Seq[ChunkRowSkipIndex]): Unit

  /**
   * Removes a ChunkSetInfo.  May need to regenerate indices.
   */
  def remove(id: ChunkID): Unit
}

object PartitionChunkIndex {
  import ChunkSetInfo.SkipMap
  def newSkip(key: java.lang.Long): SkipMap = new SkipMap()
}

/**
 * An implementation of PartitionChunkIndex which orders things by time - thus rowKeyRange and allChunks
 * will return chunk infos in increasing time order

 * TODO: improve this implementation with binary bit indices such as JavaEWAH.
 */
class TimeBasedPartitionChunkIndex(val partKeyBase: Any, val partKeyOffset: Long, val dataset: Dataset)
extends MutablePartitionChunkIndex {
  import collection.JavaConverters._

  import filodb.core._
  import PartitionChunkIndex._
  import ChunkSetInfo._

  val skipRows = new NonBlockingHashMapLong[SkipMap](64)
  val infos = new java.util.TreeMap[(Long, ChunkID), ChunkSetInfo](
                                    Ordering[(Long, ChunkID)])

  def add(info: ChunkSetInfo, skips: Seq[ChunkRowSkipIndex]): Unit = {
    infos.put((info.startTime, info.id), info)
    for { skip <- skips } {
      skipRows.put(skip.id, skipRows.getOrElseUpdate(skip.id, newSkip).or(skip.overrides))
    }
  }

  def numChunks: Int = infos.size

  def rowKeyRange(startKey: BinaryRecord, endKey: BinaryRecord): InfosSkipsIt = {
    // Exclude chunks which start after the end search range
    infos.headMap((endKey.getLong(0), Long.MaxValue)).values.iterator.asScala.collect {
      case c: ChunkSetInfo if c.intersection(startKey.getLong(0), endKey.getLong(0)).isDefined =>
        (c, skipRows.get(c.id))
    }
  }

  def allChunks: InfosSkipsIt =
    infos.values.iterator.asScala.map { info =>
      (info, skipRows.get(info.id))
    }

  def singleChunk(startKey: BinaryRecord, id: ChunkID): InfosSkipsIt =
    infos.subMap((startKey.getLong(0), id), true, (startKey.getLong(0), id), true).values.iterator.asScala.map { info =>
      (info, skipRows.get(info.id))
    }

  // NOTE: latestN does not make sense for the TimeBasedPartitionChunkIndex.
  def latestN(n: Int): InfosSkipsIt = ???

  def remove(id: ChunkID): Unit = {
    infos.keySet.iterator.asScala
         .find(_._2 == id)
         .foreach(infos.remove)
    skipRows.remove(id)
  }
}

/**
 * A PartitionChunkIndex which is ordered by increasing ChunkID
 */
class ChunkIDPartitionChunkIndex(val partKeyBase: Any, val partKeyOffset: Long, val dataset: Dataset)
extends MutablePartitionChunkIndex {
  import collection.JavaConverters._

  val infosSkips = new java.util.TreeMap[ChunkID, (ChunkSetInfo, SkipMap)]

  def add(info: ChunkSetInfo, skips: Seq[ChunkRowSkipIndex]): Unit = {
    infosSkips.put(info.id, (info, emptySkips))
    for { skip <- skips } {
      Option(infosSkips.get(skip.id)) match {
        case Some((origInfo, origSkips)) =>
          infosSkips.put(skip.id, (origInfo, origSkips.or(skip.overrides)))
        case None                =>
      }
    }
  }

  def numChunks: Int = infosSkips.size

  def rowKeyRange(startKey: BinaryRecord, endKey: BinaryRecord): InfosSkipsIt = {
    // Linear search through all infos to find intersections
    // TODO: use an interval tree to speed this up?
    infosSkips.values.iterator.asScala.filter {
      case (info, skips) => info.intersection(startKey.getLong(0), endKey.getLong(0)).isDefined
    }
  }

  def allChunks: InfosSkipsIt = infosSkips.values.iterator.asScala

  def singleChunk(startKey: BinaryRecord, id: ChunkID): InfosSkipsIt =
    infosSkips.subMap(id, true, id, true).values.iterator.asScala

  def latestN(n: Int): InfosSkipsIt =
    infosSkips.descendingMap.values.iterator.asScala.take(n).toBuffer.reverse.toIterator

  def remove(id: ChunkID): Unit = {
    infosSkips.remove(id)
  }
}