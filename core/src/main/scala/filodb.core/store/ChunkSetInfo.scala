package filodb.core.store

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import org.boon.primitive.{ByteBuf, InputByteArray}
import org.velvia.filo.{FastFiloRowReader, FiloRowReader, RowReader, SafeFiloRowReader, SeqRowReader}
import scala.collection.mutable.{ArrayBuffer, HashMap, TreeSet}
import scala.math.Ordered._
import scodec.bits._

import filodb.core._
import filodb.core.metadata.RichProjection
import filodb.core.Types._

/**
 * A ChunkSet is the set of chunks for all columns, one per column, serialized from a set of rows.
 * The rows should be ordered from firstKey to lastKey.
 * ChunkSetInfo records common metadata about a ChunkSet.
 */
case class ChunkSet(info: ChunkSetInfo,
                    skips: Seq[ChunkRowSkipIndex],
                    chunks: Map[ColumnId, ByteBuffer])

case class ChunkSetInfo(id: ChunkID,
                        numRows: Int,
                        firstKey: RowReader,
                        lastKey: RowReader) {
  /**
   * Finds intersection key ranges between two ChunkSetInfos.
   * Scenario A:    [       ]
   *                    [ other  ]
   * Scenario B:    [              ]
   *                    [ other ]
   * Scenario C:        [        ]
   *                 [  other ]
   * Scenario D:        [        ]
   *                 [  other      ]
   */
  def intersection(other: ChunkSetInfo)
                  (implicit ordering: Ordering[RowReader]): Option[(RowReader, RowReader)] = {
    if (lastKey >= other.firstKey && firstKey <= other.lastKey) {
      Some((if (firstKey > other.firstKey) firstKey else other.firstKey,
            if (lastKey < other.lastKey) lastKey else other.lastKey))
    } else {
      None
    }
  }
}

case class ChunkRowSkipIndex(id: ChunkID, overrides: Array[Int]) {
  // Unfortunately Arrays don't compare, we have to implement our own equality  :(
  override def equals(that: Any): Boolean = that match {
    case ChunkRowSkipIndex(otherId, otherOverrides) =>
      otherId == id && otherOverrides.sameElements(overrides)
    case other: Any =>
      false
  }

  override def hashCode: Int = super.hashCode
}

object ChunkSetInfo extends StrictLogging {
  type ChunkSkips = Seq[ChunkRowSkipIndex]
  type ChunkInfosAndSkips = Seq[(ChunkSetInfo, ChunkSkips)]

  def toBytes(projection: RichProjection, chunkSetInfo: ChunkSetInfo, skips: ChunkSkips): Array[Byte] = {
    val buf = ByteBuf.create(100)
    buf.add(chunkSetInfo.id)
    buf.add(chunkSetInfo.numRows)
    buf.writeMediumByteArray(rowKeyToBytes(projection, chunkSetInfo.firstKey).toArray)
    buf.writeMediumByteArray(rowKeyToBytes(projection, chunkSetInfo.lastKey).toArray)
    skips.foreach { case ChunkRowSkipIndex(id, overrides) =>
      buf.add(id)
      buf.writeMediumIntArray(overrides.toArray)
    }
    buf.toBytes
  }

  def fromBytes(projection: RichProjection, bytes: Array[Byte]): (ChunkSetInfo, ChunkSkips) = {
    val scanner = new InputByteArray(bytes)
    val id = scanner.readInt
    val numRows = scanner.readInt
    val firstKey = rowKeyFromBytes(projection, scanner.readMediumByteArray)
    val lastKey = rowKeyFromBytes(projection, scanner.readMediumByteArray)
    val skips = new ArrayBuffer[ChunkRowSkipIndex]
    while (scanner.location < bytes.size) {
      val skipId = scanner.readInt
      val skipList = scanner.readMediumIntArray
      skips.append(ChunkRowSkipIndex(skipId, skipList))
    }
    (ChunkSetInfo(id, numRows, firstKey, lastKey), skips)
  }

  // This is only necessary in the short term while we serialize keys to/from binary
  // It's super inefficient
  // In the future we move to BinaryRecord and this problem would be solved
  private def rowKeyToBytes(projection: RichProjection, key: RowReader): ByteVector = {
    projection.rowKeyType match {
      case c @ CompositeKeyType(atomTypes) =>
        val seq = atomTypes.zipWithIndex.map { case (atomTyp, i) => atomTyp.extractor.getField(key, i) }
        c.toBytes(seq)
      case keyTyp: SingleKeyTypeBase[_] =>
        keyTyp.toBytes(keyTyp.extractor.getField(key, 0))
    }
  }

  private def rowKeyFromBytes(projection: RichProjection, bytes: Array[Byte]): RowReader = {
    projection.rowKeyType.fromBytes(ByteVector(bytes)) match {
      case s: Seq[Any] => SeqRowReader(s)
      case o: Any      => SeqRowReader(Seq(o))
    }
  }

  def collectSkips(infos: ChunkInfosAndSkips): Seq[(ChunkSetInfo, Array[Int])] = {
    val skipRows = new HashMap[ChunkID, TreeSet[Int]].withDefaultValue(TreeSet[Int]())
    for { (info, skips) <- infos
          skipIndex     <- skips } {
      skipRows(skipIndex.id) = skipRows(skipIndex.id) ++ skipIndex.overrides
    }
    infos.map { case (info, _) =>
      (info, skipRows(info.id).toArray)
    }
  }

  /**
   * Scans previous ChunkSetInfos for possible row replacements.
   * TODO: replace with an interval tree algorithm.  Or the interval tree could be used to provide otherInfos
   * @param chunkInfo the chunkInfo with key range to compare against
   * @param chunkKeys array of Filo vector buffers in order of row key
   * @param otherInfos the list of other ChunkSetInfos in the segment to compare against for skips
   * @param rowKeysForChunk a function that retrieves row keys given a chunkID
   */
  // TODO: further filter using bloom filters
  // TODO: See if it is more efficient just to do a bloom filter intersection and filter out
  // non matches rather than hit every key
  def detectSkips(projection: RichProjection,
                  chunkInfo: ChunkSetInfo,
                  chunkKeys: Array[ByteBuffer],
                  otherInfos: Seq[ChunkSetInfo],
                  rowKeysForChunk: ChunkID => Array[ByteBuffer]): ChunkSkips = {
    implicit val ordering = projection.rowKeyType.rowReaderOrdering

    // Look through all chunkSetInfos (or subset from Interval tree) and compare the key ranges, filter
    // down to chunkSetInfos with intersecting keyranges
    val intersectingRanges = otherInfos.flatMap { info =>
      info.intersection(chunkInfo).map(range => (info.id, info.numRows, range))
    }
    logger.debug(s"Chunk $chunkInfo produced intersecting ranges ${intersectingRanges.toList}")

    val clazzes = projection.rowKeyColumns.map(_.columnType.clazz).toArray
    val reader = new FastFiloRowReader(chunkKeys, clazzes, chunkInfo.numRows)
    logger.debug(s"Reader parsers ${reader.parsers.toList}")

    // Match each key in range over bloom filter and return a list of hit rowkeys for each chunkID
    val hitKeysByChunk = intersectingRanges.map { case (chunkId, numRows, (key1, key2)) =>
      val (startPos, _) = binarySearchKeyChunks(reader, chunkInfo.numRows, ordering, key1)

      // Right now we are just going to slice all the keys from key1 to key2, but this would be filtered
      // by bloom filter in the future
      val keys = new ArrayBuffer[RowReader]()
      var curKey = SafeFiloRowReader(reader, startPos)
      while (curKey.rowNo < chunkInfo.numRows && ordering.lteq(curKey, key2)) {
        keys.append(curKey)
        curKey = SafeFiloRowReader(reader, curKey.rowNo + 1)
      }
      (chunkId, numRows, keys)
    }

    // For each matching chunkId and set of hit keys, find possible position to skip
    // NOTE: This will be very slow as will probably need to read back row keys from disk
    // Also, we are assuming there are very few keys that match, so binary search is effective.
    //   ie that (k log n) << n  where k = # of hit keys, and n is size of chunk
    // If above not true, then a linear scan in sort order and compare is more effective
    hitKeysByChunk.collect { case (chunkId, numRows, keys) if keys.nonEmpty =>
      val chunkKeys = rowKeysForChunk(chunkId)
      val overrides = keys.flatMap { key =>
        binarySearchKeyChunks(projection, chunkKeys, numRows, key) match {
          case (pos, true) => Some(pos)
          case (_,  false) => None
        }
      }.toArray
      ChunkRowSkipIndex(chunkId, overrides)
    }.filter(_.overrides.size > 0)
  }

  /**
   * Does a binary search through the chunks representing row keys in a segment, finding the position
   * equal to the given key or just greater than the given key, if the key is not matched
   * (ie where the nonmatched item would be inserted).
   * Note: we take advantage of the fact that row keys cannot have null values, so no need to null check.
   *
   * @param chunks an array of Filo Vector chunk bytes, in order of the projection.rowKeyColumns.
   * @param key    a RowReader representing the key to search for.  Must also have rowKeyColumns elements.
   * @return (position, true if exact match is found)  position might be equal to the number of rows in chunk
   *            if exact match not found and item compares greater than last item
   */
  def binarySearchKeyChunks(projection: RichProjection,
                            chunks: Array[ByteBuffer],
                            chunkLen: Int,
                            key: RowReader): (Int, Boolean) = {
    val clazzes = projection.rowKeyColumns.map(_.columnType.clazz).toArray
    val reader = new FastFiloRowReader(chunks, clazzes, chunkLen)
    val ordering = projection.rowKeyType.rowReaderOrdering
    binarySearchKeyChunks(reader, chunkLen, ordering, key)
  }

  // NOTE/TODO: The binary search algo below could be turned into a tail-recursive one, but be sure to do
  // a benchmark comparison first.  This is definitely in the critical path and we don't want a slowdown.
  // OTOH a tail recursive probably won't be the bottleneck.
  def binarySearchKeyChunks(reader: FiloRowReader,
                            chunkLen: Int,
                            ordering: Ordering[RowReader],
                            key: RowReader): (Int, Boolean) = {
    var len = chunkLen
    var first = 0
    while (len > 0) {
      val half = len >>> 1
      val middle = first + half
      reader.setRowNo(middle)
      val comparison = ordering.compare(reader, key)
      if (comparison == 0) {
        return (middle, true)
      } else if (comparison < 0) {
        first = middle + 1
        len = len - half - 1
      } else {
        len = half
      }
    }
    (first, ordering.equiv(reader, key))
  }
}