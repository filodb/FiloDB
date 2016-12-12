package filodb.core.store

import bloomfilter.mutable.BloomFilter
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import kamon.Kamon
import org.boon.primitive.{ByteBuf, InputByteArray}
import org.velvia.filo._
import scala.collection.mutable.{ArrayBuffer, HashMap, TreeSet}
import scala.math.Ordered._
import scodec.bits.ByteVector

import filodb.core._
import filodb.core.binaryrecord.{BinaryRecord, RecordSchema}
import filodb.core.metadata.RichProjection
import filodb.core.Types._

/**
 * A ChunkSet is the set of chunks for all columns, one per column, serialized from a set of rows.
 * The rows should be ordered from firstKey to lastKey.
 * ChunkSetInfo records common metadata about a ChunkSet.
 */
case class ChunkSet(info: ChunkSetInfo,
                    skips: Seq[ChunkRowSkipIndex],
                    bloomFilter: BloomFilter[Long],
                    rowKeys: Array[BinaryRecord],
                    chunks: Map[ColumnId, ByteBuffer])

object ChunkSet extends StrictLogging {
  val chunkSetsCreated = Kamon.metrics.counter("chunksets-created")
  val chunkSetsRowKeyCount = Kamon.metrics.histogram("chunksets-incoming-n-rowkeys")
  val chunkSetsFilteredKeyCount = Kamon.metrics.histogram("chunksets-filtered-n-rowkeys")
  val chunkSetsHitKeyCount = Kamon.metrics.histogram("chunksets-hit-n-rowkeys")
  val chunkSetsNeedReadRowKey = Kamon.metrics.counter("chunksets-need-read-row-keys")

  /**
   * Creates a new ChunkSet with empty skipList, based on existing state
   * @param state a SegmentState instance
   * @param rows rows to be chunkified sorted in order of rowkey
   * Pure, does not modify existing state.  User is responsible for updating the SegmentState.
   */
  def apply(state: SegmentState, rows: Iterator[RowReader]): ChunkSet = {
    // NOTE: some RowReaders, such as FastFiloRowReader, must be iterators
    // since rowNo in FastFiloRowReader is mutated.
    val builder = new RowToVectorBuilder(state.filoSchema)
    val rowKeys = rows.map { row =>
      builder.addRow(row)
      state.makeRowKey(row)
    }.toArray

    require(rowKeys.nonEmpty)
    chunkSetsCreated.increment

    val chunkMap = builder.convertToBytes()
    val info = ChunkSetInfo(state.nextChunkId, rowKeys.size, rowKeys.head, rowKeys.last)
    chunkSetsRowKeyCount.record(rowKeys.size)
    ChunkSet(info, Nil, state.makeBloomFilter(rowKeys), rowKeys, chunkMap)
  }

  /**
   * Same as above, but detects skips also using the current segment state.
   */
  def withSkips(state: SegmentState, rows: Iterator[RowReader]): ChunkSet = {
    val initChunkSet = apply(state, rows)
    // Now filter row keys in master bloom filter.  Should reduce # of keys significantly.
    val filteredKeys = state.filterRowKeys(initChunkSet.rowKeys)
    logger.debug(s"chunk ${initChunkSet.info}: filtered ${initChunkSet.rowKeys.size} rowKeys to " +
                 s"${filteredKeys.size} bloom hits")
    chunkSetsFilteredKeyCount.record(filteredKeys.size)
    initChunkSet.copy(skips = ChunkSetInfo.detectSkips(state, filteredKeys))
  }
}

case class ChunkSetInfo(id: ChunkID,
                        numRows: Int,
                        firstKey: BinaryRecord,
                        lastKey: BinaryRecord) extends StrictLogging {
  def keyAndId: (BinaryRecord, ChunkID) = (firstKey, id)

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
                  (implicit ordering: Ordering[RowReader]): Option[(BinaryRecord, BinaryRecord)] =
    try {
      intersection(other.firstKey, other.lastKey)
    } catch {
      case e: Exception =>
        logger.warn(s"Got error comparing $this and $other...", e)
        None
    }

  /**
   * Finds the intersection between this ChunkSetInfo and a range of keys (key1, key2)
   */
  def intersection(key1: BinaryRecord, key2: BinaryRecord)
                  (implicit ordering: Ordering[RowReader]): Option[(BinaryRecord, BinaryRecord)] = {
    if (ordering.gt(key1, key2)) {
      None
    } else if (ordering.gteq(lastKey, key1) && ordering.lteq(firstKey, key2)) {
      Some((if (ordering.gt(firstKey, key1)) firstKey else key1,
            if (ordering.lt(lastKey,  key2)) lastKey else key2))
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
  type IndexAndFilterSeq = Seq[(ChunkSetInfo, ChunkSkips, BloomFilter[Long])]
  import ChunkSet._

  val DummyKey = BinaryRecord(RecordSchema(Nil), Array[Byte]())

  /**
   * Designed only for special, non-functional ChunkSetInfos, such as special markers
   */
  def dummyInfo(id: ChunkID): ChunkSetInfo = ChunkSetInfo(id, -1, DummyKey, DummyKey)

  val missingBloomFilters = Kamon.metrics.counter("chunks-missing-bloom-filter")

  def toBytes(projection: RichProjection, chunkSetInfo: ChunkSetInfo, skips: ChunkSkips): Array[Byte] = {
    val buf = ByteBuf.create(100)
    buf.writeLong(chunkSetInfo.id)
    buf.writeInt(chunkSetInfo.numRows)
    buf.writeMediumByteArray(chunkSetInfo.firstKey.bytes)
    buf.writeMediumByteArray(chunkSetInfo.lastKey.bytes)
    skips.foreach { case ChunkRowSkipIndex(id, overrides) =>
      buf.writeLong(id)
      buf.writeMediumIntArray(overrides.toArray)
    }
    buf.toBytes
  }

  def fromBytes(projection: RichProjection, bytes: Array[Byte]): (ChunkSetInfo, ChunkSkips) = {
    val scanner = new InputByteArray(bytes)
    val id = scanner.readLong
    val numRows = scanner.readInt
    val firstKey = BinaryRecord(projection, scanner.readMediumByteArray)
    val lastKey = BinaryRecord(projection, scanner.readMediumByteArray)
    val skips = new ArrayBuffer[ChunkRowSkipIndex]
    while (scanner.location < bytes.size) {
      val skipId = scanner.readLong
      val skipList = scanner.readMediumIntArray
      skips.append(ChunkRowSkipIndex(skipId, skipList))
    }
    (ChunkSetInfo(id, numRows, firstKey, lastKey), skips)
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
   * @param state a current SegmentState instance holding bloom filters and chunkSetInfos
   * @param rowKeys array of BinaryRecord rowkeys to detect skips for.  May have been filtered.
   * @param infosAndFilters list of ChunkSetInfo and associated BloomFilter
   * @param rowKeysForChunk a function that retrieves row keys given a chunkID
   */
  def detectSkips(state: SegmentState,
                  rowKeys: Array[BinaryRecord]): ChunkSkips = {
    if (rowKeys.isEmpty) {
      Nil
    } else {
      implicit val ordering = state.projection.rowKeyType.rowReaderOrdering
      val keyInfo = ChunkSetInfo(-1, 0, rowKeys.head, rowKeys.last)

      // Check for rowkey range intersection
      // Match each key in range over bloom filter and return a list of hit rowkeys for each chunkID
      var numHitKeys = 0
      val hitKeysByChunk = state.infos.flatMap { info =>
        val bfOpt = state.filter(info.id)
        keyInfo.intersection(info).map { case (key1, key2) =>
          // Ignore the key, it's probably faster to just hit keys against bloom filter
          val hitKeys = bfOpt.map { bf => rowKeys.filter { k => bf.mightContain(k.cachedHash64) } }
                             .getOrElse {
                               missingBloomFilters.increment
                               logger.info(s"OUCH!  Missing bloom filter for chunk $info...")
                               rowKeys
                             }
          logger.debug(s"Checking chunk $info: ${hitKeys.size} hitKeys")
          numHitKeys += hitKeys.size
          (info, hitKeys)
        }.filter(_._2.nonEmpty)
      }.toBuffer

      // For each matching chunkId and set of hit keys, find possible position to skip
      // NOTE: This will be very slow as will probably need to read back row keys from disk
      // Also, we are assuming there are very few keys that match, so binary search is effective.
      //   ie that (k log n) << n  where k = # of hit keys, and n is size of chunk
      // If above not true, then a linear scan in sort order and compare is more effective
      if (numHitKeys > 0) {
        chunkSetsNeedReadRowKey.increment
        chunkSetsHitKeyCount.record(numHitKeys)
      }
      hitKeysByChunk.map { case (ChunkSetInfo(chunkId, numRows, startKey, _), keys) =>
        val chunkKeys = state.getRowKeyChunks(startKey, chunkId)
        val overrides = keys.flatMap { key =>
          binarySearchKeyChunks(state.projection, chunkKeys, numRows, key) match {
            case (pos, true) => Some(pos)
            case (_,  false) => None
          }
        }.toArray
        ChunkRowSkipIndex(chunkId, overrides)
      }.filter(_.overrides.size > 0)
    }
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