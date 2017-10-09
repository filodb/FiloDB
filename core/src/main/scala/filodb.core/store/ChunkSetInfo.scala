package filodb.core.store

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import com.googlecode.javaewah.EWAHCompressedBitmap
import com.typesafe.scalalogging.StrictLogging
import org.boon.primitive.{ByteBuf, InputByteArray}
import org.velvia.filo._

import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.{Column, RichProjection}
import filodb.core.Types._

/**
 * A ChunkSet is the set of chunks for all columns, one per column, serialized from a set of rows.
 * ChunkSetInfo records common metadata about a ChunkSet.
 */
case class ChunkSet(info: ChunkSetInfo,
                    partition: PartitionKey,
                    skips: Seq[ChunkRowSkipIndex],
                    chunks: Map[ColumnId, ByteBuffer])

object ChunkSet {
  private def makeRowKey(proj: RichProjection, row: RowReader): BinaryRecord =
    BinaryRecord(proj.rowKeyBinSchema, RoutingRowReader(row, proj.rowKeyColIndices.toArray))

  /**
   * Create a ChunkSet out of a set of rows easily.  Mostly for testing.
   */
  def apply(proj: RichProjection, part: PartitionKey, rows: Seq[RowReader]): ChunkSet = {
    require(rows.nonEmpty)
    val firstKey = makeRowKey(proj, rows.head)
    val info = ChunkSetInfo(timeUUID64, rows.length, firstKey, makeRowKey(proj, rows.last))
    val filoSchema = Column.toFiloSchema(proj.columns)
    val chunkMap = RowToVectorBuilder.buildFromRows(rows.toIterator, filoSchema)
    ChunkSet(info, part, Nil, chunkMap)
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
  def intersection(other: ChunkSetInfo): Option[(BinaryRecord, BinaryRecord)] =
    try {
      intersection(other.firstKey, other.lastKey)
    } catch {
      case e: Exception =>
        logger.warn(s"Got error comparing $this and $other...", e)
        None
    }

  /**
   * Finds the intersection between this ChunkSetInfo and a range of keys (key1, key2).
   * Note that key1 and key2 do not need to contain all the fields of firstKey and lastKey, but
   * must be a strict subset of the first fields.
   */
  def intersection(key1: BinaryRecord, key2: BinaryRecord): Option[(BinaryRecord, BinaryRecord)] = {
    if (key1 > key2) {
      None
    } else if (key1 <= lastKey && key2 >= firstKey) {
      Some((if (key1 < firstKey) firstKey else key1,
            if (key2 > lastKey) lastKey else key2))
    } else {
      None
    }
  }
}

case class ChunkRowSkipIndex(id: ChunkID, overrides: EWAHCompressedBitmap)

object ChunkRowSkipIndex {
  def apply(id: ChunkID, overrides: Array[Int]): ChunkRowSkipIndex =
    ChunkRowSkipIndex(id, EWAHCompressedBitmap.bitmapOf(overrides.sorted: _*))
}

object ChunkSetInfo extends StrictLogging {
  type ChunkSkips = Seq[ChunkRowSkipIndex]
  type SkipMap    = EWAHCompressedBitmap
  type ChunkInfosAndSkips = Seq[(ChunkSetInfo, SkipMap)]
  type InfosSkipsIt = Iterator[(ChunkSetInfo, SkipMap)]

  val emptySkips = new SkipMap()

  /**
   * Serializes ChunkSetInfo into bytes for persistence.
   *
   * Defined format:
   *   version  - byte  - 0x01
   *   chunkId  - long
   *   numRows  - int32
   *   firstKey - med. byte array (BinaryRecord)
   *   lastKey  - med. byte array (BinaryRecord)
   *   maxConsideredChunkID - long
   *   repeated - id: long, med. byte array (EWAHCompressedBitmap) - ChunkRowSkipIndex
   */
  def toBytes(projection: RichProjection, chunkSetInfo: ChunkSetInfo, skips: ChunkSkips): Array[Byte] = {
    val buf = ByteBuf.create(100)
    buf.writeByte(0x01)
    buf.writeLong(chunkSetInfo.id)
    buf.writeInt(chunkSetInfo.numRows)
    buf.writeMediumByteArray(chunkSetInfo.firstKey.bytes)
    buf.writeMediumByteArray(chunkSetInfo.lastKey.bytes)
    buf.writeLong(-1L)   // TODO: add maxConsideredChunkID
    skips.foreach { case ChunkRowSkipIndex(id, overrides) =>
      buf.writeLong(id)
      val baos = new ByteArrayOutputStream
      val dos = new DataOutputStream(baos)
      overrides.serialize(dos)
      buf.writeMediumByteArray(baos.toByteArray)
    }
    buf.toBytes
  }

  def fromBytes(projection: RichProjection, bytes: Array[Byte]): (ChunkSetInfo, ChunkSkips) = {
    val scanner = new InputByteArray(bytes)
    val versionByte = scanner.readByte
    assert(versionByte == 0x01, s"Incompatible ChunkSetInfo version $versionByte")
    val id = scanner.readLong
    val numRows = scanner.readInt
    val firstKey = BinaryRecord(projection, scanner.readMediumByteArray)
    val lastKey = BinaryRecord(projection, scanner.readMediumByteArray)
    scanner.readLong    // throw away maxConsideredChunkID for now
    val skips = new ArrayBuffer[ChunkRowSkipIndex]
    while (scanner.location < bytes.size) {
      val skipId = scanner.readLong
      val skipList = new EWAHCompressedBitmap(ByteBuffer.wrap(scanner.readMediumByteArray))
      skips.append(ChunkRowSkipIndex(skipId, skipList))
    }
    (ChunkSetInfo(id, numRows, firstKey, lastKey), skips)
  }
}