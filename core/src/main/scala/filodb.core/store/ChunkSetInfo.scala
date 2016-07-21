package filodb.core.store

import java.nio.ByteBuffer
import org.boon.primitive.{ByteBuf, InputByteArray}
import scala.collection.mutable.{ArrayBuffer, HashMap, TreeSet}
import scala.math.Ordered._
import scodec.bits._

import filodb.core._
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
                        firstKey: ByteVector,
                        lastKey: ByteVector) {
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
  def intersection(other: ChunkSetInfo): Option[(ByteVector, ByteVector)] = {
    if (lastKey >= other.firstKey && firstKey <= other.lastKey) {
      Some((if (firstKey > other.firstKey) firstKey else other.firstKey,
            if (lastKey < other.lastKey) lastKey else other.lastKey))
    } else {
      None
    }
  }
}

case class ChunkRowSkipIndex(id: ChunkID, overrides: Array[Int])

object ChunkSetInfo {
  type ChunkSkips = Seq[ChunkRowSkipIndex]
  type ChunkInfosAndSkips = Seq[(ChunkSetInfo, ChunkSkips)]

  def toBytes(chunkSetInfo: ChunkSetInfo, skips: ChunkSkips): Array[Byte] = {
    val buf = ByteBuf.create(100)
    buf.add(chunkSetInfo.id)
    buf.add(chunkSetInfo.numRows)
    buf.writeMediumByteArray(chunkSetInfo.firstKey.toArray)
    buf.writeMediumByteArray(chunkSetInfo.lastKey.toArray)
    skips.foreach { case ChunkRowSkipIndex(id, overrides) =>
      buf.add(id)
      buf.writeMediumIntArray(overrides.toArray)
    }
    buf.toBytes
  }

  def fromBytes(bytes: Array[Byte]): (ChunkSetInfo, ChunkSkips) = {
    val scanner = new InputByteArray(bytes)
    val id = scanner.readInt
    val numRows = scanner.readInt
    val firstKey = ByteVector(scanner.readMediumByteArray)
    val lastKey = ByteVector(scanner.readMediumByteArray)
    val skips = new ArrayBuffer[ChunkRowSkipIndex]
    while (scanner.location < bytes.size) {
      val skipId = scanner.readInt
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

  // NOTE: This scans all previous ChunkSetInfos for possible collisions.  Replace this
  // with an interval tree based algorithm for efficient, log n comparisons.
  def detectSkips(): Unit = {}
}