package filodb.core.store

import java.nio.ByteBuffer

import filodb.core.Types.ChunkID
import filodb.memory.format.UnsafeUtils

trait ChunkSetInfoT {
  /**
    * Chunk ID
    */
  def id: ChunkID

  /**
    * ingestion time as milliseconds from 1970
    */
  def ingestionTime: Long

  /**
    * number of rows encoded by this chunkset
    */
  def numRows: Int

  /**
    * the starting timestamp of this chunkset
    */
  def startTime: Long

  /**
    * The ending timestamp of this chunkset
    */
  def endTime: Long

  /**
    *
    * Base for vector pointers given column
    */
  def vectorBase(colId: Int): Any

  /**
    *
    * Long for vector pointers given column
    */
  def vectorOffset(colId: Int): Long

}

final case class ChunkSetInfoOnHeap(bytes: ByteBuffer, vectors: Seq[ByteBuffer]) extends ChunkSetInfoT {
  val (base, offset, numBytes) = UnsafeUtils.BOLfromBuffer(bytes)
  val vectorsBol = vectors.map(UnsafeUtils.BOLfromBuffer(_))

  def id: ChunkID = ChunkSetInfo.getChunkID(offset, base)
  def ingestionTime: Long = ChunkSetInfo.getIngestionTime(offset, base)
  def numRows: Int = ChunkSetInfo.getNumRows(offset, base)
  def startTime: Long = ChunkSetInfo.getStartTime(offset, base)
  def endTime: Long = ChunkSetInfo.getEndTime(offset, base)
  def vectorBase(colId: Int): Any = vectorsBol(colId)._1
  def vectorOffset(colId: Int): Long = vectorsBol(colId)._2
}

final case class ChunkSetInfoOffHeap(csi: ChunkSetInfo) extends ChunkSetInfoT {
  require(csi.infoAddr != 0, "Zero Pointer was used")

  def id: ChunkID = csi.id
  def ingestionTime: Long = csi.ingestionTime
  def numRows: Int = csi.numRows
  def startTime: Long = csi.startTime
  def endTime: Long = csi.endTime
  def vectorBase(colId: Int): Any = UnsafeUtils.ZeroPointer
  def vectorOffset(colId: Int): Long = csi.vectorPtr(colId)
}


