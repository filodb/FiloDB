package filodb.core.store

import java.nio.ByteBuffer

import filodb.core.Types.ChunkID
import filodb.memory.format.{BinaryVector, MemoryAccessor, VectorDataReader}
import filodb.memory.format.vectors.LongVectorDataReader

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
  def vectorAccessor(colId: Int): MemoryAccessor

  /**
    *
    * Long for vector pointers given column
    */
  def vectorAddress(colId: Int): Long

  /* Below vars are stateful fields set during query processing for query optimization */
  var tsVectorAccessor: MemoryAccessor = _
  var tsVectorAddr: BinaryVector.BinaryVectorPtr = _
  var valueVectorAccessor: MemoryAccessor = _
  var valueVectorAddr: BinaryVector.BinaryVectorPtr = _
  var tsReader: LongVectorDataReader = _
  var valueReader: VectorDataReader = _

}

final case class ChunkSetInfoOnHeap(bytes: ByteBuffer, vectors: Seq[ByteBuffer]) extends ChunkSetInfoT {
  val bytesAcc = MemoryAccessor.fromByteBuffer(bytes)
  val vectorsAcc = vectors.map(MemoryAccessor.fromByteBuffer)

  def id: ChunkID = ChunkSetInfo.getChunkID(bytesAcc, 0)
  def ingestionTime: Long = ChunkSetInfo.getIngestionTime(bytesAcc, 0)
  def numRows: Int = ChunkSetInfo.getNumRows(bytesAcc, 0)
  def startTime: Long = ChunkSetInfo.getStartTime(bytesAcc, 0)
  def endTime: Long = ChunkSetInfo.getEndTime(bytesAcc, 0)
  def vectorAccessor(colId: Int): MemoryAccessor = vectorsAcc(colId)
  def vectorAddress(colId: Int): Long = 0
}

final case class ChunkSetInfoOffHeap(csi: ChunkSetInfo) extends ChunkSetInfoT {
  require(csi.infoAddr != 0, "Zero Pointer was used")

  def id: ChunkID = csi.id
  def ingestionTime: Long = csi.ingestionTime
  def numRows: Int = csi.numRows
  def startTime: Long = csi.startTime
  def endTime: Long = csi.endTime
  def vectorAccessor(colId: Int): MemoryAccessor = MemoryAccessor.nativePointer
  def vectorAddress(colId: Int): Long = csi.vectorPtr(colId)
}


