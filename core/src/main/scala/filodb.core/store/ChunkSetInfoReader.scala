package filodb.core.store

import java.nio.ByteBuffer

import filodb.core.Types.ChunkID
import filodb.memory.format.{BinaryVector, MemoryReader, VectorDataReader}
import filodb.memory.format.vectors.LongVectorDataReader

/**
  * Abstraction for reading byte sequence representing
  * ChunkSetInfo. This implementation is typically used in the query and
  * hold placeholders so query engine can add state during query processing.
  */
trait ChunkSetInfoReader {
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
    * Memory Accessor for vectors given columnId
    */
  def vectorAccessor(colId: Int): MemoryReader

  /**
    *
    * Address within the accessor for vectors given columnId
    */
  def vectorAddress(colId: Int): Long

  /* Below vars are stateful fields set during query processing for query optimization */
  private[store] var tsVectorAccessor: MemoryReader = _
  private[store] var tsVectorAddr: BinaryVector.BinaryVectorPtr = _
  private[store] var valueVectorAccessor: MemoryReader = _
  private[store] var valueVectorAddr: BinaryVector.BinaryVectorPtr = _
  private[store] var tsReader: LongVectorDataReader = _
  private[store] var valueReader: VectorDataReader = _


  def getTsVectorAccessor: MemoryReader = tsVectorAccessor
  def getTsVectorAddr: BinaryVector.BinaryVectorPtr = tsVectorAddr
  def getValueVectorAccessor: MemoryReader = valueVectorAccessor
  def getValueVectorAddr: BinaryVector.BinaryVectorPtr = valueVectorAddr
  def getTsReader: LongVectorDataReader = tsReader
  def getValueReader: VectorDataReader = valueReader
}

/**
  * ChunkSetInfoReader implementation that has the info and vector byte sequences in byte buffers
  * @param infoBytes serialized chunkSetInfo
  * @param vectors serialized vectors
  */
final case class ChunkSetInfoOnHeap(infoBytes: ByteBuffer, vectors: Seq[ByteBuffer]) extends ChunkSetInfoReader {
  val bytesAcc = MemoryReader.fromByteBuffer(infoBytes)
  val vectorsAcc = vectors.map(MemoryReader.fromByteBuffer)

  def id: ChunkID = ChunkSetInfo.getChunkID(bytesAcc, 0)
  def ingestionTime: Long = ChunkSetInfo.getIngestionTime(bytesAcc, 0)
  def numRows: Int = ChunkSetInfo.getNumRows(bytesAcc, 0)
  def startTime: Long = ChunkSetInfo.getStartTime(bytesAcc, 0)
  def endTime: Long = ChunkSetInfo.getEndTime(bytesAcc, 0)
  def vectorAccessor(colId: Int): MemoryReader = vectorsAcc(colId)
  def vectorAddress(colId: Int): Long = 0
}

/**
  * ChunkSetInfoReader implementation that has the info as native pointer.
  *
  * @param csi native pointer to in-memory ChunkSetInfo
  */
final case class ChunkSetInfoOffHeap(csi: ChunkSetInfo) extends ChunkSetInfoReader {
  require(csi.infoAddr != 0, "Zero Pointer was used")

  def id: ChunkID = csi.id
  def ingestionTime: Long = csi.ingestionTime
  def numRows: Int = csi.numRows
  def startTime: Long = csi.startTime
  def endTime: Long = csi.endTime
  def vectorAccessor(colId: Int): MemoryReader = MemoryReader.nativePtrReader
  def vectorAddress(colId: Int): Long = csi.vectorPtr(colId)
}


