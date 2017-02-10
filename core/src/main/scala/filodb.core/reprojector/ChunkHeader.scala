package filodb.core.reprojector

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer

import filodb.core.metadata.Column

class ChunkHeader(cols : Seq[Column] = Seq() ) {

  def header: Array[Byte] = {
    ChunkHeader.fileFormatIdentifier ++
      littleEndian(ChunkHeader.columnDefinitionIndicator) ++
      littleEndian(ChunkHeader.columnCountIndicator(cols)) ++
      columnDefinitions
  }

  def columnDefinitions : Array[Byte] = {
    val colDefinitions =cols.foldLeft("")(_  + _.toString + "\u0001").dropRight(1)
    val length = colDefinitions.length.toShort
    val bytes = colDefinitions.getBytes(StandardCharsets.UTF_8)
    littleEndian(ChunkHeader.shortToBytes(length)) ++ littleEndian(bytes)
  }

  private def littleEndian (data: Array[Byte]) = data.reverse
}

object ChunkHeader{

  val chunkStartIndicator =  Array[Byte](0x00,0x02)

  val chunkSeperator = Array[Byte](0x01)

  def fileFormatIdentifier : Array[Byte] =
    Array[Byte]('F', 'i', 'l','o','W', 'A', 'L',0x00)

  def columnDefinitionIndicator : Array[Byte] =
    Array[Byte](0x00,0x01)

  def columnCountIndicator(cols: Seq[Column]) : Array[Byte] =
    shortToBytes(cols.length.toShort)

  private def shortToBytes(count: Short): Array[Byte] =
    ByteBuffer.allocate(2).putShort(count).array()
}