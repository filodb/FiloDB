package filodb.core.reprojector

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer

import filodb.core.metadata.Column

/**
  * Created by parekuti on 8/10/16.
  */
class FiloChunksBinaryFormatter(columnCount : Short ) {
  def columnDefinitions(cols: Seq[Column]) : Array[Byte] = {
    val colDefinitions =cols.foldLeft("")(_  + _.toString + "\001").dropRight(1)
    val length = colDefinitions.length.toShort
    val bytes = colDefinitions.getBytes(StandardCharsets.UTF_8)
    littleEndian(shortToBytes(length)) ++ littleEndian(bytes)
  }

  def columnCountIndicator : Array[Byte] = {
    littleEndian(shortToBytes(columnCount))
  }

  def columnDefinitionIndicator : Array[Byte] =
    littleEndian(Array[Byte](0x00,0x01))

  def fileFormatIdentifier : Array[Byte] =
    littleEndian(Array[Byte]('F', 'i', 'l','o','W', 'A', 'L',0x00))

  private def littleEndian (data: Array[Byte]) = data.reverse

  private def shortToBytes(count: Short): Array[Byte] =
    ByteBuffer.allocate(2).putShort(count).array()
}



