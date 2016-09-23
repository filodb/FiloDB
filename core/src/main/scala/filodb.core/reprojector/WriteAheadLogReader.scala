package filodb.core.reprojector

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import com.typesafe.config.Config

import scala.collection.mutable.ArrayBuffer

class WriteAheadLogReader(config: Config, path: String) {
  val walFile = new File(path)

  val channel = new RandomAccessFile(walFile, "r").getChannel

  val buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0,channel.size())

  private val chunks = new ArrayBuffer[Array[ByteBuffer]]

  private var columnCount = 0

  //noinspection ScalaStyle
  def readChunks(): Option[ArrayBuffer[Array[ByteBuffer]]]= {
    while(buffer.hasRemaining) {
      val chunkArray = new Array[ByteBuffer](columnCount)
      for{index <- 0 to columnCount-1}{
        val bytesForChunk = getLittleEndianBytesAsInt(4)
        println("bytesForChunk: "+ bytesForChunk)
        chunkArray(index) = ByteBuffer.wrap(getBytes(bytesForChunk))
        // read chunk seperator
        if (index < columnCount - 1) {
          println(index)
          validField(1, ChunkHeader.chunkSeperator,true)
        }
      }
      chunks+=chunkArray
      if(!validField(2, ChunkHeader.chunkStartIndicator, true)){
        return Some(chunks)
      }
    }
    None
  }

  def validFile: Boolean = {
    validField(8, ChunkHeader.fileFormatIdentifier, false) &&
    validField(2, ChunkHeader.columnDefinitionIndicator, true) &&
    validColumnDefinitions &&
    validField(2,ChunkHeader.chunkStartIndicator, true)
  }

  private def validColumnDefinitions: Boolean = {
    columnCount = getLittleEndianBytesAsInt(2)
    val columnDefinitionsSize = getLittleEndianBytesAsInt(2)
    val columnDefinitions = getBytes(columnDefinitionsSize)
    columnCount == columnDefinitions.count(_ == 0x01) + 1
  }

  private def validField(size: Int, target: Array[Byte],  reverseflag: Boolean): Boolean = {
    if(reverseflag) {
      getBytes(size).reverse.sameElements(target)
    } else {
      getBytes(size).sameElements(target)
    }
  }

  private def getBytes(size: Int): Array[Byte] = {
    val fieldBytes = new Array[Byte](size)
    buffer.get(fieldBytes)
    fieldBytes
  }

  private def getLittleEndianBytesAsInt(size: Int): Int ={
    getBytes(size).view.zipWithIndex.foldLeft(0) {
      (acc, e) => acc + ((e._1 & 0xff) << (8 * e._2))
    }
  }

  def close(): Unit = channel.close()

}
