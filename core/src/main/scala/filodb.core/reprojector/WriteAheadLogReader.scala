package filodb.core.reprojector

import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel

import com.typesafe.config.Config

class WriteAheadLogReader(config: Config, path: String) {

  val walFile = new File(path)

  val channel = new RandomAccessFile(walFile, "r").getChannel

  val buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0,channel.size())

  def validFile: Boolean = {
    validField(8, ChunkHeader.fileFormatIdentifier, false) &&
    validField(2, ChunkHeader.columnDefinitionIndicator, true) &&
    validColumnDefinitions &&
    validField(2, Array[Byte](0x00,0x02), true)
  }

  private def validColumnDefinitions: Boolean = {
    val columnCount = getLittleEndianBytesAsInt(2)
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
