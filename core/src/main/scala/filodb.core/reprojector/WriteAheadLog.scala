package filodb.core.reprojector

import filodb.core.DatasetRef
import java.io._
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel
import java.nio.file.{FileSystems, Files}

import com.typesafe.config.Config
import filodb.core.metadata.Column
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

class WriteAheadLog(config: Config, dataset: DatasetRef,
                    version: Int = 0, columns: Seq[Column] = Seq()) {

  private val walBuffer = new WriteAheadLogBuffer(config, dataset, version)

  val headerLength = writeHeader

  def path : String = walBuffer.walFile.getAbsolutePath

  def exists: Boolean = walBuffer.walFile.exists

  def delete(): Unit = {
    walBuffer.walFile.delete()
    walBuffer.walFile.getParentFile.delete()
    walBuffer.walFile.getParentFile.getParentFile.delete()
  }

  def writeHeader: Int = {
    val headerBytes = new ChunkHeader(columns).header
    walBuffer.bufferOfSufficientSize(headerBytes.length).put(headerBytes)
    headerBytes.length
  }

  def readHeader : Array[Byte] ={
    val readBuffer = new Array[Byte](headerLength)
    walBuffer.buffer.flip()
    walBuffer.buffer.get(readBuffer)
    readBuffer
  }

  def close(): Unit = walBuffer.close

  def writeChunks(chunkArray: Array[ByteBuffer]): Unit = {
    walBuffer.bufferOfSufficientSize(2).put(Array[Byte](0x02,0x00))
    val chunksSize = chunkArray.length
    for {index <- chunkArray.indices} {
      val chunkBytes = chunkArray(index)
      walBuffer.bufferOfSufficientSize(4).put((ByteBuffer.allocate(4).putInt(chunkBytes.limit()).array()).reverse)
      walBuffer.bufferOfSufficientSize(chunkBytes.limit()).put(chunkBytes)
      if (index < chunksSize - 1) {
        walBuffer.bufferOfSufficientSize(1).put(0x01.toByte)
      }
    }
  }
}

class WriteAheadLogBuffer(config: Config, dataset: DatasetRef, version: Int = 0) {

  private val bufferSize = config.getInt("memtable.mapped-byte-buffer-size")
  private val walDir = config.getString("memtable.memtable-wal-dir")

   val walFile = {
    Files.createDirectories(FileSystems.getDefault.getPath(s"${walDir}/${dataset}_${version}"))
    val datestr = ISODateTimeFormat.dateTime().print(new DateTime())
    new File(s"${walDir}/${dataset}_${version}/${datestr}.wal")
  }

  val channel = new RandomAccessFile(walFile, "rw").getChannel()

  var bufferOffSet : Int = 0
  var buffer =  allocateBuffer(bufferSize)

  def bufferOfSufficientSize(needed: Int): MappedByteBuffer = {
    if (buffer.remaining() < needed) {
      bufferOffSet += buffer.position()
      buffer = allocateBuffer(Math.max(bufferSize, needed))
    }
    buffer
  }

  def allocateBuffer(bufferSize: Int): MappedByteBuffer =
    channel.map(FileChannel.MapMode.READ_WRITE, bufferOffSet,bufferSize)

  def close(): Unit = channel.close()
}