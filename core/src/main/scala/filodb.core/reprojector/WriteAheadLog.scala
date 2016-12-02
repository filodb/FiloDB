package filodb.core.reprojector

import filodb.core.DatasetRef
import java.io._
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel
import java.nio.file.{FileSystems, Files, Path, Paths}

import com.typesafe.config.Config
import filodb.core.metadata.Column
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

/**
  * Class to create Memtable WAL files for a given dataset.
  * @param config
  * @param dataset
  * @param actorPath
  * @param columns
  * @param version
  * @param walFilePath
  * @param position
  */
class WriteAheadLog(config: Config,
                    dataset: DatasetRef,
                    actorPath: String,
                    columns: Seq[Column] = Seq(),
                    version: Int = 0,
                    walFilePath: Path = Paths.get(""),
                    position: Int = 0) {

  private val walBuffer = new WriteAheadLogBuffer(config, dataset, version, walFilePath, position, actorPath)

  val headerLength = writeHeader

  def path : String = walBuffer.walFile.getAbsolutePath

  def exists: Boolean = walBuffer.walFile.exists

  def delete(): Unit = walBuffer.walFile.delete()

  def deleteTestFiles(): Unit = {
    walBuffer.walFile.delete()
    walBuffer.walFile.getParentFile.delete()
    walBuffer.walFile.getParentFile.getParentFile.delete()
  }

  def writeHeader: Int = {
    val headerBytes = new ChunkHeader(columns).header
    if(walBuffer.bufferOffSet == 0){
      walBuffer.bufferOfSufficientSize(headerBytes.length).put(headerBytes)
    }
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
    walBuffer.bufferOfSufficientSize(2).put(ChunkHeader.chunkStartIndicator.reverse)
    val chunksSize = chunkArray.length
    for {index <- chunkArray.indices} {
      val chunkBytes = chunkArray(index)
      walBuffer.bufferOfSufficientSize(4).put((ByteBuffer.allocate(4).putInt(chunkBytes.limit()).array()).reverse)
      walBuffer.bufferOfSufficientSize(chunkBytes.limit()).put(chunkBytes)
      if (index < chunksSize - 1) {
        walBuffer.bufferOfSufficientSize(1).put(ChunkHeader.chunkSeperator)
      }
    }
  }
}

class WriteAheadLogBuffer(config: Config,
                          dataset: DatasetRef,
                          version: Int = 0,
                          walFilePath: Path,
                          position: Int,
                          actorPath: String) {

  private val bufferSize = config.getInt("write-ahead-log.mapped-byte-buffer-size")
  private val walDir = config.getString("write-ahead-log.memtable-wal-dir")

  val walFile = initWalFile

  private def initWalFile = {
    if(walFilePath.compareTo(Paths.get("")) == 0) {
      Files.createDirectories(FileSystems.getDefault.getPath(s"${walDir}/${actorPath}_${dataset}_${version}"))
      val datestr = ISODateTimeFormat.dateTime().print(new DateTime())
      new File(s"${walDir}/${actorPath}_${dataset}_${version}/${datestr}.wal")
    }else{
      walFilePath.toFile
    }
  }

  val channel = new RandomAccessFile(walFile, "rw").getChannel

  var bufferOffSet : Int = position
  var buffer =  allocateBuffer(bufferSize)

  def bufferOfSufficientSize(needed: Int): MappedByteBuffer = {
    if (buffer.remaining() < needed) {
      bufferOffSet += buffer.position()
      buffer = allocateBuffer(Math.max(bufferSize, needed))
    }
    buffer
  }

  def allocateBuffer(bufferSize: Int): MappedByteBuffer =
    channel.map(FileChannel.MapMode.READ_WRITE, bufferOffSet, bufferSize)

  def close(): Unit = channel.close()
}
