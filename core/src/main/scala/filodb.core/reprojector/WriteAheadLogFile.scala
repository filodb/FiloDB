package filodb.core.reprojector

import filodb.core.DatasetRef
import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{FileSystems, Files, Path}

import com.typesafe.config.Config
import filodb.core.metadata.Column
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

/**
  * Created by parekuti on 8/10/16.
  */
class WriteAheadLogFile( config: Config, dataset: DatasetRef,
                         version: Int = 0, columns: Seq[Column] = Seq()) {

  val file = createWalFile

  val fileChannel = new RandomAccessFile(file, "rw").getChannel()

  val buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096 * 8 * 8)

  val headerLength = writeHeader


  def writeHeader: Int = {
    val headerBytes = new ChunkHeader(columns).header
    buffer.put(headerBytes)
    headerBytes.length
  }

  def readHeader : Array[Byte] ={
    val readBuffer = new Array[Byte](headerLength)
    buffer.flip()
    buffer.get(readBuffer)
    readBuffer
  }

  private def createWalFile = {
    val walDir = config.getString("memtable.memtable-wal-dir")
    Files.createDirectories(FileSystems.getDefault.getPath(s"${walDir}/${dataset}_${version}"))
    val datestr = ISODateTimeFormat.dateTime().print(new DateTime())
    new File(s"${walDir}/${dataset}_${version}/${datestr}.wal")
  }

}
