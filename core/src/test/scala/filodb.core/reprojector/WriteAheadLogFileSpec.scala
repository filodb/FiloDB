package filodb.core.reprojector

import java.io.{File, FileOutputStream, FileWriter, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{FileSystems, Files}

import com.typesafe.config.ConfigFactory
import filodb.core.NamesTestData._
import filodb.core.{DatasetRef, GdeltTestData, NamesTestData}
import filodb.core.metadata.{Column, DataColumn}
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.velvia.filo.{ArrayStringRowReader, RowToVectorBuilder, TupleRowReader}

class WriteAheadLogFileSpec extends FunSpec with Matchers with BeforeAndAfter{
  var config = ConfigFactory.load("application_test.conf")
    .getConfig("filodb")

  before {
    val tempDir = Files.createTempDirectory("wal")

    config = ConfigFactory.parseString(
      s"""filodb.memtable.memtable-wal-dir = ${tempDir}
          filodb.memtable.mapped-byte-buffer-size = 1024
       """)
      .withFallback(ConfigFactory.load("application_test.conf"))
      .getConfig("filodb")
  }

  it("creates memory mapped file with no data") {
    val wal = new WriteAheadLog(config, new DatasetRef("test"), 0)
    wal.exists should equal(true)
    wal.delete
  }

  it("write header to the file") {
    val wal = new WriteAheadLog(config, new DatasetRef("test"), 0, GdeltTestData.createColumns(2))
    val expectedHeader = "\000LAWoliF\001\000\002\000\065\000]nmuloCgnirtS,0,1nmuloc,1[\001]nmuloCgnirtS,0,2nmuloc,2["
      .getBytes(StandardCharsets.UTF_8)
    wal.readHeader should equal(expectedHeader)
    wal.delete
  }

  ignore("write filochunks indicator to the file") {
    val wal = new WriteAheadLog(config, NamesTestData.datasetRef, 0, NamesTestData.schema)
    wal.writeChunks(new Array[ByteBuffer](0))
    val chunksInd = new Array[Byte](2)
    // wal.buffer.position(wal.headerLength)
    // wal.buffer.get(chunksInd)
    chunksInd should equal(Array[Byte](0x02,0x00))
    wal.delete
  }

  it("Prepare filochunks to write to the file") {
    createChunkData.length should equal(4)
  }

  it("write filochunks to the file") {
    val wal = new WriteAheadLog(config, datasetRef, 0, schema)
    wal.writeChunks(createChunkData)
    wal.delete
  }

  it("Able to write chunk data greater than the size of the mapped byte buffer") {
    val wal = new WriteAheadLog(config, datasetRef, 0, schema)
    wal.writeChunks(createChunkData)
    wal.writeChunks(createChunkData)
    wal.delete
  }

  it("Able to write large header greater than the size of the mapped byte buffer") {
    val wal = new WriteAheadLog(config, new DatasetRef(GdeltTestData.dataset1.name), 0, GdeltTestData.schema)
    wal.writeChunks(createChunkData(GdeltTestData.schema,GdeltTestData.readers))
    wal.writeChunks(createChunkData(GdeltTestData.schema,GdeltTestData.readers))
    wal.delete
  }

  it("Valid WAL header"){
    val wal = new WriteAheadLog(config, datasetRef, 0, schema)
    wal.writeChunks(createChunkData)
    wal.close

    val reader = new WriteAheadLogReader(config,wal.path)
    reader.validFile should equal(true)

    wal.delete()
  }

  it("Invalid file identifier in header") {
    val wal = new WriteAheadLog(config, new DatasetRef(GdeltTestData.dataset1.name), 0, GdeltTestData.schema)
    replaceBytes(wal, 1, Array[Byte]('X', 'X', 'X'))
    val reader = new WriteAheadLogReader(config, wal.path)
    reader.validFile should equal(false)
    wal.delete()
  }

  it("Invalid column definition header" ){
    val wal = new WriteAheadLog(config, new DatasetRef(GdeltTestData.dataset1.name), 0, GdeltTestData.schema)
    replaceBytes(wal, 8, Array[Byte]('X', 'X'))
    val reader = new WriteAheadLogReader(config,wal.path)
    reader.validFile should equal(false)
    wal.delete()
  }

  it("Invalid column count indicator" ){
    val wal = new WriteAheadLog(config, new DatasetRef(GdeltTestData.dataset1.name), 0, GdeltTestData.schema)
    replaceBytes(wal, 10, Array[Byte](0x11, 0x11))
    val reader = new WriteAheadLogReader(config,wal.path)
    reader.validFile should equal(false)
    wal.delete()
  }

  it("Invalid column definitions size" ){
    val wal = new WriteAheadLog(config, new DatasetRef(GdeltTestData.dataset1.name), 0, GdeltTestData.schema)
    replaceBytes(wal, 12, Array[Byte](0x01, 0x01))
    val reader = new WriteAheadLogReader(config,wal.path)
    reader.validFile should equal(false)
    wal.delete()
  }


  private def createChunkData : Array[ByteBuffer] = {
    val filoSchema = Column.toFiloSchema(schema)
    val colIds = filoSchema.map(_.name).toArray
    val builder = new RowToVectorBuilder(filoSchema)
    names.map(TupleRowReader).foreach(builder.addRow)
    val colIdToBuffers = builder.convertToBytes()
    colIds.map(colIdToBuffers)
  }

  private def createChunkData(schema: Seq[DataColumn],
                              readers: Seq[ArrayStringRowReader]) : Array[ByteBuffer] = {
    val filoSchema = Column.toFiloSchema(schema)
    val colIds = filoSchema.map(_.name).toArray
    val builder = new RowToVectorBuilder(filoSchema)
    readers.foreach(builder.addRow)
    val colIdToBuffers = builder.convertToBytes()
    colIds.map(colIdToBuffers)
  }

  private def replaceBytes(wal: WriteAheadLog, l: Long, bytes: Array[Byte]): Unit = {
    val channel = new RandomAccessFile(wal.path, "rw")
    channel.seek(l)
    channel.write(bytes)
    channel.close()
  }
}
