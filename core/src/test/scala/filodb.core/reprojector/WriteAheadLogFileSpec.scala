package filodb.core.reprojector

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.typesafe.config.ConfigFactory
import filodb.core.GdeltTestData.{schema => _, _}
import filodb.core.NamesTestData._
import filodb.core.metadata.{Column, DataColumn}
import filodb.core.{DatasetRef, GdeltTestData, NamesTestData}
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.velvia.filo.{ArrayStringRowReader, FastFiloRowReader, RowToVectorBuilder, TupleRowReader}
import org.velvia.filo.ZeroCopyUTF8String._

import scala.collection.mutable.ArrayBuffer

class WriteAheadLogFileSpec extends FunSpec with Matchers with BeforeAndAfter{

  val tempDir = Files.createTempDirectory("wal")
  // /var/folders/tv/qrqnpyzj0qdfgw122hf1d7zr0000gn/T

  val config = ConfigFactory.parseString(
    s"""filodb.write-ahead-log.memtable-wal-dir = ${tempDir}
          filodb.write-ahead-log.mapped-byte-buffer-size = 2048
       """)
    .withFallback(ConfigFactory.load("application_test.conf"))
    .getConfig("filodb")

  it("creates memory mapped file with no data") {
    val wal = new WriteAheadLog(config, new DatasetRef("test"),"localhost")
    wal.exists should equal(true)
    wal.close()
    wal.deleteTestFiles()
  }

  it("creates memory mapped buffer for an existing file") {

    val wal = new WriteAheadLog(config, new DatasetRef("test"),"localhost")
    wal.exists should equal(true)
    wal.close()

    val walNew = new WriteAheadLog(config, new DatasetRef("test"),"localhost",
      GdeltTestData.createColumns(2), 0, Paths.get(wal.path))
    walNew.exists should equal(true)
    walNew.close()
    walNew.deleteTestFiles()
  }

  it("write header to the file") {
    val wal = new WriteAheadLog(config, new DatasetRef("test"),"localhost", GdeltTestData.createColumns(2))
    val expectedHeader = "FiloWAL\u0000\u0001\u0000\u0002\u0000\u0035\u0000]nmuloCgnirtS,0,1nmuloc,1[\u0001]nmuloCgnirtS,0,2nmuloc,2["
      .getBytes(StandardCharsets.UTF_8)
    wal.readHeader should equal(expectedHeader)
    wal.close()
    wal.deleteTestFiles
  }

  ignore("write filochunks indicator to the file") {
    val wal = new WriteAheadLog(config, NamesTestData.datasetRef,"localhost", NamesTestData.schema)
    wal.writeChunks(new Array[ByteBuffer](0))
    val chunksInd = new Array[Byte](2)
    // wal.buffer.position(wal.headerLength)
    // wal.buffer.get(chunksInd)
    chunksInd should equal(Array[Byte](0x02,0x00))
    wal.close()
    wal.deleteTestFiles
  }

  it("write filochunks to the file") {
    val wal = new WriteAheadLog(config, datasetRef,"localhost", schema)
    wal.writeChunks(createChunkData)
    wal.close()
    wal.deleteTestFiles
  }

  it("Able to write chunk data greater than the size of the mapped byte buffer") {
    val wal = new WriteAheadLog(config, datasetRef, "localhost",schema)
    wal.writeChunks(createChunkData)
    wal.writeChunks(createChunkData)
    wal.close()
    wal.deleteTestFiles
  }

  it("Able to write large header greater than the size of the mapped byte buffer") {
    val wal = new WriteAheadLog(config, new DatasetRef(GdeltTestData.dataset1.name),"localhost", GdeltTestData.schema)
    wal.writeChunks(createChunkData(GdeltTestData.schema,GdeltTestData.readers))
    wal.writeChunks(createChunkData(GdeltTestData.schema,GdeltTestData.readers))
    wal.close()
    wal.deleteTestFiles
  }

  it("Valid WAL header"){
    val wal = new WriteAheadLog(config, datasetRef,"localhost", schema)
    wal.writeChunks(createChunkData)
    wal.close

    val reader = new WriteAheadLogReader(config,schema, wal.path)
    reader.validFile should equal(true)

    wal.deleteTestFiles()
    reader.close()
    wal.close()
  }

  it("Invalid file identifier in header") {
    val wal = new WriteAheadLog(config, new DatasetRef(GdeltTestData.dataset1.name),"localhost", GdeltTestData.schema)
    replaceBytes(wal, 1, Array[Byte]('X', 'X', 'X'))
    val reader = new WriteAheadLogReader(config, GdeltTestData.schema, wal.path)
    reader.validFile should equal(false)
    wal.deleteTestFiles()
  }

  it("Invalid column definition header" ){
    val wal = new WriteAheadLog(config, new DatasetRef(GdeltTestData.dataset1.name),"localhost", GdeltTestData.schema)
    replaceBytes(wal, 8, Array[Byte]('X', 'X'))
    val reader = new WriteAheadLogReader(config, GdeltTestData.schema, wal.path)
    reader.validFile should equal(false)
    wal.deleteTestFiles()
    reader.close()
    wal.close()
  }

  it("Invalid column count indicator" ){
    val wal = new WriteAheadLog(config, new DatasetRef(GdeltTestData.dataset1.name),"localhost", GdeltTestData.schema)
    replaceBytes(wal, 10, Array[Byte](0x11, 0x11))
    val reader = new WriteAheadLogReader(config,GdeltTestData.schema, wal.path)
    reader.validFile should equal(false)
    wal.deleteTestFiles()
    reader.close()
    wal.close()
  }

  it("Invalid column definitions size" ){
    val wal = new WriteAheadLog(config, new DatasetRef(GdeltTestData.dataset1.name),"localhost", GdeltTestData.schema)
    replaceBytes(wal, 12, Array[Byte](0x01, 0x01))
    val reader = new WriteAheadLogReader(config, GdeltTestData.schema, wal.path)
    reader.validFile should equal(false)
    wal.deleteTestFiles()
    reader.close()
    wal.close()
  }

  it("Able to read filo chunks successfully"){
    val wal = new WriteAheadLog(config, datasetRef, "localhost", schema)
    val chunkData = createChunkData
    wal.writeChunks(chunkData)
    wal.close

    val reader = new WriteAheadLogReader(config, schema, wal.path)
    reader.validFile should equal(true)

    val chunks = reader.readChunks().getOrElse(new ArrayBuffer[Array[ByteBuffer]])

    val filoSchema = Column.toFiloSchema(schema)
    val colIds = filoSchema.map(_.name).toArray
    val clazzes = filoSchema.map(_.dataType).toArray

    val chunkArray = chunks(0)

    val rowreader = new FastFiloRowReader(chunks(0), clazzes)
    rowreader.setRowNo(2)

    rowreader.filoUTF8String(0) should equal ("Rodney".utf8)
    rowreader.getLong(2) should equal (25L)

    wal.deleteTestFiles()
    reader.close()
    wal.close()
  }

  it("Able to read filo chunks for GdeltTestData successfully"){
    val projectionDB = projection4.withDatabase("unittest2")
    val ref = projectionDB.datasetRef
    val wal = new WriteAheadLog(config, ref,"localhost", GdeltTestData.schema)
    wal.writeChunks(createChunkData(GdeltTestData.schema, GdeltTestData.readers))
    wal.close

    val reader = new WriteAheadLogReader(config, GdeltTestData.schema, wal.path)
    reader.validFile should equal(true)

    val chunks = reader.readChunks().getOrElse(new ArrayBuffer[Array[ByteBuffer]])

    val filoSchema = Column.toFiloSchema(GdeltTestData.schema)
    val colIds = filoSchema.map(_.name).toArray
    val clazzes = filoSchema.map(_.dataType).toArray
    chunks(0) should have length (8)

    val rowreader = new FastFiloRowReader(chunks(0), clazzes)
    rowreader.setRowNo(2)
    rowreader.getInt(2) should equal (197901)

    wal.deleteTestFiles()
    reader.close()
    wal.close()
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

