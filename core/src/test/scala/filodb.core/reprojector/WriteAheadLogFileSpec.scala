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

import scala.collection.mutable.ArrayBuffer

class WriteAheadLogFileSpec extends FunSpec with Matchers with BeforeAndAfter{

    val tempDir = Files.createTempDirectory("wal")
    // /var/folders/tv/qrqnpyzj0qdfgw122hf1d7zr0000gn/T

    val config = ConfigFactory.parseString(
      s"""filodb.memtable.memtable-wal-dir = ${tempDir}
          filodb.memtable.mapped-byte-buffer-size = 1024
       """)
      .withFallback(ConfigFactory.load("application_test.conf"))
      .getConfig("filodb")

  it("creates memory mapped file with no data") {
    val wal = new WriteAheadLog(config, new DatasetRef("test"))
    wal.exists should equal(true)
    wal.close()
    wal.deleteTestFiles()
  }

  it("creates memory mapped buffer for an existing file") {

    val wal = new WriteAheadLog(config, new DatasetRef("test"))
    wal.exists should equal(true)
    wal.close()

    val walNew = new WriteAheadLog(config, new DatasetRef("test"),
              GdeltTestData.createColumns(2), 0, Paths.get(wal.path))
    walNew.exists should equal(true)
    walNew.close()
    walNew.deleteTestFiles()
  }

  it("write header to the file") {
    val wal = new WriteAheadLog(config, new DatasetRef("test"), GdeltTestData.createColumns(2))
    val expectedHeader = "FiloWAL\000\001\000\002\000\065\000]nmuloCgnirtS,0,1nmuloc,1[\001]nmuloCgnirtS,0,2nmuloc,2["
      .getBytes(StandardCharsets.UTF_8)
    wal.readHeader should equal(expectedHeader)
    wal.close()
    wal.deleteTestFiles
  }

  ignore("write filochunks indicator to the file") {
    val wal = new WriteAheadLog(config, NamesTestData.datasetRef, NamesTestData.schema)
    wal.writeChunks(new Array[ByteBuffer](0))
    val chunksInd = new Array[Byte](2)
    // wal.buffer.position(wal.headerLength)
    // wal.buffer.get(chunksInd)
    chunksInd should equal(Array[Byte](0x02,0x00))
    wal.close()
    wal.deleteTestFiles
  }

  it("write filochunks to the file") {
    val wal = new WriteAheadLog(config, datasetRef, schema)
    wal.writeChunks(createChunkData)
    wal.close()
    wal.deleteTestFiles
  }

  it("Able to write chunk data greater than the size of the mapped byte buffer") {
    val wal = new WriteAheadLog(config, datasetRef, schema)
    wal.writeChunks(createChunkData)
    wal.writeChunks(createChunkData)
    wal.close()
    wal.deleteTestFiles
  }

  it("Able to write large header greater than the size of the mapped byte buffer") {
    val wal = new WriteAheadLog(config, new DatasetRef(GdeltTestData.dataset1.name), GdeltTestData.schema)
    wal.writeChunks(createChunkData(GdeltTestData.schema,GdeltTestData.readers))
    wal.writeChunks(createChunkData(GdeltTestData.schema,GdeltTestData.readers))
    wal.close()
    wal.deleteTestFiles
  }

  it("Valid WAL header"){
    val wal = new WriteAheadLog(config, datasetRef, schema)
    wal.writeChunks(createChunkData)
    wal.close

    val reader = new WriteAheadLogReader(config,schema, wal.path)
    reader.validFile should equal(true)

    wal.deleteTestFiles()
    reader.close()
    wal.close()
  }

  it("Invalid file identifier in header") {
    val wal = new WriteAheadLog(config, new DatasetRef(GdeltTestData.dataset1.name), GdeltTestData.schema)
    replaceBytes(wal, 1, Array[Byte]('X', 'X', 'X'))
    val reader = new WriteAheadLogReader(config, GdeltTestData.schema, wal.path)
    reader.validFile should equal(false)
    wal.deleteTestFiles()
  }

  it("Invalid column definition header" ){
    val wal = new WriteAheadLog(config, new DatasetRef(GdeltTestData.dataset1.name), GdeltTestData.schema)
    replaceBytes(wal, 8, Array[Byte]('X', 'X'))
    val reader = new WriteAheadLogReader(config, GdeltTestData.schema, wal.path)
    reader.validFile should equal(false)
    wal.deleteTestFiles()
    reader.close()
    wal.close()
  }

  it("Invalid column count indicator" ){
    val wal = new WriteAheadLog(config, new DatasetRef(GdeltTestData.dataset1.name), GdeltTestData.schema)
    replaceBytes(wal, 10, Array[Byte](0x11, 0x11))
    val reader = new WriteAheadLogReader(config,GdeltTestData.schema, wal.path)
    reader.validFile should equal(false)
    wal.deleteTestFiles()
    reader.close()
    wal.close()
  }

  it("Invalid column definitions size" ){
    val wal = new WriteAheadLog(config, new DatasetRef(GdeltTestData.dataset1.name), GdeltTestData.schema)
    replaceBytes(wal, 12, Array[Byte](0x01, 0x01))
    val reader = new WriteAheadLogReader(config, GdeltTestData.schema, wal.path)
    reader.validFile should equal(false)
    wal.deleteTestFiles()
    reader.close()
    wal.close()
  }

  it("Able to read filo chunks successfully"){
    val wal = new WriteAheadLog(config, datasetRef, schema)
    val chunkData = createChunkData
    wal.writeChunks(chunkData)
    wal.close

    val reader = new WriteAheadLogReader(config, schema, wal.path)
    reader.validFile should equal(true)

    val chunks = reader.readChunks().getOrElse(new ArrayBuffer[Array[ByteBuffer]])

    val filoSchema = Column.toFiloSchema(schema)
    val colIds = filoSchema.map(_.name).toArray
    val clazzes = filoSchema.map(_.dataType).toArray
    //noinspection ScalaStyle
    println(chunks(0).length)

    val chunkArray = chunks(0)

    val rowreader = new FastFiloRowReader(chunks(0), clazzes)
    rowreader.setRowNo(2)
    println("parsers:"+rowreader.parsers.head.length)
    val actualStr = rowreader.getString(0) + ";" + rowreader.getString(1) + ";" +
      rowreader.getLong(2) + ";" + rowreader.getInt(3)

    println(actualStr)


    // val expectedStr = rowreader2.getString(0) +
    // rowreader2.getString(1) + rowreader2.getLong(2) + rowreader2.getInt(3)


   // actualStr should equal (expectedStr)

    wal.deleteTestFiles()
    reader.close()
    wal.close()
  }

  it("Able to read filo chunks for GdeltTestData successfully"){
    val projectionDB = projection4.withDatabase("unittest2")
    val ref = projectionDB.datasetRef
    val wal = new WriteAheadLog(config, ref, GdeltTestData.schema)
    // val chunkData = createChunkData
    wal.writeChunks(createChunkData(GdeltTestData.schema,GdeltTestData.readers))
    wal.close

    val reader = new WriteAheadLogReader(config, GdeltTestData.schema, wal.path)
    reader.validFile should equal(true)

    val chunks = reader.readChunks().getOrElse(new ArrayBuffer[Array[ByteBuffer]])

    val filoSchema = Column.toFiloSchema(GdeltTestData.schema)
    val colIds = filoSchema.map(_.name).toArray
    val clazzes = filoSchema.map(_.dataType).toArray
    //noinspection ScalaStyle
    println(chunks(0).length)

    val chunkArray = chunks(0)

    val rowreader = new FastFiloRowReader(chunks(0), clazzes)
    rowreader.setRowNo(2)
    println("parsers:"+rowreader.parsers.head.length)
    val actualStr = rowreader.getInt(0)  + ";" +
      rowreader.getInt(2) + ";" + rowreader.getInt(3)

    println(actualStr)


    // val expectedStr = rowreader2.getString(0) + rowreader2.getString(1) + rowreader2.getLong(2) + rowreader2.getInt(3)


    // actualStr should equal (expectedStr)

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
