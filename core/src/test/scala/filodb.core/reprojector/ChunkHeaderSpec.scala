package filodb.core.reprojector

import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import java.nio.charset.{Charset, StandardCharsets}

import filodb.core.GdeltTestData._
import filodb.core.metadata.{Column, DataColumn}

class ChunkHeaderSpec extends FunSpec with Matchers with BeforeAndAfter {
  val binaryFormatter = new ChunkHeader

  it("create UTF8 string with FiloWAL of 8 bytes") {

    ChunkHeader.fileFormatIdentifier should equal (Array[Byte]('F','i','l','o','W','A','L',0x00))
  }

  it("create column identifer in 2 bytes") {
    ChunkHeader.columnDefinitionIndicator should equal (Array[Byte](0x00,0x01))
  }

  it("Add no of columns to header of 2 bytes") {
    ChunkHeader.columnCountIndicator(Seq()) should equal (Array[Byte](0x00,0x00))

    ChunkHeader.columnCountIndicator(createColumns(2)) should equal (Array[Byte](0x00,0x02))

    ChunkHeader.columnCountIndicator(createColumns(250)) should equal (Array[Byte](0x00,-0x06))

    ChunkHeader.columnCountIndicator(createColumns(1000)) should equal (Array[Byte](0x03,-0x18))
  }

  it("Single column definition") {
    val col1 = new DataColumn(0,"column1","testtable",0,Column.ColumnType.StringColumn)
    val cols = Seq (col1)
    val expectedStr = "[0,column1,0,StringColumn]".getBytes(StandardCharsets.UTF_8).reverse
    new ChunkHeader(cols).
      columnDefinitions should equal (Array[Byte](0x1A,0x00) ++ expectedStr)
  }

  it("Multi column definitions") {
    val expectedStr = "[2,column2,0,StringColumn]\u0001[1,column1,0,StringColumn]"
      .getBytes(StandardCharsets.UTF_8).reverse
    new ChunkHeader(createColumns(2)).
      columnDefinitions should equal (Array[Byte](0x35,0x00) ++ expectedStr)
  }

  it("Order of methods to write full header"){
    val col1 = new DataColumn(0,"column1","testtable",0,Column.ColumnType.StringColumn)
    val cols = Seq (col1)

    val expectedHeader = Array[Byte]('F','i','l','o','W','A','L',0x00) ++ Array[Byte](0x01,0x00) ++
      (Array[Byte](0x01,0x00)) ++ Array[Byte](0x1A,0x00) ++
      "[0,column1,0,StringColumn]".getBytes(StandardCharsets.UTF_8).reverse

    new ChunkHeader(cols).
      header should equal (expectedHeader)
  }
}