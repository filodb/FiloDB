package filodb.core.reprojector

import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import java.nio.charset.{Charset, StandardCharsets}

import filodb.core.GdeltTestData._
import filodb.core.metadata.{Column, DataColumn}

/**
  * Created by parekuti on 8/10/16.
  */
class ChunkHeaderSpec extends FunSpec with Matchers with BeforeAndAfter {
  val binaryFormatter = new ChunkHeader

  it("create UTF8 string with FiloWAL of 8 bytes") {

    binaryFormatter.fileFormatIdentifier should equal (Array[Byte](0x00,'L','A','W','o','l','i','F'))
  }

  it("create column identifer in 2 bytes") {
   binaryFormatter.columnDefinitionIndicator should equal (Array[Byte](0x01,0x00))
  }

  it("Add no of columns to header of 2 bytes") {
    new ChunkHeader().columnCountIndicator should equal (Array[Byte](0x00,0x00))

    new ChunkHeader(createColumns(2)).columnCountIndicator should equal (Array[Byte](0x02,0x00))

    new ChunkHeader(createColumns(250)).columnCountIndicator should equal (Array[Byte](-0x06,0x00))

    new ChunkHeader(createColumns(1000)).columnCountIndicator should equal (Array[Byte](-0x18,0x03))
  }

  it("Single column definition") {
    val col1 = new DataColumn(0,"column1","testtable",0,Column.ColumnType.StringColumn)
    val cols = Seq (col1)
    val expectedStr = "[0,column1,0,StringColumn]".getBytes(StandardCharsets.UTF_8).reverse
    new ChunkHeader(cols).
      columnDefinitions should equal (Array[Byte](0x1A,0x00) ++ expectedStr)
  }

  it("Multi column definitions") {
    val expectedStr = "[2,column2,0,StringColumn]\001[1,column1,0,StringColumn]"
      .getBytes(StandardCharsets.UTF_8).reverse
    new ChunkHeader(createColumns(2)).
      columnDefinitions should equal (Array[Byte](0x35,0x00) ++ expectedStr)
  }

  it("Order of methods to write full header"){
    val col1 = new DataColumn(0,"column1","testtable",0,Column.ColumnType.StringColumn)
    val cols = Seq (col1)

    val expectedHeader = Array[Byte](0x00,'L','A','W','o','l','i','F') ++ Array[Byte](0x01,0x00) ++
      (Array[Byte](0x01,0x00)) ++ Array[Byte](0x1A,0x00) ++
      "[0,column1,0,StringColumn]".getBytes(StandardCharsets.UTF_8).reverse

    new ChunkHeader(cols).
      header should equal (expectedHeader)
  }
}