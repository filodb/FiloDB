package filodb.core.store

import filodb.core._

import org.scalatest.FunSpec
import org.scalatest.Matchers

class ChunkSetInfoSpec extends FunSpec with Matchers {
  import SingleKeyTypes._
  import NamesTestData._

  val info1 = ChunkSetInfo(13, 5000, firstKey, lastKey)

  def skipsShouldEqual(skip1: Seq[ChunkRowSkipIndex], skip2: Seq[ChunkRowSkipIndex]): Unit = {
    skip1 should have length (skip2.length)
    skip1.zip(skip2).foreach { case (ChunkRowSkipIndex(id1, aray1), ChunkRowSkipIndex(id2, aray2)) =>
      id1 should equal (id2)
      aray1 should equal (aray2)
    }
  }

  def infosShouldEqual(info1: ChunkSetInfo, info2: ChunkSetInfo, numCols: Int = 1): Unit = {
    info1.id should equal (info2.id)
    info1.numRows should equal (info2.numRows)
    for { i <- 0 until numCols } {
      info1.firstKey.getAny(i) should equal (info2.firstKey.getAny(i))
      info1.lastKey.getAny(i) should equal (info2.lastKey.getAny(i))
    }
  }

  val prj = projection

  it("should serialize and deserialize ChunkSetInfo and no skips") {
    val (infoRead1, skips1) = ChunkSetInfo.fromBytes(prj, ChunkSetInfo.toBytes(prj, info1, Nil))
    infosShouldEqual(infoRead1, info1)
    skips1 should equal (Nil)
  }

  it("should serialize and deserialize ChunkSetInfo and skips") {
    val skips1 = ChunkRowSkipIndex(10, Array[Int]())
    val skips2 = ChunkRowSkipIndex(9, Array(2, 5))
    val skips3 = ChunkRowSkipIndex(11, Array(10, 11, 12, 20, 25))

    val (infoRead1, skipsRead1) = ChunkSetInfo.fromBytes(prj, ChunkSetInfo.toBytes(prj, info1, Seq(skips1)))
    infosShouldEqual(infoRead1, info1)
    skipsShouldEqual(skipsRead1, Seq(skips1))

    val (infoRead2, skipsRead2) = ChunkSetInfo.fromBytes(prj, ChunkSetInfo.toBytes(
                                                                prj, info1, Seq(skips2, skips3)))
    infosShouldEqual(infoRead2, info1)
    skipsShouldEqual(skipsRead2, Seq(skips2, skips3))
  }

  it("should find intersection range for several ChunkSetInfos") (pending)

  it("should find intersection range of composite keys with strings") (pending)

  it("should collectSkips properly even with overlapping skip row numbers") (pending)
}