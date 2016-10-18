package filodb.core.store

import filodb.core._
import org.velvia.filo.{RoutingRowReader, SeqRowReader}

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

  import GdeltTestData._
  private def getCSI(id: Int, firstLine: Int, lastLine: Int): ChunkSetInfo =
    ChunkSetInfo(id, 5000, RoutingRowReader(readers(firstLine), Array(4, 0)),
                           RoutingRowReader(readers(lastLine), Array(4, 0)))

  it("should find intersection range of composite keys with strings") {
    implicit val ordering = projection2.rowKeyType.rowReaderOrdering

    val info1 = getCSI(1, 0, 7)
    val intersect1 = info1.intersection(getCSI(2, 7, 17))
    intersect1 should be ('defined)
    intersect1.get._1 should equal (info1.lastKey)
    intersect1.get._2 should equal (info1.lastKey)
  }

  it("should return None if error with one of the RowReaders") {
    import GdeltTestData._
    implicit val ordering = projection2.rowKeyType.rowReaderOrdering

    val info1 = getCSI(1, 0, 7)
    info1.intersection(ChunkSetInfo(2, 100, null, null)) should be (None)
  }


  it("should collectSkips properly even with overlapping skip row numbers") {
    val origInfo = info1.copy(id = 9)
    val info2 = info1.copy(id = 14)
    val skips1 = ChunkRowSkipIndex(9, Array(2, 5))
    val skips2 = ChunkRowSkipIndex(9, Array(3, 5, 8))
    val infoAndSkips = ChunkSetInfo.collectSkips(Seq((origInfo, Nil),
                                                     (info1, Seq(skips1)),
                                                     (info2, Seq(skips2))))
    infoAndSkips should have length (3)
    infoAndSkips(0)._1.id should equal (9)
    infoAndSkips(0)._2.toList should equal (Seq(2, 3, 5, 8))
  }
}