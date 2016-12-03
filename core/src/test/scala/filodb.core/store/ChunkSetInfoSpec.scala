package filodb.core.store

import java.sql.Timestamp
import org.velvia.filo.{RoutingRowReader, SeqRowReader}
import org.scalatest.FunSpec
import org.scalatest.Matchers

import filodb.core._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.{RichProjection, Dataset}

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

  private def getCSI(id: Int, firstLine: Int, lastLine: Int): ChunkSetInfo = {
    import GdeltTestData._
    ChunkSetInfo(id, 5000,
                 BinaryRecord(projection2.rowKeyBinSchema, RoutingRowReader(readers(firstLine), Array(4, 0))),
                 BinaryRecord(projection2.rowKeyBinSchema, RoutingRowReader(readers(lastLine), Array(4, 0))))
  }

  it("should find intersection range of composite keys with strings") {
    import GdeltTestData._
    implicit val ordering = projection2.rowKeyType.rowReaderOrdering

    val info1 = getCSI(1, 0, 7)
    val intersect1 = info1.intersection(getCSI(2, 7, 17))
    intersect1 should be ('defined)
    intersect1.get._1 should equal (info1.lastKey)
    intersect1.get._2 should equal (info1.lastKey)

    // info3 is wholly contained inside info1
    val info3 = getCSI(3, 3, 5)
    val intersect2 = info1.intersection(info3)
    intersect2 should be ('defined)
    intersect2.get._1 should equal (info3.firstKey)
    intersect2.get._2 should equal (info3.lastKey)

    // left side of info5 overlaps with info6
    // NOTE: on purpose test BinaryRecord keys where the second component, here an int, does not compare
    // in the same order as the first key.
    // Here, [(FRA, 55) - (VNM, 52)] intersect [(CHL, 60) - (MOSGOV, 62)] = ((FRA, 55), (MOSGOV, 62))
    // BUT: if you used bytewise comparison with BinaryRecord, above would not work because
    // the int is seen first and 55 < 60 and 52 < 62
    val info5 = getCSI(5, 55, 52)
    val info6 = getCSI(6, 60, 62)
    info5.intersection(info6) should equal (Some((info5.firstKey, info6.lastKey)))
    info6.intersection(info5) should equal (Some((info5.firstKey, info6.lastKey)))

    // wholly outside / no intersection
    info1.intersection(getCSI(5, 28, 30)) should equal (None)
  }

  it("should find intersection range of keys with timestamps") {
    import GdeltTestData._
    // Timestamp, String, String for rowkey / 0 seg / Year partition
    val dataset5 = Dataset("gdelt", Seq("SQLDATE", "Actor2Code", "Actor2Name"), ":string 0", Seq("Year"))
    val proj5 = RichProjection(dataset5, schema)

    val key1 = BinaryRecord(proj5, Seq(Timestamp.valueOf("2013-01-02 08:00:00"), "0", "0"))
    val key2 = BinaryRecord(proj5, Seq(Timestamp.valueOf("2013-01-05 17:30:00"), "0", "0"))
    val key3 = BinaryRecord(proj5, Seq(Timestamp.valueOf("2013-01-10 12:00:00"), "AABCDE", "0"))
    val key4 = BinaryRecord(proj5, Seq(Timestamp.valueOf("2013-01-10 12:00:00"), "GHTI", "0"))
    val key5 = BinaryRecord(proj5, Seq(Timestamp.valueOf("2013-01-18 20:59:00"), "0", "0"))
    val key6 = BinaryRecord(proj5, Seq(Timestamp.valueOf("2013-01-21 23:59:59"), "0", "0"))

    val info1 = ChunkSetInfo(1, 1, key2, key4)

    implicit val ordering = proj5.rowKeyType.rowReaderOrdering

    // wholly inside
    info1.intersection(key3, key3) should equal (Some((key3, key3)))

    // partially inside to left
    info1.intersection(key1, key3) should equal (Some((key2, key3)))

    // partially inside to right
    info1.intersection(key3, key5) should equal (Some((key3, key4)))

    // wholly outside
    info1.intersection(key5, key6) should equal (None)
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