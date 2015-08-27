package filodb.core.datastore2

import java.nio.ByteBuffer

import org.scalatest.FunSpec
import org.scalatest.Matchers

class SegmentSpec extends FunSpec with Matchers {
  implicit val keyHelper = TimestampKeyHelper(10000L)
  val keyRange = KeyRange("dataset", "partition", 0L, 10000L)

  val bytes1 = ByteBuffer.wrap("apple".getBytes("UTF-8"))
  val bytes2 = ByteBuffer.wrap("orange".getBytes("UTF-8"))

  val rowIndex = new UpdatableSegmentRowIndex[Long]

  it("should add and get chunks back out") {
    val segment = new GenericSegment(keyRange, rowIndex)
    segment.addChunks(0, Map("columnA" -> bytes1, "columnB" -> bytes2))
    segment.addChunks(1, Map("columnA" -> bytes1, "columnB" -> bytes2))

    segment.getColumns should equal (Set("columnA", "columnB"))
    segment.getChunks.toSet should equal (Set(("columnA", 0, bytes1),
                                              ("columnA", 1, bytes1),
                                              ("columnB", 0, bytes2),
                                              ("columnB", 1, bytes2)))
  }
}