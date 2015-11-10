package filodb.core.columnstore

import filodb.core.KeyRange

import org.scalatest.{FunSpec, Matchers}

class SegmentChopperSpec extends FunSpec with Matchers {
  import SegmentSpec._

  it("should add segment when no segment exists before") {
    val chopper = SegmentChopper(projection, Nil, 10, 20)
    chopper.insertKeysForPartition("foo", Seq(1L, 2L, 5000L, 15678L))

    chopper.segmentMetaMap.keys should equal (Set("foo"))
    val metas = chopper.segmentMetaMap("foo")
    metas should have length (1)
    metas.head.numRows should equal (4)
    metas.head.start should equal (None)
    metas.head.end should equal (Some(15678L))
    metas.head.updated should equal (true)

    // Adding the same keys should still make numRows go up
    chopper.insertKeysForPartition("foo", Seq(1L, 2L))
    metas.head.numRows should equal (6)
  }

  it("should add multiple initial segments if rows exceed minRowsPerSegment") {
    val chopper = SegmentChopper(projection, Nil, 10, 20)
    chopper.insertKeysForPartition("foo", (0 to 15).map(_.toLong))

    val segInfos = chopper.updatedSegments.values.head
    segInfos should have length (2)
    segInfos should equal (Seq(SegmentInfo(None, Some(10L), 10), SegmentInfo(Some(10L), Some(15L), 6)))
  }

  it("should add to existing segments with new row keys") {
    val chopper = SegmentChopper(projection, Nil, 10, 20)
    chopper.insertKeysForPartition("foo", (0 to 15).map(_.toLong))
    chopper.updatedSegments.values.head should equal (Seq(SegmentInfo(None, Some(10L), 10),
                                                          SegmentInfo(Some(10L), Some(15L), 6)))

    // Now insert a few keys to the segments, making sure to not split anything.  Verify.
    chopper.insertKeysForPartition("foo", Seq(5L, 6L, 30L, 40L, 50L))
    chopper.updatedSegments.values.head should equal (Seq(SegmentInfo(None, Some(10L), 12),
                                                          SegmentInfo(Some(10L), Some(50L), 9)))

    // Insert enough new keys to the final segment to split it
    chopper.insertKeysForPartition("foo", Seq(44L, 55L, 66L))
    chopper.updatedSegments.values.head should equal (Seq(SegmentInfo(None, Some(10L), 12),
                                                          SegmentInfo(Some(10L), Some(55L), 10),
                                                          SegmentInfo(Some(55L), Some(66L), 2)))
  }

  it("should keep extending final segment only if keys are in range") {
    val chopper = SegmentChopper(projection, Nil, 10, 20)
    chopper.insertKeysForPartition("foo", Seq(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L))

    // New keys all below previous high.  Segment created, but only contains high value.
    chopper.insertKeysForPartition("foo", (44L to 48L))
    chopper.updatedSegments.values.head should equal (Seq(SegmentInfo(None, Some(80L), 12),
                                                           SegmentInfo(Some(80L), None, 1)))

    // New keys partly above prev high, but not before split point.  New split will be populated.
    val chopper3 = SegmentChopper(projection, Nil, 10, 20)
    chopper3.insertKeysForPartition("foo", Seq(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L))
    chopper3.insertKeysForPartition("foo", (76L to 80L))
    chopper3.updatedSegments.values.head should equal (Seq(SegmentInfo(None, Some(80L), 11),
                                                           SegmentInfo(Some(80L), Some(80L), 2)))
  }

  it("should extend final segment with exactly minRowsPerSegment rows") {
    val chopper = SegmentChopper(projection, Nil, 10, 20)
    chopper.insertKeysForPartition("foo", Seq(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L, 90L))
    chopper.updatedSegments.values.head should equal (Seq(SegmentInfo(None, Some(90L), 9)))

    // Now insert just one key.  Total is 10, which is at minRowsPerSegment, but last key excluded, so
    // no split.
    chopper.insertKeysForPartition("foo", Seq(89L))
    chopper.updatedSegments.values.head should equal (Seq(SegmentInfo(None, Some(90L), 10)))

    // Add just one more key and witness a split.
    chopper.insertKeysForPartition("foo", Seq(91L))
    chopper.updatedSegments.values.head should equal (Seq(SegmentInfo(None, Some(91L), 10),
                                                          SegmentInfo(Some(91L), None, 1)))
  }

  implicit val helper = projection.helper

  // Prepare existing segmentMetaMap where some things are updated already
  it("updatedSegments should return only updated segments") {
    val chopper = SegmentChopper(projection, Nil, 10, 20)
    chopper.insertKeysForPartition("foo", (0 to 15).map(_.toLong))

    val chopper2 = new SegmentChopper(projection, chopper.segmentMetaMap, 10, 20)
    chopper2.insertKeysForPartition("baz", (10499L to 10507L))
    chopper2.insertKeysForPartition("foo", (25L to 35L))

    val segs = chopper2.updatedSegments
    segs.keys should equal (Set("foo", "baz"))
    segs("foo") should equal (Seq(SegmentInfo(Some(10L), Some(29L), 10),
                                  SegmentInfo(Some(29L), Some(35L), 7)))
    segs("baz") should equal (Seq(SegmentInfo(None, Some(10507L), 9)))
  }

  it("keyRanges should return only updated segments in partition/start order") {
    val chopper = SegmentChopper(projection, Nil, 10, 20)
    chopper.insertKeysForPartition("foo", (0 to 15).map(_.toLong))

    val chopper2 = new SegmentChopper(projection, chopper.segmentMetaMap, 10, 20)
    chopper2.insertKeysForPartition("baz", (10499L to 10507L))
    chopper2.insertKeysForPartition("foo", (25L to 35L))

    chopper2.keyRanges("dataset") should equal (Seq(KeyRange[Long]("dataset", "baz", None, Some(10507L)),
                                                    KeyRange[Long]("dataset", "foo", Some(10L), Some(29L)),
                                                    KeyRange[Long]("dataset", "foo", Some(29L), Some(35L))))
  }
}