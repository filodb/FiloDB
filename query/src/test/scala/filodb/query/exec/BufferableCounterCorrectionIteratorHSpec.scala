package filodb.query.exec

import filodb.core.query.TransientHistRow
import filodb.memory.format.vectors.{CustomBuckets, HistogramWithBuckets, MutableHistogram}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BufferableCounterCorrectionIteratorHSpec extends AnyFunSpec with Matchers {

  // Helper method to create histogram with bucket values
  private def createHistogram(bucketValues: Array[Double]): HistogramWithBuckets = {
    // TODO: How would GeometricHistogram and ExpHistogram Work?
    val buckets = CustomBuckets(bucketValues.indices.map(_.toDouble).toArray)
    MutableHistogram(buckets, bucketValues)
  }

  // Helper method to create TransientHistRow
  private def createHistRow(timestamp: Long, bucketValues: Array[Double]): TransientHistRow = {
    new TransientHistRow(timestamp, createHistogram(bucketValues))
  }

  it("should correct dips in histogram counter buckets") {
    val input = Seq(
      createHistRow(0, Array(3L, 5L, 7L)),
      createHistRow(1, Array(5L, 7L, 13L)),
      createHistRow(2, Array(7L, 13L, 15L)),
      createHistRow(3, Array(13L, 15L, 20L)),
      createHistRow(4, Array(2L, 5L, 8L)),    // dip in all buckets
      createHistRow(5, Array(34L, 40L, 50L))
    )

    val result = new BufferableCounterCorrectionIteratorH(input.iterator).buffered

    // Expected: corrections should be applied per bucket
    // Bucket 0: 3, 5, 7, 13, 15 (2+13=15), 47 (34+13=47)
    // Bucket 1: 5, 7, 13, 15, 20 (5+15=20), 55 (40+15=55)
    // Bucket 2: 7, 13, 15, 20, 28 (8+20=28), 70 (50+20=70)

    // First sample - no correction
    var res = result.next()
    res.value.bucketValue(0) shouldEqual 3.0
    res.value.bucketValue(1) shouldEqual 5.0
    res.value.bucketValue(2) shouldEqual 7.0

    res = result.next()

    // Second sample - no correction
    res.value.bucketValue(0) shouldEqual 5.0
    res.value.bucketValue(1) shouldEqual 7.0
    res.value.bucketValue(2) shouldEqual 13.0

    res = result.next()

    res.value.bucketValue(0) shouldEqual 7.0
    res.value.bucketValue(1) shouldEqual 13.0
    res.value.bucketValue(2) shouldEqual 15.0

    res = result.next()

    // Fourth sample - no correction
    res.value.bucketValue(0) shouldEqual 13.0
    res.value.bucketValue(1) shouldEqual 15.0
    res.value.bucketValue(2) shouldEqual 20.0

    res = result.next()

    // Fifth sample - correction applied (dip detected)
    res.value.bucketValue(0) shouldEqual 15.0  // 2 + 13 = 15
    res.value.bucketValue(1) shouldEqual 20.0  // 5 + 15 = 20
    res.value.bucketValue(2) shouldEqual 28.0  // 8 + 20 = 28

    res = result.next()

    // Sixth sample - correction continues
    res.value.bucketValue(0) shouldEqual 47.0  // 34 + 13 = 47
    res.value.bucketValue(1) shouldEqual 55.0  // 40 + 15 = 55
    res.value.bucketValue(2) shouldEqual 70.0  // 50 + 20 = 70
  }

  it("should correct multiple dips in histogram counter buckets") {
    val input = Seq(
      createHistRow(0, Array(3L, 5L)),
      createHistRow(1, Array(5L, 7L)),
      createHistRow(2, Array(7L, 13L)),
      createHistRow(3, Array(13L, 15L)),
      createHistRow(4, Array(2L, 5L)),     // first dip
      createHistRow(5, Array(34L, 40L)),
      createHistRow(6, Array(4L, 6L)),     // second dip
      createHistRow(7, Array(6L, 8L))
    )

    val result = new BufferableCounterCorrectionIteratorH(input.iterator).buffered

    var res = result.next()
    res = result.next()
    res = result.next()
    res = result.next()
    res = result.next()

    // After first dip (sample 4): correction = [13, 15]
    res.value.bucketValue(0) shouldEqual 15.0  // 2 + 13 = 15
    res.value.bucketValue(1) shouldEqual 20.0  // 5 + 15 = 20

    res = result.next()

    res.value.bucketValue(0) shouldEqual 47.0  // 34 + 13 = 47
    res.value.bucketValue(1) shouldEqual 55.0  // 40 + 15 = 55

    res = result.next()

    // After second dip (sample 6): additional correction = [34, 40]
    res.value.bucketValue(0) shouldEqual 51.0  // 4 + 13 + 34 = 51
    res.value.bucketValue(1) shouldEqual 61.0  // 6 + 15 + 40 = 61

    res = result.next()

    // Sample 7: continues with accumulated corrections
    res.value.bucketValue(0) shouldEqual 53.0  // 6 + 13 + 34 = 53
    res.value.bucketValue(1) shouldEqual 63.0  // 8 + 15 + 40 = 63
  }

  it("should not correct when no dips in histogram counter buckets") {
    val input = Seq(
      createHistRow(0, Array(3L, 5L, 7L)),
      createHistRow(1, Array(5L, 7L, 13L)),
      createHistRow(2, Array(7L, 13L, 15L)),
    )

    val result = new BufferableCounterCorrectionIteratorH(input.iterator).buffered

    // All values should remain unchanged (no corrections)

    var res = result.next()
    res.value.bucketValue(0) shouldEqual 3.0
    res.value.bucketValue(1) shouldEqual 5.0

    res = result.next()
    res.value.bucketValue(0) shouldEqual 5.0
    res.value.bucketValue(1) shouldEqual 7.0

    res = result.next()

    res.value.bucketValue(0) shouldEqual 7.0
    res.value.bucketValue(1) shouldEqual 13.0
  }

  it("should be bufferable") {
    val input = Seq(
      createHistRow(0, Array(3L, 5L)),
      createHistRow(1, Array(5L, 7L)),
      createHistRow(2, Array(7L, 13L)),
      createHistRow(3, Array(13L, 15L)),
      createHistRow(4, Array(2L, 5L)),     // dip
      createHistRow(5, Array(34L, 40L)),
      createHistRow(6, Array(4L, 6L)),     // dip
      createHistRow(7, Array(6L, 8L))
    )

    val iter = new BufferableCounterCorrectionIteratorH(input.iterator).buffered

    // Test buffered behavior - head should not advance iterator
    iter.head.value.bucketValue(0) shouldEqual 3.0
    iter.next().value.bucketValue(0) shouldEqual 3.0

    iter.head.value.bucketValue(0) shouldEqual 5.0
    iter.next().value.bucketValue(0) shouldEqual 5.0

    iter.head.value.bucketValue(0) shouldEqual 7.0
    iter.next().value.bucketValue(0) shouldEqual 7.0

    iter.head.value.bucketValue(0) shouldEqual 13.0
    iter.next().value.bucketValue(0) shouldEqual 13.0

    // After dip correction
    iter.head.value.bucketValue(0) shouldEqual 15.0  // 2 + 13 = 15
    iter.next().value.bucketValue(0) shouldEqual 15.0

    iter.head.value.bucketValue(0) shouldEqual 47.0  // 34 + 13 = 47
    iter.next().value.bucketValue(0) shouldEqual 47.0

    // After second dip correction
    iter.head.value.bucketValue(0) shouldEqual 51.0  // 4 + 13 + 34 = 51
    iter.next().value.bucketValue(0) shouldEqual 51.0

    iter.head.value.bucketValue(0) shouldEqual 53.0  // 6 + 13 + 34 = 53
    iter.next().value.bucketValue(0) shouldEqual 53.0
  }

  it("should handle empty set properly") {
    val input = Seq.empty[TransientHistRow]
    new BufferableCounterCorrectionIteratorH(input.iterator).hasNext shouldEqual false
  }

  it("should handle NaN values as counter resets") {
    val input = Seq(
      createHistRow(0, Array(3L, 5L)),
      createHistRow(1, Array(5L, 7L)),
      new TransientHistRow(2, createHistogram(Array(Double.NaN, Double.NaN))),
      createHistRow(3, Array(10L, 15L))
    )

    val result = new BufferableCounterCorrectionIteratorH(input.iterator).buffered

    var res = result.next()

    // First two samples normal
    res.value.bucketValue(0) shouldEqual 3.0
    res.value.bucketValue(1) shouldEqual 5.0

    res = result.next()

    // NaN should be treated as 0 (explicit counter reset)
    res.value.bucketValue(0) shouldEqual 5.0  // 0 + 5 = 5 (correction from previous)
    res.value.bucketValue(1) shouldEqual 7.0  // 0 + 7 = 7 (correction from previous)

    res = result.next()
    res.value.bucketValue(0) shouldEqual 5.0  // 0 + 5 = 5 (correction from previous)
    res.value.bucketValue(1) shouldEqual 7.0  // 0 + 7 = 7 (correction from previous)

    res = result.next()

    // Next sample continues with correction
    res.value.bucketValue(0) shouldEqual 15.0  // 10 + 5 = 15
    res.value.bucketValue(1) shouldEqual 22.0  // 15 + 7 = 22
  }
}
