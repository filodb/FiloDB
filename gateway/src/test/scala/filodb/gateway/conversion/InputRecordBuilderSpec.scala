package filodb.gateway.conversion


import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schemas
import filodb.memory.MemFactory
import filodb.memory.format.vectors.{CustomBuckets, LongHistogram}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class InputRecordBuilderSpec extends AnyFunSpec with Matchers {
  val builder = new RecordBuilder(MemFactory.onHeapFactory)
  val builder2 = new RecordBuilder(MemFactory.onHeapFactory)

  val baseTags = Map("dataset" -> "timeseries",
                     "host" -> "MacBook-Pro-229.local",
                     "shard" -> "0")
  val metric = "my_hist"

  val counts  = Array(10L, 20L, 25, 38, 50, 66)
  val sum = counts.sum.toDouble
  val count = 50.0
  val min = counts.min.toDouble
  val max = counts.max.toDouble
  val sumCountKVs = Seq("sum" -> sum, "count" -> count)
  val sumCountMinMaxKVs = Seq("sum" -> sum, "count" -> count, "min" -> min, "max" -> max)

  it("should writePromHistRecord to BR and be able to deserialize it") {
    val buckets = Array(0.5, 1, 2.5, 5, 10, Double.PositiveInfinity)
    val expected = LongHistogram(CustomBuckets(buckets), counts)

    val bucketKVs = buckets.zip(counts).map {
      case (Double.PositiveInfinity, c) => "+Inf" -> c.toDouble
      case (b, c)                       => b.toString -> c.toDouble
    }.toSeq
    // 1 - sum/count at end
    InputRecord.writePromHistRecord(builder, metric, baseTags, 100000L, bucketKVs ++ sumCountKVs)
    builder.allContainers.head.iterate(Schemas.promHistogram.ingestionSchema).foreach { row =>
      row.getDouble(1) shouldEqual sum
      row.getDouble(2) shouldEqual count
      row.getHistogram(3) shouldEqual expected
    }
  }

  it("should writeDeltaHistRecord to BR and be able to deserialize it") {
    val buckets = Array(0.5, 1, 2.5, 5, 10, Double.PositiveInfinity)
    val expected = LongHistogram(CustomBuckets(buckets), counts)

    val bucketKVs = buckets.zip(counts).map {
      case (Double.PositiveInfinity, c) => "+Inf" -> c.toDouble
      case (b, c) => b.toString -> c.toDouble
    }.toSeq
    // 1 - sum/count at end
    InputRecord.writeDeltaHistRecord(builder, metric, baseTags, 100000L, bucketKVs ++ sumCountKVs)
    builder.allContainers.head.iterate(Schemas.deltaHistogram.ingestionSchema).foreach { row =>
      row.getDouble(1) shouldEqual sum
      row.getDouble(2) shouldEqual count
      row.getHistogram(3) shouldEqual expected
    }
  }

  it("should otelDeltaHistogram to BR and be able to deserialize it") {
    val buckets = Array(0.5, 1, 2.5, 5, 10, Double.PositiveInfinity)
    val expected = LongHistogram(CustomBuckets(buckets), counts)

    val bucketKVs = buckets.zip(counts).map {
      case (Double.PositiveInfinity, c) => "+Inf" -> c.toDouble
      case (b, c) => b.toString -> c.toDouble
    }.toSeq
    // 1 - sum/count at end
    InputRecord.writeOtelDeltaHistRecord(builder2, metric, baseTags, 100000L, bucketKVs ++ sumCountMinMaxKVs)
    builder2.allContainers.head.iterate(Schemas.otelDeltaHistogram.ingestionSchema).foreach { row =>
      row.getDouble(1) shouldEqual sum
      row.getDouble(2) shouldEqual count
      row.getDouble(4) shouldEqual min
      row.getDouble(5) shouldEqual max
      row.getHistogram(3) shouldEqual expected
    }
  }

  it("should otelCumulativeHistogram to BR and be able to deserialize it") {
    val buckets = Array(0.5, 1, 2.5, 5, 10, Double.PositiveInfinity)
    val expected = LongHistogram(CustomBuckets(buckets), counts)

    val bucketKVs = buckets.zip(counts).map {
      case (Double.PositiveInfinity, c) => "+Inf" -> c.toDouble
      case (b, c) => b.toString -> c.toDouble
    }.toSeq
    // 1 - sum/count at end
    InputRecord.writeOtelCumulativeHistRecord(builder2, metric, baseTags, 100000L, bucketKVs ++ sumCountMinMaxKVs)
    builder2.allContainers.head.iterate(Schemas.otelCumulativeHistogram.ingestionSchema).foreach { row =>
      row.getDouble(1) shouldEqual sum
      row.getDouble(2) shouldEqual count
      row.getDouble(4) shouldEqual min
      row.getDouble(5) shouldEqual max
      row.getHistogram(3) shouldEqual expected
    }
  }

  it("should skip empty histograms via writePromHistRecord, and write subsequent records") {
    builder.reset()
    InputRecord.writePromHistRecord(builder, metric, baseTags, 100000L, sumCountKVs)
    InputRecord.writeGaugeRecord(builder, metric, baseTags, 100000L, 5.5)

    // The empty histogram should have been skipped, so we should have only one record
    builder.allContainers.head.countRecords shouldEqual 1
  }

  it("should skip empty histograms via writeDeltaHistRecord, and write subsequent records") {
    builder.reset()
    InputRecord.writeDeltaHistRecord(builder, metric, baseTags, 100000L, sumCountKVs)
    InputRecord.writeGaugeRecord(builder, metric, baseTags, 100000L, 5.5)

    // The empty histogram should have been skipped, so we should have only one record
    builder.allContainers.head.countRecords shouldEqual 1
  }
}