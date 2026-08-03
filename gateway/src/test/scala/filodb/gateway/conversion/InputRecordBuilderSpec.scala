package filodb.gateway.conversion


import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schemas
import filodb.memory.MemFactory
import filodb.memory.format.vectors.{Base2ExpHistogramBuckets, CustomBuckets, LongHistogram}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class InputRecordBuilderSpec extends AnyFunSpec with Matchers {
  val builder = new RecordBuilder(MemFactory.onHeapFactory)
  val builder2 = new RecordBuilder(MemFactory.onHeapFactory)
  val builder3 = new RecordBuilder(MemFactory.onHeapFactory)

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

  it("should otelExpDeltaHistogram to BR and be able to deserialize it") {
    val bucketScheme = Base2ExpHistogramBuckets(3, -5, 10)
    val bucketsCounts = Array(1L, 2, 3,4, 5, 6, 7, 8, 9, 10, 11) // require cumulative counts
    val expected = LongHistogram(bucketScheme, bucketsCounts)

    val bucketKVs = bucketsCounts.zipWithIndex.map {
      case (bucketCount, i) => i.toString -> bucketCount.toDouble
    }.toSeq

    // add posBucketOffset and scale
    val more = Seq("posBucketOffset" -> bucketScheme.startIndexPositiveBuckets.toDouble,
                   "scale" -> bucketScheme.scale.toDouble)

    InputRecord.writeOtelExponentialHistRecord(builder3, metric, baseTags, 100000L,
                                               bucketKVs ++ sumCountMinMaxKVs ++ more, isDelta = true)
    builder3.allContainers.head.iterate(Schemas.otelExpDeltaHistogram.ingestionSchema).foreach { row =>
      row.getDouble(1) shouldEqual sum
      row.getDouble(2) shouldEqual count
      row.getDouble(4) shouldEqual min
      row.getDouble(5) shouldEqual max
      val hist = row.getHistogram(3).asInstanceOf[LongHistogram]
      hist.buckets shouldEqual expected.buckets
      hist.values shouldEqual Array(1L, 2, 3,4, 5, 6, 7, 8, 9, 10, 11)
    }
  }

  it("should skip empty histograms via writePromHistRecord, and write subsequent records") {
    builder.reset()
    InputRecord.writePromHistRecord(builder, metric, baseTags, 100000L, sumCountKVs)
    InputRecord.writeGaugeRecord(builder, metric, baseTags, 100000L, 5.5)

    // The empty histogram should have been skipped, so we should have only one record
    builder.allContainers.head.countRecords() shouldEqual 1
  }

  it("should skip empty histograms via writeDeltaHistRecord, and write subsequent records") {
    builder.reset()
    InputRecord.writeDeltaHistRecord(builder, metric, baseTags, 100000L, sumCountKVs)
    InputRecord.writeGaugeRecord(builder, metric, baseTags, 100000L, 5.5)

    // The empty histogram should have been skipped, so we should have only one record
    builder.allContainers.head.countRecords() shouldEqual 1
  }

  // ── Java TreeMap overload compatibility tests ──────────────────────
  // Verifies that TreeMap overloads produce byte-for-byte identical records
  // to the original Scala Map overloads.

  import filodb.memory.format.UnsafeUtils
  import filodb.core.binaryrecord2.RecordBuilder.ContainerHeaderLen

  val treeTags = {
    val t = new java.util.TreeMap[String, String]()
    baseTags.foreach { case (k, v) => t.put(k, v) }
    t
  }

  /** Extract full record bytes from a builder's first container, first record */
  private def firstRecordBytes(b: RecordBuilder, schema: filodb.core.metadata.Schema): Array[Byte] = {
    val container = b.allContainers.head
    val base = container.base
    val off = container.offset + ContainerHeaderLen
    val len = UnsafeUtils.getInt(base, off) + 4
    val recBytes = new Array[Byte](len)
    UnsafeUtils.unsafe.copyMemory(base, off, recBytes, UnsafeUtils.arayOffset, len)
    recBytes
  }

  it("TreeMap writeGaugeRecord should match Scala Map version") {
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)

    InputRecord.writeGaugeRecord(b1, metric, baseTags, 100000L, 42.5)
    InputRecord.writeGaugeRecord(b2, metric, treeTags, 100000L, 42.5)

    firstRecordBytes(b1, Schemas.gauge) shouldEqual
      firstRecordBytes(b2, Schemas.gauge)
  }

  it("TreeMap writePromCounterRecord should match Scala Map version") {
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)

    InputRecord.writePromCounterRecord(b1, metric, baseTags, 100000L, 99.0)
    InputRecord.writePromCounterRecord(b2, metric, treeTags, 100000L, 99.0)

    firstRecordBytes(b1, Schemas.promCounter) shouldEqual
      firstRecordBytes(b2, Schemas.promCounter)
  }

  it("TreeMap writeDeltaCounterRecord should match Scala Map version") {
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)

    InputRecord.writeDeltaCounterRecord(b1, metric, baseTags, 100000L, 7.0)
    InputRecord.writeDeltaCounterRecord(b2, metric, treeTags, 100000L, 7.0)

    firstRecordBytes(b1, Schemas.deltaCounter) shouldEqual
      firstRecordBytes(b2, Schemas.deltaCounter)
  }

  it("TreeMap writeUntypedRecord should match Scala Map version") {
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)

    InputRecord.writeUntypedRecord(b1, metric, baseTags, 100000L, 3.14)
    InputRecord.writeUntypedRecord(b2, metric, treeTags, 100000L, 3.14)

    firstRecordBytes(b1, Schemas.untyped) shouldEqual
      firstRecordBytes(b2, Schemas.untyped)
  }

  it("TreeMap writePromHistRecord should match Scala Map version") {
    val buckets = Array(0.5, 1, 2.5, 5, 10, Double.PositiveInfinity)
    val bucketKVs = buckets.zip(counts).map {
      case (Double.PositiveInfinity, c) => "+Inf" -> c.toDouble
      case (b, c) => b.toString -> c.toDouble
    }.toSeq ++ sumCountKVs

    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)

    InputRecord.writePromHistRecord(b1, metric, baseTags, 100000L, bucketKVs)
    InputRecord.writePromHistRecord(b2, metric, treeTags, 100000L, bucketKVs)

    firstRecordBytes(b1, Schemas.promHistogram) shouldEqual
      firstRecordBytes(b2, Schemas.promHistogram)
  }

  it("TreeMap writeDeltaHistRecord should match Scala Map version") {
    val buckets = Array(0.5, 1, 2.5, 5, 10, Double.PositiveInfinity)
    val bucketKVs = buckets.zip(counts).map {
      case (Double.PositiveInfinity, c) => "+Inf" -> c.toDouble
      case (b, c) => b.toString -> c.toDouble
    }.toSeq ++ sumCountKVs

    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)

    InputRecord.writeDeltaHistRecord(b1, metric, baseTags, 100000L, bucketKVs)
    InputRecord.writeDeltaHistRecord(b2, metric, treeTags, 100000L, bucketKVs)

    firstRecordBytes(b1, Schemas.deltaHistogram) shouldEqual
      firstRecordBytes(b2, Schemas.deltaHistogram)
  }

  it("TreeMap writeOtelCumulativeHistRecord should match Scala Map version") {
    val buckets = Array(0.5, 1, 2.5, 5, 10, Double.PositiveInfinity)
    val bucketKVs = buckets.zip(counts).map {
      case (Double.PositiveInfinity, c) => "+Inf" -> c.toDouble
      case (b, c) => b.toString -> c.toDouble
    }.toSeq ++ sumCountMinMaxKVs

    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)

    InputRecord.writeOtelCumulativeHistRecord(b1, metric, baseTags, 100000L, bucketKVs)
    InputRecord.writeOtelCumulativeHistRecord(b2, metric, treeTags, 100000L, bucketKVs)

    firstRecordBytes(b1, Schemas.otelCumulativeHistogram) shouldEqual
      firstRecordBytes(b2, Schemas.otelCumulativeHistogram)
  }

  it("TreeMap writeOtelDeltaHistRecord should match Scala Map version") {
    val buckets = Array(0.5, 1, 2.5, 5, 10, Double.PositiveInfinity)
    val bucketKVs = buckets.zip(counts).map {
      case (Double.PositiveInfinity, c) => "+Inf" -> c.toDouble
      case (b, c) => b.toString -> c.toDouble
    }.toSeq ++ sumCountMinMaxKVs

    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)

    InputRecord.writeOtelDeltaHistRecord(b1, metric, baseTags, 100000L, bucketKVs)
    InputRecord.writeOtelDeltaHistRecord(b2, metric, treeTags, 100000L, bucketKVs)

    firstRecordBytes(b1, Schemas.otelDeltaHistogram) shouldEqual
      firstRecordBytes(b2, Schemas.otelDeltaHistogram)
  }

  it("TreeMap overloads should handle many tags identically") {
    val manyTags = baseTags ++ (0 until 15).map(i => f"label_$i%02d" -> s"value_$i").toMap
    val manyTreeTags = {
      val t = new java.util.TreeMap[String, String]()
      manyTags.foreach { case (k, v) => t.put(k, v) }
      t
    }

    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)

    InputRecord.writeGaugeRecord(b1, metric, manyTags, 100000L, 1.0)
    InputRecord.writeGaugeRecord(b2, metric, manyTreeTags, 100000L, 1.0)

    firstRecordBytes(b1, Schemas.gauge) shouldEqual
      firstRecordBytes(b2, Schemas.gauge)
  }

  it("TreeMap overloads should handle empty tags identically") {
    val emptyTags = Map.empty[String, String]
    val emptyTreeTags = new java.util.TreeMap[String, String]()

    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)

    InputRecord.writeGaugeRecord(b1, metric, emptyTags, 100000L, 1.0)
    InputRecord.writeGaugeRecord(b2, metric, emptyTreeTags, 100000L, 1.0)

    firstRecordBytes(b1, Schemas.gauge) shouldEqual
      firstRecordBytes(b2, Schemas.gauge)
  }

  // ── Deserialization tests for scalar TreeMap overloads ─────────────

  it("TreeMap writeGaugeRecord data should deserialize correctly") {
    val b = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeGaugeRecord(b, metric, treeTags, 100000L, 42.5)
    b.allContainers.head.iterate(Schemas.gauge.ingestionSchema).foreach { row =>
      row.getLong(0) shouldEqual 100000L
      row.getDouble(1) shouldEqual 42.5
    }
  }

  it("TreeMap writePromCounterRecord data should deserialize correctly") {
    val b = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writePromCounterRecord(b, metric, treeTags, 100000L, 99.0)
    b.allContainers.head.iterate(Schemas.promCounter.ingestionSchema).foreach { row =>
      row.getLong(0) shouldEqual 100000L
      row.getDouble(1) shouldEqual 99.0
    }
  }

  it("TreeMap writeDeltaCounterRecord data should deserialize correctly") {
    val b = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeDeltaCounterRecord(b, metric, treeTags, 100000L, 7.0)
    b.allContainers.head.iterate(Schemas.deltaCounter.ingestionSchema).foreach { row =>
      row.getLong(0) shouldEqual 100000L
      row.getDouble(1) shouldEqual 7.0
    }
  }

  it("TreeMap writeUntypedRecord data should deserialize correctly") {
    val b = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeUntypedRecord(b, metric, treeTags, 100000L, 3.14)
    b.allContainers.head.iterate(Schemas.untyped.ingestionSchema).foreach { row =>
      row.getLong(0) shouldEqual 100000L
      row.getDouble(1) shouldEqual 3.14
    }
  }

  // ── Many tags tests for scalar TreeMap overloads ───────────────────

  it("TreeMap writePromCounterRecord with many tags should match") {
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writePromCounterRecord(b1, metric, manyTags, 100000L, 99.0)
    InputRecord.writePromCounterRecord(b2, metric, manyTreeTags, 100000L, 99.0)
    firstRecordBytes(b1, Schemas.promCounter) shouldEqual
      firstRecordBytes(b2, Schemas.promCounter)
  }

  it("TreeMap writeDeltaCounterRecord with many tags should match") {
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeDeltaCounterRecord(b1, metric, manyTags, 100000L, 7.0)
    InputRecord.writeDeltaCounterRecord(b2, metric, manyTreeTags, 100000L, 7.0)
    firstRecordBytes(b1, Schemas.deltaCounter) shouldEqual
      firstRecordBytes(b2, Schemas.deltaCounter)
  }

  it("TreeMap writeUntypedRecord with many tags should match") {
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeUntypedRecord(b1, metric, manyTags, 100000L, 3.14)
    InputRecord.writeUntypedRecord(b2, metric, manyTreeTags, 100000L, 3.14)
    firstRecordBytes(b1, Schemas.untyped) shouldEqual
      firstRecordBytes(b2, Schemas.untyped)
  }

  // ── No-bucket skip tests for all histogram TreeMap overloads ───────

  it("TreeMap writePromHistRecord should skip when no buckets") {
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writePromHistRecord(b1, metric, baseTags, 100000L, sumCountKVs)
    InputRecord.writePromHistRecord(b2, metric, treeTags, 100000L, sumCountKVs)
    b1.allContainers shouldBe empty
    b2.allContainers shouldBe empty
  }

  it("TreeMap writeDeltaHistRecord should skip when no buckets") {
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeDeltaHistRecord(b1, metric, baseTags, 100000L, sumCountKVs)
    InputRecord.writeDeltaHistRecord(b2, metric, treeTags, 100000L, sumCountKVs)
    b1.allContainers shouldBe empty
    b2.allContainers shouldBe empty
  }

  it("TreeMap writeOtelDeltaHistRecord should skip when no buckets") {
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeOtelDeltaHistRecord(b1, metric, baseTags, 100000L, sumCountMinMaxKVs)
    InputRecord.writeOtelDeltaHistRecord(b2, metric, treeTags, 100000L, sumCountMinMaxKVs)
    b1.allContainers shouldBe empty
    b2.allContainers shouldBe empty
  }

  it("TreeMap writeOtelCumulativeHistRecord should skip when no buckets") {
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeOtelCumulativeHistRecord(b1, metric, baseTags, 100000L, sumCountMinMaxKVs)
    InputRecord.writeOtelCumulativeHistRecord(b2, metric, treeTags, 100000L, sumCountMinMaxKVs)
    b1.allContainers shouldBe empty
    b2.allContainers shouldBe empty
  }

  // ── Deserialization tests for histogram TreeMap overloads ──────────

  private val histBuckets = Array(0.5, 1, 2.5, 5, 10, Double.PositiveInfinity)
  private val expectedHist = LongHistogram(CustomBuckets(histBuckets), counts)
  private def histBucketKVs: Seq[(String, Double)] = histBuckets.zip(counts).map {
    case (Double.PositiveInfinity, c) => "+Inf" -> c.toDouble
    case (b, c) => b.toString -> c.toDouble
  }.toSeq

  it("TreeMap writePromHistRecord data should deserialize correctly") {
    val b = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writePromHistRecord(b, metric, treeTags, 100000L,
      histBucketKVs ++ sumCountKVs)
    b.allContainers.head.iterate(Schemas.promHistogram.ingestionSchema).foreach { row =>
      row.getDouble(1) shouldEqual sum
      row.getDouble(2) shouldEqual count
      row.getHistogram(3) shouldEqual expectedHist
    }
  }

  it("TreeMap writeDeltaHistRecord data should deserialize correctly") {
    val b = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeDeltaHistRecord(b, metric, treeTags, 100000L,
      histBucketKVs ++ sumCountKVs)
    b.allContainers.head.iterate(Schemas.deltaHistogram.ingestionSchema).foreach { row =>
      row.getDouble(1) shouldEqual sum
      row.getDouble(2) shouldEqual count
      row.getHistogram(3) shouldEqual expectedHist
    }
  }

  it("TreeMap writeOtelDeltaHistRecord data should deserialize correctly") {
    val b = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeOtelDeltaHistRecord(b, metric, treeTags, 100000L,
      histBucketKVs ++ sumCountMinMaxKVs)
    b.allContainers.head.iterate(Schemas.otelDeltaHistogram.ingestionSchema).foreach { row =>
      row.getDouble(1) shouldEqual sum
      row.getDouble(2) shouldEqual count
      row.getHistogram(3) shouldEqual expectedHist
      row.getDouble(4) shouldEqual min
      row.getDouble(5) shouldEqual max
    }
  }

  it("TreeMap writeOtelCumulativeHistRecord data should deserialize correctly") {
    val b = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeOtelCumulativeHistRecord(b, metric, treeTags, 100000L,
      histBucketKVs ++ sumCountMinMaxKVs)
    b.allContainers.head.iterate(Schemas.otelCumulativeHistogram.ingestionSchema).foreach { row =>
      row.getDouble(1) shouldEqual sum
      row.getDouble(2) shouldEqual count
      row.getHistogram(3) shouldEqual expectedHist
      row.getDouble(4) shouldEqual min
      row.getDouble(5) shouldEqual max
    }
  }

  // ── Many tags tests for histogram TreeMap overloads ────────────────

  private val manyTags = baseTags ++ (0 until 10).map(i => f"label_$i%02d" -> s"val_$i").toMap
  private val manyTreeTags = {
    val t = new java.util.TreeMap[String, String]()
    manyTags.foreach { case (k, v) => t.put(k, v) }
    t
  }

  it("TreeMap writePromHistRecord with many tags should match") {
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writePromHistRecord(b1, metric, manyTags, 100000L, histBucketKVs ++ sumCountKVs)
    InputRecord.writePromHistRecord(b2, metric, manyTreeTags, 100000L, histBucketKVs ++ sumCountKVs)
    firstRecordBytes(b1, Schemas.promHistogram) shouldEqual firstRecordBytes(b2, Schemas.promHistogram)
  }

  it("TreeMap writeDeltaHistRecord with many tags should match") {
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeDeltaHistRecord(b1, metric, manyTags, 100000L, histBucketKVs ++ sumCountKVs)
    InputRecord.writeDeltaHistRecord(b2, metric, manyTreeTags, 100000L, histBucketKVs ++ sumCountKVs)
    firstRecordBytes(b1, Schemas.deltaHistogram) shouldEqual firstRecordBytes(b2, Schemas.deltaHistogram)
  }

  it("TreeMap writeOtelDeltaHistRecord with many tags should match") {
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeOtelDeltaHistRecord(b1, metric, manyTags, 100000L, histBucketKVs ++ sumCountMinMaxKVs)
    InputRecord.writeOtelDeltaHistRecord(b2, metric, manyTreeTags, 100000L, histBucketKVs ++ sumCountMinMaxKVs)
    firstRecordBytes(b1, Schemas.otelDeltaHistogram) shouldEqual firstRecordBytes(b2, Schemas.otelDeltaHistogram)
  }

  it("TreeMap writeOtelCumulativeHistRecord with many tags should match") {
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeOtelCumulativeHistRecord(b1, metric, manyTags, 100000L, histBucketKVs ++ sumCountMinMaxKVs)
    InputRecord.writeOtelCumulativeHistRecord(b2, metric, manyTreeTags, 100000L, histBucketKVs ++ sumCountMinMaxKVs)
    firstRecordBytes(b1, Schemas.otelCumulativeHistogram) shouldEqual
      firstRecordBytes(b2, Schemas.otelCumulativeHistogram)
  }

  // ── OTel Exponential Histogram TreeMap tests ──────────────────────

  private val expBucketScheme = Base2ExpHistogramBuckets(3, -5, 10)
  private val expBucketCounts = Array(1L, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
  private val expBucketKVs: Seq[(String, Double)] = expBucketCounts.zipWithIndex.map {
    case (c, i) => i.toString -> c.toDouble
  }.toSeq
  private val expExtra = Seq(
    "posBucketOffset" -> expBucketScheme.startIndexPositiveBuckets.toDouble,
    "scale" -> expBucketScheme.scale.toDouble)

  it("TreeMap writeOtelExponentialHistRecord should match Scala Map version") {
    val kvs = expBucketKVs ++ sumCountMinMaxKVs ++ expExtra
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeOtelExponentialHistRecord(b1, metric, baseTags, 100000L, kvs, isDelta = true)
    InputRecord.writeOtelExponentialHistRecord(b2, metric, treeTags, 100000L, kvs, isDelta = true)
    firstRecordBytes(b1, Schemas.otelExpDeltaHistogram) shouldEqual
      firstRecordBytes(b2, Schemas.otelExpDeltaHistogram)
  }

  it("TreeMap writeOtelExponentialHistRecord data should deserialize correctly") {
    val kvs = expBucketKVs ++ sumCountMinMaxKVs ++ expExtra
    val b = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeOtelExponentialHistRecord(b, metric, treeTags, 100000L, kvs, isDelta = true)
    val expectedHist = LongHistogram(expBucketScheme, expBucketCounts)
    b.allContainers.head.iterate(Schemas.otelExpDeltaHistogram.ingestionSchema).foreach { row =>
      row.getLong(0) shouldEqual 100000L
      row.getDouble(1) shouldEqual sum
      row.getDouble(2) shouldEqual count
      val hist = row.getHistogram(3).asInstanceOf[LongHistogram]
      hist.buckets shouldEqual expectedHist.buckets
      hist.values shouldEqual expBucketCounts
      row.getDouble(4) shouldEqual min
      row.getDouble(5) shouldEqual max
    }
  }

  it("TreeMap writeOtelExponentialHistRecord should skip when no buckets") {
    val kvs = sumCountMinMaxKVs ++ expExtra
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeOtelExponentialHistRecord(b1, metric, baseTags, 100000L, kvs, isDelta = true)
    InputRecord.writeOtelExponentialHistRecord(b2, metric, treeTags, 100000L, kvs, isDelta = true)
    b1.allContainers shouldBe empty
    b2.allContainers shouldBe empty
  }

  it("TreeMap writeOtelExponentialHistRecord with many tags should match") {
    val kvs = expBucketKVs ++ sumCountMinMaxKVs ++ expExtra
    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeOtelExponentialHistRecord(b1, metric, manyTags, 100000L, kvs, isDelta = true)
    InputRecord.writeOtelExponentialHistRecord(b2, metric, manyTreeTags, 100000L, kvs, isDelta = true)
    firstRecordBytes(b1, Schemas.otelExpDeltaHistogram) shouldEqual
      firstRecordBytes(b2, Schemas.otelExpDeltaHistogram)
  }

  it("TreeMap writeLastDeltaHistRecord should match Scala Map version") {
    val buckets = Array(0.5, 1, 2.5, 5, 10, Double.PositiveInfinity)
    val sumLast = 99.0
    val bucketKVs = buckets.zip(counts).map {
      case (Double.PositiveInfinity, c) => "+Inf" -> c.toDouble
      case (b, c) => b.toString -> c.toDouble
    }.toSeq ++ sumCountMinMaxKVs :+ ("sumLast" -> sumLast)

    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)

    InputRecord.writeLastDeltaHistRecord(b1, metric, baseTags, 100000L, bucketKVs)
    InputRecord.writeLastDeltaHistRecord(b2, metric, treeTags, 100000L, bucketKVs)

    firstRecordBytes(b1, Schemas.deltaHistogramV2) shouldEqual
      firstRecordBytes(b2, Schemas.deltaHistogramV2)
  }

  it("TreeMap writeLastDeltaHistRecord with many tags should match") {
    val manyTags = baseTags ++ (0 until 10).map(i => f"label_$i%02d" -> s"value_$i").toMap
    val manyTreeTags = {
      val t = new java.util.TreeMap[String, String]()
      manyTags.foreach { case (k, v) => t.put(k, v) }
      t
    }
    val buckets = Array(0.5, 1, 2.5, 5, 10, Double.PositiveInfinity)
    val bucketKVs = buckets.zip(counts).map {
      case (Double.PositiveInfinity, c) => "+Inf" -> c.toDouble
      case (b, c) => b.toString -> c.toDouble
    }.toSeq ++ sumCountMinMaxKVs :+ ("sumLast" -> 42.0)

    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)

    InputRecord.writeLastDeltaHistRecord(b1, metric, manyTags, 100000L, bucketKVs)
    InputRecord.writeLastDeltaHistRecord(b2, metric, manyTreeTags, 100000L, bucketKVs)

    firstRecordBytes(b1, Schemas.deltaHistogramV2) shouldEqual
      firstRecordBytes(b2, Schemas.deltaHistogramV2)
  }

  it("TreeMap writeLastDeltaHistRecord with shuffled kvs should match") {
    // Verify that sum/count/min/max/sumLast extraction works regardless of position
    val buckets = Array(0.5, 1, 2.5, 5, 10, Double.PositiveInfinity)
    val shuffledKVs = Seq(
      "sumLast" -> 77.0,
      "count" -> count,
      "+Inf" -> counts(5).toDouble,
      "0.5" -> counts(0).toDouble,
      "min" -> min,
      "1.0" -> counts(1).toDouble,
      "sum" -> sum,
      "2.5" -> counts(2).toDouble,
      "max" -> max,
      "5.0" -> counts(3).toDouble,
      "10.0" -> counts(4).toDouble
    )

    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)

    InputRecord.writeLastDeltaHistRecord(b1, metric, baseTags, 100000L, shuffledKVs)
    InputRecord.writeLastDeltaHistRecord(b2, metric, treeTags, 100000L, shuffledKVs)

    firstRecordBytes(b1, Schemas.deltaHistogramV2) shouldEqual
      firstRecordBytes(b2, Schemas.deltaHistogramV2)
  }

  it("TreeMap writeLastDeltaHistRecord should skip when no buckets") {
    // Only sum/count/min/max/sumLast, no actual bucket data → should be skipped
    val noBucketKVs = sumCountMinMaxKVs :+ ("sumLast" -> 55.0)

    val b1 = new RecordBuilder(MemFactory.onHeapFactory)
    val b2 = new RecordBuilder(MemFactory.onHeapFactory)

    InputRecord.writeLastDeltaHistRecord(b1, metric, baseTags, 100000L, noBucketKVs)
    InputRecord.writeLastDeltaHistRecord(b2, metric, treeTags, 100000L, noBucketKVs)

    // Both should skip — no record written
    b1.allContainers shouldBe empty
    b2.allContainers shouldBe empty
  }

  it("TreeMap writeLastDeltaHistRecord data should deserialize correctly") {
    val buckets = Array(0.5, 1, 2.5, 5, 10, Double.PositiveInfinity)
    val sumLast = 123.456
    val bucketKVs = buckets.zip(counts).map {
      case (Double.PositiveInfinity, c) => "+Inf" -> c.toDouble
      case (b, c) => b.toString -> c.toDouble
    }.toSeq ++ sumCountMinMaxKVs :+ ("sumLast" -> sumLast)

    val b = new RecordBuilder(MemFactory.onHeapFactory)
    InputRecord.writeLastDeltaHistRecord(b, metric, treeTags, 100000L, bucketKVs)

    val expectedHist = LongHistogram(CustomBuckets(buckets), counts)
    b.allContainers.head.iterate(Schemas.deltaHistogramV2.ingestionSchema).foreach { row =>
      row.getLong(0) shouldEqual 100000L  // timestamp
      row.getDouble(1) shouldEqual sum    // sum
      row.getDouble(2) shouldEqual count  // count
      row.getHistogram(3) shouldEqual expectedHist  // histogram
      row.getDouble(4) shouldEqual min    // min
      row.getDouble(5) shouldEqual max    // max
      row.getDouble(6) shouldEqual sumLast // sumLast
    }
  }
}