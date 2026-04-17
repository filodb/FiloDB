package filodb.core.binaryrecord2

import java.nio.charset.StandardCharsets
import java.util.TreeMap

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.GlobalConfig
import filodb.core.metadata.{Schema, Schemas}
import filodb.memory._
import filodb.memory.format.UnsafeUtils
import filodb.memory.format.vectors.{BinaryHistogram, CustomBuckets}

class MapEncoderSpec extends AnyFunSpec with Matchers {

  // Load schemas from filodb-defaults.conf
  private val schemas = Schemas.fromConfig(GlobalConfig.defaultFiloConfig).get
  private val partSchema = schemas.part.binSchema

  // All preagg schema types
  private val gaugeSchema = schemas.schemas("preagg-gauge")
  private val deltaCounterSchema = schemas.schemas("preagg-delta-counter")
  private val deltaHistogramSchema = schemas.schemas("preagg-delta-histogram")
  private val otelDeltaHistogramSchema = schemas.schemas("preagg-otel-delta-histogram")
  private val otelExpDeltaHistogramSchema = schemas.schemas("preagg-otel-exp-delta-histogram")
  private val deltaHistogramV2Schema = schemas.schemas("preagg-delta-histogram-v2")

  // Non-preagg schema (raw Prometheus counter)
  private val promCounterSchema = schemas.schemas("prom-counter")

  // Multiple tag maps with varying key/value sizes for exhaustive testing
  private val testTagMaps: Seq[(String, java.util.TreeMap[String, String])] = Seq(
    "small keys and small values" -> sortedMap(
      "_ns_" -> "ns",
      "_ws_" -> "ws",
      "a" -> "1"
    ),
    "small keys and large values" -> sortedMap(
      "_ns_" -> "a" * 100,
      "_ws_" -> "b" * 200,
      "dc" -> "c" * 500
    ),
    "large keys and small values" -> sortedMap(
      "_ns_" -> "x",
      "_ws_" -> "y",
      "k" * 100 -> "v",
      "long_label_name_that_is_verbose" -> "1"
    ),
    "large keys and large values" -> sortedMap(
      "_ns_" -> "a" * 300,
      "_ws_" -> "b" * 300,
      "k" * 80 -> "v" * 500,
      "another_long_key_name" -> "another_long_value_" * 20
    ),
    "mixed predefined and non-predefined keys" -> sortedMap(
      "_ns_" -> "filodb-local",
      "_ws_" -> "aci-telemetry",
      "dc" -> "us-west-2",
      "instance" -> "i-12345",
      "label1" -> "value1",
      "region" -> "us-east-1"
    ),
    "predefined keys only" -> sortedMap(
      "_ns_" -> "filodb-local",
      "_ws_" -> "aci-telemetry"
    )
  )

  describe("MapEncoder.encode and toJavaMap round-trip") {
    testTagMaps.foreach { case (desc, tags) =>
      it(s"should round-trip: $desc") {
        val encoded = MapEncoder.encode(tags, partSchema)
        encoded.length should be > 0
        MapEncoder.toJavaMap(encoded, partSchema) shouldEqual tags
      }
    }

    it("should handle empty map") {
      val encoded = MapEncoder.encode(new TreeMap[String, String](), partSchema)
      MapEncoder.isEmpty(encoded) shouldBe true
      encoded.length shouldEqual 0
      MapEncoder.toJavaMap(encoded, partSchema).isEmpty shouldBe true
    }

    it("should handle null map") {
      val encoded = MapEncoder.encode(null, partSchema)
      MapEncoder.isEmpty(encoded) shouldBe true
    }
  }

  describe("MapEncoder round-trip via byte[]") {
    testTagMaps.foreach { case (desc, tags) =>
      it(s"should decode back correctly: $desc") {
        val encoded = MapEncoder.encode(tags, partSchema)
        val decoded = MapEncoder.toJavaMap(encoded, partSchema)
        decoded shouldEqual tags
      }
    }

    it("should handle null bytes") {
      MapEncoder.isEmpty(null) shouldBe true
      MapEncoder.toJavaMap(null, partSchema).isEmpty shouldBe true
    }

    it("should handle empty bytes") {
      MapEncoder.isEmpty(MapEncoder.EMPTY) shouldBe true
      MapEncoder.toJavaMap(MapEncoder.EMPTY, partSchema).isEmpty shouldBe true
    }
  }

  describe("predefined key compression") {
    it("should produce smaller bytes when keys are predefined") {
      val tags = sortedMap("_ns_" -> "filodb-local", "_ws_" -> "aci-telemetry", "instance" -> "i-001")
      val encoded = MapEncoder.encode(tags, partSchema)
      encoded.length should be < (4 + 4 + 12 + 8 + 8 + 5 + 6 + 6) // raw size without compression
    }
  }

  describe("different tags produce different hashes") {
    it("should produce different partition hashes for records with different tags") {
      val tags1 = sortedMap("_ns_" -> "ns1", "_ws_" -> "ws1")
      val tags2 = sortedMap("_ns_" -> "ns2", "_ws_" -> "ws2")

      val builder1 = new RecordBuilder(MemFactory.onHeapFactory, 4096)
      builder1.startNewRecord(gaugeSchema)
      builder1.addLong(12345L); builder1.addDouble(1.0); builder1.addDouble(0.0)
      builder1.addDouble(1.0); builder1.addDouble(1.0)
      builder1.addString("metric1")
      builder1.addEncodedMap(MapEncoder.encode(tags1, gaugeSchema.ingestionSchema))
      val off1 = builder1.endRecord()
      val hash1 = gaugeSchema.ingestionSchema.partitionHash(builder1.currentContainer.get.base, off1)

      val builder2 = new RecordBuilder(MemFactory.onHeapFactory, 4096)
      builder2.startNewRecord(gaugeSchema)
      builder2.addLong(12345L); builder2.addDouble(1.0); builder2.addDouble(0.0)
      builder2.addDouble(1.0); builder2.addDouble(1.0)
      builder2.addString("metric1")
      builder2.addEncodedMap(MapEncoder.encode(tags2, gaugeSchema.ingestionSchema))
      val off2 = builder2.endRecord()
      val hash2 = gaugeSchema.ingestionSchema.partitionHash(builder2.currentContainer.get.base, off2)

      hash1 should not equal hash2
    }
  }

  describe("consumeMapItems reads back MapEncoder bytes correctly") {
    it("should read back all key-value pairs from a record built with addEncodedMap") {
      val tags = sortedMap(
        "_ns_" -> "filodb-local",
        "_ws_" -> "aci-telemetry",
        "dc" -> "us-west-2",
        "region" -> "us-east-1"
      )
      val ingestion = gaugeSchema.ingestionSchema
      val encoded = MapEncoder.encode(tags, ingestion)

      val builder = new RecordBuilder(MemFactory.onHeapFactory, 4096)
      builder.startNewRecord(gaugeSchema)
      builder.addLong(12345L); builder.addDouble(1.0); builder.addDouble(0.0)
      builder.addDouble(1.0); builder.addDouble(1.0)
      builder.addString("test_metric")
      builder.addEncodedMap(encoded)
      val recOff = builder.endRecord()

      val base = builder.currentContainer.get.base
      val mapFieldIdx = ingestion.numFields - 1
      val readBack = new StringifyMapItemConsumer()
      ingestion.consumeMapItems(base, recOff, mapFieldIdx, readBack)

      val result = new TreeMap[String, String]()
      val pairs = readBack.stringPairs
      for (i <- 0 until pairs.size) {
        val t = pairs.apply(i)
        result.put(t._1, t._2)
      }
      result shouldEqual tags
    }
  }

  describe("addEncodedMap produces identical records to startMap/addMapKeyValue/endMap") {
    val schemaTests: Seq[(String, Schema, RecordBuilder => Unit)] = Seq(
      ("prom-counter", promCounterSchema, { b: RecordBuilder =>
        b.addLong(12345L)
        b.addDouble(42.0)
        b.addString("http_requests_total")
      }),
      ("preagg-gauge", gaugeSchema, { b: RecordBuilder =>
        b.addLong(12345L); b.addDouble(5.0); b.addDouble(1.0)
        b.addDouble(15.0); b.addDouble(5.0)
        b.addString("test_gauge:::suffix")
      }),
      ("preagg-delta-counter", deltaCounterSchema, { b: RecordBuilder =>
        b.addLong(12345L); b.addDouble(10.0); b.addDouble(2.0)
        b.addDouble(20.0); b.addDouble(8.0)
        b.addString("test_counter:::suffix")
      }),
      ("preagg-delta-histogram", deltaHistogramSchema, { b: RecordBuilder =>
        b.addLong(12345L); b.addDouble(100.0); b.addDouble(50.0)
        b.addDouble(10.0); b.addBlob(testHistogramBlob())
        b.addString("test_histogram:::suffix")
      }),
      ("preagg-otel-delta-histogram", otelDeltaHistogramSchema, { b: RecordBuilder =>
        b.addLong(12345L); b.addDouble(100.0); b.addDouble(50.0)
        b.addDouble(10.0); b.addBlob(testHistogramBlob())
        b.addDouble(1.0); b.addDouble(99.0)
        b.addString("test_otel_hist:::suffix")
      }),
      ("preagg-otel-exp-delta-histogram", otelExpDeltaHistogramSchema, { b: RecordBuilder =>
        b.addLong(12345L); b.addDouble(100.0); b.addDouble(50.0)
        b.addDouble(10.0); b.addBlob(testHistogramBlob())
        b.addDouble(1.0); b.addDouble(99.0)
        b.addString("test_otel_exp_hist:::suffix")
      }),
      ("preagg-delta-histogram-v2", deltaHistogramV2Schema, { b: RecordBuilder =>
        b.addLong(12345L); b.addDouble(100.0); b.addDouble(50.0)
        b.addDouble(10.0); b.addBlob(testHistogramBlob())
        b.addDouble(1.0); b.addDouble(99.0); b.addDouble(42.0)
        b.addString("test_hist_v2:::suffix")
      })
    )

    for {
      (schemaName, schema, addCols) <- schemaTests
      (tagDesc, tags) <- testTagMaps
    } {
      it(s"$schemaName with $tagDesc") {
        verifySchemaCompatibility(schema, addCols, tags)
      }
    }
  }

  private def sortedMap(entries: (String, String)*): java.util.TreeMap[String, String] = {
    val map = new TreeMap[String, String]()
    entries.foreach { case (k, v) => map.put(k, v) }
    map
  }

  /** Creates a test histogram blob for hist columns. */
  private def testHistogramBlob(): org.agrona.DirectBuffer = {
    val buckets = new CustomBuckets(Array(1.0, 5.0, 10.0, 50.0, 100.0))
    val values = Array(10L, 20L, 30L, 40L, 50L, 60L)
    val buf = BinaryHistogram.histBuf
    BinaryHistogram.writeDelta(buckets, values, buf)
    buf
  }

  /**
   * Builds a record using startMap/addMapKeyValue/endMap (the traditional path)
   * and another using addEncodedMap, then verifies:
   * 1. Full record bytes are identical
   * 2. Map bytes are identical
   * 3. Partition hashes are identical
   */
  private def verifySchemaCompatibility(schema: Schema,
                                        addDataColumns: RecordBuilder => Unit,
                                        tags: java.util.TreeMap[String, String]): Unit = {
    // Traditional path
    val builder1 = new RecordBuilder(MemFactory.onHeapFactory, 16384)
    builder1.startNewRecord(schema)
    addDataColumns(builder1)
    builder1.startMap()
    tags.entrySet().forEach { entry =>
      builder1.addMapKeyValue(
        entry.getKey.getBytes(StandardCharsets.UTF_8),
        entry.getValue.getBytes(StandardCharsets.UTF_8))
    }
    builder1.endMap()
    val recOff1 = builder1.endRecord()

    // addEncodedMap path
    val ingestion = schema.ingestionSchema
    val encoded = MapEncoder.encode(tags, ingestion)

    val builder2 = new RecordBuilder(MemFactory.onHeapFactory, 16384)
    builder2.startNewRecord(schema)
    addDataColumns(builder2)
    builder2.addEncodedMap(encoded)
    val recOff2 = builder2.endRecord()

    val container1 = builder1.currentContainer.get
    val container2 = builder2.currentContainer.get

    // Compare full record bytes
    val recLen1 = UnsafeUtils.getInt(container1.base, recOff1) + 4
    val recLen2 = UnsafeUtils.getInt(container2.base, recOff2) + 4
    val recBytes1 = new Array[Byte](recLen1)
    val recBytes2 = new Array[Byte](recLen2)
    UnsafeUtils.unsafe.copyMemory(container1.base, recOff1, recBytes1, UnsafeUtils.arayOffset, recLen1)
    UnsafeUtils.unsafe.copyMemory(container2.base, recOff2, recBytes2, UnsafeUtils.arayOffset, recLen2)
    recBytes1 shouldEqual recBytes2

    // Compare map bytes
    val mapFieldIdx = ingestion.numFields - 1
    val mapFieldOffset1 = recOff1 + UnsafeUtils.getInt(container1.base, recOff1 + ingestion.fieldOffset(mapFieldIdx))
    val mapLen1 = UnsafeUtils.getShort(container1.base, mapFieldOffset1) & 0xFFFF
    val mapBytes1 = new Array[Byte](mapLen1)
    UnsafeUtils.unsafe.copyMemory(container1.base, mapFieldOffset1 + 2, mapBytes1, UnsafeUtils.arayOffset, mapLen1)
    encoded shouldEqual mapBytes1

    // Compare partition hashes
    val hash1 = ingestion.partitionHash(container1.base, recOff1)
    val hash2 = ingestion.partitionHash(container2.base, recOff2)
    hash1 shouldEqual hash2
  }
}
