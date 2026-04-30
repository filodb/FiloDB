package filodb.core.binaryrecord2

import java.nio.charset.StandardCharsets
import java.util.TreeMap
import java.util.{HashSet => JHashSet, LinkedHashMap => JLinkedHashMap}

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

  // ── RecordBuilder.encodeMapFrom(TreeMap) tests ─────────────────────
  // Verifies that encodeMapFrom produces identical records to addMap (Scala path).

  describe("RecordBuilder.encodeMapFrom(TreeMap)") {

    /**
     * Helper: builds a record using the Scala addMap path (baseline).
     * Returns (container base, record offset).
     */
    def buildWithScalaMap(schema: Schema,
                          tags: java.util.TreeMap[String, String]): (Any, Long) = {
      import filodb.memory.format.{ZeroCopyUTF8String => ZCUTF8}
      import ZCUTF8._
      val builder = new RecordBuilder(MemFactory.onHeapFactory, 16384)
      builder.startNewRecord(schema)
      builder.addLong(1000000L)       // timestamp
      builder.addDouble(42.0)         // value
      builder.addString("test-metric")
      val scalaMap = {
        val m = scala.collection.mutable.LinkedHashMap[ZCUTF8, ZCUTF8]()
        val iter = tags.entrySet().iterator()
        while (iter.hasNext) {
          val e = iter.next()
          m += (e.getKey.utf8 -> e.getValue.utf8)
        }
        m.toMap
      }
      builder.addMap(scalaMap)
      val off = builder.endRecord()
      (builder.currentContainer.get.base, off)
    }

    /**
     * Helper: builds a record using the new encodeMapFrom path.
     */
    def buildWithTreeMap(schema: Schema,
                         tags: java.util.TreeMap[String, String]): (Any, Long) = {
      val builder = new RecordBuilder(MemFactory.onHeapFactory, 16384)
      builder.startNewRecord(schema)
      builder.addLong(1000000L)
      builder.addDouble(42.0)
      builder.addString("test-metric")
      builder.encodeMapFrom(tags)
      val off = builder.endRecord()
      (builder.currentContainer.get.base, off)
    }

    /** Extract full record bytes for comparison */
    def recordBytes(base: Any, offset: Long): Array[Byte] = {
      val len = UnsafeUtils.getInt(base, offset) + 4
      val bytes = new Array[Byte](len)
      UnsafeUtils.unsafe.copyMemory(
        base, offset, bytes, UnsafeUtils.arayOffset, len)
      bytes
    }

    testTagMaps.foreach { case (desc, tags) =>
      it(s"should produce identical bytes to addMap: $desc") {
        val schema = gaugeSchema
        val (scalaBase, scalaOff) = buildWithScalaMap(schema, tags)
        val (treeBase, treeOff) = buildWithTreeMap(schema, tags)

        recordBytes(scalaBase, scalaOff) shouldEqual
          recordBytes(treeBase, treeOff)
      }
    }

    it("should produce identical partition hash") {
      val tags = sortedMap(
        "_ns_" -> "filodb-local",
        "_ws_" -> "aci-telemetry",
        "dc" -> "us-west-2",
        "instance" -> "i-12345"
      )
      val schema = gaugeSchema
      val ingestion = schema.ingestionSchema
      val (scalaBase, scalaOff) = buildWithScalaMap(schema, tags)
      val (treeBase, treeOff) = buildWithTreeMap(schema, tags)

      ingestion.partitionHash(scalaBase, scalaOff) shouldEqual
        ingestion.partitionHash(treeBase, treeOff)
    }

    it("should handle empty TreeMap") {
      val tags = new TreeMap[String, String]()
      val schema = gaugeSchema
      val (_, treeOff) = buildWithTreeMap(schema, tags)
      // Should not throw, produces a valid record with empty map
      treeOff should be > 0L
    }

    it("should handle single entry") {
      val tags = sortedMap("_ws_" -> "demo")
      val schema = gaugeSchema
      val (scalaBase, scalaOff) = buildWithScalaMap(schema, tags)
      val (treeBase, treeOff) = buildWithTreeMap(schema, tags)

      recordBytes(scalaBase, scalaOff) shouldEqual
        recordBytes(treeBase, treeOff)
    }

    it("should handle non-ASCII UTF-8 values") {
      val tags = sortedMap(
        "_ns_" -> "名前空間",
        "_ws_" -> "作業空間",
        "city" -> "München"
      )
      val schema = gaugeSchema
      val (scalaBase, scalaOff) = buildWithScalaMap(schema, tags)
      val (treeBase, treeOff) = buildWithTreeMap(schema, tags)

      recordBytes(scalaBase, scalaOff) shouldEqual
        recordBytes(treeBase, treeOff)
    }

    it("should handle many tags (10+)") {
      val tags = new TreeMap[String, String]()
      (0 until 15).foreach { i =>
        tags.put(f"label_$i%02d", s"value_$i")
      }
      tags.put("_ns_", "myapp")
      tags.put("_ws_", "prod")
      val schema = gaugeSchema
      val (scalaBase, scalaOff) = buildWithScalaMap(schema, tags)
      val (treeBase, treeOff) = buildWithTreeMap(schema, tags)

      recordBytes(scalaBase, scalaOff) shouldEqual
        recordBytes(treeBase, treeOff)
    }

    it("should work with different schemas") {
      val tags = sortedMap(
        "_ns_" -> "app",
        "_ws_" -> "demo",
        "job" -> "prometheus"
      )
      Seq(gaugeSchema, deltaCounterSchema, promCounterSchema).foreach { schema =>
        val (scalaBase, scalaOff) = buildWithScalaMap(schema, tags)
        val (treeBase, treeOff) = buildWithTreeMap(schema, tags)
        recordBytes(scalaBase, scalaOff) shouldEqual
          recordBytes(treeBase, treeOff)
      }
    }

    it("should handle max-length key (191 bytes)") {
      val longKey = "k" * 191
      val tags = sortedMap(
        "_ws_" -> "demo",
        longKey -> "value"
      )
      val schema = gaugeSchema
      val (scalaBase, scalaOff) = buildWithScalaMap(schema, tags)
      val (treeBase, treeOff) = buildWithTreeMap(schema, tags)

      recordBytes(scalaBase, scalaOff) shouldEqual
        recordBytes(treeBase, treeOff)
    }

    it("should handle empty string values") {
      val tags = sortedMap(
        "_ns_" -> "",
        "_ws_" -> "",
        "key" -> ""
      )
      val schema = gaugeSchema
      val (scalaBase, scalaOff) = buildWithScalaMap(schema, tags)
      val (treeBase, treeOff) = buildWithTreeMap(schema, tags)

      recordBytes(scalaBase, scalaOff) shouldEqual
        recordBytes(treeBase, treeOff)
    }

    it("should handle single-char keys and values") {
      val tags = sortedMap(
        "a" -> "1",
        "b" -> "2",
        "z" -> "0"
      )
      val schema = gaugeSchema
      val (scalaBase, scalaOff) = buildWithScalaMap(schema, tags)
      val (treeBase, treeOff) = buildWithTreeMap(schema, tags)

      recordBytes(scalaBase, scalaOff) shouldEqual
        recordBytes(treeBase, treeOff)
    }

    // Sort order test: TreeMap sorts by String.compareTo (UTF-16 code unit order)
    // and addMap sorts by ZCUTF8 (UTF-8 byte order). For BMP characters these are
    // equivalent because UTF-8 preserves Unicode code point ordering, and for BMP
    // chars UTF-16 code unit == code point. This test verifies the order matches.
    it("should sort identically for keys with underscores, digits, uppercase") {
      // _ = 0x5F, A = 0x41, Z = 0x5A, a = 0x61, z = 0x7A, 0 = 0x30
      // Sort order: 0 < A < Z < _ < a < z
      val tags = sortedMap(
        "Zulu" -> "v1",
        "_metric_" -> "v2",
        "alpha" -> "v3",
        "0start" -> "v4",
        "Azure" -> "v5",
        "_ns_" -> "v6",
        "_ws_" -> "v7"
      )
      val schema = gaugeSchema
      val (scalaBase, scalaOff) = buildWithScalaMap(schema, tags)
      val (treeBase, treeOff) = buildWithTreeMap(schema, tags)

      recordBytes(scalaBase, scalaOff) shouldEqual
        recordBytes(treeBase, treeOff)
    }

    it("sort order should match for Latin-1 chars (accented)") {
      // Latin-1 chars: UTF-8 2-byte encoding, but sort order preserved
      val tags = sortedMap(
        "_ws_" -> "demo",
        "café" -> "latte",
        "naïve" -> "yes",
        "über" -> "alles"
      )
      val schema = gaugeSchema
      val (scalaBase, scalaOff) = buildWithScalaMap(schema, tags)
      val (treeBase, treeOff) = buildWithTreeMap(schema, tags)

      recordBytes(scalaBase, scalaOff) shouldEqual
        recordBytes(treeBase, treeOff)
    }

    it("should produce identical bytes for all predefined keys") {
      // Build a map using ONLY predefined keys from the schema
      val predefKeys = partSchema.predefinedKeys
      val tags = new TreeMap[String, String]()
      predefKeys.zipWithIndex.foreach { case (key, i) =>
        tags.put(key, s"value_$i")
      }
      if (!tags.isEmpty) {
        val schema = gaugeSchema
        val (scalaBase, scalaOff) = buildWithScalaMap(schema, tags)
        val (treeBase, treeOff) = buildWithTreeMap(schema, tags)

        recordBytes(scalaBase, scalaOff) shouldEqual
          recordBytes(treeBase, treeOff)
      }
    }

    it("should handle value near max size (1000 bytes)") {
      val tags = sortedMap(
        "_ws_" -> "demo",
        "big" -> "x" * 1000
      )
      val schema = gaugeSchema
      val (scalaBase, scalaOff) = buildWithScalaMap(schema, tags)
      val (treeBase, treeOff) = buildWithTreeMap(schema, tags)

      recordBytes(scalaBase, scalaOff) shouldEqual
        recordBytes(treeBase, treeOff)
    }

    it("should handle keys that look similar to predefined but aren't") {
      // Keys that might hash-collide with predefined keys but are different strings
      val tags = sortedMap(
        "_NS_" -> "uppercase-ns",
        "_WS_" -> "uppercase-ws",
        "_ns" -> "missing-trailing-underscore",
        "_ws" -> "missing-trailing-underscore",
        "ns_" -> "missing-leading-underscore"
      )
      val schema = gaugeSchema
      val (scalaBase, scalaOff) = buildWithScalaMap(schema, tags)
      val (treeBase, treeOff) = buildWithTreeMap(schema, tags)

      recordBytes(scalaBase, scalaOff) shouldEqual
        recordBytes(treeBase, treeOff)
    }

    it("should handle duplicate-length keys that sort differently") {
      // Same length keys where byte order matters
      val tags = sortedMap(
        "aaa" -> "1",
        "aab" -> "2",
        "aba" -> "3",
        "baa" -> "4"
      )
      val schema = gaugeSchema
      val (scalaBase, scalaOff) = buildWithScalaMap(schema, tags)
      val (treeBase, treeOff) = buildWithTreeMap(schema, tags)

      recordBytes(scalaBase, scalaOff) shouldEqual
        recordBytes(treeBase, treeOff)
    }
  }

  describe("MapEncoder.getValue") {
    it("should return value for predefined key") {
      val tags = sortedMap("_ws_" -> "aci-telemetry", "_ns_" -> "filodb-local", "dc" -> "us-west-2")
      val encoded = MapEncoder.encode(tags, partSchema)
      MapEncoder.getValue(encoded, partSchema, "_ws_") shouldEqual "aci-telemetry"
      MapEncoder.getValue(encoded, partSchema, "_ns_") shouldEqual "filodb-local"
    }

    it("should return value for non-predefined key") {
      val tags = sortedMap("_ws_" -> "ws", "region" -> "us-east-1", "custom_key" -> "custom_val")
      val encoded = MapEncoder.encode(tags, partSchema)
      MapEncoder.getValue(encoded, partSchema, "region") shouldEqual "us-east-1"
      MapEncoder.getValue(encoded, partSchema, "custom_key") shouldEqual "custom_val"
    }

    it("should return null for missing key") {
      val tags = sortedMap("_ws_" -> "ws", "_ns_" -> "ns")
      val encoded = MapEncoder.encode(tags, partSchema)
      MapEncoder.getValue(encoded, partSchema, "missing") shouldBe null
    }

    it("should return null for null/empty data") {
      MapEncoder.getValue(null, partSchema, "_ws_") shouldBe null
      MapEncoder.getValue(MapEncoder.EMPTY, partSchema, "_ws_") shouldBe null
    }

    it("should return null for null key") {
      val tags = sortedMap("_ws_" -> "ws")
      val encoded = MapEncoder.encode(tags, partSchema)
      MapEncoder.getValue(encoded, partSchema, null) shouldBe null
    }

    it("should match getValue results with toJavaMap.get for all test maps") {
      testTagMaps.foreach { case (desc, tags) =>
        val encoded = MapEncoder.encode(tags, partSchema)
        val decoded = MapEncoder.toJavaMap(encoded, partSchema)
        val iter = tags.entrySet().iterator()
        while (iter.hasNext) {
          val entry = iter.next()
          MapEncoder.getValue(encoded, partSchema, entry.getKey) shouldEqual decoded.get(entry.getKey)
        }
      }
    }
  }

  describe("MapEncoder.forEach") {
    it("should iterate all entries in order matching toJavaMap") {
      val tags = sortedMap(
        "_ns_" -> "filodb-local", "_ws_" -> "aci-telemetry",
        "dc" -> "us-west-2", "region" -> "us-east-1"
      )
      val encoded = MapEncoder.encode(tags, partSchema)

      val collected = new JLinkedHashMap[String, String]()
      MapEncoder.forEach(encoded, partSchema, new MapEntryConsumer {
        override def consume(key: String, value: String): Unit = collected.put(key, value)
      })

      val decoded = MapEncoder.toJavaMap(encoded, partSchema)
      collected.size() shouldEqual decoded.size()
      val collectedIter = collected.entrySet().iterator()
      val decodedIter = decoded.entrySet().iterator()
      while (collectedIter.hasNext) {
        val c = collectedIter.next()
        val d = decodedIter.next()
        c.getKey shouldEqual d.getKey
        c.getValue shouldEqual d.getValue
      }
    }

    it("should handle empty/null data") {
      var called = false
      val consumer = new MapEntryConsumer {
        override def consume(key: String, value: String): Unit = called = true
      }
      MapEncoder.forEach(null, partSchema, consumer)
      called shouldBe false
      MapEncoder.forEach(MapEncoder.EMPTY, partSchema, consumer)
      called shouldBe false
    }

    it("should iterate all test maps correctly") {
      testTagMaps.foreach { case (_, tags) =>
        val encoded = MapEncoder.encode(tags, partSchema)
        val collected = new TreeMap[String, String]()
        MapEncoder.forEach(encoded, partSchema, new MapEntryConsumer {
          override def consume(key: String, value: String): Unit = collected.put(key, value)
        })
        collected shouldEqual tags
      }
    }
  }

  describe("MapEncoder.retain") {
    it("should keep only specified keys") {
      val tags = sortedMap("_ws_" -> "ws", "_ns_" -> "ns", "dc" -> "us-west-2", "region" -> "us-east-1")
      val encoded = MapEncoder.encode(tags, partSchema)

      val keysToKeep = new JHashSet[String]()
      keysToKeep.add("_ws_")
      keysToKeep.add("_ns_")

      val retained = MapEncoder.retain(encoded, partSchema, keysToKeep)
      val result = MapEncoder.toJavaMap(retained, partSchema)
      result.size() shouldEqual 2
      result.get("_ws_") shouldEqual "ws"
      result.get("_ns_") shouldEqual "ns"
    }

    it("should return EMPTY when no keys match") {
      val tags = sortedMap("_ws_" -> "ws", "_ns_" -> "ns")
      val encoded = MapEncoder.encode(tags, partSchema)

      val keysToKeep = new JHashSet[String]()
      keysToKeep.add("missing")

      val retained = MapEncoder.retain(encoded, partSchema, keysToKeep)
      MapEncoder.isEmpty(retained) shouldBe true
    }

    it("should return EMPTY for empty keysToKeep") {
      val tags = sortedMap("_ws_" -> "ws")
      val encoded = MapEncoder.encode(tags, partSchema)
      val retained = MapEncoder.retain(encoded, partSchema, new JHashSet[String]())
      MapEncoder.isEmpty(retained) shouldBe true
    }

    it("should handle null/empty data") {
      val keys = new JHashSet[String]()
      keys.add("_ws_")
      MapEncoder.isEmpty(MapEncoder.retain(null, partSchema, keys)) shouldBe true
      MapEncoder.isEmpty(MapEncoder.retain(MapEncoder.EMPTY, partSchema, keys)) shouldBe true
    }

    it("should produce valid encoded bytes that round-trip correctly") {
      testTagMaps.foreach { case (_, tags) =>
        val encoded = MapEncoder.encode(tags, partSchema)
        val keysToKeep = new JHashSet[String]()
        keysToKeep.add("_ws_")
        keysToKeep.add("_ns_")

        val retained = MapEncoder.retain(encoded, partSchema, keysToKeep)
        val decoded = MapEncoder.toJavaMap(retained, partSchema)

        val iter = decoded.entrySet().iterator()
        while (iter.hasNext) {
          val entry = iter.next()
          keysToKeep.contains(entry.getKey) shouldBe true
          entry.getValue shouldEqual tags.get(entry.getKey)
        }
      }
    }
  }

  describe("MapEncoder.remove") {
    it("should remove specified keys") {
      val tags = sortedMap("_ws_" -> "ws", "_ns_" -> "ns", "dc" -> "us-west-2", "region" -> "us-east-1")
      val encoded = MapEncoder.encode(tags, partSchema)

      val keysToRemove = new JHashSet[String]()
      keysToRemove.add("_ws_")
      keysToRemove.add("_ns_")

      val removed = MapEncoder.remove(encoded, partSchema, keysToRemove)
      val result = MapEncoder.toJavaMap(removed, partSchema)
      result.size() shouldEqual 2
      result.get("dc") shouldEqual "us-west-2"
      result.get("region") shouldEqual "us-east-1"
    }

    it("should return all entries when no keys match") {
      val tags = sortedMap("_ws_" -> "ws", "_ns_" -> "ns")
      val encoded = MapEncoder.encode(tags, partSchema)

      val keysToRemove = new JHashSet[String]()
      keysToRemove.add("missing")

      val removed = MapEncoder.remove(encoded, partSchema, keysToRemove)
      MapEncoder.toJavaMap(removed, partSchema) shouldEqual tags
    }

    it("should return copy of all entries for empty keysToRemove") {
      val tags = sortedMap("_ws_" -> "ws", "_ns_" -> "ns")
      val encoded = MapEncoder.encode(tags, partSchema)
      val removed = MapEncoder.remove(encoded, partSchema, new JHashSet[String]())
      MapEncoder.toJavaMap(removed, partSchema) shouldEqual tags
    }

    it("should handle null/empty data") {
      val keys = new JHashSet[String]()
      keys.add("_ws_")
      MapEncoder.isEmpty(MapEncoder.remove(null, partSchema, keys)) shouldBe true
      MapEncoder.isEmpty(MapEncoder.remove(MapEncoder.EMPTY, partSchema, keys)) shouldBe true
    }
  }

  describe("retain and remove are complementary") {
    it("should produce non-overlapping partitions that union to original") {
      testTagMaps.foreach { case (desc, tags) =>
        val encoded = MapEncoder.encode(tags, partSchema)
        val keysToKeep = new JHashSet[String]()
        keysToKeep.add("_ws_")
        keysToKeep.add("_ns_")

        val retained = MapEncoder.toJavaMap(MapEncoder.retain(encoded, partSchema, keysToKeep), partSchema)
        val removed = MapEncoder.toJavaMap(MapEncoder.remove(encoded, partSchema, keysToKeep), partSchema)

        // No overlap
        val retainedIter = retained.keySet().iterator()
        while (retainedIter.hasNext) {
          removed.containsKey(retainedIter.next()) shouldBe false
        }

        // Union equals original
        val union = new TreeMap[String, String]()
        union.putAll(retained)
        union.putAll(removed)
        union shouldEqual tags
      }
    }
  }
}
