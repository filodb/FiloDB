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

class PreEncodedMapSpec extends AnyFunSpec with Matchers {

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

  private val testTags: java.util.TreeMap[String, String] = {
    val map = new TreeMap[String, String]()
    map.put("_ns_", "filodb-local")
    map.put("_ws_", "aci-telemetry")
    map.put("dc", "us-west-2")
    map.put("instance", "i-12345")
    map.put("label1", "value1")
    map.put("region", "us-east-1")
    map
  }

  describe("PreEncodedMap.encode and toJavaMap round-trip") {
    it("should round-trip with predefined keys only") {
      val t = sortedMap("_ns_" -> "filodb-local", "_ws_" -> "aci-telemetry")
      val pem = PreEncodedMap.encode(t, partSchema)
      pem.isEmpty shouldBe false
      pem.toJavaMap() shouldEqual t
    }

    it("should round-trip with mixed predefined and non-predefined keys") {
      val pem = PreEncodedMap.encode(testTags, partSchema)
      pem.toJavaMap() shouldEqual testTags
    }

    it("should handle empty map") {
      val pem = PreEncodedMap.encode(new TreeMap[String, String](), partSchema)
      pem.isEmpty shouldBe true
      pem.size() shouldEqual 0
      pem.toJavaMap().isEmpty shouldBe true
    }

    it("should handle null map") {
      val pem = PreEncodedMap.encode(null, partSchema)
      pem.isEmpty shouldBe true
    }
  }

  describe("PreEncodedMap.fromBytes") {
    it("should wrap bytes zero-copy") {
      val pem = PreEncodedMap.encode(testTags, partSchema)
      val restored = PreEncodedMap.fromBytes(pem.bytes(), partSchema)
      restored.bytes() should be theSameInstanceAs pem.bytes()
      restored.toJavaMap() shouldEqual testTags
    }

    it("should handle null bytes") {
      PreEncodedMap.fromBytes(null, partSchema).isEmpty shouldBe true
    }

    it("should handle empty bytes") {
      PreEncodedMap.fromBytes(new Array[Byte](0), partSchema).isEmpty shouldBe true
    }
  }

  describe("PreEncodedMap.empty") {
    it("should create an empty map") {
      val pem = PreEncodedMap.empty(partSchema)
      pem.isEmpty shouldBe true
      pem.size() shouldEqual 0
      pem.toJavaMap().isEmpty shouldBe true
    }
  }

  describe("wire format compatibility with RecordBuilder") {
    it("prom-counter (non-preagg schema)") {
      // columns: timestamp, count, metric, tags
      verifySchemaCompatibility(promCounterSchema, { b =>
        b.addLong(12345L)
        b.addDouble(42.0)
        b.addString("http_requests_total")
      })
    }

    it("preagg-gauge") {
      // columns: timestamp, count, min, sum, max, metric, tags
      verifySchemaCompatibility(gaugeSchema, { b =>
        b.addLong(12345L); b.addDouble(5.0); b.addDouble(1.0)
        b.addDouble(15.0); b.addDouble(5.0)
        b.addString("test_gauge:::suffix")
      })
    }

    it("preagg-delta-counter") {
      // columns: timestamp, count, min, sum, max, metric, tags
      verifySchemaCompatibility(deltaCounterSchema, { b =>
        b.addLong(12345L); b.addDouble(10.0); b.addDouble(2.0)
        b.addDouble(20.0); b.addDouble(8.0)
        b.addString("test_counter:::suffix")
      })
    }

    it("preagg-delta-histogram") {
      // columns: timestamp, sum, count, tscount, h(hist), metric, tags
      verifySchemaCompatibility(deltaHistogramSchema, { b =>
        b.addLong(12345L); b.addDouble(100.0); b.addDouble(50.0)
        b.addDouble(10.0); b.addBlob(testHistogramBlob())
        b.addString("test_histogram:::suffix")
      })
    }

    it("preagg-otel-delta-histogram") {
      // columns: timestamp, sum, count, tscount, h(hist), min, max, metric, tags
      verifySchemaCompatibility(otelDeltaHistogramSchema, { b =>
        b.addLong(12345L); b.addDouble(100.0); b.addDouble(50.0)
        b.addDouble(10.0); b.addBlob(testHistogramBlob())
        b.addDouble(1.0); b.addDouble(99.0)
        b.addString("test_otel_hist:::suffix")
      })
    }

    it("preagg-otel-exp-delta-histogram") {
      // columns: timestamp, sum, count, tscount, h(hist:exp), min, max, metric, tags
      verifySchemaCompatibility(otelExpDeltaHistogramSchema, { b =>
        b.addLong(12345L); b.addDouble(100.0); b.addDouble(50.0)
        b.addDouble(10.0); b.addBlob(testHistogramBlob())
        b.addDouble(1.0); b.addDouble(99.0)
        b.addString("test_otel_exp_hist:::suffix")
      })
    }

    it("preagg-delta-histogram-v2") {
      // columns: timestamp, sum, count, tscount, h(hist), min, max, sumLast, metric, tags
      verifySchemaCompatibility(deltaHistogramV2Schema, { b =>
        b.addLong(12345L); b.addDouble(100.0); b.addDouble(50.0)
        b.addDouble(10.0); b.addBlob(testHistogramBlob())
        b.addDouble(1.0); b.addDouble(99.0); b.addDouble(42.0)
        b.addString("test_hist_v2:::suffix")
      })
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
   * and another using addPreEncodedMap, then verifies:
   * 1. Map bytes are identical
   * 2. Full record bytes are identical
   * 3. Partition hashes are identical
   */
  private def verifySchemaCompatibility(schema: Schema,
                                        addDataColumns: RecordBuilder => Unit): Unit = {
    // Traditional path
    val builder1 = new RecordBuilder(MemFactory.onHeapFactory, 4096)
    builder1.startNewRecord(schema)
    addDataColumns(builder1)
    builder1.startMap()
    testTags.entrySet().forEach { entry =>
      builder1.addMapKeyValue(
        entry.getKey.getBytes(StandardCharsets.UTF_8),
        entry.getValue.getBytes(StandardCharsets.UTF_8))
    }
    builder1.endMap()
    val recOff1 = builder1.endRecord()

    // addPreEncodedMap path
    val ingestion = schema.ingestionSchema
    val pem = PreEncodedMap.encode(testTags, ingestion)

    val builder2 = new RecordBuilder(MemFactory.onHeapFactory, 4096)
    builder2.startNewRecord(schema)
    addDataColumns(builder2)
    builder2.addPreEncodedMap(pem.bytes())
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
    pem.bytes() shouldEqual mapBytes1

    // Compare partition hashes
    val hash1 = ingestion.partitionHash(container1.base, recOff1)
    val hash2 = ingestion.partitionHash(container2.base, recOff2)
    hash1 shouldEqual hash2
  }
}
