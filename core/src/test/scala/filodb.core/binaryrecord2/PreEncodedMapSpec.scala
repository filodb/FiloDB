package filodb.core.binaryrecord2

import java.nio.charset.StandardCharsets
import java.util.TreeMap

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.GlobalConfig
import filodb.core.metadata.Schemas
import filodb.memory._
import filodb.memory.format.UnsafeUtils

class PreEncodedMapSpec extends AnyFunSpec with Matchers {

  // Load schemas from filodb-defaults.conf
  private val schemas = Schemas.fromConfig(GlobalConfig.defaultFiloConfig).get
  private val partSchema = schemas.part.binSchema

  // Use preagg-gauge as a representative ingestion schema (has map column for tags)
  private val gaugeSchema = schemas.schemas("preagg-gauge")
  private val ingestionSchema = gaugeSchema.ingestionSchema

  private def sortedMap(entries: (String, String)*): java.util.TreeMap[String, String] = {
    val map = new TreeMap[String, String]()
    entries.foreach { case (k, v) => map.put(k, v) }
    map
  }

  describe("PreEncodedMap.encode and toJavaMap round-trip") {
    it("should round-trip with predefined keys only") {
      val tags = sortedMap("_ns_" -> "filodb-local", "_ws_" -> "aci-telemetry")
      val pem = PreEncodedMap.encode(tags, partSchema)

      pem.isEmpty shouldBe false
      pem.toJavaMap() shouldEqual tags
    }

    it("should round-trip with mixed predefined and non-predefined keys") {
      val tags = sortedMap(
        "_ns_" -> "filodb-local",
        "_ws_" -> "aci-telemetry",
        "dc" -> "us-west-2",
        "instance" -> "i-12345",
        "label1" -> "value1",
        "region" -> "us-east-1"
      )
      val pem = PreEncodedMap.encode(tags, partSchema)
      pem.toJavaMap() shouldEqual tags
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
      val tags = sortedMap("_ns_" -> "filodb-local", "_ws_" -> "aci-telemetry", "dc" -> "us-west-2")
      val pem = PreEncodedMap.encode(tags, partSchema)
      val restored = PreEncodedMap.fromBytes(pem.bytes(), partSchema)
      restored.bytes() should be theSameInstanceAs pem.bytes()
      restored.toJavaMap() shouldEqual tags
    }

    it("should handle null bytes") {
      val pem = PreEncodedMap.fromBytes(null, partSchema)
      pem.isEmpty shouldBe true
    }

    it("should handle empty bytes") {
      val pem = PreEncodedMap.fromBytes(new Array[Byte](0), partSchema)
      pem.isEmpty shouldBe true
    }
  }

  describe("wire format compatibility with RecordBuilder") {
    it("should produce bytes identical to RecordBuilder.startMap/addMapKeyValue/endMap") {
      val tags = sortedMap(
        "_ns_" -> "filodb-local",
        "_ws_" -> "aci-telemetry",
        "dc" -> "us-west-2",
        "instance" -> "i-12345",
        "label1" -> "value1",
        "region" -> "us-east-1"
      )

      val pem = PreEncodedMap.encode(tags, ingestionSchema)

      // Encode same tags via RecordBuilder for byte-level comparison
      val builder = new RecordBuilder(MemFactory.onHeapFactory, 4096)
      builder.startNewRecord(gaugeSchema)
      // preagg-gauge columns: timestamp, count, min, sum, max, metric, tags
      builder.addLong(12345L)          // timestamp
      builder.addDouble(1.0)           // count
      builder.addDouble(0.0)           // min
      builder.addDouble(1.0)           // sum
      builder.addDouble(1.0)           // max
      builder.addString("test_metric") // metric
      builder.startMap()
      tags.entrySet().forEach { entry =>
        builder.addMapKeyValue(
          entry.getKey.getBytes(StandardCharsets.UTF_8),
          entry.getValue.getBytes(StandardCharsets.UTF_8))
      }
      builder.endMap()
      val recOffset = builder.endRecord()

      // Extract map bytes from the binary record
      val base = builder.currentContainer.get.base
      val mapFieldIdx = 6 // tags is the 7th column (0-indexed)
      val mapFieldOffset = recOffset + UnsafeUtils.getInt(base, recOffset + ingestionSchema.fieldOffset(mapFieldIdx))
      val mapLen = UnsafeUtils.getShort(base, mapFieldOffset) & 0xFFFF
      val mapBytes = new Array[Byte](mapLen)
      UnsafeUtils.unsafe.copyMemory(base, mapFieldOffset + 2, mapBytes, UnsafeUtils.arayOffset, mapLen)

      pem.bytes() shouldEqual mapBytes
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

  describe("addPreEncodedMap hash compatibility") {
    it("should produce same partition hash as startMap/addMapKeyValue/endMap") {
      val tags = sortedMap(
        "_ns_" -> "filodb-local",
        "_ws_" -> "aci-telemetry",
        "dc" -> "us-west-2",
        "instance" -> "i-12345",
        "label1" -> "value1",
        "region" -> "us-east-1"
      )

      val pem = PreEncodedMap.encode(tags, ingestionSchema)

      // Build record via traditional startMap/addMapKeyValue/endMap path
      val builder1 = new RecordBuilder(MemFactory.onHeapFactory, 4096)
      builder1.startNewRecord(gaugeSchema)
      builder1.addLong(12345L)
      builder1.addDouble(1.0)
      builder1.addDouble(0.0)
      builder1.addDouble(1.0)
      builder1.addDouble(1.0)
      builder1.addString("test_metric")
      builder1.startMap()
      tags.entrySet().forEach { entry =>
        builder1.addMapKeyValue(
          entry.getKey.getBytes(StandardCharsets.UTF_8),
          entry.getValue.getBytes(StandardCharsets.UTF_8))
      }
      builder1.endMap()
      val recOff1 = builder1.endRecord()
      val hash1 = ingestionSchema.partitionHash(builder1.currentContainer.get.base, recOff1)

      // Build record via addPreEncodedMap path
      val builder2 = new RecordBuilder(MemFactory.onHeapFactory, 4096)
      builder2.startNewRecord(gaugeSchema)
      builder2.addLong(12345L)
      builder2.addDouble(1.0)
      builder2.addDouble(0.0)
      builder2.addDouble(1.0)
      builder2.addDouble(1.0)
      builder2.addString("test_metric")
      builder2.addPreEncodedMap(pem.bytes())
      val recOff2 = builder2.endRecord()
      val hash2 = ingestionSchema.partitionHash(builder2.currentContainer.get.base, recOff2)

      hash1 shouldEqual hash2
    }
  }
}
