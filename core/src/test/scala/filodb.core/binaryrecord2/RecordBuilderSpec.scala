package filodb.core.binaryrecord2

import java.nio.charset.StandardCharsets

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.MachineMetricsData
import filodb.core.metadata.Column.ColumnType
import filodb.core.query.ColumnInfo
import filodb.memory._
import filodb.memory.format.{SeqRowReader, UnsafeUtils, ZeroCopyUTF8String => ZCUTF8}

/**
 * Comprehensive unit tests for RecordBuilder designed as a refactoring safety net.
 *
 * These tests verify behavior across:
 *  - Constructor and container lifecycle
 *  - All field-addition methods (addInt, addLong, addDouble, addString, addBlob, addMap, etc.)
 *  - Hash computation (partition hashes written/not written, values)
 *  - Container management (rollover, reuseOneContainer, reset, rewind, removeAndFreeContainers)
 *  - High-level builders (addFromReader, partKeyFromObjects, addSlowly, addPartKeyRecordFields)
 *  - Serialization (optimalContainerBytes, nonCurrentContainerBytes)
 *  - Companion-object utilities (combineHash, sortAndComputeHashes, shardKeyHash,
 *                                partitionKeyHash, combineHashExcluding, trimShardColumn)
 */
class RecordBuilderSpec extends AnyFunSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll {
  import MachineMetricsData._
  import RecordBuilder.ContainerHeaderLen

  // --- Simple schemas for focused field-type tests ---
  val intLongSchema = new RecordSchema(Seq(
    ColumnInfo("i", ColumnType.IntColumn),
    ColumnInfo("l", ColumnType.LongColumn)
  ))

  val longStrSchema = new RecordSchema(Seq(
    ColumnInfo("ts", ColumnType.LongColumn),
    ColumnInfo("s", ColumnType.StringColumn)
  ))

  val doubleSchema = new RecordSchema(Seq(
    ColumnInfo("d", ColumnType.DoubleColumn)
  ))

  // Partition key schema: single string, partition starts at field 0
  val partStrSchema = new RecordSchema(
    Seq(ColumnInfo("series", ColumnType.StringColumn)),
    partitionFieldStart = Some(0)
  )

  // Non-partition schema with a map field
  val longMapSchema = new RecordSchema(Seq(
    ColumnInfo("ts", ColumnType.LongColumn),
    ColumnInfo("tags", ColumnType.MapColumn)
  ))

  val nativeMem = new NativeMemoryManager(20 * 1024 * 1024)

  override def afterAll(): Unit = nativeMem.shutdown()

  // ==================== 1. Constructor ====================

  describe("RecordBuilder constructor") {
    it("should throw when containerSize is below MinContainerSize") {
      intercept[IllegalArgumentException] {
        new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.MinContainerSize - 1)
      }
    }

    it("should have no containers and no currentContainer on startup (default)") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.allContainers shouldBe empty
      builder.currentContainer shouldBe None
    }

    it("containerRemaining should be 0 when no container is allocated") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.containerRemaining shouldEqual 0
    }

    it("reuseOneContainer=true should allocate one container immediately") {
      val builder = new RecordBuilder(
        MemFactory.onHeapFactory, RecordBuilder.MinContainerSize, reuseOneContainer = true)
      builder.allContainers should have length 1
      builder.currentContainer shouldBe defined
      builder.lastContainer.base should not equal null
    }

    it("lastContainer should throw when no containers are allocated") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      intercept[Exception] { builder.lastContainer }
    }

    it("make() factory should produce a usable builder") {
      val builder = RecordBuilder.make(MemFactory.onHeapFactory, RecordBuilder.MinContainerSize)
      addToBuilder(builder, linearMultiSeries().take(1))
      builder.allContainers should have length 1
      builder.lastContainer.countRecords() shouldEqual 1
    }
  }

  // ==================== 2. Record lifecycle ====================

  describe("Record lifecycle") {
    it("addInt before startNewRecord should throw") {
      intercept[IllegalArgumentException] {
        new RecordBuilder(MemFactory.onHeapFactory).addInt(1)
      }
    }

    it("addLong before startNewRecord should throw") {
      intercept[IllegalArgumentException] {
        new RecordBuilder(MemFactory.onHeapFactory).addLong(1L)
      }
    }

    it("addDouble before startNewRecord should throw") {
      intercept[IllegalArgumentException] {
        new RecordBuilder(MemFactory.onHeapFactory).addDouble(1.0)
      }
    }

    it("addString before startNewRecord should throw") {
      intercept[IllegalArgumentException] {
        new RecordBuilder(MemFactory.onHeapFactory).addString("foo")
      }
    }

    it("adding a field beyond the schema field count should throw") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(longStrSchema)
      builder.addLong(1L)
      builder.addString("a")
      intercept[IllegalArgumentException] { builder.addLong(2L) }
    }

    it("endRecord should increment numRecords in the container") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(longStrSchema)
      builder.addLong(1L); builder.addString("a")
      builder.endRecord()
      builder.lastContainer.numRecords shouldEqual 1

      builder.startNewRecord(longStrSchema)
      builder.addLong(2L); builder.addString("b")
      builder.endRecord()
      builder.lastContainer.numRecords shouldEqual 2
    }

    it("endRecord should align successive record offsets to a 4-byte boundary") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(longStrSchema)
      builder.addLong(1L); builder.addString("abc")   // 3-byte string
      val off1 = builder.endRecord()

      builder.startNewRecord(longStrSchema)
      builder.addLong(2L); builder.addString("de")   // 2-byte string
      val off2 = builder.endRecord()

      (off2 - off1) % 4 shouldEqual 0
    }

    it("startNewRecord(RecordSchema) should require schema with no partitionFieldStart") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      // partStrSchema has partitionFieldStart=Some(0) – must use the schemaID overload
      intercept[IllegalArgumentException] {
        builder.startNewRecord(partStrSchema)
      }
    }

    it("startNewRecord(Schema) should write the ingestion schemaHash into the record") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      addToBuilder(builder, linearMultiSeries().take(1))
      val off = builder.lastContainer.allOffsets(0)
      RecordSchema.schemaID(builder.lastContainer.base, off) shouldEqual schema1.schemaHash
    }

    it("startNewRecord(partSchema, schemaID) should write the supplied schemaID") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(schema1.partKeySchema, 12345)
      builder.addString("myMetric")
      val off = builder.endRecord()
      RecordSchema.schemaID(builder.lastContainer.base, off) shouldEqual 12345
    }
  }

  // ==================== 3. addInt ====================

  describe("addInt") {
    it("should write and read back a typical int value") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(intLongSchema)
      builder.addInt(42); builder.addLong(0L)
      val off = builder.endRecord()
      intLongSchema.getInt(builder.lastContainer.base, off, 0) shouldEqual 42
    }

    it("should correctly round-trip Int.MinValue and Int.MaxValue") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)

      builder.startNewRecord(intLongSchema)
      builder.addInt(Int.MinValue); builder.addLong(0L)
      val off1 = builder.endRecord()
      intLongSchema.getInt(builder.lastContainer.base, off1, 0) shouldEqual Int.MinValue

      builder.startNewRecord(intLongSchema)
      builder.addInt(Int.MaxValue); builder.addLong(0L)
      val off2 = builder.endRecord()
      intLongSchema.getInt(builder.lastContainer.base, off2, 0) shouldEqual Int.MaxValue
    }

    it("addSlowly should dispatch Int correctly") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(intLongSchema)
      builder.addSlowly(77: Int)
      builder.addSlowly(0L)
      val off = builder.endRecord()
      intLongSchema.getInt(builder.lastContainer.base, off, 0) shouldEqual 77
    }
  }

  // ==================== 4. addLong / addDouble ====================

  describe("addLong") {
    it("should round-trip Long.MinValue and Long.MaxValue") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)

      builder.startNewRecord(longStrSchema)
      builder.addLong(Long.MinValue); builder.addString("x")
      val off1 = builder.endRecord()
      longStrSchema.getLong(builder.lastContainer.base, off1, 0) shouldEqual Long.MinValue

      builder.startNewRecord(longStrSchema)
      builder.addLong(Long.MaxValue); builder.addString("y")
      val off2 = builder.endRecord()
      longStrSchema.getLong(builder.lastContainer.base, off2, 0) shouldEqual Long.MaxValue
    }

    it("addSlowly should dispatch Long correctly") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(longStrSchema)
      builder.addSlowly(12345L); builder.addSlowly("test")
      val off = builder.endRecord()
      longStrSchema.getLong(builder.lastContainer.base, off, 0) shouldEqual 12345L
    }
  }

  describe("addDouble") {
    it("should write and read back an arbitrary double") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(doubleSchema)
      builder.addDouble(3.14159)
      val off = builder.endRecord()
      doubleSchema.getDouble(builder.lastContainer.base, off, 0) shouldEqual 3.14159 +- 1e-5
    }

    it("should preserve Double.NaN") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(doubleSchema)
      builder.addDouble(Double.NaN)
      val off = builder.endRecord()
      doubleSchema.getDouble(builder.lastContainer.base, off, 0).isNaN shouldBe true
    }

    it("should preserve Double.PositiveInfinity and NegativeInfinity") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)

      builder.startNewRecord(doubleSchema)
      builder.addDouble(Double.PositiveInfinity)
      val off1 = builder.endRecord()
      doubleSchema.getDouble(builder.lastContainer.base, off1, 0) shouldEqual Double.PositiveInfinity

      builder.startNewRecord(doubleSchema)
      builder.addDouble(Double.NegativeInfinity)
      val off2 = builder.endRecord()
      doubleSchema.getDouble(builder.lastContainer.base, off2, 0) shouldEqual Double.NegativeInfinity
    }

    it("addSlowly should dispatch Double correctly") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(doubleSchema)
      builder.addSlowly(2.718: Double)
      val off = builder.endRecord()
      doubleSchema.getDouble(builder.lastContainer.base, off, 0) shouldEqual 2.718 +- 1e-3
    }

    it("addSlowly should throw UnsupportedOperationException for mismatched type") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(doubleSchema)
      // Passing Int to a DoubleColumn field: no pattern matches → UnsupportedOperationException
      intercept[UnsupportedOperationException] {
        builder.addSlowly(3: Int)
      }
    }
  }

  // ==================== 5. addString and addBlob ====================

  describe("addString and addBlob") {
    it("addString(String) should write and read back UTF-8 text") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(longStrSchema)
      builder.addLong(1L); builder.addString("hello world")
      val off = builder.endRecord()
      longStrSchema.asJavaString(builder.lastContainer.base, off, 1) shouldEqual "hello world"
    }

    it("addString(Array[Byte]) should write and read back raw bytes") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      val bytes = "こんにちは".getBytes(StandardCharsets.UTF_8)
      builder.startNewRecord(longStrSchema)
      builder.addLong(1L); builder.addString(bytes)
      val off = builder.endRecord()
      longStrSchema.asJavaString(builder.lastContainer.base, off, 1) shouldEqual "こんにちは"
    }

    it("addSlowly should dispatch String correctly") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(longStrSchema)
      builder.addSlowly(1L); builder.addSlowly("mystring")
      val off = builder.endRecord()
      longStrSchema.asJavaString(builder.lastContainer.base, off, 1) shouldEqual "mystring"
    }

    it("addString should throw for strings of 65536 bytes or more") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(longStrSchema); builder.addLong(1L)
      intercept[IllegalArgumentException] { builder.addString("x" * 65536) }
    }

    it("addBlob(base, offset, numBytes) should write and read back correctly") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      val bytes = "blob data".getBytes(StandardCharsets.UTF_8)
      builder.startNewRecord(longStrSchema)
      builder.addLong(5L); builder.addBlob(bytes, UnsafeUtils.arayOffset, bytes.length)
      val off = builder.endRecord()
      longStrSchema.asJavaString(builder.lastContainer.base, off, 1) shouldEqual "blob data"
    }

    it("addBlob(ZCUTF8) should write and read back correctly") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(longStrSchema)
      builder.addLong(1L); builder.addBlob(ZCUTF8("zcutf8test"))
      val off = builder.endRecord()
      longStrSchema.asJavaString(builder.lastContainer.base, off, 1) shouldEqual "zcutf8test"
    }

    it("addBlob(DirectBuffer) should read the 2-byte length prefix and write the body") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      val data = "directbuf".getBytes(StandardCharsets.UTF_8)
      // buffer layout: [2-byte length][data bytes]
      val bufArr = new Array[Byte](2 + data.length)
      UnsafeUtils.unsafe.putShort(bufArr, UnsafeUtils.arayOffset, data.length.toShort)
      System.arraycopy(data, 0, bufArr, 2, data.length)
      val buf = new org.agrona.concurrent.UnsafeBuffer(bufArr)

      builder.startNewRecord(longStrSchema)
      builder.addLong(1L); builder.addBlob(buf)
      val off = builder.endRecord()
      longStrSchema.asJavaString(builder.lastContainer.base, off, 1) shouldEqual "directbuf"
    }

    it("string fields that are partition key fields should update partition hash") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)

      builder.startNewRecord(partStrSchema, 1)
      builder.addString("series-A")
      val off1 = builder.endRecord()

      builder.startNewRecord(partStrSchema, 1)
      builder.addString("series-B")
      val off2 = builder.endRecord()

      val base = builder.lastContainer.base
      partStrSchema.partitionHash(base, off1) should not equal partStrSchema.partitionHash(base, off2)
      partStrSchema.partitionHash(base, off1) should not equal RecordBuilder.HASH_INIT
    }

    it("identical partition-key strings should produce identical hashes") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)

      builder.startNewRecord(partStrSchema, 1); builder.addString("same")
      val off1 = builder.endRecord()
      builder.startNewRecord(partStrSchema, 1); builder.addString("same")
      val off2 = builder.endRecord()

      val base = builder.lastContainer.base
      partStrSchema.partitionHash(base, off1) shouldEqual partStrSchema.partitionHash(base, off2)
    }

    it("non-partition string fields should NOT affect the partition hash field") {
      // longStrSchema has no partition, so no hash is written; record should still be readable
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(longStrSchema); builder.addLong(9L); builder.addString("nohash")
      val off = builder.endRecord()
      longStrSchema.getLong(builder.lastContainer.base, off, 0) shouldEqual 9L
    }
  }

  // ==================== 6. Map field operations ====================

  describe("Map field operations") {
    it("startMap called twice without endMap should throw") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(longMapSchema); builder.addLong(1L)
      builder.startMap()
      intercept[IllegalArgumentException] { builder.startMap() }
    }

    it("addMapKeyValue without calling startMap should throw") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(longMapSchema); builder.addLong(1L)
      intercept[IllegalArgumentException] {
        builder.addMapKeyValue("k".getBytes, "v".getBytes)
      }
    }

    it("addMapKeyValue should throw for keys >= 192 bytes") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(longMapSchema); builder.addLong(1L); builder.startMap()
      intercept[IllegalArgumentException] {
        builder.addMapKeyValue(Array.fill(192)(65.toByte), "v".getBytes)
      }
    }

    it("addMapKeyValue should throw for values >= 64KB") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(longMapSchema); builder.addLong(1L); builder.startMap()
      intercept[IllegalArgumentException] {
        builder.addMapKeyValue("k".getBytes, Array.fill(65536)(65.toByte))
      }
    }

    it("addMap(Map[ZCUTF8, ZCUTF8]) should sort keys before encoding") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(longMapSchema)
      builder.addLong(100L)
      builder.addMap(Map(
        ZCUTF8("zoo")   -> ZCUTF8("v1"),
        ZCUTF8("apple") -> ZCUTF8("v2"),
        ZCUTF8("mango") -> ZCUTF8("v3")
      ))
      val off = builder.endRecord()

      val keys   = new collection.mutable.ArrayBuffer[String]
      val values = new collection.mutable.ArrayBuffer[String]
      longMapSchema.consumeMapItems(builder.lastContainer.base, off, 1, new MapItemConsumer {
        def consume(kb: Any, ko: Long, vb: Any, vo: Long, idx: Int): Unit = {
          keys   += UTF8StringShort.toString(kb, ko)
          values += UTF8StringMedium.toString(vb, vo)
        }
      })
      keys   shouldEqual Seq("apple", "mango", "zoo")
      values shouldEqual Seq("v2", "v3", "v1")
    }

    it("encodeMapFrom(TreeMap) should produce identical map content as addMapKeyValue loop") {
      val tags = new java.util.TreeMap[String, String]()
      tags.put("__name__", "cpu_usage")
      tags.put("instance", "host:9090")
      tags.put("job", "myapp")

      def buildWithManualLoop(): (RecordBuilder, Long) = {
        val b = new RecordBuilder(MemFactory.onHeapFactory)
        b.startNewRecord(longMapSchema); b.addLong(1L)
        b.startMap()
        tags.entrySet().forEach(e => b.addMapKeyValue(e.getKey.getBytes, e.getValue.getBytes))
        b.endMap()
        (b, b.endRecord())
      }

      def buildWithEncodeFrom(): (RecordBuilder, Long) = {
        val b = new RecordBuilder(MemFactory.onHeapFactory)
        b.startNewRecord(longMapSchema); b.addLong(1L)
        b.encodeMapFrom(tags)
        (b, b.endRecord())
      }

      def readItems(b: RecordBuilder, off: Long): Seq[(String, String)] = {
        val buf = new collection.mutable.ArrayBuffer[(String, String)]
        longMapSchema.consumeMapItems(b.lastContainer.base, off, 1, new MapItemConsumer {
          def consume(kb: Any, ko: Long, vb: Any, vo: Long, idx: Int): Unit =
            buf += UTF8StringShort.toString(kb, ko) -> UTF8StringMedium.toString(vb, vo)
        })
        buf.toSeq
      }

      val (b1, off1) = buildWithManualLoop()
      val (b2, off2) = buildWithEncodeFrom()
      readItems(b1, off1) shouldEqual readItems(b2, off2)
    }

    it("addEncodedMap should produce the same partition hash as addMapKeyValue loop") {
      val tags = new java.util.TreeMap[String, String]()
      tags.put("job", "prometheus"); tags.put("instance", "host:9090")

      def addFields(b: RecordBuilder): Unit = {
        b.startNewRecord(schema2)
        b.addLong(1L); b.addDouble(1.0); b.addDouble(2.0); b.addDouble(3.0); b.addLong(10L)
        b.addString("s1")
      }

      val b1 = new RecordBuilder(MemFactory.onHeapFactory)
      addFields(b1)
      b1.startMap()
      tags.entrySet().forEach(e => b1.addMapKeyValue(e.getKey.getBytes, e.getValue.getBytes))
      b1.endMap()
      val off1 = b1.endRecord()

      val encoded = MapEncoder.encode(tags, schema2.ingestionSchema)
      val b2 = new RecordBuilder(MemFactory.onHeapFactory)
      addFields(b2); b2.addEncodedMap(encoded)
      val off2 = b2.endRecord()

      schema2.ingestionSchema.partitionHash(b1.lastContainer.base, off1) shouldEqual
        schema2.ingestionSchema.partitionHash(b2.lastContainer.base, off2)
    }

    it("addMapKeyValueHash should update the partition hash (non-initial value)") {
      val partSchema = schema2.partKeySchema  // [series:string, tags:map]
      val keyBytes = "job".getBytes(StandardCharsets.UTF_8)
      val valBytes = "prometheus".getBytes(StandardCharsets.UTF_8)
      val keyHash  = BinaryRegion.hash32(keyBytes)

      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(partSchema, schema2.schemaHash)
      builder.addString("s1")
      builder.startMap()
      builder.addMapKeyValueHash(keyBytes, keyHash, valBytes, 0, valBytes.length)
      builder.endMap(bulkHash = false)
      val off = builder.endRecord()

      partSchema.partitionHash(builder.lastContainer.base, off) should not equal RecordBuilder.HASH_INIT
    }

    it("endMap(bulkHash=false) should leave hash from addMapKeyValueHash unmodified") {
      val partSchema = schema2.partKeySchema
      val keyBytes = "job".getBytes(StandardCharsets.UTF_8)
      val valBytes = "prometheus".getBytes(StandardCharsets.UTF_8)
      val keyHash  = BinaryRegion.hash32(keyBytes)

      // bulkHash=false means endMap does NOT re-hash the map bytes
      val b1 = new RecordBuilder(MemFactory.onHeapFactory)
      b1.startNewRecord(partSchema, schema2.schemaHash); b1.addString("s1")
      b1.startMap()
      b1.addMapKeyValueHash(keyBytes, keyHash, valBytes, 0, valBytes.length)
      b1.endMap(bulkHash = false)
      val off1 = b1.endRecord()

      // Same data, bulkHash=true: endMap re-computes hash from raw bytes
      val b2 = new RecordBuilder(MemFactory.onHeapFactory)
      b2.startNewRecord(partSchema, schema2.schemaHash); b2.addString("s1")
      b2.startMap()
      b2.addMapKeyValue(keyBytes, valBytes)
      b2.endMap(bulkHash = true)
      val off2 = b2.endRecord()

      // Both records are valid (non-initial hash) even though the two strategies differ
      partSchema.partitionHash(b1.lastContainer.base, off1) should not equal RecordBuilder.HASH_INIT
      partSchema.partitionHash(b2.lastContainer.base, off2) should not equal RecordBuilder.HASH_INIT
    }

    it("updatePartitionHash should combine the supplied value into the current hash") {
      val b1 = new RecordBuilder(MemFactory.onHeapFactory)
      b1.startNewRecord(partStrSchema, 1); b1.addString("series")
      val offNoUpdate = b1.endRecord()

      val b2 = new RecordBuilder(MemFactory.onHeapFactory)
      b2.startNewRecord(partStrSchema, 1); b2.addString("series")
      b2.updatePartitionHash(12345)
      val offWithUpdate = b2.endRecord()

      val base1 = b1.lastContainer.base
      val base2 = b2.lastContainer.base
      // Updating with an extra value must change the hash
      partStrSchema.partitionHash(base1, offNoUpdate) should not equal
        partStrSchema.partitionHash(base2, offWithUpdate)
    }
  }

  // ==================== 7. Container rollover ====================

  describe("Container rollover") {
    val recordSize    = 64   // bytes per record for schema1 (linearMultiSeries)
    val maxNumRecords = (RecordBuilder.MinContainerSize - ContainerHeaderLen) / recordSize

    it("should create a second container when the first fills up") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.MinContainerSize)
      addToBuilder(builder, linearMultiSeries().take(maxNumRecords))
      builder.allContainers should have length 1

      addToBuilder(builder, linearMultiSeries().take(1))
      builder.allContainers should have length 2
    }

    it("reuseOneContainer should reset instead of allocating a new container") {
      val builder = new RecordBuilder(
        MemFactory.onHeapFactory, RecordBuilder.MinContainerSize, reuseOneContainer = true)
      addToBuilder(builder, linearMultiSeries().take(maxNumRecords))
      builder.allContainers should have length 1

      addToBuilder(builder, linearMultiSeries().take(1))
      // Container is reused, not grown
      builder.allContainers should have length 1
      // Only the one record added after reset should be present
      builder.lastContainer.countRecords() shouldEqual 1
    }

    it("a partial record in progress should be moved to the new container on rollover") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.MinContainerSize)
      // Fill up all records so only ~48 bytes remain — not enough for an 80-byte record
      addToBuilder(builder, linearMultiSeries().take(maxNumRecords))

      // Start a new record that spans across the old and new container
      builder.startNewRecord(schema1)
      builder.addLong(99999L)
      builder.addDouble(1.0); builder.addDouble(2.0); builder.addDouble(3.0)
      builder.addLong(5L)
      builder.addString("cross-boundary-record")
      val off = builder.endRecord()

      builder.allContainers should have length 2
      // The completed record lives in the new container
      schema1.ingestionSchema.getLong(builder.lastContainer.base, off, 0) shouldEqual 99999L
    }
  }

  // ==================== 8. reset and rewind ====================

  describe("reset") {
    it("reset should clear records and leave an empty container") {
      val builder = new RecordBuilder(nativeMem, RecordBuilder.MinContainerSize)
      addToBuilder(builder, linearMultiSeries().take(5))
      builder.lastContainer.isEmpty shouldBe false

      builder.optimalContainerBytes(reset = true)

      builder.allContainers should have length 1
      builder.lastContainer.isEmpty shouldBe true
      builder.lastContainer.numBytes shouldEqual RecordBuilder.EmptyNumBytes
    }

    it("reset(skipTime=false) should update the container timestamp") {
      val builder = new RecordBuilder(nativeMem)
      addToBuilder(builder, linearMultiSeries().take(3))
      val tsBefore = builder.lastContainer.timestamp

      Thread.sleep(100L)
      builder.reset(skipTime = false)

      builder.lastContainer.timestamp should be > tsBefore
    }

    it("reset(skipTime=true) should NOT update the container timestamp") {
      val builder = new RecordBuilder(nativeMem)
      addToBuilder(builder, linearMultiSeries().take(3))
      val tsBefore = builder.lastContainer.timestamp

      Thread.sleep(50L)
      builder.reset(skipTime = true)

      builder.lastContainer.timestamp shouldEqual tsBefore
    }
  }

  describe("rewind") {
    it("rewind should discard a partial record and allow a new complete record") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      addToBuilder(builder, linearMultiSeries().take(2))

      // Start a partial record and deliberately rewind
      builder.startNewRecord(schema1)
      builder.addLong(99L)
      builder.rewind()

      // Now add a proper record
      builder.startNewRecord(schema1)
      builder.addLong(1L); builder.addDouble(1.0); builder.addDouble(2.0)
      builder.addDouble(3.0); builder.addLong(5L); builder.addString("after-rewind")
      builder.endRecord()

      // Partial record must not appear in count
      builder.lastContainer.countRecords() shouldEqual 3
    }

    it("startNewRecord auto-rewinds when called after a partial record") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      addToBuilder(builder, linearMultiSeries().take(2))

      // Partial record: only addLong, no endRecord
      builder.startNewRecord(schema1)
      builder.addLong(77L)
      // Don't call endRecord – call startNewRecord which will auto-rewind

      builder.startNewRecord(schema1)
      builder.addLong(1L); builder.addDouble(1.0); builder.addDouble(2.0)
      builder.addDouble(3.0); builder.addLong(5L); builder.addString("clean-record")
      builder.endRecord()

      builder.lastContainer.countRecords() shouldEqual 3
    }
  }

  // ==================== 9. Container bytes ====================

  describe("optimalContainerBytes and nonCurrentContainerBytes") {
    val recordSize    = 64
    val maxNumRecords = (RecordBuilder.MinContainerSize - ContainerHeaderLen) / recordSize

    it("optimalContainerBytes should return full + trimmed last container bytes") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.MinContainerSize)
      addToBuilder(builder, linearMultiSeries().take(maxNumRecords + 1))
      builder.allContainers should have length 2

      val bytes = builder.optimalContainerBytes()
      bytes should have length 2
      bytes.head.length shouldEqual RecordBuilder.MinContainerSize
      bytes.last.length should be < RecordBuilder.MinContainerSize
    }

    it("optimalContainerBytes(reset=true) should reset state to one empty container") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.MinContainerSize)
      addToBuilder(builder, linearMultiSeries().take(maxNumRecords + 1))

      builder.optimalContainerBytes(reset = true)

      builder.allContainers should have length 1
      builder.lastContainer.isEmpty shouldBe true
    }

    it("optimalContainerBytes should return empty seq when no records have been written") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.optimalContainerBytes() shouldBe empty
    }

    it("nonCurrentContainerBytes should return only completed (non-current) containers") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.MinContainerSize)
      addToBuilder(builder, linearMultiSeries().take(maxNumRecords + 1))
      builder.allContainers should have length 2

      val bytes = builder.nonCurrentContainerBytes()
      bytes should have length 1
      bytes.head.length shouldEqual RecordBuilder.MinContainerSize
    }

    it("nonCurrentContainerBytes(reset=true) should remove non-current containers") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.MinContainerSize)
      addToBuilder(builder, linearMultiSeries().take(maxNumRecords + 1))

      builder.nonCurrentContainerBytes(reset = true)

      builder.allContainers should have length 1
    }

    it("optimalContainerBytes should throw UnsupportedOperationException for off-heap containers") {
      // off-heap: trimmedArray works for the last container, but .array (called on non-last
      // containers by optimalContainerBytes) throws — so we need 2+ containers
      val builder = new RecordBuilder(nativeMem, RecordBuilder.MinContainerSize)
      addToBuilder(builder, linearMultiSeries().take(maxNumRecords + 1))
      builder.allContainers should have length 2
      intercept[UnsupportedOperationException] { builder.optimalContainerBytes() }
    }
  }

  // ==================== 10. removeAndFreeContainers ====================

  describe("removeAndFreeContainers") {
    val recordSize    = 64
    val maxNumRecords = (RecordBuilder.MinContainerSize - ContainerHeaderLen) / recordSize

    it("should remove the first N containers") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.MinContainerSize)
      addToBuilder(builder, linearMultiSeries().take(maxNumRecords * 2 + 1))
      builder.allContainers should have length 3

      builder.removeAndFreeContainers(1)
      builder.allContainers should have length 2

      builder.removeAndFreeContainers(1)
      builder.allContainers should have length 1
    }

    it("should do nothing when numContainers is 0") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.MinContainerSize)
      addToBuilder(builder, linearMultiSeries().take(3))
      val before = builder.allContainers.length
      builder.removeAndFreeContainers(0)
      builder.allContainers should have length before
    }

    it("should reset all state when all containers are removed") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.MinContainerSize)
      addToBuilder(builder, linearMultiSeries().take(3))
      builder.allContainers should have length 1
      builder.removeAndFreeContainers(1)
      builder.allContainers shouldBe empty
    }

    it("should throw when numContainers exceeds the number of available containers") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.MinContainerSize)
      addToBuilder(builder, linearMultiSeries().take(3))
      intercept[IllegalArgumentException] { builder.removeAndFreeContainers(10) }
    }
  }

  // ==================== 11. High-level builders ====================

  describe("addFromReader") {
    it("addFromReader(row, RecordSchema, schemaID) should build a complete record") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      val row = SeqRowReader(Seq(1000L, "hello"))
      val off = builder.addFromReader(row, longStrSchema, 0)
      longStrSchema.getLong(builder.lastContainer.base, off, 0) shouldEqual 1000L
      longStrSchema.asJavaString(builder.lastContainer.base, off, 1) shouldEqual "hello"
    }

    it("addFromReader(row, Schema) should use ingestionSchema and write schemaHash") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      val row = SeqRowReader(Seq[Any](100L, 1.0, 2.0, 3.0, 10L, "series-test"))
      val off = builder.addFromReader(row, schema1)
      schema1.ingestionSchema.getLong(builder.lastContainer.base, off, 0) shouldEqual 100L
      RecordSchema.schemaID(builder.lastContainer.base, off) shouldEqual schema1.schemaHash
    }
  }

  describe("partKeyFromObjects") {
    it("should build a partition key record from vararg objects") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      val off = builder.partKeyFromObjects(schema1, "myMetric")
      RecordSchema.schemaID(builder.lastContainer.base, off) shouldEqual schema1.schemaHash
      schema1.partKeySchema.asJavaString(builder.lastContainer.base, off, 0) shouldEqual "myMetric"
    }

    it("different metric names should produce different partition hashes") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      val off1 = builder.partKeyFromObjects(schema1, "metric-A")
      val off2 = builder.partKeyFromObjects(schema1, "metric-B")
      val base = builder.lastContainer.base
      schema1.partKeySchema.partitionHash(base, off1) should not equal
        schema1.partKeySchema.partitionHash(base, off2)
    }
  }

  describe("addPartKeyRecordFields") {
    it("should copy partition fields from an existing partition key into the new ingestion record") {
      // Build a standalone partition key
      val pkBuilder = new RecordBuilder(MemFactory.onHeapFactory)
      val pkOff     = pkBuilder.partKeyFromObjects(schema1, "myMetric")
      val pkBase    = pkBuilder.lastContainer.base

      // Build an ingestion record, copying the partition fields from the partition key
      val ingestBuilder = new RecordBuilder(MemFactory.onHeapFactory)
      ingestBuilder.startNewRecord(schema1)
      ingestBuilder.addLong(500L)
      ingestBuilder.addDouble(1.0); ingestBuilder.addDouble(2.0); ingestBuilder.addDouble(3.0)
      ingestBuilder.addLong(5L)
      ingestBuilder.addPartKeyRecordFields(pkBase, pkOff, schema1.partKeySchema)
      val ingestOff = ingestBuilder.endRecord()

      val ingestBase = ingestBuilder.lastContainer.base
      // The series field should match what was in the partition key
      schema1.ingestionSchema.asJavaString(ingestBase, ingestOff, 5) shouldEqual "myMetric"
      // Partition hash in ingestion record should match hash from standalone partition key
      schema1.ingestionSchema.partitionHash(ingestBase, ingestOff) shouldEqual
        schema1.partKeySchema.partitionHash(pkBase, pkOff)
    }
  }

  // ==================== 12. align ====================

  describe("align") {
    it("should return the offset unchanged when already 4-byte aligned") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.align(0L)  shouldEqual 0L
      builder.align(4L)  shouldEqual 4L
      builder.align(8L)  shouldEqual 8L
      builder.align(100L) shouldEqual 100L
    }

    it("should round up to the next 4-byte boundary") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.align(1L) shouldEqual 4L
      builder.align(2L) shouldEqual 4L
      builder.align(3L) shouldEqual 4L
      builder.align(5L) shouldEqual 8L
      builder.align(6L) shouldEqual 8L
      builder.align(7L) shouldEqual 8L
      builder.align(101L) shouldEqual 104L
    }
  }

  // ==================== 13. containerRemaining ====================

  describe("containerRemaining") {
    it("should decrease as records are added") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.MinContainerSize)
      addToBuilder(builder, linearMultiSeries().take(1))
      val after1 = builder.containerRemaining
      addToBuilder(builder, linearMultiSeries().take(1))
      val after2 = builder.containerRemaining
      after2 should be < after1
    }

    it("should be positive but less than containerSize after adding a record") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.MinContainerSize)
      addToBuilder(builder, linearMultiSeries().take(1))
      builder.containerRemaining should be > 0L
      builder.containerRemaining should be < RecordBuilder.MinContainerSize.toLong
    }
  }

  // ==================== 14. Container metadata ====================

  describe("Container metadata") {
    it("container version should equal RecordBuilder.Version") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      addToBuilder(builder, linearMultiSeries().take(1))
      builder.lastContainer.version shouldEqual RecordBuilder.Version
      builder.lastContainer.isCurrentVersion shouldBe true
    }

    it("container timestamp should be set to the current time at creation") {
      val before  = System.currentTimeMillis
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      addToBuilder(builder, linearMultiSeries().take(1))
      val after = System.currentTimeMillis
      builder.lastContainer.timestamp should be >= before
      builder.lastContainer.timestamp should be <= after
    }

    it("isEmpty should reflect whether any records have been committed") {
      val builder = new RecordBuilder(
        MemFactory.onHeapFactory, RecordBuilder.MinContainerSize, reuseOneContainer = true)
      builder.lastContainer.isEmpty shouldBe true
      addToBuilder(builder, linearMultiSeries().take(1))
      builder.lastContainer.isEmpty shouldBe false
    }

    it("currentContainer and allContainers should agree") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      addToBuilder(builder, linearMultiSeries().take(1))
      builder.currentContainer shouldEqual Some(builder.lastContainer)
      builder.allContainers should contain(builder.lastContainer)
    }
  }

  // ==================== 15. RecordBuilder companion: combineHash ====================

  describe("RecordBuilder.combineHash") {
    it("should compute 31 * h1 + h2") {
      RecordBuilder.combineHash(7, 100)  shouldEqual (31 * 7 + 100)
      RecordBuilder.combineHash(0, 0)    shouldEqual 0
      RecordBuilder.combineHash(1, 0)    shouldEqual 31
      RecordBuilder.combineHash(0, 1)    shouldEqual 1
      RecordBuilder.combineHash(100, 200) shouldEqual (31 * 100 + 200)
    }

    it("combineHash is not commutative") {
      RecordBuilder.combineHash(1, 2) should not equal RecordBuilder.combineHash(2, 1)
    }
  }

  // ==================== 16. RecordBuilder companion: sortAndComputeHashes ====================

  describe("RecordBuilder.sortAndComputeHashes") {
    import collection.JavaConverters._

    it("should sort pairs by key and produce one hash per pair") {
      val pairs  = new java.util.ArrayList(Seq("zoo" -> "v1", "apple" -> "v2", "mango" -> "v3").asJava)
      val hashes = RecordBuilder.sortAndComputeHashes(pairs)
      pairs.asScala.map(_._1) shouldEqual Seq("apple", "mango", "zoo")
      hashes.length shouldEqual 3
    }

    it("should produce distinct hashes for distinct key-value pairs") {
      val pairs  = new java.util.ArrayList(Seq("a" -> "1", "b" -> "2", "c" -> "3").asJava)
      val hashes = RecordBuilder.sortAndComputeHashes(pairs)
      hashes.toSet.size shouldEqual 3
    }

    it("should produce the same hash for the same key-value pair across calls") {
      val p1 = new java.util.ArrayList(Seq("k" -> "v").asJava)
      val p2 = new java.util.ArrayList(Seq("k" -> "v").asJava)
      RecordBuilder.sortAndComputeHashes(p1)(0) shouldEqual
        RecordBuilder.sortAndComputeHashes(p2)(0)
    }

    it("should produce different hashes for the same key with different values") {
      val p1 = new java.util.ArrayList(Seq("job" -> "prometheus").asJava)
      val p2 = new java.util.ArrayList(Seq("job" -> "thanos").asJava)
      RecordBuilder.sortAndComputeHashes(p1)(0) should not equal
        RecordBuilder.sortAndComputeHashes(p2)(0)
    }
  }

  // ==================== 17. RecordBuilder companion: combineHashExcluding ====================

  describe("RecordBuilder.combineHashExcluding") {
    import collection.JavaConverters._

    it("should include all pairs when excludeKeys is empty") {
      val pairs  = new java.util.ArrayList(Seq("a" -> "1", "b" -> "2").asJava)
      val hashes = RecordBuilder.sortAndComputeHashes(pairs)
      val h1 = RecordBuilder.combineHashExcluding(pairs, hashes, Set.empty)
      val h2 = RecordBuilder.combineHashExcluding(pairs, hashes, Set("nonexistent"))
      h1 shouldEqual h2
    }

    it("should exclude specified keys from the combined hash") {
      val pairs  = new java.util.ArrayList(Seq("a" -> "1", "le" -> "0.5", "b" -> "2").asJava)
      val hashes = RecordBuilder.sortAndComputeHashes(pairs)
      val hashAll    = RecordBuilder.combineHashExcluding(pairs, hashes, Set.empty)
      val hashNoLe   = RecordBuilder.combineHashExcluding(pairs, hashes, Set("le"))
      hashAll should not equal hashNoLe

      // Excluding "le" should equal a hash computed without "le" in the first place
      val pairsNoLe  = new java.util.ArrayList(Seq("a" -> "1", "b" -> "2").asJava)
      val hashesNoLe = RecordBuilder.sortAndComputeHashes(pairsNoLe)
      RecordBuilder.combineHashExcluding(pairsNoLe, hashesNoLe, Set.empty) shouldEqual hashNoLe
    }
  }

  // ==================== 18. RecordBuilder companion: shardKeyHash ====================

  describe("RecordBuilder.shardKeyHash") {
    val metric   = "host_cpu_load"
    val job      = "prometheus"
    val metricHash = BinaryRegion.hash32(metric.getBytes(StandardCharsets.UTF_8))
    val jobHash    = BinaryRegion.hash32(job.getBytes(StandardCharsets.UTF_8))

    it("metric-only shard key hash should equal 7*31 + metricHash") {
      RecordBuilder.shardKeyHash(Nil, "__name__", metric) shouldEqual (7 * 31 + metricHash)
    }

    it("should combine shard-key values and metric in left-to-right order") {
      val expected = (7 * 31 + jobHash) * 31 + metricHash
      RecordBuilder.shardKeyHash(Seq(job), "__name__", metric) shouldEqual expected
    }

    it("with a targetSchema that excludes metric, metric should be omitted") {
      val tsNoMetric = Seq("job")
      RecordBuilder.shardKeyHash(Seq(job), "__name__", metric, tsNoMetric) shouldEqual
        (31 * 7 + jobHash)
    }

    it("with a targetSchema that includes metric, metric should be included") {
      val tsWithMetric = Seq("job", "__name__")
      RecordBuilder.shardKeyHash(Seq(job), "__name__", metric, tsWithMetric) shouldEqual
        ((7 * 31 + jobHash) * 31 + metricHash)
    }

    it("empty targetSchema should behave like includeMetric=true (default)") {
      RecordBuilder.shardKeyHash(Nil, "__name__", metric, Seq.empty) shouldEqual
        (7 * 31 + metricHash)
    }
  }

  // ==================== 19. RecordBuilder companion: partitionKeyHash ====================

  describe("RecordBuilder.partitionKeyHash") {
    val labels = Map(
      "__name__" -> "host_cpu_load",
      "dc"       -> "AWS-USE",
      "instance" -> "0123892E342342A90",
      "job"      -> "prometheus"
    )
    val nonShardKeys = labels.filter(kv => kv._1 != "job" && kv._1 != "__name__")
    val shardKeys    = labels.filter(kv => kv._1 == "job" || kv._1 == "__name__")

    it("should produce a non-HASH_INIT result when non-shard keys exist") {
      val h = RecordBuilder.partitionKeyHash(
        nonShardKeys, shardKeys, Seq.empty, "__name__", labels("__name__"))
      h should not equal RecordBuilder.HASH_INIT
    }

    it("with targetSchema, should sort and use the specified labels") {
      val instanceHash = BinaryRegion.hash32(labels("instance").getBytes(StandardCharsets.UTF_8))
      val jobHash      = BinaryRegion.hash32(labels("job").getBytes(StandardCharsets.UTF_8))
      val expected = (7 * 31 + instanceHash) * 31 + jobHash
      RecordBuilder.partitionKeyHash(
        nonShardKeys, shardKeys, Seq("job", "instance"), "__name__", labels("__name__")) shouldEqual expected
    }

    it("should ignore the metric label when it is present in nonShardKeyLabelPair") {
      val metricName  = "metric"
      val metricValue = "my-metric"
      val nonShard    = Map("nonShard1" -> "ns1")
      val shardK      = Map("shard1"    -> "s1")
      val targetSchema = Seq("shard1", "nonShard1")

      val h1 = RecordBuilder.partitionKeyHash(nonShard, shardK, targetSchema, metricName, metricValue)
      val h2 = RecordBuilder.partitionKeyHash(
        nonShard + (metricName -> metricValue), shardK, targetSchema, metricName, metricValue)
      h1 shouldEqual h2
    }
  }
  
  describe("RecordBuilder.trimShardColumn") {
    val opts = schema1.options

    it("should trim _bucket suffix") {
      RecordBuilder.trimShardColumn(opts, "__name__", "heap_usage_bucket") shouldEqual "heap_usage"
    }

    it("should trim _sum suffix") {
      RecordBuilder.trimShardColumn(opts, "__name__", "heap_usage_sum") shouldEqual "heap_usage"
    }

    it("should trim _count suffix") {
      RecordBuilder.trimShardColumn(opts, "__name__", "heap_usage_count") shouldEqual "heap_usage"
    }

    it("should not modify metric names without a matching suffix") {
      RecordBuilder.trimShardColumn(opts, "__name__", "heap_usage")       shouldEqual "heap_usage"
      RecordBuilder.trimShardColumn(opts, "__name__", "heap_usage_total") shouldEqual "heap_usage_total"
    }

    it("should not trim for a column that has no configured suffixes") {
      RecordBuilder.trimShardColumn(opts, "job", "prometheus_bucket") shouldEqual "prometheus_bucket"
    }

    it("should trim only the last matching suffix (rightmost match)") {
      // "heap_usage_sum_count" → trim "_count" → "heap_usage_sum"
      RecordBuilder.trimShardColumn(opts, "__name__", "heap_usage_sum_count") shouldEqual "heap_usage_sum"
    }
  }
}
