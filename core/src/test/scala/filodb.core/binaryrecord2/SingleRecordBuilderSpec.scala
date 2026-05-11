package filodb.core.binaryrecord2

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.metadata.Column.ColumnType
import filodb.core.query.ColumnInfo
import filodb.memory.BinaryRegionLarge
import filodb.memory.format.{SeqRowReader, UnsafeUtils}

class SingleRecordBuilderSpec extends AnyFunSpec with Matchers {

  val schema = new RecordSchema(Seq(
    ColumnInfo("ts",  ColumnType.LongColumn),
    ColumnInfo("val", ColumnType.DoubleColumn),
    ColumnInfo("s",   ColumnType.StringColumn)
  ))

  describe("SingleRecordBuilder") {
    it("should write a record into on-heap memory and read back all fields") {
      val buf     = new Array[Byte](1024)
      val base    = buf
      val offset  = UnsafeUtils.arayOffset
      val builder = new SingleRecordBuilder(base, offset, buf.length)( {
        throw new IllegalStateException("Should not have needed to grow buffer")
      })

      val startOff = builder.addFromReader(SeqRowReader(Seq(1234L, 3.14, "hello")), schema, 0)

      schema.getLong(base, startOff, 0)      shouldEqual 1234L
      schema.getDouble(base, startOff, 1)    shouldEqual 3.14
      schema.asJavaString(base, startOff, 2) shouldEqual "hello"

      val recordLen = (BinaryRegionLarge.numBytes(base, startOff) + 7) & ~3
      builder.nextRecordOffset shouldEqual startOff + recordLen
    }

    it("should write two successive records after reset, both readable independently") {
      val buf     = new Array[Byte](1024)
      val base    = buf
      val offset  = UnsafeUtils.arayOffset
      val builder = new SingleRecordBuilder(base, offset, buf.length)( {
        throw new IllegalStateException("Should not have needed to grow buffer")
      })

      val off1 = builder.addFromReader(SeqRowReader(Seq(1L, 1.1, "first")), schema, 0)
      val next = builder.nextRecordOffset
      builder.reset(base, next, (UnsafeUtils.arayOffset + buf.length - next).toInt)

      val off2 = builder.addFromReader(SeqRowReader(Seq(2L, 2.2, "second")), schema, 0)

      schema.getLong(base, off1, 0)      shouldEqual 1L
      schema.asJavaString(base, off1, 2) shouldEqual "first"
      schema.getLong(base, off2, 0)      shouldEqual 2L
      schema.asJavaString(base, off2, 2) shouldEqual "second"
    }

    it("should spill a partial record to a new buffer when variable-length data exceeds remaining capacity") {
      // Schema fixed area: 4 header + 8 Long + 8 Double + 4 String-ptr = 24 bytes.
      // Initial capacity is 30 bytes: fits the fixed area (24 bytes) but not a 13-byte string
      // (needs 15 bytes in the variable area), so the spill fires inside addString.
      // After the spill, the partially-written fixed area is copied to buf2 and the
      // string is appended there, producing a complete, readable record.
      val buf1 = Array.fill[Byte](256)(0)
      val buf2 = Array.fill[Byte](256)(0)
      var spillCount = 0

      val builder = new SingleRecordBuilder(buf1, UnsafeUtils.arayOffset, 30)({
        spillCount += 1
        (buf2, UnsafeUtils.arayOffset, buf2.length)
      })

      val recOff = builder.addFromReader(SeqRowReader(Seq(99L, 1.5, "hello, world!")), schema, 0)

      spillCount                           shouldEqual 1
      recOff                               shouldEqual UnsafeUtils.arayOffset
      schema.getLong(buf2, recOff, 0)      shouldEqual 99L
      schema.getDouble(buf2, recOff, 1)    shouldEqual 1.5
      schema.asJavaString(buf2, recOff, 2) shouldEqual "hello, world!"
      // fixed(24) + length-short(2) + content(13) = 39 bytes, aligned to 40
      builder.nextRecordOffset             shouldEqual UnsafeUtils.arayOffset + 40
    }

    it("should spill on the very first requireBytes call when constructed with zero capacity") {
      // Matches the production pattern in ArrowSerializedRangeVectorOps where the
      // SingleRecordBuilder is created with (ZeroPointer, 0L, 0) so that the first
      // requireBytes call always spills into the allocateNewBuf-supplied buffer.
      // Here we use on-heap arrays so the zero-byte copyMemory is safely a no-op.
      val buf1 = Array.fill[Byte](256)(0)
      val buf2 = Array.fill[Byte](256)(0)
      var spillCount = 0

      val builder = new SingleRecordBuilder(buf1, UnsafeUtils.arayOffset, 0)({
        spillCount += 1
        (buf2, UnsafeUtils.arayOffset, buf2.length)
      })

      val recOff = builder.addFromReader(SeqRowReader(Seq(7L, 2.0, "hi")), schema, 0)

      spillCount                           shouldEqual 1
      recOff                               shouldEqual UnsafeUtils.arayOffset
      schema.getLong(buf2, recOff, 0)      shouldEqual 7L
      schema.getDouble(buf2, recOff, 1)    shouldEqual 2.0
      schema.asJavaString(buf2, recOff, 2) shouldEqual "hi"
    }
  }
}
