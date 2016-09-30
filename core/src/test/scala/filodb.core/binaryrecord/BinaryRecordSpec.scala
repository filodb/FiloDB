package filodb.core.binaryrecord

import java.sql.Timestamp
import org.velvia.filo.{RowReader, TupleRowReader, ZeroCopyUTF8String}
import org.scalatest.{Matchers, FunSpec}

class BinaryRecordSpec extends FunSpec with Matchers {
  import filodb.core.metadata.Column.ColumnType._

  val schema1_i = new RecordSchema(Seq(IntColumn))
  val schema1_s = new RecordSchema(Seq(StringColumn))

  val schema2_sl = new RecordSchema(Seq(StringColumn, LongColumn))
  val schema2_is = new RecordSchema(Seq(IntColumn, StringColumn))

  val schema3_bdt = new RecordSchema(Seq(BitmapColumn, DoubleColumn, TimestampColumn))

  val reader1 = TupleRowReader((Some("data"), Some(-15L)))
  val reader2 = TupleRowReader((Some(1234),   Some("one-two-three")))
  val reader3 = TupleRowReader((Some(true), Some(5.7), Some(new Timestamp(1000000L))))

  it("should create and extract individual fields and match when all fields present") {
    BinaryRecord(schema1_i, reader2).getInt(0) should equal (1234)
    BinaryRecord(schema1_s, reader1).getString(0) should equal ("data")
    BinaryRecord(schema1_s, reader1).filoUTF8String(0) should equal (ZeroCopyUTF8String("data"))

    val binRec3 = BinaryRecord(schema2_sl, reader1)
    binRec3.notNull(0) should equal (true)
    binRec3.notNull(1) should equal (true)
    binRec3.getString(0) should equal ("data")
    binRec3.getLong(1) should equal (-15L)
    intercept[ClassCastException] {
      binRec3.getString(1)
    }

    val binRec4 = BinaryRecord(schema2_is, reader2)
    binRec4.getString(1) should equal ("one-two-three")
    binRec4.getInt(0) should equal (1234)

    val binRec5 = BinaryRecord(schema3_bdt, reader3)
    binRec5.getBoolean(0) should equal (true)
    binRec5.getDouble(1) should equal (5.7)
    binRec5.as[Timestamp](2) should equal (new Timestamp(1000000L))
  }

  it("should create and extract fields and check notNull correctly") {
    val binRec1 = BinaryRecord(schema2_sl, TupleRowReader((None, Some(10L))))
    binRec1.notNull(0) should equal (false)
    binRec1.notNull(1) should equal (true)
    binRec1.getLong(1) should equal (10L)
  }

  it("should get default values back for null fields") {
    val binRec1 = BinaryRecord(schema2_sl, TupleRowReader((None, None)))
    binRec1.notNull(0) should equal (false)
    binRec1.notNull(1) should equal (false)
    binRec1.getLong(1) should equal (0L)
    binRec1.getString(0) should equal ("")
  }

  it("should get bytes out and get back same BinaryRecord") {
    val bytes = BinaryRecord(schema3_bdt, reader3).bytes
    val binRec = BinaryRecord(schema3_bdt, bytes)
    binRec.getBoolean(0) should equal (true)
    binRec.getDouble(1) should equal (5.7)
    binRec.as[Timestamp](2) should equal (new Timestamp(1000000L))
  }
}