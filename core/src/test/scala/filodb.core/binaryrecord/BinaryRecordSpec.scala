package filodb.core.binaryrecord

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.sql.Timestamp

import org.scalatest.{FunSpec, Matchers}

import filodb.core.Types._
import filodb.memory.format.{SeqRowReader, TupleRowReader, ZeroCopyUTF8String}

class BinaryRecordSpec extends FunSpec with Matchers {
  import filodb.core.metadata.Column.ColumnType._
  import ZeroCopyUTF8String._

  val schema1_i = new RecordSchema(Seq(IntColumn))
  val schema1_s = new RecordSchema(Seq(StringColumn))

  val schema2_sl = new RecordSchema(Seq(StringColumn, LongColumn))
  val schema2_is = new RecordSchema(Seq(IntColumn, StringColumn))

  val schema3_bdt = new RecordSchema(Seq(IntColumn, DoubleColumn, TimestampColumn))

  val schema4 = new RecordSchema(Seq(StringColumn, MapColumn))

  val reader1 = TupleRowReader((Some("data"), Some(-15L)))
  val reader2 = TupleRowReader((Some(1234),   Some("one-two-three")))
  val reader3 = TupleRowReader((Some(1), Some(5.7), Some(new Timestamp(1000000L))))

  it("should create and extract individual fields and match when all fields present") {
    BinaryRecord(schema1_i, reader2).getInt(0) should equal (1234)
    BinaryRecord(schema1_s, reader1).getString(0) should equal ("data")
    BinaryRecord(schema1_s, reader1).filoUTF8String(0) should equal (ZeroCopyUTF8String("data"))

    val binRec3 = BinaryRecord(schema2_sl, reader1)
    binRec3.notNull(0) should equal (true)
    binRec3.notNull(1) should equal (true)
    binRec3.getString(0) should equal ("data")
    binRec3.getLong(1) should equal (-15L)
    binRec3.noneNull should equal (true)
    intercept[ClassCastException] {
      binRec3.getString(1)
    }

    val binRec4 = BinaryRecord(schema2_is, reader2)
    binRec4.getString(1) should equal ("one-two-three")
    binRec4.getInt(0) should equal (1234)
    binRec4.noneNull should equal (true)

    val binRec5 = BinaryRecord(schema3_bdt, reader3)
    binRec5.getDouble(1) should equal (5.7)
    binRec5.getLong(2) should equal (1000000L)
    binRec5.noneNull should equal (true)
  }

  it("should create and extract fields and check notNull correctly") {
    val binRec1 = BinaryRecord(schema2_sl, TupleRowReader((None, Some(10L))))
    binRec1.notNull(0) should equal (false)
    binRec1.notNull(1) should equal (true)
    binRec1.noneNull should equal (false)
    binRec1.getLong(1) should equal (10L)
  }

  it("should get default values back for null fields") {
    val binRec1 = BinaryRecord(schema2_sl, TupleRowReader((None, None)))
    binRec1.notNull(0) should equal (false)
    binRec1.notNull(1) should equal (false)
    binRec1.noneNull should equal (false)
    binRec1.getLong(1) should equal (0L)
    binRec1.getString(0) should equal ("")
  }

  it("should get bytes out and get back same BinaryRecord") {
    val bytes = BinaryRecord(schema3_bdt, reader3).bytes
    val binRec = BinaryRecord(schema3_bdt, bytes)
    binRec.getDouble(1) should equal (5.7)
    binRec.getLong(2) should equal (1000000L)
  }

  it("should generate same hashcode for different instances of the same RecordSchema") {
    val schema3_is = new RecordSchema(Seq(IntColumn, StringColumn))
    schema2_is.hashCode should equal (schema3_is.hashCode)
  }

  it("should produce shorter BinaryRecords if smaller number of items fed") {
    import filodb.core.GdeltTestData._

    val shortBR1 = BinaryRecord(dataset2, Seq(30L))
    shortBR1.schema.numFields should equal (1)
  }

  it("should semantically compare BinaryRecords field by field") {
    import filodb.core.GdeltTestData._

    // Should compare semantically rather than by binary.  Int occurs first byte-wise, but 2nd semantically
    val rec1 = BinaryRecord(dataset2, Seq(55L, "FRA"))
    rec1 should be < (BinaryRecord(dataset2, Seq(60L, "CHL")))
    rec1 should equal (BinaryRecord(dataset2, Seq(55L, "FRA")))

    // Should be able to compare shorter record with longer one
    BinaryRecord(dataset2, Seq(55L)) should equal (rec1)
    BinaryRecord(dataset2, Seq(56L)) should be > (rec1)
  }

  it("should semantically compare BinaryRecord Int and Long fields correctly") (pending)

  it("should read, write, and compare Map fields") {
    val map1 = Map("application".utf8 -> "FiloDB".utf8, "env".utf8 -> "staging".utf8)
    val binRec1 = BinaryRecord(schema4, SeqRowReader(Seq("400s", map1)))
    binRec1.as[UTF8Map](1) should equal (map1)

    val binRec2 = BinaryRecord(schema4, binRec1.bytes)
    binRec2 should equal (binRec1)
    binRec2.as[UTF8Map](1) should equal (map1)
  }

  it("should serialize and deserialize RecordSchema and BinaryRecordWrapper") {
    RecordSchema(schema3_bdt.toString).fields should equal (schema3_bdt.fields)

    val binRec1 = BinaryRecord(schema2_is, reader2)
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(BinaryRecordWrapper(binRec1))

    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    val readWrapper = ois.readObject().asInstanceOf[BinaryRecordWrapper]
    readWrapper.binRec should equal (binRec1)
  }
}