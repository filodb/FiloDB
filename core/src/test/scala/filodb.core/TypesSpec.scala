package filodb.core

import org.velvia.filo.{RowReader, TupleRowReader}
import scodec.bits._

import org.scalatest.{FunSpec, Matchers}

class TypesSpec extends FunSpec with Matchers {
  import Types._
  import SingleKeyTypes._
  import Ordered._   // enables a < b

  describe("ByteVectorOrdering") {
    it("should compare by length if contents equal") {
      hex"0001" should equal (hex"0001")
      hex"0001" should be > hex"00"
      hex"0001" should be < hex"000102"
    }

    it("should compare by unsigned bytes") {
      hex"00" should be < hex"ff"
      hex"00ff" should be < hex"0100"
      hex"0080" should be > hex"007f"
    }
  }

  describe("KeyTypes") {
    it("should serialize and deserialize SingleKeyTypes correctly") {
      IntKeyType.fromBytes(IntKeyType.toBytes(-500)) should equal (-500)
      LongKeyType.fromBytes(LongKeyType.toBytes(123456789L)) should equal (123456789L)
      StringKeyType.fromBytes(StringKeyType.toBytes("baz")) should equal ("baz")
    }

    it("should serialize and unserialize CompositeKeyTypes correctly") {
      val types = Seq(StringKeyType, IntKeyType, BooleanKeyType)
      val compositeType = CompositeKeyType(types)

      val orig1 = Seq("AdamAndEve", 1001, true)
      compositeType.fromBytes(compositeType.toBytes(orig1)) should equal (orig1)

      val orig2 = Seq("", 2002, false)
      compositeType.fromBytes(compositeType.toBytes(orig2)) should equal (orig2)

    }

    it("should compare CompositeKeyTypes using ordering trait") {
      val types = Seq(IntKeyType, StringKeyType)
      val compositeType = CompositeKeyType(types)

      val orig1 = Seq(1001, "AdamAndEve")
      val orig2 = Seq(1001, "Noah")
      implicit val ordering = compositeType.ordering
      assert(orig1 < orig2)
    }

    it("getKeyFunc should resolve null values to default values") {
      val intKeyFunc = IntKeyType.getKeyFunc(Array(1))
      val row1 = TupleRowReader((Some("ape"), None))
      intKeyFunc(row1) should equal (0)

      val types = Seq(StringKeyType, IntKeyType)
      val compositeType = CompositeKeyType(types)
      val compositeKeyFunc = compositeType.getKeyFunc(Array(0, 1))
      compositeKeyFunc(row1) should equal (Seq("ape", 0))
    }

    it("CompositeKeyType should use getKeyFunc from individual KeyTypes") {
      val row1 = TupleRowReader((Some("ape"), None))

      import filodb.core.metadata.ComputedKeyTypes.ComputedIntKeyType
      val intKeyType = new ComputedIntKeyType((r: RowReader) => -2)
      val types = Seq(StringKeyType, intKeyType)
      val compositeType = CompositeKeyType(types)
      val compositeKeyFunc = compositeType.getKeyFunc(Array(0, 1))
      // Should not throw NullKeyValue anymore, since ComputedIntKeyType should always return -2
      compositeKeyFunc(row1) should equal (Seq("ape", -2))
    }

    it("should binary compare Int and Long key types correctly") {
      IntKeyType.toBytes(-1) should be < IntKeyType.toBytes(1)
      IntKeyType.toBytes(Int.MaxValue) should be > IntKeyType.toBytes(0)
      LongKeyType.toBytes(10L) should be > LongKeyType.toBytes(-20L)
    }
  }
}