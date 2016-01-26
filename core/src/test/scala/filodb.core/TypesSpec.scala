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
    it("should serialize and unserialize CompositeKeyTypes correctly") {
      val types = Seq(IntKeyType, StringKeyType)
      val compositeType = CompositeKeyType(types)

      val orig1 = Seq(1001, "AdamAndEve")
      compositeType.fromBytes(compositeType.toBytes(orig1)) should equal (orig1)
    }

    it("should compare CompositeKeyTypes using ordering trait") {
      val types = Seq(IntKeyType, StringKeyType)
      val compositeType = CompositeKeyType(types)

      val orig1 = Seq(1001, "AdamAndEve")
      val orig2 = Seq(1001, "Noah")
      implicit val ordering = compositeType.ordering
      assert(orig1 < orig2)
    }

    it("should check for null keys with getKeyFunc") {
      val intKeyFunc = IntKeyType.getKeyFunc(Array(1))
      val row1 = TupleRowReader((Some("ape"), None))
      intercept[NullKeyValue] {
        intKeyFunc(row1)
      }

      val types = Seq(StringKeyType, IntKeyType)
      val compositeType = CompositeKeyType(types)
      val compositeKeyFunc = compositeType.getKeyFunc(Array(0, 1))
      intercept[NullKeyValue] {
        compositeKeyFunc(row1)
      }
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

    it("should binary compare Int and Long key types correctly") (pending)
  }
}