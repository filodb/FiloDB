package filodb.core

import org.velvia.filo.{RowReader, TupleRowReader}
import org.velvia.filo.ZeroCopyUTF8String._
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
    it("should compare CompositeKeyTypes using ordering trait") {
      val types = Seq(IntKeyType, StringKeyType)
      val compositeType = CompositeKeyType(types)

      val orig1 = Seq[Any](1001, "AdamAndEve".utf8)
      val orig2 = Seq[Any](1001, "Noah".utf8)
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
      compositeKeyFunc(row1) should equal (Seq("ape".utf8, 0))
    }
  }
}