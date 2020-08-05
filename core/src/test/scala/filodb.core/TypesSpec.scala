package filodb.core


import filodb.memory.format.TupleRowReader
import filodb.memory.format.ZeroCopyUTF8String._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class TypesSpec extends AnyFunSpec with Matchers {
  import Ordered._

  import SingleKeyTypes._

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