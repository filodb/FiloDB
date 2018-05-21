package filodb.memory

import org.scalatest.{FunSpec, Matchers, BeforeAndAfterAll}
import org.scalatest.prop.PropertyChecks

import filodb.memory.format.UnsafeUtils.ZeroPointer

class UTF8StringTest extends FunSpec with Matchers with BeforeAndAfterAll with PropertyChecks {
  import UTF8StringMedium._

  val nativeMem = new NativeMemoryManager(10 * 1024 * 1024)

  override def afterAll(): Unit = {
    nativeMem.shutdown()   // free all memory
  }

  describe("UTF8StringMedium") {
    it("should compare two strings properly") {
      // Unequal lengths, equal prefix
      compare(native("boobeebob", nativeMem).address, native("boobee", nativeMem).address) should be > 0

      // Equal lengths, different content
      // First comparison fights against int comparisons without proper byte ordering
      native("aaab", nativeMem).compare(native("baaa", nativeMem)) should be < 0
      UTF8StringMedium.equals("bobcat".utf8(nativeMem).address, "bobcat".utf8(nativeMem).address) shouldEqual true

      // Strings longer than 8 chars (in case comparison uses long compare)
      compare("dictionary".utf8(nativeMem).address, "pictionar".utf8(nativeMem).address) should be < 0
      compare("dictionary".utf8(nativeMem).address, "dictionaries".utf8(nativeMem).address) should be > 0

      // Calling equals to some other type should return false
      // Not applicable to non objects
      // UTF8StringMedium("dictionary") should not equal ("dictionary")
    }

    it("should compare on and offheap UTF8Strings") {
      val (base1, off1) = UTF8StringMedium("bobcat")
      val nativeStr1    = "bobcat".utf8(nativeMem)
      UTF8StringMedium.equals(base1, off1, ZeroPointer, nativeStr1.address) shouldEqual true
      UTF8StringMedium.equals(ZeroPointer, nativeStr1.address, base1, off1) shouldEqual true

      val (base2, off2) = UTF8StringMedium("dictionary")
      compare(base2, off2, ZeroPointer, "pictionar".utf8(nativeMem).address) should be < 0
      compare(ZeroPointer, "pictionar".utf8(nativeMem).address, base2, off2) should be > 0
    }

    it("should compare random strings properly") {
      import java.lang.Integer.signum
      forAll { (strs: (String, String)) =>
        val nativeCmp = signum(strs._1.compare(strs._2))
        signum(strs._1.utf8(nativeMem).compare(strs._2.utf8(nativeMem))) shouldEqual nativeCmp
      }
    }

    def getHash32(s: String): Int = {
      val (base, off) = UTF8StringMedium(s)
      UTF8StringMedium.hashCode(base, off)
    }

    def getHash64(s: String): Long = {
      val (base, off) = UTF8StringMedium(s)
      UTF8StringMedium.hashCode64(base, off)
    }

    it("should generate same hashcode for same content") {
      getHash32("bobcat") shouldEqual getHash32("bobcat")
      getHash32("bobcat") should not equal (getHash32("bob"))

      getHash64("bobcat") should equal (getHash64("bobcat"))
      getHash64("bobcat") should not equal (getHash64("bob"))
    }

    val str1 = UTF8StringMedium.native("1234", nativeMem)
    val str2 = UTF8StringMedium.native("一2三4", nativeMem)
    val str3 = UTF8StringMedium.native("一二34", nativeMem)

    it("should startsWith and endsWith correctly") {
      str2.startsWith("一2".utf8(nativeMem)) shouldEqual true
      str2.startsWith("2三".utf8(nativeMem)) shouldEqual false
      str2.startsWith(str1) shouldEqual false

      str2.endsWith(str3) shouldEqual false
      str2.endsWith("4".utf8(nativeMem)) shouldEqual true
    }
  }
}