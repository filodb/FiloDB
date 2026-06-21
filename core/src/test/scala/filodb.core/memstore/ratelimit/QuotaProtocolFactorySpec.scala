package filodb.core.memstore.ratelimit

import com.typesafe.config.ConfigFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.DatasetRef

class QuotaProtocolFactorySpec extends AnyFunSpec with Matchers {

  describe("QuotaProtocolFactory.fromConfig") {
    it("returns NoActionQuotaProtocol when the block is missing") {
      val cfg = ConfigFactory.parseString("foo = bar")
      QuotaProtocolFactory.fromConfig(cfg) shouldBe NoActionQuotaProtocol
    }

    it("returns NoActionQuotaProtocol when enabled = false") {
      val cfg = ConfigFactory.parseString(
        """quota-protocol { enabled = false, class = "x.Y" }""")
      QuotaProtocolFactory.fromConfig(cfg) shouldBe NoActionQuotaProtocol
    }

    it("returns NoActionQuotaProtocol when enabled = true but class is missing") {
      val cfg = ConfigFactory.parseString("""quota-protocol { enabled = true }""")
      QuotaProtocolFactory.fromConfig(cfg) shouldBe NoActionQuotaProtocol
    }

    it("falls back to NoActionQuotaProtocol when the configured class fails to load") {
      val cfg = ConfigFactory.parseString(
        """quota-protocol { enabled = true, class = "does.not.Exist" }""")
      QuotaProtocolFactory.fromConfig(cfg) shouldBe NoActionQuotaProtocol
    }

    it("loads a valid (Config) ctor-based impl and routes calls to it") {
      val cfg = ConfigFactory.parseString(
        s"""quota-protocol {
           |  enabled = true
           |  class = "${classOf[QuotaProtocolFactorySpec.RecordingProtocol].getName}"
           |  marker = "hello"
           |}""".stripMargin)
      val proto = QuotaProtocolFactory.fromConfig(cfg)
      proto shouldBe a[QuotaProtocolFactorySpec.RecordingProtocol]
      val rec = proto.asInstanceOf[QuotaProtocolFactorySpec.RecordingProtocol]
      rec.marker shouldEqual "hello"
      proto.quotaExceeded(DatasetRef("ds"), 0, Seq("ws", "ns"), 100L)
      rec.calls.size() shouldEqual 1
    }

    it("falls back to no-arg ctor when no (Config) ctor matches") {
      val cfg = ConfigFactory.parseString(
        s"""quota-protocol {
           |  enabled = true
           |  class = "${classOf[QuotaProtocolFactorySpec.NoArgProtocol].getName}"
           |}""".stripMargin)
      val proto = QuotaProtocolFactory.fromConfig(cfg)
      proto shouldBe a[QuotaProtocolFactorySpec.NoArgProtocol]
    }
  }
}

object QuotaProtocolFactorySpec {

  /** (Config) ctor — should be picked first by the factory. */
  class RecordingProtocol(cfg: com.typesafe.config.Config) extends QuotaExceededProtocol {
    val marker: String = cfg.getString("marker")
    val calls = new java.util.concurrent.ConcurrentLinkedQueue[Seq[String]]()
    override def quotaExceeded(
        ref: DatasetRef, shardNum: Int, shardKeyPrefix: Seq[String], quota: Long): Unit = {
      calls.add(shardKeyPrefix); ()
    }
  }

  /** No-arg ctor — exercises the fallback path. */
  class NoArgProtocol extends QuotaExceededProtocol {
    override def quotaExceeded(
        ref: DatasetRef, shardNum: Int, shardKeyPrefix: Seq[String], quota: Long): Unit = ()
  }
}
