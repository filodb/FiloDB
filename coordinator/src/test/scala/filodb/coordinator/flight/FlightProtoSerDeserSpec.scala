package filodb.coordinator.flight

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.query._
import filodb.memory.format.{ZeroCopyUTF8String => UTF8Str}
import filodb.query.ProtoConverters._

class FlightProtoSerDeserSpec extends AnyFunSpec with Matchers {

  private val rvKey = CustomRangeVectorKey(Map(
    UTF8Str("__name__") -> UTF8Str("http_requests"),
    UTF8Str("job")      -> UTF8Str("api-server")
  ))

  private val rvRange = RvRange(1000L, 15000L, 1060000L)

  describe("FlightProtoSerDeser") {

    it("should round-trip key + output range") {
      val proto = FlightProtoSerDeser.deserializeFromBytes(
        FlightProtoSerDeser.rvKeyToProtoBytes(rvKey, Some(rvRange)))
      proto.hasRvKey shouldBe true
      proto.hasSrv  shouldBe false
      proto.getRvKey.getKey.fromProto.labelValues shouldEqual rvKey.labelValues
      proto.getRvKey.hasRvRange shouldBe true
      proto.getRvKey.getRvRange.fromProto shouldEqual rvRange
    }

    it("should round-trip key with no output range") {
      val proto = FlightProtoSerDeser.deserializeFromBytes(
        FlightProtoSerDeser.rvKeyToProtoBytes(rvKey, None))
      proto.hasRvKey shouldBe true
      proto.getRvKey.getKey.fromProto.labelValues shouldEqual rvKey.labelValues
      proto.getRvKey.hasRvRange shouldBe false
    }

    it("should round-trip a ScalarFixedDouble srv") {
      val sfd = ScalarFixedDouble(RangeParams(100, 15, 200), 42.0)
      val proto = FlightProtoSerDeser.deserializeFromBytes(
        FlightProtoSerDeser.srvToProtoBytes(sfd))
      proto.hasSrv  shouldBe true
      proto.hasRvKey shouldBe false
      proto.getSrv.fromProto shouldEqual sfd
    }

    it("should parse empty bytes without throwing (proto3 default), with no fields set") {
      val proto = FlightProtoSerDeser.deserializeFromBytes(Array.emptyByteArray)
      proto.hasSrv   shouldBe false
      proto.hasRvKey shouldBe false
    }
  }
}
