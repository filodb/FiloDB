package filodb.coordinator.flight

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.binaryrecord2.SingleRecordBuilder
import filodb.core.query._
import filodb.grpc.ProtoRangeVector
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String => UTF8Str}
import filodb.query.ProtoConverters._

class FlightProtoSerDeserSpec extends AnyFunSpec with Matchers {

  private val rvKey = CustomRangeVectorKey(Map(
    UTF8Str("__name__") -> UTF8Str("http_requests"),
    UTF8Str("job")      -> UTF8Str("api-server")
  ))

  private val rvRange = RvRange(1000L, 15000L, 1060000L)

  // Mirrors the encoding in serializeRvKeyToArrowVsr without needing a live VSR/allocator.
  private def encodeRvKey(key: RangeVectorKey, range: Option[RvRange]): ProtoRangeVector.RvMetadata = {
    val buf = new Array[Byte](8192)
    val srb = new SingleRecordBuilder(buf, UnsafeUtils.arayOffset, 8192)(
      throw new IllegalStateException("key exceeded 8 KB"))
    key.writeToMapBr(srb)
    val contentBytes = UnsafeUtils.getInt(buf, UnsafeUtils.arayOffset)
    val rkBuilder = ProtoRangeVector.RvKey.newBuilder()
      .setRvKey(com.google.protobuf.UnsafeByteOperations.unsafeWrap(buf, 0, contentBytes + 4))
    range.foreach(r => rkBuilder.setRvRange(r.toProto))
    FlightProtoSerDeser.deserializeFromBytes(
      ProtoRangeVector.RvMetadata.newBuilder().setRvKey(rkBuilder.build()).build().toByteArray)
  }

  // Mirrors the decoding in convertVsrsIntoArrowSrvs.
  private def decodeRvKey(proto: ProtoRangeVector.RvMetadata): BrMapRangeVectorKey = {
    val rvKeyProto = proto.getRvKey
    val (base, offset, _) = UnsafeUtils.BOLfromBuffer(rvKeyProto.getRvKey.asReadOnlyByteBuffer())
    BrMapRangeVectorKey(base, offset)
  }

  describe("FlightProtoSerDeser") {

    it("should round-trip RvKey with output range") {
      val proto = encodeRvKey(rvKey, Some(rvRange))
      proto.hasRvKey shouldBe true
      proto.hasSrv   shouldBe false
      decodeRvKey(proto).labelValues shouldEqual rvKey.labelValues
      proto.getRvKey.hasRvRange shouldBe true
      proto.getRvKey.getRvRange.fromProto shouldEqual rvRange
    }

    it("should round-trip RvKey with no output range") {
      val proto = encodeRvKey(rvKey, None)
      proto.hasRvKey shouldBe true
      decodeRvKey(proto).labelValues shouldEqual rvKey.labelValues
      proto.getRvKey.hasRvRange shouldBe false
    }

    it("should round-trip BrMapRangeVectorKey (re-encode an already-decoded key)") {
      val first  = decodeRvKey(encodeRvKey(rvKey, None))
      val second = decodeRvKey(encodeRvKey(first, None))
      second.labelValues shouldEqual rvKey.labelValues
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
