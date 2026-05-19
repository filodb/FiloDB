package filodb.coordinator.flight

import org.apache.arrow.memory.ArrowBuf

import filodb.coordinator.flight.ArrowSerializedRangeVectorOps.{maxNumRows, VsrPopulationState}
import filodb.core.binaryrecord2.SingleRecordBuilder
import filodb.core.query.{FlightAllocator, QueryStats, RangeVectorKey, ResultSchema, RvRange, SerializableRangeVector}
import filodb.grpc.{GrpcMultiPartitionQueryService, ProtoRangeVector}
import filodb.memory.format.UnsafeUtils
import filodb.query.ProtoConverters._

object FlightProtoSerDeser {

  // 8 KB covers any realistic partition-key label set.
  // One (buf, builder) pair per thread; reset before each key write — no per-call heap allocation.
  // Watch out when changing to use virtual threads, this thread-local memory can explode
  private val RvKeySerBufSize = 8192 // TODO make configurable
  private val threadLocalKeyBuf = new ThreadLocal[(Array[Byte], SingleRecordBuilder)] {
    override def initialValue(): (Array[Byte], SingleRecordBuilder) = {
      val buf = new Array[Byte](RvKeySerBufSize)
      val srb = new SingleRecordBuilder(buf, UnsafeUtils.arayOffset, RvKeySerBufSize)({
        throw new IllegalStateException(s"RV key binary record exceeds $RvKeySerBufSize bytes")
      })
      (buf, srb)
    }
  }

  def serializeSrvToArrowVsr(srv: SerializableRangeVector, state: VsrPopulationState)
                             (needNewVec: () => Unit): Unit =
    writeProto(ProtoRangeVector.RvMetadata.newBuilder().setSrv(srv.toProto).build(), state, needNewVec)

  def serializeRvKeyToArrowVsr(key: RangeVectorKey, outputRange: Option[RvRange],
                                state: VsrPopulationState)(needNewVec: () => Unit): Unit = {
    val (buf, srb) = threadLocalKeyBuf.get()
    srb.reset(buf, UnsafeUtils.arayOffset, RvKeySerBufSize)
    key.writeToMapBr(srb)
    val contentBytes = UnsafeUtils.getInt(buf, UnsafeUtils.arayOffset)
    // unsafeWrap avoids copying buf into a new ByteString. The ByteString wraps the thread-local
    // array directly, so it must not outlive this call. Safety: writeProto serialises the message
    // synchronously into the Arrow buffer, consuming the bytes before returning. buf is only reused
    // on the next call to this method, by which point the ByteString is no longer referenced.
    val rkBuilder = ProtoRangeVector.RvKey.newBuilder()
      .setRvKey(com.google.protobuf.UnsafeByteOperations.unsafeWrap(buf, 0, contentBytes + 4))
    outputRange.foreach(r => rkBuilder.setRvRange(r.toProto))
    writeProto(ProtoRangeVector.RvMetadata.newBuilder().setRvKey(rkBuilder.build()).build(), state, needNewVec)
  }

  def deserializeFromBytes(bytes: Array[Byte]): ProtoRangeVector.RvMetadata =
    ProtoRangeVector.RvMetadata.parseFrom(bytes)

  // Writes proto bytes directly into the VarBinaryVector data buffer without an intermediate byte[].
  // Mirrors the manual offset-chain bookkeeping in addFromReader — see the comment there for why
  // VarBinaryVector.set() is not used.
  private def writeProto(msg: com.google.protobuf.MessageLite, state: VsrPopulationState,
                         needNewVec: () => Unit): Unit = {
    val size = msg.getSerializedSize
    require(size <= ArrowSerializedRangeVectorOps.maxVecLen,
      s"Serialized proto size $size exceeds Arrow buffer capacity ${ArrowSerializedRangeVectorOps.maxVecLen}")
    if (state.bytesRemaining < size || state.rowNum >= maxNumRows) needNewVec()
    // IMPROVE we are allocating a new buffer and output stream for every proto?
    // Ok for now since it is once per RVK and not once per data point, and not in hot path.
    // But see if we can avoid this as well later.
    val out = com.google.protobuf.CodedOutputStream.newInstance(
      state.currentRvkBrVec.getDataBuffer.nioBuffer(state.currentWriteOffset().toLong, size))
    msg.writeTo(out)
    state.commitRow(size, isRvk = 1)
  }

  def serializeHeaderToArrowBuf(resultSchema: ResultSchema, fAllocator: FlightAllocator): ArrowBuf =
    toArrowBuf(GrpcMultiPartitionQueryService.FlightMetadata.newBuilder()
      .setHeader(GrpcMultiPartitionQueryService.FlightResultHeader.newBuilder()
        .setResultSchema(resultSchema.toProto)).build().toByteArray, fAllocator)

  def serializeFooterToArrowBuf(queryStats: QueryStats, throwable: Option[Throwable],
                                 fAllocator: FlightAllocator): ArrowBuf = {
    val footerBuilder = GrpcMultiPartitionQueryService.FlightResultFooter.newBuilder()
      .setQueryStats(queryStats.toProto)
    throwable.foreach(t => footerBuilder.setThrowable(t.toProto))
    toArrowBuf(GrpcMultiPartitionQueryService.FlightMetadata.newBuilder()
      .setFooter(footerBuilder.build()).build().toByteArray, fAllocator)
  }

  def deserializeMetadata(buf: ArrowBuf): GrpcMultiPartitionQueryService.FlightMetadata =
    GrpcMultiPartitionQueryService.FlightMetadata.parseFrom(
      com.google.protobuf.CodedInputStream.newInstance(buf.nioBuffer()))

  // It still allocates byte array, but used only in flight response header/footer and it is not in the hot path
  private def toArrowBuf(bytes: Array[Byte], fAllocator: FlightAllocator): ArrowBuf =
    fAllocator.withRequestAllocator { allocator =>
      val buf = allocator.buffer(bytes.length)
      buf.writeBytes(bytes, 0, bytes.length)
      buf
    } {
      throw new IllegalStateException("FlightAllocator is already closed, cannot serialize to ArrowBuf")
    }

  private[flight] def srvToProtoBytes(srv: SerializableRangeVector): Array[Byte] =
    ProtoRangeVector.RvMetadata.newBuilder().setSrv(srv.toProto).build().toByteArray
}
