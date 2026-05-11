package filodb.coordinator.flight

import org.apache.arrow.memory.ArrowBuf

import filodb.coordinator.flight.ArrowSerializedRangeVectorOps.{maxNumRows, VsrPopulationState}
import filodb.core.query.{FlightAllocator, QueryStats, RangeVectorKey, ResultSchema, RvRange, SerializableRangeVector}
import filodb.grpc.{GrpcMultiPartitionQueryService, ProtoRangeVector}
import filodb.query.ProtoConverters._

object FlightProtoSerDeser {

  def serializeSrvToArrowVsr(srv: SerializableRangeVector, state: VsrPopulationState)
                             (needNewVec: () => Unit): Unit =
    writeProto(ProtoRangeVector.RvMetadata.newBuilder().setSrv(srv.toProto).build(), state, needNewVec)

  def serializeRvKeyToArrowVsr(key: RangeVectorKey, outputRange: Option[RvRange],
                                state: VsrPopulationState)(needNewVec: () => Unit): Unit = {
    val rkBuilder = ProtoRangeVector.RvKey.newBuilder().setKey(key.toProto)
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

  private[flight] def rvKeyToProtoBytes(key: RangeVectorKey, outputRange: Option[RvRange]): Array[Byte] = {
    val rkBuilder = ProtoRangeVector.RvKey.newBuilder().setKey(key.toProto)
    outputRange.foreach(r => rkBuilder.setRvRange(r.toProto))
    ProtoRangeVector.RvMetadata.newBuilder().setRvKey(rkBuilder.build()).build().toByteArray
  }

  private[flight] def srvToProtoBytes(srv: SerializableRangeVector): Array[Byte] =
    ProtoRangeVector.RvMetadata.newBuilder().setSrv(srv.toProto).build().toByteArray
}
