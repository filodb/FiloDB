package filodb.coordinator.flight

import java.util

import scala.collection.mutable.ArrayBuffer
import scala.util.Using

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BitVector, BitVectorHelper, VarBinaryVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.jctools.queues.MpscArrayQueue
import spire.syntax.cfor.cforRange

import filodb.core.Utils
import filodb.core.binaryrecord2.{RecordSchema, SingleRecordBuilder}
import filodb.core.metadata.Column.ColumnType.{DoubleColumn, HistogramColumn}
import filodb.core.query._
import filodb.memory.data.ChunkMap
import filodb.memory.format.{RowReader, UnsafeUtils}
import filodb.query.ProtoConverters._

object ArrowSerializedRangeVectorOps {
  // scalastyle:off null
  val arrowSrvSchema: Schema = {
    // stores boolean value indicating whether the row contains RVK or data, and
    // if RVK then the RVK is stored in the second column as binary
    val isRvk = new Field("isRvk", FieldType.notNullable(new ArrowType.Bool()), null)
    // if isRvk is true, then this column stores the proto-serialized RvMetadata
    // if isRvk is false, then this column stores the serialized BR for the data row
    val rvkBr = new Field("rvkBr", FieldType.nullable(new ArrowType.Binary()), null)
    new Schema(util.Arrays.asList(isRvk, rvkBr))
  }

  // TODO later, this should not be hard-coded
  private[flight] val maxVecLen = 1048576 // 1 MB
  val maxNumRows = maxVecLen / 15 // assume 15 bytes per row on average, so we don't exceed maxVecLen
  private[flight] val maxVsrs = 50

  def emptyVectorSchemaRoot(allocator: BufferAllocator): VectorSchemaRoot = {
    VectorSchemaRoot.create(arrowSrvSchema, allocator)
  }

  /**
   * Holds the state of the VSR population process, including the current VSR being populated, the current row number,
   * and the remaining bytes in the current VSR. It also maintains a queue of free VSRs and a list of finished VSRs.
   * @param flightVsr the VSR owned by flight, which is used to hold the serialized data for the current flight request
   * @param currentVsr the VSR currently being populated with data from the RangeVector
   * @param currentIsRvkVec the isRvkVector BitVector cached from current VSR
   * @param currentRvkBrVec the rvkBrVector VarBinaryVector cached from current VSR
   * @param rowNum the last row number populated in the current VSR, -1 if no rows have been populated yet
   * @param bytesRemaining the number of bytes remaining in the current VSR, initialized to maxVecLen
   * @param freeVsrs a queue of free VSRs that can be reused for future population
   * @param finishedVsrs a list of finished VSRs that have been populated and are ready to be sent back to the client
   */
  case class VsrPopulationState(var flightVsr: VectorSchemaRoot = null,
                        var currentVsr: VectorSchemaRoot = null,
                        var currentIsRvkVec: BitVector = null,
                        var currentRvkBrVec: VarBinaryVector = null,
                        var rowNum: Int = -1,
                        var bytesRemaining: Int = maxVecLen,
                        freeVsrs: MpscArrayQueue[VectorSchemaRoot] = new MpscArrayQueue[VectorSchemaRoot](maxVsrs),
                        finishedVsrs: ArrayBuffer[VectorSchemaRoot] = ArrayBuffer.empty)   {

    def finishAndGetCurrentVsr(): VectorSchemaRoot = {
      if (currentVsr != null) {
        currentIsRvkVec.setValueCount(rowNum)
        currentRvkBrVec.setValueCount(rowNum)
        currentVsr.setRowCount(rowNum)
        val result = currentVsr
        currentVsr = null
        result
      } else {
        null
      }
    }

    def currentWriteOffset(): Int = currentRvkBrVec.getOffsetBuffer.getInt(rowNum.toLong * 4)

    // Commits one row into the VarBinary/isRvk vectors after the caller has written `bytesWritten`
    // bytes into the data buffer starting at currentWriteOffset().  bytesWritten == 0 encodes a
    // null row (validity bit unset, zero-length offset range).
    //
    // Why we maintain the offset chain here rather than using VarBinaryVector.set() / setNull():
    //   Arrow's setNull(i) only unsets the validity bit; it never touches the offset buffer.
    //   This leaves offsetBuffer[i+1] == 0, so the next write would land at the start of the
    //   data buffer and silently overwrite existing bytes.  VarBinaryVector.set() avoids this via
    //   fillHoles(), but bypassing set() (as we do for zero-copy BinaryRecord and proto writes)
    //   means fillHoles is never called.  We therefore maintain the offset chain eagerly: copy
    //   offsetBuffer[rowNum] to offsetBuffer[rowNum+1] for null rows, or advance it by bytesWritten
    //   for non-null rows.  This invariant is confirmed against Arrow 11 and the latest arrow-java.
    def commitRow(bytesWritten: Int, isRvk: Int): Unit = {
      val offsetBuf   = currentRvkBrVec.getOffsetBuffer
      val writeOffset = offsetBuf.getInt(rowNum.toLong * 4)
      offsetBuf.setInt((rowNum + 1).toLong * 4, writeOffset + bytesWritten)
      if (bytesWritten > 0) BitVectorHelper.setBit(currentRvkBrVec.getValidityBuffer, rowNum)
      else BitVectorHelper.unsetBit(currentRvkBrVec.getValidityBuffer, rowNum)
      currentRvkBrVec.setLastSet(rowNum)
      currentIsRvkVec.set(rowNum, isRvk)
      rowNum += 1
      bytesRemaining -= bytesWritten
    }
  }

  /**
   * Populates the given VectorSchemaRoot data from the given RangeVector
   * @param rv the RangeVector to read data from
   * @param recordSchema the RecordSchema of the RangeVector rows
   * @param execPlan the execution plan string for this RangeVector, used for logging and
   *                 debugging purposes
   * @param queryStats the QueryStats to update with CPU time spent in this method
   * @param allocator the Arrow BufferAllocator to use for allocating Arrow buffers
   * @param state the container that holds vsr pointers, to be retained across RV population calls
   */
  // scalastyle:off method.length
  def populateRvContentsIntoVsrs(rv: RangeVector,
                                 recordSchema: RecordSchema,
                                 execPlan: String,
                                 queryStats: QueryStats,
                                 allocator: BufferAllocator,
                                 state: VsrPopulationState): Unit = {

    def updateVsrLengthInState(): Unit = {
        state.currentIsRvkVec.setValueCount(state.rowNum)
        state.currentRvkBrVec.setValueCount(state.rowNum)
        state.currentVsr.setRowCount(state.rowNum)
    }

    def addNewVsr(): Unit = {
      if (state.currentVsr != null) {
        updateVsrLengthInState()
        state.finishedVsrs += state.currentVsr
      }

      if (state.freeVsrs.isEmpty) {
        state.currentVsr = VectorSchemaRoot.create(arrowSrvSchema, allocator)
        state.currentVsr.allocateNew()
        state.currentIsRvkVec = state.currentVsr.getVector(0).asInstanceOf[org.apache.arrow.vector.BitVector]
        state.currentIsRvkVec.allocateNew(maxNumRows)
        state.currentRvkBrVec = state.currentVsr.getVector(1)
          .asInstanceOf[org.apache.arrow.vector.VarBinaryVector]
        state.currentRvkBrVec.allocateNew(maxVecLen, maxNumRows)
      } else {
        state.currentVsr = state.freeVsrs.poll()
        state.currentIsRvkVec = state.currentVsr.getVector(0).asInstanceOf[org.apache.arrow.vector.BitVector]
        state.currentRvkBrVec = state.currentVsr.getVector(1)
          .asInstanceOf[org.apache.arrow.vector.VarBinaryVector]
        state.currentVsr.getFieldVectors.forEach(_.reset())
      }

      state.rowNum = 0
      state.bytesRemaining = maxVecLen
    }

    val srb = new SingleRecordBuilder(UnsafeUtils.ZeroPointer, 0L, 0) ({
      addNewVsr()
      val writeOffset = state.currentRvkBrVec.getOffsetBuffer.getInt(state.rowNum.toLong * 4)
      (UnsafeUtils.ZeroPointer,
       state.currentRvkBrVec.getDataBufferAddress + writeOffset,
       state.bytesRemaining)
    })

    def addFromReader(row: RowReader): Unit = {
      if (state.rowNum >= maxNumRows) {
        addNewVsr()
      }
      val writeOffsetBefore = state.currentWriteOffset()
      val bufAddrBefore     = state.currentRvkBrVec.getDataBufferAddress
      srb.reset(UnsafeUtils.ZeroPointer, bufAddrBefore + writeOffsetBefore, state.bytesRemaining)
      srb.addFromReader(row, recordSchema, 0)
      // If srb ran out of space mid-record, its requireBytes callback called addNewVsr(), which
      // switched state.currentRvkBrVec to a fresh buffer (new address) and reset state.rowNum to 0.
      // Detect the spill via the address change so bytesWritten is always anchored to the buffer
      // that srb actually wrote into, regardless of any future change to addNewVsr().
      val writeBase =
        if (state.currentRvkBrVec.getDataBufferAddress != bufAddrBefore)
          state.currentRvkBrVec.getDataBufferAddress  // spilled: new buffer, write starts at offset 0
        else
          bufAddrBefore + writeOffsetBefore            // no spill: same buffer, original offset
      val bytesWritten = (srb.nextRecordOffset - writeBase).toInt
      state.commitRow(bytesWritten, isRvk = 0)
    }

    def addNullRow(): Unit = {
      if (state.rowNum >= maxNumRows) addNewVsr()
      state.commitRow(bytesWritten = 0, isRvk = 0)
    }

    if (state.currentVsr == null) addNewVsr()

    rv match {
      case srv: SerializableRangeVector if srv.hasFormulatedRows =>
        // If the RV has formulated rows, serialize the entire RV as a single proto object
        // and skip row iteration and BR building
        FlightProtoSerDeser.serializeSrvToArrowVsr(srv, state) { () => addNewVsr() }
        updateVsrLengthInState()
      case _ =>
        // Serialize the RV key and output range as a proto header row, then iterate data rows
        FlightProtoSerDeser.serializeRvKeyToArrowVsr(rv.key, rv.outputRange, state) { () => addNewVsr() }
        val startNs = Utils.currentThreadCpuTimeNanos
        try {
          ChunkMap.validateNoSharedLocks(execPlan)
          val canRemoveEmptyRows = SerializedRangeVector.canRemoveEmptyRows(rv.outputRange, recordSchema)
          lazy val col1Type = recordSchema.columns(1).colType
          Using.resource(rv.rows()) { rows =>
            while (rows.hasNext) {
              val nextRow = rows.next()
              // Don't encode empty-histogram / NaN data over the wire
              if (!canRemoveEmptyRows ||
                col1Type == DoubleColumn && !java.lang.Double.isNaN(nextRow.getDouble(1)) ||
                col1Type == HistogramColumn && !nextRow.getHistogram(1).isEmpty) {
                addFromReader(nextRow)
              } else {
                addNullRow()
              }
            }
            updateVsrLengthInState()
          }
        } finally {
          ChunkMap.releaseAllSharedLocks()
          queryStats.getCpuNanosCounter(Nil).addAndGet(Utils.currentThreadCpuTimeNanos - startNs)
        }
    }

  }

  /**
   * Invokes `f` with each set-bit row index (ascending) in a bit-packed Arrow BitVector's data
   * buffer at `bufAddr`, without testing cleared bits individually. Reads the buffer one 64-bit
   * word at a time so runs of unset bits (the common case: many data rows between sparse RVK
   * marker rows) are skipped in O(1) rather than tested one bit at a time. Arrow bit buffers are
   * always allocated padded to an 8-byte boundary, so reading a full trailing word past `rowCount`
   * is always in-bounds; the `rowIndex < rowCount` guard just discards any padding bits.
   *
   * @param bufAddr the address of the BitVector's data buffer
   * @param rowCount the number of rows in the BitVector (the number of valid bits to consider)
   * @param f the function to invoke with each set-bit row index
   */
  private def cforSetBitPositions(bufAddr: Long, rowCount: Int)(f: Int => Unit): Unit = {
    val numWords = (rowCount + 63) >> 6 // ceil(rowCount / 64.0) in integer math
    cforRange (0 until numWords) { wordIdx =>
      var bits = UnsafeUtils.getLong(bufAddr + (wordIdx.toLong << 3))
      while (bits != 0L) {
        val rowIndex = (wordIdx << 6) + java.lang.Long.numberOfTrailingZeros(bits)
        if (rowIndex < rowCount) f(rowIndex)
        bits &= bits - 1 // clear the lowest set bit
      }
    }
  }

  def convertVsrsIntoArrowSrvs(vsrs: Seq[VectorSchemaRoot],
                               schema: ResultSchema): Seq[SerializableRangeVector] = {
    val result = ArrayBuffer[SerializableRangeVector]()

    var currentKey: RangeVectorKey = null
    var currentRvRange: Option[RvRange] = None
    var currentStartVsrIndex = 0
    var currentStartRowIndex = 0
    var currentNumDataRows = 0
    lazy val rs = schema.toRecordSchema

    def flushCurrentRv(): Unit = {
      if (currentKey != null) {
        result += new ArrowSerializedRangeVector(
          currentKey, vsrs, rs, currentStartVsrIndex,
          currentStartRowIndex, currentNumDataRows, currentRvRange)
      }
    }

    // Iterate through all VSRs, jumping directly between RV-key marker rows instead of testing
    // every row's isRvk bit individually (see cforSetBitPositions). All rows between markers are
    // known to be data rows, so their count is derived from position arithmetic, not counted one
    // at a time.
    cforRange ( 0 until vsrs.size) { vsrIndex =>
      val vsr = vsrs(vsrIndex)
      val rowCount = vsr.getRowCount
      val isRvkVec = vsr.getVector(0).asInstanceOf[BitVector]
      val rvkBrVec = vsr.getVector(1).asInstanceOf[VarBinaryVector]

      var afterLastMarker = 0
      cforSetBitPositions(isRvkVec.getDataBufferAddress, rowCount) { rowIndex =>
        // Rows [afterLastMarker, rowIndex), since the previous marker (or the start of this VSR),
        // are data rows belonging to whatever RV is currently open.
        if (currentKey != null) currentNumDataRows += rowIndex - afterLastMarker
        // Found a new RV key row — flush the previous RV if any
        flushCurrentRv()

        val proto = FlightProtoSerDeser.deserializeFromBytes(rvkBrVec.get(rowIndex))
        if (proto.hasSrv) {
          currentKey = null
          currentRvRange = None
          result += proto.getSrv.fromProto
        } else if (proto.hasRvKey) {
          val rvKeyProto = proto.getRvKey
          val (base, offset, _) = UnsafeUtils.BOLfromBuffer(rvKeyProto.getRvKey.asReadOnlyByteBuffer())
          currentKey = BrMapRangeVectorKey(base, offset)
          currentRvRange = if (rvKeyProto.hasRvRange) Some(rvKeyProto.getRvRange.fromProto) else None
          currentStartVsrIndex = vsrIndex
          currentStartRowIndex = rowIndex
          currentNumDataRows = 0
        } else {
          throw new IllegalStateException(s"Invalid RV metadata in VSR at index $vsrIndex, row $rowIndex")
        }
        afterLastMarker = rowIndex + 1
      }
      // Remaining rows after the last marker (or all rows, if this VSR had none) are data rows
      // for whatever RV is still open.
      if (currentKey != null) currentNumDataRows += rowCount - afterLastMarker
    }

    // Flush the last RV
    flushCurrentRv()

    result.toSeq
  }

}
