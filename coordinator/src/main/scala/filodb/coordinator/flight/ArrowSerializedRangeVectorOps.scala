package filodb.coordinator.flight

import java.util

import scala.collection.mutable.ArrayBuffer
import scala.util.Using

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BitVector, VarBinaryVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.jctools.queues.MpscArrayQueue

import filodb.core.Utils
import filodb.core.binaryrecord2.{RecordBuilder, RecordSchema}
import filodb.core.binaryrecord2.RecordContainer.BRIterator
import filodb.core.metadata.Column.ColumnType.{DoubleColumn, HistogramColumn}
import filodb.core.query._
import filodb.core.query.ArrowSerializedRangeVector2.{maxNumRows, maxVecLen}
import filodb.memory.data.ChunkMap
import filodb.memory.format.{RowReader, UnsafeUtils}

object ArrowSerializedRangeVectorOps {
  // scalastyle:off null
  val arrowSrvSchema: Schema = {
    val isRvk = new Field("rvk", FieldType.notNullable(new ArrowType.Bool()), null)
    val rvkBr = new Field("rvkBr", FieldType.nullable(new ArrowType.Binary()), null)
    new Schema(util.Arrays.asList(isRvk, rvkBr))
  }

  def emptyVectorSchemaRoot(allocator: BufferAllocator): VectorSchemaRoot = {
    VectorSchemaRoot.create(arrowSrvSchema, allocator)
  }

  case class VsrPopulationState(var flightVsr: VectorSchemaRoot = null,
                                var currentVsr: VectorSchemaRoot = null,
                                var currentIsRvkVec: BitVector = null,
                                var currentRvkBrVec: VarBinaryVector = null,
                                var rowNum: Int = -1,
                                var bytesRemaining: Int = maxVecLen,
                                // FIXME hard coded 5
                                freeVsrs: MpscArrayQueue[VectorSchemaRoot] = new MpscArrayQueue[VectorSchemaRoot](5),
                                finishedVsrs: ArrayBuffer[VectorSchemaRoot] = ArrayBuffer.empty)

  /**
   * Populates the given VectorSchemaRoot data from the given RangeVector
   * @param rv the RangeVector to read data from
   * @param recordSchema the RecordSchema of the RangeVector rows
   * @param execPlan the execution plan string for this RangeVector, used for logging and
   *                 debugging purposes
   * @param builder the RecordBuilder to use for building the BR records from the RangeVector rows.
   *                This is used for now since we are not able to write BR to arrow vec directly yet.
   * @param queryStats the QueryStats to update with CPU time spent in this method
   * @param allocator the Arrow BufferAllocator to use for allocating Arrow buffers
   * @param state the container that holds vsr pointers, to be retained across RV population calls
   * @param brIterator the BRIterator to use for iterating through the BR records without
   *                   new allocations
   */
  // scalastyle:off parameter.number method.length
  def populateRvContentsIntoVsrs(rv: RangeVector,
                                 recordSchema: RecordSchema,
                                 execPlan: String,
                                 builder: RecordBuilder, // should go away in future once we write to Arrow directly
                                 queryStats: QueryStats,
                                 allocator: BufferAllocator,
                                 state: VsrPopulationState,
                                 brIterator: BRIterator): Unit = {

    // TODO update metrics & query statistics on result bytes during RV serialization

    def addNewVsr(): Unit = {
      if (state.currentIsRvkVec != null) state.currentIsRvkVec.setValueCount(state.rowNum)
      if (state.currentRvkBrVec != null) state.currentRvkBrVec.setValueCount(state.rowNum)
      if (state.currentVsr != null) state.currentVsr.setRowCount(state.rowNum)
      if (state.currentVsr != null) state.finishedVsrs += state.currentVsr

      if (state.freeVsrs.isEmpty) {
        state.currentVsr = VectorSchemaRoot.create(arrowSrvSchema, allocator)
        state.currentVsr.allocateNew()
        state.currentIsRvkVec = state.currentVsr.getVector(0).asInstanceOf[org.apache.arrow.vector.BitVector]
        state.currentIsRvkVec.allocateNew(maxNumRows)
        state.currentRvkBrVec = state.currentVsr.getVector(1)
          .asInstanceOf[org.apache.arrow.vector.VarBinaryVector]
        state.currentRvkBrVec.allocateNew(maxNumRows)
        state.currentRvkBrVec.allocateNew(maxVecLen, maxNumRows)
      } else {
        state.currentVsr = state.freeVsrs.poll()
        state.currentIsRvkVec = state.currentVsr.getVector(0).asInstanceOf[org.apache.arrow.vector.BitVector]
        state.currentRvkBrVec = state.currentVsr.getVector(1)
          .asInstanceOf[org.apache.arrow.vector.VarBinaryVector]
        state.currentVsr.getFieldVectors.forEach(_.clear())
      }

      state.rowNum = 0
      state.bytesRemaining = maxVecLen
    }

    def addFromReader(row: RowReader): Unit = {
      // TODO - we should write the BR record directly to Arrow buffers instead of using on-heap RecordBuilder and
      // hen copying to Arrow buffers. This is just a temporary solution to get the data into Arrow format for now.
      builder.reset()
      builder.addFromReader(row, recordSchema, 0)
      // avoid allocation by reusing brIterator
      builder.lastContainer.iterate(brIterator).foreach { br =>
        // check and ensure br.recordLength is available in vector capacity,
        // if not then create a new vector and add to vsrs
        if (state.bytesRemaining < br.recordLength || state.rowNum > maxNumRows) {
          addNewVsr()
        }
        state.currentIsRvkVec.set(state.rowNum, 0)
        state.currentRvkBrVec.set(state.rowNum, br.recordBase.asInstanceOf[Array[Byte]],
          br.recordOffset.toInt - UnsafeUtils.arayOffset, br.recordLength)
        state.rowNum += 1
        state.bytesRemaining -= br.recordLength
      }
    }

    if (state.currentVsr == null) addNewVsr()

    // Begin by serializing the RangeVector key into the VSR.
    FlightKryoSerDeser.serializeToArrowVsr(rv.key, state) { () =>
      addNewVsr()
    }

    val startNs = Utils.currentThreadCpuTimeNanos
    try {
      ChunkMap.validateNoSharedLocks(execPlan)
      Using.resource(rv.rows()) { rows =>
        while (rows.hasNext) {
          val nextRow = rows.next()
          // Don't encode empty / NaN data over the wire
          if (!SerializedRangeVector.canRemoveEmptyRows(rv.outputRange, recordSchema) ||
            recordSchema.columns(1).colType == DoubleColumn && !java.lang.Double.isNaN(nextRow.getDouble(1)) ||
            recordSchema.columns(1).colType == HistogramColumn && !nextRow.getHistogram(1).isEmpty) {
            addFromReader(nextRow)
          } else {
            state.currentRvkBrVec.setNull(state.rowNum)
            state.currentIsRvkVec.set(state.rowNum, 0)
            state.rowNum += 1
          }
        }
        state.currentIsRvkVec.setValueCount(state.rowNum)
        state.currentRvkBrVec.setValueCount(state.rowNum)
        state.currentVsr.setRowCount(state.rowNum)
      }
    } finally {
      ChunkMap.releaseAllSharedLocks()
      queryStats.getCpuNanosCounter(Nil).addAndGet(Utils.currentThreadCpuTimeNanos - startNs)
    }
  }

  def convertVsrsIntoArrowSrvs(vsrs: Seq[VectorSchemaRoot],
                               outputRange: Option[RvRange],
                               schema: RecordSchema): Seq[ArrowSerializedRangeVector2] = {
    val result = ArrayBuffer[ArrowSerializedRangeVector2]()

    var currentKey: RangeVectorKey = null
    var currentStartVsrIndex = 0
    var currentStartRowIndex = 0
    var currentNumDataRows = 0

    // Iterate through all VSRs and rows to find RV boundaries
    for (vsrIndex <- vsrs.indices) {
      val vsr = vsrs(vsrIndex)
      val isRvkVec = vsr.getVector(0).asInstanceOf[BitVector]
      val rvkBrVec = vsr.getVector(1).asInstanceOf[VarBinaryVector]

      for (rowIndex <- 0 until vsr.getRowCount) {
        if (isRvkVec.get(rowIndex) == 1) {
          // Found a new RV key row
          if (currentKey != null) {
            // Save the previous RV
            result += new ArrowSerializedRangeVector2(
              currentKey, vsrs, schema, currentStartVsrIndex,
              currentStartRowIndex, currentNumDataRows, outputRange)
          }

          // Deserialize the new RV key
          val keyBytes = rvkBrVec.get(rowIndex)
          currentKey = FlightKryoSerDeser.deserialize(keyBytes).asInstanceOf[RangeVectorKey]
          currentStartVsrIndex = vsrIndex
          currentStartRowIndex = rowIndex
          currentNumDataRows = 0 // Reset data row count
        } else {
          // Data row (rvkVec is null)
          currentNumDataRows += 1
        }
      }
    }

    // Don't forget the last RV
    if (currentKey != null) {
      result += new ArrowSerializedRangeVector2(
        currentKey, vsrs, schema, currentStartVsrIndex,
        currentStartRowIndex, currentNumDataRows, outputRange)
    }

    result
  }

}
