package filodb.core.query

import java.util.Collections

import scala.util.Using

import com.typesafe.scalalogging.StrictLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.joda.time.DateTime

import filodb.core.Utils
import filodb.core.binaryrecord2.{BinaryRecordRowReader, RecordBuilder, RecordSchema}
import filodb.core.metadata.Column.ColumnType.{BinaryRecordColumn, DoubleColumn, HistogramColumn}
import filodb.core.query.ArrowSerializedRangeVector.maxBrSize
import filodb.core.query.SerializedRangeVector.canRemoveEmptyRows
import filodb.memory.data.ChunkMap
import filodb.memory.format.{RowReader, UnsafeUtils}
import filodb.memory.format.vectors.Histogram

/**
 * Arrow-based SerializedRangeVector that uses off-heap columnar storage.
 * Compared to the RecordBuilder-based implementation, this provides:
 * - Native off-heap storage (no MemFactory needed)
 * - Columnar layout for better cache locality
 * - Compatibility with Arrow ecosystem
 */
final class ArrowSerializedRangeVector(
  val key: RangeVectorKey,
  val numRowsSerialized: Int,
  val schema: RecordSchema,
  val vectorSchemaRoot: VectorSchemaRoot,
  override val outputRange: Option[RvRange]) extends RangeVector with SerializableRangeVector with AutoCloseable {

  override val numRows = {
    if (SerializedRangeVector.canRemoveEmptyRows(outputRange, schema)) {
      Some(((outputRange.get.endMs - outputRange.get.startMs) / outputRange.get.stepMs).toInt + 1)
    } else {
      Some(numRowsSerialized)
    }
  }

  private def toRowReaderIterator(root: VectorSchemaRoot): RangeVectorCursor = {
    new RangeVectorCursor {
      private val array = new Array[Byte](maxBrSize)
      private val reader = new BinaryRecordRowReader(schema, array, UnsafeUtils.arayOffset)
      private var curRow = 0
      private val vec = root.getVector(0).asInstanceOf[org.apache.arrow.vector.VarBinaryVector]
      final def hasNext: Boolean = {
        curRow < root.getRowCount
      }
      final def next: BinaryRecordRowReader = {
        val start   = vec.getStartOffset(curRow)
        val end     = vec.getStartOffset(curRow + 1)
        val length  = end - start
        vec.getDataBuffer.getBytes(start, array, 0, length)
        curRow += 1
        reader
      }
      override def close(): Unit = root.close()
    }
  }

  override def rows: RangeVectorCursor = {
    val it = toRowReaderIterator(vectorSchemaRoot)
    if (SerializedRangeVector.canRemoveEmptyRows(outputRange, schema)) {
      new RangeVectorCursor {
        var curTime = outputRange.get.startMs
        val bufIt = it.buffered
        val emptyDouble = new TransientRow(0L, Double.NaN)
        val emptyHist = new TransientHistRow(0L, Histogram.empty)
        override def hasNext: Boolean = curTime <= outputRange.get.endMs
        override def next(): RowReader = {
          if (bufIt.hasNext && bufIt.head.getLong(0) == curTime) {
            curTime += outputRange.get.stepMs
            bufIt.next()
          }
          else {
            if (schema.columns(1).colType == DoubleColumn) {
              emptyDouble.timestamp = curTime
              curTime += outputRange.get.stepMs
              emptyDouble
            } else {
              emptyHist.timestamp = curTime
              curTime += outputRange.get.stepMs
              emptyHist
            }
          }
        }
        override def close(): Unit = it.close()
      }
    } else it
  }

  override def estimateSerializedRowBytes: Long = {
    val vec = vectorSchemaRoot.getVector(0).asInstanceOf[org.apache.arrow.vector.VarBinaryVector]
    vec.getOffsetBuffer().getInt(vec.getValueCount() * 4);
  }

  override def close(): Unit = {
    vectorSchemaRoot.close()
  }

  override def prettyPrint(formatTime: Boolean = true): String = {
    val curTime = System.currentTimeMillis
    key.toString + "\n\t" +
      rows.map { reader =>
        val firstCol = if (formatTime && schema.isTimeSeries) {
          val timeStamp = reader.getLong(0)
          s"${new DateTime(timeStamp).toString()} (${(curTime - timeStamp)/1000}s ago) $timeStamp"
        } else {
          schema.columnTypes.head match {
            case BinaryRecordColumn => schema.brSchema(0).stringify(reader.getBlobBase(0), reader.getBlobOffset(0))
            case _ => reader.getAny(0).toString
          }
        }
        (firstCol +: (1 until schema.numColumns).map(reader.getAny(_).toString)).mkString("\t")
      }.mkString("\n\t") + "\n"
  }

}

object ArrowSerializedRangeVector extends StrictLogging {

  val maxBrSize = 5000

  // scalastyle:off null
  val arrowSrvSchema: Schema = {
    val f = new Field("br", FieldType.nullable(new ArrowType.Binary()), null)
    new Schema(Collections.singletonList(f))
  }

  def emptyVectorSchemaRoot(allocator: BufferAllocator): VectorSchemaRoot = {
    VectorSchemaRoot.create(arrowSrvSchema, allocator)
  }

  /**
   * Creates an ArrowSerializedRangeVector from a RangeVector using ArrowSrvBuilder
   */
  def apply(rv: RangeVector, recordSchema: RecordSchema,
            vectorSchemaRoot: VectorSchemaRoot, execPlan: String,
            builder: RecordBuilder,
            queryStats: QueryStats): ArrowSerializedRangeVector = {

    populateVectorSchemaRoot(rv, recordSchema, vectorSchemaRoot, execPlan, builder, queryStats)
    new ArrowSerializedRangeVector(rv.key, vectorSchemaRoot.getRowCount,
      recordSchema, vectorSchemaRoot, rv.outputRange)
  }

  /**
   * Populates the given VectorSchemaRoot data from the given RangeVector
   */
  def populateVectorSchemaRoot(rv: RangeVector, recordSchema: RecordSchema,
                               vsr: VectorSchemaRoot, execPlan: String,
                               builder: RecordBuilder,
                               queryStats: QueryStats): Unit = {

    vsr.clear()
    val vec = vsr.getVector(0).asInstanceOf[org.apache.arrow.vector.VarBinaryVector]
    var numRows = 0

    def addFromReader(row: RowReader): Unit = {
      builder.reset()
      builder.addFromReader(row, recordSchema, 0)
      builder.allContainers.flatMap(_.iterate(recordSchema)).foreach { br =>
        // TODO ensure capacity only when numRows reaches capacity
        vec.setSafe(numRows, br.recordBase.asInstanceOf[Array[Byte]],
          br.recordOffset.toInt - UnsafeUtils.arayOffset, br.recordLength)
        numRows += 1
        vsr.setRowCount(numRows)
      }
    }

    val startNs = Utils.currentThreadCpuTimeNanos
    try {
      ChunkMap.validateNoSharedLocks(execPlan)
      Using.resource(rv.rows()) {
        rows => while (rows.hasNext) {
          val nextRow = rows.next()
          // Don't encode empty / NaN data over the wire
          if (!canRemoveEmptyRows(rv.outputRange, recordSchema) ||
            recordSchema.columns(1).colType == DoubleColumn && !java.lang.Double.isNaN(nextRow.getDouble(1)) ||
            recordSchema.columns(1).colType == HistogramColumn && !nextRow.getHistogram(1).isEmpty) {
            addFromReader(nextRow)
          }
        }
      }
    } finally {
      ChunkMap.releaseAllSharedLocks()
      queryStats.getCpuNanosCounter(Nil).addAndGet(Utils.currentThreadCpuTimeNanos - startNs)
    }
  }
}
