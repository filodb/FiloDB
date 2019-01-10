package filodb.core.downsample

import filodb.core.binaryrecord2.{RecordBuilder, RecordSchema}
import filodb.core.memstore.TimeSeriesPartition
import filodb.core.metadata.{DataColumn, Dataset}
import filodb.core.metadata.Column.{ColumnType, DownsampleType}
import filodb.core.metadata.Column.DownsampleType._
import filodb.core.query.ColumnInfo
import filodb.core.store.ChunkInfoIterator
import filodb.memory.format.vectors.{DoubleVectorDataReader, LongVectorDataReader}

trait ChunkDownsampler {
  def downsampleChunk(reader: DoubleVectorDataReader, startRow: Int, endRow: Int): Double = ???
  def downsampleChunk(reader: LongVectorDataReader, startRow: Int, endRow: Int): Long = ???
}

object MinDownsampler extends ChunkDownsampler {
  override def downsampleChunk(reader: DoubleVectorDataReader, startRow: Int, endRow: Int): Double = ???
}

object MaxDownsampler extends ChunkDownsampler {
  override def downsampleChunk(reader: DoubleVectorDataReader, startRow: Int, endRow: Int): Double = ???
}

object SumDownsampler extends ChunkDownsampler {
  override def downsampleChunk(reader: DoubleVectorDataReader, startRow: Int, endRow: Int): Double = ???
}

object CountDownsampler extends ChunkDownsampler {
  override def downsampleChunk(reader: DoubleVectorDataReader, startRow: Int, endRow: Int): Double = ???
}

/**
  * Emits the last value within the row range requested. Typically used for timestamp column.
  */
object LastValueDownsampler extends ChunkDownsampler {
  override def downsampleChunk(reader: LongVectorDataReader, startRow: Int, endRow: Int): Long = ???
}

object ChunkDownsampler {

  def apply(typ: DownsampleType): ChunkDownsampler = {
    typ match {
      case MinDownsample       => MinDownsampler
      case MaxDownsample       => MaxDownsampler
      case SumDownsample       => SumDownsampler
      case CountDownsample     => CountDownsampler
      case LastValueDownsample => LastValueDownsampler
      case a => throw new UnsupportedOperationException(s"Invalid downsample type $a")
    }
  }

  def downsampleIngestSchema(dataset: Dataset): RecordSchema = {
    val downsampleCols = dataset.dataColumns.flatMap(c => c.asInstanceOf[DataColumn].downsamplerTypes.map {dt =>
      ColumnInfo(dt.downsampleColName(c.name), c.columnType)
    })
    new RecordSchema(dataset.partKeySchema.columns ++ downsampleCols, Some(0), dataset.ingestionSchema.predefinedKeys)
  }

  def makeDownsamplers(dataset: Dataset, resolutions: Array[Long]): Seq[Seq[Array[ChunkDownsampler]]] = {
    val downsamplers = dataset.dataColumns.map { dc =>
      dc.columnType match {
        case ColumnType.DoubleColumn =>
          dc.asInstanceOf[DataColumn].downsamplerTypes.map(c=> resolutions.map(l=> ChunkDownsampler(c)))
        case _ => ??? // TODO not supported yet
      }
    }
    downsamplers
  }

  // scalastyle:off method.length
  // scalastyle:off null
  def downsample(dataset: Dataset,
                 part: TimeSeriesPartition,
                 chunksets: ChunkInfoIterator,
                 downsamplers: Seq[Seq[Array[ChunkDownsampler]]],
                 resolutions: Array[Long],
                 builders: Array[RecordBuilder]): Unit = {

    while (chunksets.hasNext) {
      val chunkset = chunksets.nextInfo
      val startTime = chunkset.startTime
      val endTime = chunkset.endTime
      // for each downsample resolution
      resolutions.zipWithIndex.foreach { case (res, resId) =>
        var pStart = (startTime / res) * res
        var pEnd = pStart + res
        // for each downsample period
        while (pStart <= endTime) {
          val startRowNum = 0  // TODO calculate with binary search for pStart
          val endRowNum = 0 // TODO calculate with binary search for pEnd
          builders(resId).startNewRecord()
          // add partKey
          builders(resId).addFieldsFromBinRecord(part.partKeyBase, part.partKeyOffset, dataset.partKeySchema)
          // for each column
          dataset.dataColumns.foreach { col =>
            val vecPtr = chunkset.vectorPtr(col.id)
            val colReader = part.chunkReader(1, vecPtr)
            col.columnType match {
              case ColumnType.DoubleColumn =>
                // for each downsampler for the column
                downsamplers(col.id)(resId).foreach { d =>
                  val downsampled = d.downsampleChunk(colReader.asDoubleReader, startRowNum, endRowNum)
                  builders(resId).addDouble(downsampled)
                }
              case ColumnType.TimestampColumn | ColumnType.LongColumn =>
                downsamplers(col.id)(resId).foreach { d =>
                  val downsampled = d.downsampleChunk(colReader.asLongReader, startRowNum, endRowNum)
                  builders(resId).addLong(downsampled)
                }
              case _ => ???
            }
          }
          builders(resId).endRecord(true)
          pStart += res
          pEnd += res
        }
      }
    }
  }
}