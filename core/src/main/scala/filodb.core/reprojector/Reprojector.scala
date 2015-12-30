package filodb.core.reprojector

import java.nio.ByteBuffer

import filodb.core.metadata._
import filodb.core.reprojector.Reprojector.SegmentFlush
import org.velvia.filo.BuilderEncoder.{AutoDetect, EncodingHint}
import org.velvia.filo.{RowReader, RowToVectorBuilder, VectorInfo}

import scala.collection.mutable.ArrayBuffer


trait Reprojector extends Serializable {

  def project(projection: Projection,
              rows: Iterator[RowReader],
              rowSchema: Option[Seq[Column]] = None): Iterator[SegmentFlush]
}

object Reprojector extends Reprojector {

  case class SegmentFlush(projection: Projection,
                          partition: Any,
                          segment: Any,
                          keys: Seq[Any],
                          columnVectors: Array[ByteBuffer])


  override def project(projection: Projection,
                       rows: Iterator[RowReader],
                       passedSchema: Option[Seq[Column]] = None): Iterator[SegmentFlush] = {

    val rowSchema = passedSchema.getOrElse(projection.schema)
    val columnIndexes = rowSchema.zipWithIndex.map { case (col, i) => col.name -> i }.toMap
    val keyFunction = projection.keyFunction(columnIndexes)
    val partitionFunction = projection.partitionFunction(columnIndexes)
    import filodb.core.util.Iterators._
    // group rows by partition
    val partitionedRows = rows.sortedGroupBy(partitionFunction)
    partitionedRows.flatMap { case (partitionKey, partRows) =>
      // then group rows within partition by segment
      val segmentedRows = partRows.sortedGroupBy(projection.segmentFunction(columnIndexes))
      val segmentChunks = segmentedRows.map { case (segment, segmentRowsIter) =>
        // For each segment grouping of rows... set up a SegmentInfo
        // then write the rows as a chunk to the segment flush
        // we also separate the keys for summarizing
        val (keys, columnVectorMap) = buildFromRows(keyFunction,
          segmentRowsIter,
          Projection.toFiloSchema(rowSchema))
        val columnVectors = new Array[ByteBuffer](projection.schema.length)
        projection.schema.zipWithIndex.foreach { case (c, i) => columnVectors(i) = columnVectorMap(c.name) }
        SegmentFlush(projection, partitionKey, segment, keys, columnVectors)
      }
      segmentChunks
    }
  }

  def buildFromRows(keyFunction: (RowReader => Any),
                    rows: Iterator[RowReader],
                    schema: Seq[VectorInfo],
                    hint: EncodingHint = AutoDetect): (Seq[Any], Map[String, ByteBuffer]) = {
    val builder = new RowToVectorBuilder(schema)
    val keys = ArrayBuffer[Any]()
    rows.foreach { row =>
      builder.addRow(row)
      keys += keyFunction(row)
    }
    (keys, builder.convertToBytes(hint))
  }


}
