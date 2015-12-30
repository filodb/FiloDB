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
              rowSchema: Option[Seq[Column]] = None): Iterator[(Any, Seq[SegmentFlush])]
}

object Reprojector extends Reprojector {

  case class SegmentFlush(projection: Projection,
                          partition: Any,
                          segment: Any,
                          keys: Seq[Any],
                          columnVectors: Array[ByteBuffer])


  override def project(projection: Projection,
                       rows: Iterator[RowReader],
                       passedSchema: Option[Seq[Column]] = None): Iterator[(Any, Seq[SegmentFlush])] = {

    val rowSchema = passedSchema.getOrElse(projection.schema)
    val columnIndexes = rowSchema.zipWithIndex.map { case (col, i) => col.name -> i }.toMap
    val keyFunction = projection.keyFunction(columnIndexes)
    // lets group rows within partition by segment
    import filodb.core.util.Iterators._
    val partitionedRows = rows.sortedGroupBy(projection.partitionFunction(columnIndexes))
    partitionedRows.map { case (partitionKey, partRows) =>

      val segmentedRows = partRows.sortedGroupBy(projection.segmentFunction(columnIndexes))
      val segmentChunks = segmentedRows.map { case (segment, segmentRowsIter) =>
        // For each segment grouping of rows... set up a SegmentInfo
        // within a segment we sort rows by sort order
        // then write the rows as a chunk to the segment
        val (keys, columnVectorMap) = buildFromRows(keyFunction,
          segmentRowsIter,
          Projection.toFiloSchema(rowSchema))
        val columnVectors = new Array[ByteBuffer](projection.schema.length)
        projection.schema.zipWithIndex.foreach { case (c, i) => columnVectors(i) = columnVectorMap(c.name) }
        // we also separate the keys for summarizing

        SegmentFlush(projection, partitionKey, segment, keys, columnVectors)
      }.toSeq
      (partitionKey, segmentChunks)
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
