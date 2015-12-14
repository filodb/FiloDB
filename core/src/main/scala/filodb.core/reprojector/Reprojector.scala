package filodb.core.reprojector

import java.nio.ByteBuffer

import filodb.core.metadata._
import filodb.core.reprojector.Reprojector.SegmentFlush
import org.velvia.filo.{RowReader, RowToVectorBuilder}


trait Reprojector extends Serializable {

  def project(projection: Projection,
              rows: Seq[RowReader],
              rowSchema: Option[Seq[Column]] = None): Iterator[(Any, Seq[SegmentFlush])]
}

object Reprojector extends Reprojector {

  case class SegmentFlush(projection: Projection,
                          partition: Any,
                          segment: Any,
                          keys: Seq[Any],
                          sortedKeyRange: KeyRange[Any],
                          columnVectors: Array[ByteBuffer])


  override def project(projection: Projection,
                       rows: Seq[RowReader],
                       passedSchema: Option[Seq[Column]] = None): Iterator[(Any, Seq[SegmentFlush])] = {
    val rowSchema = passedSchema.getOrElse(projection.schema)
    val columnIndexes = rowSchema.zipWithIndex.map { case (col, i) => col.name -> i }.toMap
    // lets group rows within partition by segment
    import filodb.core.util.Iterators._
    val partitionedRows = rows.iterator.sortedGroupBy(projection.partitionFunction(columnIndexes))
    partitionedRows.map { case (partitionKey, partRows) =>

      val segmentedRows = partRows.sortedGroupBy(projection.segmentFunction(columnIndexes))
      val segmentChunks = segmentedRows.map { case (segment, segmentRowsIter) =>
        val segmentRows = segmentRowsIter.toSeq
        // For each segment grouping of rows... set up a SegmentInfo
        // within a segment we sort rows by sort order
        implicit val ordering = projection.sortType.ordering
        val sortFunction = projection.sortFunction(columnIndexes)
        val rows = segmentRows.sortBy(sortFunction)
        val sortKeyRange = KeyRange(
          Some(sortFunction(segmentRows.head)),
          Some(sortFunction(segmentRows.last))
        )

        // then write the rows as a chunk to the segment
        val columnVectorMap = RowToVectorBuilder.buildFromRows(rows.iterator,
          Projection.toFiloSchema(rowSchema))
        val columnVectors = new Array[ByteBuffer](projection.schema.length)
        projection.schema.zipWithIndex.foreach { case (c, i) => columnVectors(i) = columnVectorMap(c.name) }
        // we also separate the keys for summarizing
        val keys = rows.map(i => projection.keyFunction(columnIndexes)(i))
        SegmentFlush(projection, partitionKey, segment, keys, sortKeyRange, columnVectors)
      }.toSeq
      (partitionKey, segmentChunks)
    }
  }


}
