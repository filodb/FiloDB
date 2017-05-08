package filodb.core.query

import com.googlecode.javaewah.{EWAHCompressedBitmap, IntIterator}
import org.jctools.maps.NonBlockingHashMap
import org.velvia.filo.ZeroCopyUTF8String
import scalaxy.loops._

import filodb.core.metadata.{Column, RichProjection}
import filodb.core.store.ChunkSetInfo.emptySkips
import filodb.core.Types.PartitionKey

/**
 * A high performance index using BitmapIndex for partition keys.
 * TODO: revamp when we have to deal with maps. Also this assumes all parts of a partitionkey are strings.
 */
class PartitionKeyIndex(proj: RichProjection) {
  import filodb.core._
  import collection.JavaConverters._

  require(proj.partitionColumns.forall(_.columnType == Column.ColumnType.StringColumn))
  private final val numPartColumns = proj.partitionColumns.length

  private final val indices = new NonBlockingHashMap[String, BitmapIndex[ZeroCopyUTF8String]]

  def addKey(key: PartitionKey, partIndex: Int): Unit = {
    for { i <- 0 until numPartColumns optimized } {
      val colName = proj.columns(proj.partIndices(i)).name
      val value = key.filoUTF8String(i)
      val index = indices.getOrElseUpdate(colName, { s => new BitmapIndex[ZeroCopyUTF8String](colName) })
      index.addEntry(value, partIndex)
    }
  }

  /**
   * Combines (ANDs) the different columnFilters using bitmap indexing to produce a single list of
   * numeric indices given the filter conditions.  The conditions are ANDed together.
   * @return (partitionIndices, unfoundColumnNames)
   */
  def parseFilters(columnFilters: Seq[ColumnFilter]): (IntIterator, Seq[String]) = {
    val bitmapsAndUnfoundeds = columnFilters.map { case ColumnFilter(colName, filter) =>
      Option(indices.get(colName))
        .map { index => (Some(index.parseFilter(filter)), None) }
        .getOrElse((None, Some(colName)))
    }
    val bitmaps = bitmapsAndUnfoundeds.collect { case (Some(bm), _) => bm }
    val unfoundColumns = bitmapsAndUnfoundeds.collect { case (_, Some(col)) => col }
    val andedBitmap = if (bitmaps.isEmpty) emptySkips else EWAHCompressedBitmap.and(bitmaps :_*)
    (andedBitmap.intIterator, unfoundColumns)
  }

  /**
   * Obtains an Iterator over the key/tag names in the index
   */
  def indexNames: Iterator[String] = indices.keySet.iterator.asScala

  /**
   * Obtains an Iterator over the specific values or entries for a given key/tag name
   */
  def indexValues(indexName: String): Iterator[ZeroCopyUTF8String] =
    Option(indices.get(indexName)).map { bitmapIndex => bitmapIndex.keys }
                                  .getOrElse(Iterator.empty)

  def reset(): Unit = {
    indices.clear()
  }
}