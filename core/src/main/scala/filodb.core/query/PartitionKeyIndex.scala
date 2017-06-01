package filodb.core.query

import com.googlecode.javaewah.{EWAHCompressedBitmap, IntIterator}
import org.jctools.maps.NonBlockingHashMap
import org.velvia.filo.{ZeroCopyUTF8String => UTF8Str}
import scalaxy.loops._

import filodb.core.metadata.{Column, RichProjection}
import filodb.core.store.ChunkSetInfo.emptySkips
import filodb.core.Types.PartitionKey

trait Indexer {
  def fromKey(key: PartitionKey, partIndex: Int): Unit
}

/**
 * A high performance index using BitmapIndex for partition keys.
 */
class PartitionKeyIndex(proj: RichProjection) {
  import filodb.core._
  import collection.JavaConverters._
  import Column.ColumnType._

  require(proj.partitionColumns.forall(c => c.columnType == StringColumn || c.columnType == MapColumn))
  private final val numPartColumns = proj.partitionColumns.length
  private final val indices = new NonBlockingHashMap[UTF8Str, BitmapIndex[UTF8Str]]

  private final val indexers = proj.partitionColumns.zipWithIndex.map { case (c, pos) =>
    c.columnType match {
      case StringColumn => new Indexer {
                             val colName = UTF8Str(c.name)
                             def fromKey(key: PartitionKey, partIndex: Int): Unit = {
                               addIndexEntry(colName, key.filoUTF8String(pos), partIndex)
                             }
                           }
      case MapColumn => new Indexer {
                          def fromKey(key: PartitionKey, partIndex: Int): Unit = {
                            // loop through map and add index entries
                            key.as[Types.UTF8Map](pos).foreach { case (k, v) =>
                              addIndexEntry(k, v, partIndex)
                            }
                          }
                        }
      case other: Any => ???
    }
  }.toArray

  def addKey(key: PartitionKey, partIndex: Int): Unit = {
    for { i <- 0 until numPartColumns optimized } {
      indexers(i).fromKey(key, partIndex)
    }
  }

  private final def addIndexEntry(indexName: UTF8Str, value: UTF8Str, partIndex: Int): Unit = {
    val index = indices.getOrElseUpdate(indexName, { s => new BitmapIndex[UTF8Str](indexName) })
    index.addEntry(value, partIndex)
  }

  /**
   * Combines (ANDs) the different columnFilters using bitmap indexing to produce a single list of
   * numeric indices given the filter conditions.  The conditions are ANDed together.
   * @return (partitionIndices, unfoundColumnNames)
   */
  def parseFilters(columnFilters: Seq[ColumnFilter]): (IntIterator, Seq[String]) = {
    val bitmapsAndUnfoundeds = columnFilters.map { case ColumnFilter(colName, filter) =>
      Option(indices.get(UTF8Str(colName)))
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
  def indexNames: Iterator[String] = indices.keySet.iterator.asScala.map(_.toString)

  /**
   * Obtains an Iterator over the specific values or entries for a given key/tag name
   */
  def indexValues(indexName: String): Iterator[UTF8Str] =
    Option(indices.get(UTF8Str(indexName))).map { bitmapIndex => bitmapIndex.keys }
                                  .getOrElse(Iterator.empty)

  def reset(): Unit = {
    indices.clear()
  }
}