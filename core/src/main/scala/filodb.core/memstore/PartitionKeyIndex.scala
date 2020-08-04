package filodb.core.memstore

import com.googlecode.javaewah.{EWAHCompressedBitmap, IntIterator}
import com.typesafe.scalalogging.StrictLogging
import org.jctools.maps.NonBlockingHashMap
import spire.syntax.cfor._

import filodb.core.Types.PartitionKey
import filodb.core.binaryrecord2.MapItemConsumer
import filodb.core.metadata.{Column, Dataset}
import filodb.core.query.ColumnFilter
import filodb.core.store.ChunkSetInfo.emptySkips
import filodb.memory.{UTF8StringMedium, UTF8StringShort}
import filodb.memory.format.{ZeroCopyUTF8String => UTF8Str}

trait Indexer {
  def fromPartKey(base: Any, offset: Long, partIndex: Int): Unit
  /** Obtains pairs of index (name, value) from a partition key */
  def getNamesValues(key: PartitionKey): Seq[(UTF8Str, UTF8Str)]
}

object NoOpIndexer extends Indexer {
  def fromPartKey(base: Any, offset: Long, partIndex: Int): Unit = {}
  def getNamesValues(key: PartitionKey): Seq[(UTF8Str, UTF8Str)] = Nil
}

/**
 * A high performance index using BitmapIndex for partition keys.
 */
class PartitionKeyIndex(dataset: Dataset) extends StrictLogging {
  import collection.JavaConverters._

  import filodb.core._
  import Column.ColumnType._

  class IndexingMapConsumer(partIndex: Int) extends MapItemConsumer {
    def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
      val keyUtf8 = new UTF8Str(keyBase, keyOffset + 1, UTF8StringShort.numBytes(keyBase, keyOffset))
      val valUtf8 = new UTF8Str(valueBase, valueOffset + 2, UTF8StringMedium.numBytes(valueBase, valueOffset))
      addIndexEntry(keyUtf8, valUtf8, partIndex)
    }
  }

  private final val numPartColumns = dataset.partitionColumns.length
  private final val indices = new NonBlockingHashMap[UTF8Str, BitmapIndex[UTF8Str]]

  private final val indexers = dataset.partitionColumns.zipWithIndex.map { case (c, pos) =>
    c.columnType match {
      case StringColumn => new Indexer {
                             val colName = UTF8Str(c.name)
                             def fromPartKey(base: Any, offset: Long, partIndex: Int): Unit = {
                               addIndexEntry(colName, dataset.partKeySchema.asZCUTF8Str(base, offset, pos), partIndex)
                             }
                             def getNamesValues(key: PartitionKey): Seq[(UTF8Str, UTF8Str)] =
                               Seq((colName, dataset.partKeySchema.asZCUTF8Str(key, pos)))
                           }
      case MapColumn => new Indexer {
                          def fromPartKey(base: Any, offset: Long, partIndex: Int): Unit = {
                            dataset.partKeySchema.consumeMapItems(base, offset,
                                                                    pos, new IndexingMapConsumer(partIndex))
                          }
                          def getNamesValues(key: PartitionKey): Seq[(UTF8Str, UTF8Str)] = ???
                        }
      case other: Any =>
        logger.warn(s"Column $c has type that cannot be indexed and will be ignored right now")
        NoOpIndexer
    }
  }.toArray

  /**
   * Adds fields from a partition key to the index
   */
  def addPartKey(base: Any, offset: Long, partIndex: Int): Unit = {
    cforRange { 0 until numPartColumns } { i =>
      indexers(i).fromPartKey(base, offset, partIndex)
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
    val andedBitmap = if (bitmaps.isEmpty) emptySkips else EWAHCompressedBitmap.and(bitmaps: _*)
    (andedBitmap.intIterator, unfoundColumns)
  }

  /**
   * Removes entries denoted by a bitmap from multiple values from a single index.   If a bitmap for
   * a value becomes empty, then that value will be removed entirely.  For efficiency, this should be done
   * in larger batches of entries, say at least a few hundred.
   * @param indexName
   * @param values all of the values whose bitmaps will have the entries removed
   * @param entries a bitmap of entries to remove
   */
  def removeEntries(indexName: UTF8Str, values: Seq[UTF8Str], entries: EWAHCompressedBitmap): Unit =
    Option(indices.get(indexName)).foreach(_.removeEntries(values, entries))

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

  /**
   * Obtains an estimate of the combined size in bytes of all bitmap indices, not including the associated hashmaps
   */
  def indexBytes(): Long = indices.values.asScala.foldLeft(0L)(_ + _.bitmapBytes)

  /**
   * Obtains the total number of index entries across all tags/columns
   */
  def indexSize(): Int = indices.values.asScala.foldLeft(0)(_ + _.size)

  def reset(): Unit = {
    indices.clear()
  }
}
