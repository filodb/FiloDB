package filodb.core.memstore

import java.util.concurrent.ConcurrentSkipListMap

import com.googlecode.javaewah.EWAHCompressedBitmap

import filodb.core._
import filodb.core.query.{Filter, KeyFilter}
import filodb.core.store.ChunkSetInfo.emptySkips
import filodb.memory.format.{ZeroCopyUTF8String => UTF8Str}

/**
 * A sorted index of all values belonging to a single column/tag, with a bitmap per value.
 * Could be used to index partitions in a MemStore, records within a partition, or anything representable
 * with an Int/Long.
 * Suitable for multithreaded adds and queries.
 */
class BitmapIndex[K](indexName: UTF8Str) {
  import collection.JavaConverters._

  import Filter._
  import KeyFilter.decode

  private final val bitmaps = new ConcurrentSkipListMap[K, EWAHCompressedBitmap]()

  def size: Int = bitmaps.size

  def bitmapBytes: Long = bitmaps.values.asScala.foldLeft(0L)(_ + _.sizeInBytes.toLong)

  /**
   * Adds a new entry to the index with value indexValue and numeric index n.
   * getOrElseUpdate is actually a computeIfAbsent, which is atomic.
   * n must be monotonically increasing; calling n smaller than a previous n added will be a NOP.
   */
  def addEntry(indexValue: K, n: Int): Unit = {
    val bitmap = bitmaps.getOrElseUpdate(indexValue, { k => new EWAHCompressedBitmap() })
    bitmap.set(n)
  }

  /**
   * Removes entries denoted by a bitmap from multiple values.   If a bitmap for
   * a value becomes empty, then that value will be removed entirely.
   * @param values all of the values whose bitmaps will have the entries removed
   * @param entries a bitmap of entries to remove
   */
  def removeEntries(values: Seq[K], entries: EWAHCompressedBitmap): Unit = {
    for { value <- values
          oldBitmap <- Option(bitmaps.get(value)) } {
      val newBitmap = oldBitmap.andNot(entries)
      if (newBitmap.isEmpty) {
        bitmaps.remove(value)
      } else {
        bitmaps.put(value, newBitmap)
      }
    }
  }

  /**
   * Returns an iterator over all the keys in this BitmapIndex
   */
  def keys: Iterator[K] = bitmaps.keySet.iterator.asScala

  /**
   * Returns an iterator over all the keys in this BitmapIndex within a range.  By default it is
   * [start, end) <-- end is exclusive
   */
  def keysInRange(start: K, end: K, endExclusive: Boolean = true): Iterator[K] =
    bitmaps.subMap(start, true, end, !endExclusive).keySet.iterator.asScala

  /**
   * Obtains the bitmap from a single value in the index
   */
  def get(value: K): Option[EWAHCompressedBitmap] = Option(bitmaps.get(value))

  /**
   * Obtains the combined bitmap from ORing the bitmaps occurring between start and end.
   * Returns the empty bitmap if no values occur between start and end.
   * @param endInclusive true if the end of the range is inclusive. start is always inclusive.
   */
  def range(start: K, end: K, endInclusive: Boolean = true): EWAHCompressedBitmap =
    bitmaps.subMap(start, true, end, endInclusive).values.asScala
           .foldLeft(emptySkips) { case (bitMap, newMap) => bitMap.or(newMap) }

  /**
   * Obtains the combined bitmap from ORing the bitmaps returned by the values in the set.
   * The equivalent of saying value can be IN set {a, b, ....} ie any of the values in the set
   * Returns the empty bitmap if no bitmaps are found for values.
   */
  def in(values: Set[K]): EWAHCompressedBitmap =
    values.foldLeft(emptySkips) { case (bitmap, key) => bitmap.or(bitmaps.getOrDefault(key, emptySkips)) }

  /**
   * Parses the query Filter to produce a bitmap
   */
  def parseFilter(f: Filter): EWAHCompressedBitmap = f match {
    case Equals(v: Any)       => get(decode(v).asInstanceOf[K]).getOrElse(emptySkips)
    case In(values: Set[Any]) => in(values.map(decode).asInstanceOf[Set[K]])
    case And(left, right)     => parseFilter(left).or(parseFilter(right))
    case o: Any               => ???
  }
}
