package filodb.core.reprojector

import com.typesafe.config.Config
import org.mapdb._
import org.velvia.filo.RowReader
import scala.collection.Map
import scala.math.Ordered
import scalaxy.loops._

import filodb.core.{KeyRange, SortKeyHelper}
import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import RowReader._

/**
 * A MemTable backed by MapDB.  Need to do some experimentation to come up with a setup that is fast
 * yet safe, for reading and writing. Not there yet.
 *
 * We serialize RowReaders for storage into MapDB, because some RowReaders can store deep graphs and be
 * very expensive for the default serialization (for example SparkRowReader).
 *
 * NOTE: MapDB apparently has an issue with memory leaks, deleteTable / deleteRows does not appear to
 * compact the table properly.
 *
 * ==Configuration==
 * {{{
 *   memtable {
 *   }
 * }}}
 */
class MapDBMemTable[K](val projection: RichProjection[K], config: Config) extends MemTable[K] {
  import collection.JavaConversions._

  def close(): Unit = { db.close() }

  // According to MapDB examples, use incremental backup with memory-only store
  // Also, the cache was causing us to run out of memory because it's unbounded.
  private val db = DBMaker.newMemoryDirectDB
                          .transactionDisable
                          .closeOnJvmShutdown
                          .cacheDisable
                          .make()

  val serializer = new RowReaderSerializer(projection.columns)
  val sortKeyFunc = projection.sortKeyFunc

  implicit lazy val sortKeyOrdering: Ordering[K] = projection.helper.ordering
  lazy val rowMap = db.createTreeMap("filo")
                      .comparator(Ordering[(PartitionKey, K)])
                      // Otherwise will pull out all values in a BTree Node at once
                      .valuesOutsideNodesEnable()
                      .counterEnable()
                      .valueSerializer(Serializer.BYTE_ARRAY)
                      .makeOrGet[(PartitionKey, K), Array[Byte]]()

  /**
   * === Row ingest, read, delete operations ===
   */
  def ingestRows(rows: Seq[RowReader])(callback: => Unit): Unit = {
    // For each row: insert into rows map
    for { row <- rows } {
      val sortKey = sortKeyFunc(row)
      rowMap.put((projection.partitionFunc(row), sortKey), serializer.serialize(row))
    }
    // Since this is an in-memory table only, just call back right away.
    callback
  }

  def readRows(keyRange: KeyRange[K]): Iterator[RowReader] = {
    rowMap.subMap((keyRange.partition, keyRange.start), (keyRange.partition, keyRange.end))
      .keySet.iterator.map { k => serializer.deserialize(rowMap.get(k)) }
  }

  def readAllRows(): Iterator[(PartitionKey, K, RowReader)] = {
    rowMap.keySet.iterator.map { case index @ (part, k) =>
      (part, k, serializer.deserialize(rowMap.get(index)))
    }
  }

  def removeRows(keyRange: KeyRange[K]): Unit = {
    rowMap.subMap((keyRange.partition, keyRange.start), (keyRange.partition, keyRange.end))
          .keySet.iterator.foreach { k => rowMap.remove(k) }
  }

  def numRows: Int = rowMap.size().toInt

  def forceCommit(): Unit = {}

  def clearAllData(): Unit = {
    logger.info(s"MemTable: ERASING ALL TABLES!!")
    db.getAll().keys.foreach(db.delete)
  }
}

// This is not going to be the most efficient serializer around, but it would be better than serializing
// using Java serializer complex objects such as Spark's Row, which includes references to Schema etc.
class RowReaderSerializer(schema: Seq[Column]) {
  import org.velvia.MsgPack
  import Column.ColumnType._

  val fillerFuncs: Array[(RowReader, Array[Any]) => Unit] = schema.map(_.columnType).zipWithIndex.map {
    case (IntColumn, i)    => (r: RowReader, a: Array[Any]) => a(i) = r.getInt(i)
    case (LongColumn, i)   => (r: RowReader, a: Array[Any]) => a(i) = r.getLong(i)
    case (DoubleColumn, i) => (r: RowReader, a: Array[Any]) => a(i) = r.getDouble(i)
    case (StringColumn, i) => (r: RowReader, a: Array[Any]) => a(i) = r.getString(i)
    case (x, i)            => throw new RuntimeException(s"RowReaderSerializer: Unsupported column type $x")
  }.toArray

  def serialize(r: RowReader): Array[Byte] = {
    val aray = new Array[Any](schema.length)
    for { i <- 0 until schema.length optimized } {
      //scalastyle:off
      if (r.notNull(i)) { fillerFuncs(i)(r, aray) }
      else              { aray(i) = null }
      //scalastyle:on
    }
    MsgPack.pack(aray)
  }

  def deserialize(bytes: Array[Byte]): RowReader = {
    val data = MsgPack.unpack(bytes)
    VectorRowReader(data.asInstanceOf[Vector[Any]])
  }
}

case class VectorRowReader(vector: Vector[Any]) extends RowReader {
  import org.velvia.MsgPackUtils

  //scalastyle:off
  def notNull(columnNo: Int): Boolean = vector(columnNo) != null
  //scalastyle:on
  def getBoolean(columnNo: Int): Boolean = vector(columnNo).asInstanceOf[Boolean]
  def getInt(columnNo: Int): Int = MsgPackUtils.getInt(vector(columnNo))
  def getLong(columnNo: Int): Long = MsgPackUtils.getLong(vector(columnNo))
  def getDouble(columnNo: Int): Double = vector(columnNo).asInstanceOf[Double]
  def getFloat(columnNo: Int): Float = vector(columnNo).asInstanceOf[Float]
  def getString(columnNo: Int): String = vector(columnNo).asInstanceOf[String]
}
