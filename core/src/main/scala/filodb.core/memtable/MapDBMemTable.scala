package filodb.core.memtable

import filodb.core.metadata._
import org.mapdb._
import org.velvia.filo.RowReader

import scalaxy.loops._

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
class MapDBMemTable(val projection: Projection) extends MemTable {

  import collection.JavaConversions._

  def close(): Unit = {
    db.close()
  }

  // According to MapDB examples, use incremental backup with memory-only store
  // Also, the cache was causing us to run out of memory because it's unbounded.
  private val db = DBMaker.newMemoryDirectDB
    .transactionDisable
    .closeOnJvmShutdown
    .cacheDisable
    .make()

  val serializer = new RowReaderSerializer(projection.schema)
  val keyFunc = projection.keyFunction
  val partitionFunc = projection.partitionFunction
  val partitionType = projection.partitionType
  val keyType = projection.keyType

  implicit lazy val keyOrdering: Ordering[(partitionType.T, keyType.T)] =
    Ordering.Tuple2(partitionType.ordering, keyType.ordering)

  lazy val rowMap = db.createTreeMap("filo")
    .comparator(Ordering[(partitionType.T, keyType.T)])
    // Otherwise will pull out all values in a BTree Node at once
    .valuesOutsideNodesEnable()
    .counterEnable()
    .valueSerializer(Serializer.BYTE_ARRAY)
    .makeOrGet[(partitionType.T, keyType.T), Array[Byte]]()

  /**
   * === Row ingest, read, delete operations ===
   */
  def ingestRows(rows: Seq[RowReader]): Unit = {
    // For each row: insert into rows map
    for {row <- rows} {
      val key = keyFunc(row).asInstanceOf[keyType.T]
      val partKey = partitionFunc(row).asInstanceOf[partitionType.T]
      rowMap.put((partKey, key), serializer.serialize(row))
    }
  }

  def readRows(partitionKey: Any, keyRange: KeyRange[_]): Iterator[RowReader] = {
    val pk = partitionKey.asInstanceOf[partitionType.T]
    val start = keyRange.start.asInstanceOf[keyType.T]
    val end = keyRange.end.asInstanceOf[keyType.T]
    rowMap.subMap((pk, start), (pk, end))
      .keySet.iterator.map { k => serializer.deserialize(rowMap.get(k)) }
  }

  def readAllRows(): Iterator[(_, _, RowReader)] = {
    rowMap.keySet.iterator.map { case index@(part, k) =>
      (part, k, serializer.deserialize(rowMap.get(index)))
    }
  }

  def removeRows(partitionKey: Any, keyRange: KeyRange[_]): Unit = {
    val pk = partitionKey.asInstanceOf[partitionType.T]
    val start = keyRange.start.asInstanceOf[keyType.T]
    val end = keyRange.end.asInstanceOf[keyType.T]

    rowMap.subMap((pk, start), (pk, end))
      .keySet.iterator.foreach { k => rowMap.remove(k) }
  }

  def numRows: Int = rowMap.size().toInt

  def clearAllData(): Unit = {
    logger.info(s"MemTable: ERASING ALL TABLES!!")
    db.getAll().keys.foreach(db.delete)
  }
}

// This is not going to be the most efficient serializer around, but it would be better than serializing
// using Java serializer complex objects such as Spark's Row, which includes references to Schema etc.
class RowReaderSerializer(schema: Seq[Column]) {

  import filodb.core.metadata.Column.ColumnType._
  import org.velvia.MsgPack

  val fillerFuncs: Array[(RowReader, Array[Any]) => Unit] = schema.map(_.columnType).zipWithIndex.map {
    case (IntColumn, i) => (r: RowReader, a: Array[Any]) => a(i) = r.getInt(i)
    case (LongColumn, i) => (r: RowReader, a: Array[Any]) => a(i) = r.getLong(i)
    case (DoubleColumn, i) => (r: RowReader, a: Array[Any]) => a(i) = r.getDouble(i)
    case (StringColumn, i) => (r: RowReader, a: Array[Any]) => a(i) = r.getString(i)
    case (x, i) => throw new RuntimeException(s"RowReaderSerializer: Unsupported column type $x")
  }.toArray

  def serialize(r: RowReader): Array[Byte] = {
    val aray = new Array[Any](schema.length)
    for {i <- 0 until schema.length optimized} {
      //scalastyle:off
      if (r.notNull(i)) {
        fillerFuncs(i)(r, aray)
      }
      else {
        aray(i) = null
      }
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
