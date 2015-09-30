package filodb.core.reprojector

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.mapdb._
import org.velvia.filo.RowReader
import scala.collection.Map
import scala.math.Ordered
import scalaxy.loops._

import filodb.core.{KeyRange, SortKeyHelper}
import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset}

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
 * MapDB table namespacing scheme:
 *   datasetName/version/active
 *   datasetName/version/locked
 *
 * ==Configuration==
 * {{{
 *   memtable {
 *     backup-dir = "/tmp/filodb.memtable/"
 *     max-rows-per-table = 1000000
 *     min-free-mb = 512
 *   }
 * }}}
 */
class MapDBMemTable(config: Config) extends MemTable {
  import MemTable._
  import RowReader._
  import collection.JavaConversions._

  def close(): Unit = { db.close() }

  // Data structures

  private val backupDir = config.as[Option[String]]("memtable.backup-dir")
  private val maxRowsPerTable = config.getInt("memtable.max-rows-per-table")
  val minFreeMb = config.as[Option[Int]]("memtable.min-free-mb").getOrElse(DefaultMinFreeMb)

  // According to MapDB examples, use incremental backup with memory-only store
  // Also, the cache was causing us to run out of memory because it's unbounded.
  private val db = DBMaker.newMemoryDB
                          .transactionDisable
                          .closeOnJvmShutdown
                          .cacheDisable
                          .make()

  /**
   * === Row ingest, read, delete operations ===
   */
  def ingestRowsInner[K: TypedFieldExtractor](setup: IngestionSetup,
                                              version: Int,
                                              rows: Seq[RowReader]): IngestionResponse = {
    val extractor = implicitly[TypedFieldExtractor[K]]
    implicit val helper = setup.helper[K]

    // For each row: insert into rows map
    val rowMap = getRowMap[K](setup.dataset.name, version, Active).get
    val serializer = serializers((setup.dataset.name, version))
    if (rowMap.size() >= maxRowsPerTable) return PleaseWait
    for { row <- rows } {
      val sortKey = extractor.getField(row, setup.sortColumnNum)
      rowMap.put((setup.partitioningFunc(row), sortKey), serializer.serialize(row))
    }
    Ingested
  }

  def canIngestInner(dataset: TableName, version: Int): Boolean = {
    val rowMap = getRowMap[Any](dataset, version, Active).get
    rowMap.size() < maxRowsPerTable
  }

  def readRows[K: SortKeyHelper](keyRange: KeyRange[K],
                                 version: Int,
                                 buffer: BufferType): Iterator[RowReader] = {
    getRowMap[K](keyRange.dataset, version, buffer).map { rowMap =>
      val serializer = serializers((keyRange.dataset, version))
      rowMap.subMap((keyRange.partition, keyRange.start), (keyRange.partition, keyRange.end))
        .keySet.iterator.map { k => serializer.deserialize(rowMap.get(k)) }
    }.getOrElse {
      Iterator.empty
    }
  }

  def readAllRows[K](dataset: TableName, version: Int, buffer: BufferType):
      Iterator[(PartitionKey, K, RowReader)] = {
    getRowMap[K](dataset, version, buffer).map { rowMap =>
      val serializer = serializers((dataset, version))
      rowMap.keySet.iterator.map { case index @ (part, k) =>
        (part, k, serializer.deserialize(rowMap.get(index)))
      }
    }.getOrElse {
      Iterator.empty
    }
  }

  def removeRows[K: SortKeyHelper](keyRange: KeyRange[K], version: Int): Unit = {
    getRowMap[K](keyRange.dataset, version, Locked).map { rowMap =>
      rowMap.subMap((keyRange.partition, keyRange.start), (keyRange.partition, keyRange.end))
            .keySet.iterator.foreach { k => rowMap.remove(k) }
    }
  }

  def deleteLockedTable(dataset: TableName, version: Int): Unit = {
    val lockedName = tableName(dataset, version, Locked)
    logger.debug(s"Deleting locked memtable for dataset $dataset / version $version")
    db.delete(lockedName)
  }

  def flipBuffers(dataset: TableName, version: Int): FlipResponse = {
    val lockedName = tableName(dataset, version, Locked)
    // First check that lock table is empty
    getRowMap[Any](dataset, version, Locked).map { lockedMap =>
      if (lockedMap.size != 0) {
        logger.warn(s"Cannot flip buffers for ($dataset/$version) because Locked memtable nonempty")
        return LockedNotEmpty
      }
      deleteLockedTable(dataset, version)
      db.compact()
    }.getOrElse(return NoSuchDatasetVersion)

    logger.debug(s"Flipping active to locked memtable for dataset $dataset / version $version")
    db.rename(tableName(dataset, version, Active), lockedName)
    Flipped
  }

  def numRows(dataset: TableName, version: Int, buffer: BufferType): Option[Long] = {
    getRowMap[Any](dataset, version, buffer).map { rowMap =>
      rowMap.size()
    }
  }

  def clearAllDataInner(): Unit = {
    logger.info(s"MemTable: ERASING ALL TABLES!!")
    db.getAll().keys.foreach(db.delete)
  }

  // private funcs
  private def tableName(dataset: TableName, version: Int, buffer: BufferType) =
    s"$dataset/$version/" + (if (buffer == Active) "active" else "locked")

  var serializers = Map.empty[(TableName, Int), RowReaderSerializer]

  private def getRowMap[K](dataset: TableName, version: Int, buffer: BufferType) = {
    getIngestionSetup(dataset, version).map { setup =>
      serializers.synchronized {
        val nameVer = (setup.dataset.name, version)
        if (!(serializers contains nameVer)) {
          serializers = serializers + (nameVer -> new RowReaderSerializer(setup.schema))
        }
      }
      implicit val sortKeyOrdering: Ordering[K] = setup.helper[K].ordering
      db.createTreeMap(tableName(setup.dataset.name, version, buffer))
        .comparator(Ordering[(PartitionKey, K)])
        .valuesOutsideNodesEnable()   // Otherwise will pull out all values in a BTree Node at once
        .counterEnable()
        .valueSerializer(Serializer.BYTE_ARRAY)
        .makeOrGet().asInstanceOf[BTreeMap[(PartitionKey, K), Array[Byte]]]
    }
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
  def getInt(columnNo: Int): Int = MsgPackUtils.getInt(vector(columnNo))
  def getLong(columnNo: Int): Long = MsgPackUtils.getLong(vector(columnNo))
  def getDouble(columnNo: Int): Double = vector(columnNo).asInstanceOf[Double]
  def getString(columnNo: Int): String = vector(columnNo).asInstanceOf[String]
}
