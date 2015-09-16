package filodb.core.reprojector

import com.typesafe.config.Config
import org.mapdb._
import scala.math.Ordered
import scala.util.Try

import filodb.core.{KeyRange, SortKeyHelper}
import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset}
import filodb.core.columnstore.RowReader

/**
 * A MemTable backed by MapDB.  Need to do some experimentation to come up with a setup that is fast
 * yet safe, for reading and writing. Not there yet.
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
 *   }
 * }}}
 */
class MapDBMemTable(config: Config) extends MemTable {
  import MemTable._
  import RowReader._
  import collection.JavaConversions._

  def close(): Unit = { db.close() }

  // Data structures

  private val backupDir = Try(config.getString("memtable.backup-dir")).toOption
  private val maxRowsPerTable = config.getInt("memtable.max-rows-per-table")

  // According to MapDB examples, use incremental backup with memory-only store
  private val db = DBMaker.newHeapDB.transactionDisable.closeOnJvmShutdown.make()

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
    if (rowMap.size() >= maxRowsPerTable) return PleaseWait
    for { row <- rows } {
      val sortKey = extractor.getField(row, setup.sortColumnNum)
      rowMap.put((setup.partitioningFunc(row), sortKey), row)
    }
    Ingested
  }

  def readRows[K: SortKeyHelper](keyRange: KeyRange[K],
                                 version: Int,
                                 buffer: BufferType): Iterator[RowReader] = {
    getRowMap[K](keyRange.dataset, version, buffer).map { rowMap =>
      rowMap.subMap((keyRange.partition, keyRange.start), (keyRange.partition, keyRange.end))
        .keySet.iterator.map { k => rowMap.get(k) }
    }.getOrElse {
      Iterator.empty
    }
  }

  def readAllRows[K](dataset: TableName, version: Int, buffer: BufferType):
      Iterator[(PartitionKey, K, RowReader)] = {
    getRowMap[K](dataset, version, buffer).map { rowMap =>
      rowMap.keySet.iterator.map { case index @ (part, k) =>
        (part, k, rowMap.get(index))
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

  def flipBuffers(dataset: TableName, version: Int): FlipResponse = {
    val lockedName = tableName(dataset, version, Locked)
    // First check that lock table is empty
    getRowMap[Any](dataset, version, Locked).map { lockedMap =>
      if (lockedMap.size() != 0) {
        logger.warn(s"Cannot flip buffers for ($dataset/$version) because Locked memtable nonempty")
        return LockedNotEmpty
      }
      logger.debug(s"Deleting locked memtable for dataset $dataset / version $version")
      db.delete(lockedName)
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

  private def getRowMap[K](dataset: TableName, version: Int, buffer: BufferType) = {
    // We don't really need the IngestionSetup, just want to make sure users setup tables first
    getIngestionSetup(dataset, version).map { setup =>
      implicit val sortKeyOrdering: Ordering[K] = setup.helper[K].ordering
      db.createTreeMap(tableName(dataset, version, buffer))
        .comparator(Ordering[(PartitionKey, K)])
        .valuesOutsideNodesEnable()   // Otherwise will pull out all values in a BTree Node at once
        .counterEnable()
        .makeOrGet().asInstanceOf[BTreeMap[(PartitionKey, K), RowReader]]
    }
  }
}

