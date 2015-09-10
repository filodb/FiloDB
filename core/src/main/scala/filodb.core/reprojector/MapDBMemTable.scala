package filodb.core.reprojector

import com.typesafe.config.Config
import org.mapdb._
import scala.math.Ordered
import scala.util.Try

import filodb.core.{KeyRange, SortKeyHelper}
import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset}
import filodb.core.columnstore.RowReader

// TODO: Implement MapDB Serializer for IndexKey
case class IndexKey[K: SortKeyHelper](partition: PartitionKey, k: K) extends Ordered[IndexKey[K]] {
  val helper = implicitly[SortKeyHelper[K]]
  def compare(that: IndexKey[K]): Int =
    if (partition == that.partition) { helper.ordering.compare(k, that.k) }
    else                             { partition compare that.partition }
}

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
 *     local-filename = "/tmp/filodb.memtable"
 *   }
 * }}}
 */
class MapDBMemTable(config: Config) extends MemTable {
  import MemTable._
  import RowReader._
  import collection.JavaConversions._

  def close(): Unit = { db.close() }

  // Data structures

  private val backingDbFile = Try(config.getString("memtable.local-filename")).toOption

  // Use MMap-backed DB for fast I/O, yet persisted for easy recovery.
  // Leave the WAL for safer commits.  Experiment with this, maybe we can use WAL
  // for crash recovery.
  private val db = {
    backingDbFile.map { dbFile =>
      DBMaker.fileDB(new java.io.File(dbFile))
             .fileMmapEnable()            // always enable mmap
             .fileMmapEnableIfSupported() // only enable on supported platforms
             .fileMmapCleanerHackEnable() // closes file on DB.close()
             .closeOnJvmShutdown
             .make()
    }.getOrElse {
      DBMaker.memoryDB.closeOnJvmShutdown.make()
    }
  }

  /**
   * === Row ingest, read, delete operations ===
   */
  def ingestRowsInner[K: TypedFieldExtractor](setup: IngestionSetup,
                                              version: Int,
                                              rows: Seq[RowReader]): IngestionResponse = {
    val extractor = implicitly[TypedFieldExtractor[K]]
    implicit val helper = setup.helper[K]

    // First check if the DB is too full

    // For each row: insert into rows map
    val rowMap = getRowMap[K](setup.dataset.name, version, Active).get
    for { row <- rows } {
      val sortKey = extractor.getField(row, setup.sortColumnNum)
      rowMap.put(IndexKey(setup.partitioningFunc(row), sortKey), row)
    }

    // commit the whole thing
    db.commit()

    Ingested
  }

  def readRows[K: SortKeyHelper](keyRange: KeyRange[K],
                                 version: Int,
                                 buffer: BufferType): Iterator[RowReader] = {
    getRowMap[K](keyRange.dataset, version, buffer).map { rowMap =>
      rowMap.subMap(IndexKey(keyRange.partition, keyRange.start), IndexKey(keyRange.partition, keyRange.end))
        .keySet.iterator.map { k => rowMap.get(k) }
    }.getOrElse {
      Iterator.empty
    }
  }

  def removeRows[K: SortKeyHelper](keyRange: KeyRange[K], version: Int): Unit = {
    getRowMap[K](keyRange.dataset, version, Locked).map { rowMap =>
      rowMap.subMap(IndexKey(keyRange.partition, keyRange.start), IndexKey(keyRange.partition, keyRange.end))
        .keySet.iterator.foreach { k => rowMap.remove(k) }
    }
    db.commit()
  }

  def flipBuffers(dataset: TableName, version: Int): FlipResponse = {
    val lockedName = tableName(dataset, version, Locked)
    // First check that lock table is empty
    getRowMap[Any](dataset, version, Locked).map { lockedMap =>
      if (lockedMap.size() != 0) return LockedNotEmpty
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

  // private funcs
  private def tableName(dataset: TableName, version: Int, buffer: BufferType) =
    s"$dataset/$version/" + (if (buffer == Active) "active" else "locked")

  private def getRowMap[K](dataset: TableName, version: Int, buffer: BufferType) = {
    // We don't really need the IngestionSetup, just want to make sure users setup tables first
    getIngestionSetup(dataset, version).map { setup =>
      db.treeMapCreate(tableName(dataset, version, buffer))
        .keySerializer(db.getDefaultSerializer())
        .valueSerializer(db.getDefaultSerializer())
        .valuesOutsideNodesEnable()   // Otherwise will pull out all values in a BTree Node at once
        .counterEnable()
        .makeOrGet().asInstanceOf[BTreeMap[IndexKey[K], RowReader]]
    }
  }
}

