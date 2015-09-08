package filodb.core.reprojector

import com.typesafe.config.Config
import org.mapdb._
import scala.collection.mutable.HashSet
import scala.util.Try

import filodb.core.{KeyRange, SortKeyHelper}
import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset}
import filodb.core.columnstore.RowReader

/**
 * A MemTable backed by MapDB.  Need to do some experimentation to come up with a setup that is fast
 * yet safe, for reading and writing. Not there yet.
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

  def totalNumRows: Long = ???
  def totalBytesUsed: Long = ???
  def datasets: Set[String] = _datasets.toSet

  def mostStaleDatasets(k: Int = DefaultTopK): Seq[String] = ???
  def mostStalePartitions(dataset: String, k: Int = DefaultTopK): Seq[PartitionKey] = ???

  def close(): Unit = { db.close() }

  // Data structures

  private val backingDbFile = Try(config.getString("memtable.local-filename")).toOption
  private val _datasets = new HashSet[String]()

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

  // TODO: heap structures for timestamp / most stale calculations

  /**
   * === Row ingest, read, delete operations ===
   */

  def ingestRows[K: TypedFieldExtractor](dataset: Dataset,
                                         schema: Seq[Column],
                                         rows: Seq[RowReader],
                                         timestamp: Long): IngestionResponse = {
    val extractor = implicitly[TypedFieldExtractor[K]]
    _datasets += dataset.name

    // First check if the DB is too full

    // Get the ordinal/col# for partition key, and sort key
    val partitionFunc = getPartitioningFunc(dataset, schema).getOrElse(return BadSchema)

    val sortColNo = schema.indexWhere(_.hasId(dataset.projections.head.sortColumn))
    if (sortColNo < 0) return BadSchema

    // Next. group all the rows by partition
    val rowsByPart = rows.groupBy(partitionFunc)

    // For each partition: get the database
    for { (partition, partRows) <- rowsByPart } {
      val partMap = db.treeMap[K, RowReader](getDBName(dataset.name, partition))

      // Insert the rows by sort key
      partRows.foreach { row => partMap.put(extractor.getField(row, sortColNo), row) }

      // Update timestamp for each partition
    }

    // commit the whole thing
    db.commit()

    Ingested
  }

  def readRows[K](keyRange: KeyRange[K]): Iterator[RowReader] = {
    val partMap = db.treeMap[K, RowReader](getDBName(keyRange.dataset, keyRange.partition))
    partMap.subMap(keyRange.start, keyRange.end).keySet.iterator.map { k => partMap.get(k) }
  }

  def removeRows[K](keyRange: KeyRange[K]): Unit = {
    // get database for partition
    val partMap = db.treeMap[K, RowReader](getDBName(keyRange.dataset, keyRange.partition))

    // remove rows
    partMap.subMap(keyRange.start, keyRange.end).keySet.iterator.foreach { k => partMap.remove(k) }
    db.commit()

    // is partition empty?  Then delete DB, remove staleness timestamp from heap, update dataset staleness
  }

  /**
   * Returns the key range encompassing all the rows in a given partition of a dataset.
   */
  def getKeyRange[K: SortKeyHelper](dataset: Dataset, partition: PartitionKey): KeyRange[K] = {
    val partMap = db.treeMap[K, RowReader](getDBName(dataset.name, partition))
    KeyRange(dataset.name, partition, partMap.firstKey, partMap.lastKey)
  }

  // private funcs
  private def getDBName(datasetName: String, partition: PartitionKey) = s"$datasetName/$partition"
}

