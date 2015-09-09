package filodb.core.reprojector

import com.typesafe.config.Config
import org.mapdb._
import scala.collection.mutable.HashSet
import scala.math.Ordered
import scala.util.Try

import filodb.core.{KeyRange, SortKeyHelper}
import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset}
import filodb.core.columnstore.RowReader

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
 * Right now, for each dataset, there exists two BTreeMaps:
 * - One sorted by (partition key, sort key) for reading rows in a segment
 * - One sorted by atomic row counter, for determining flush order
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

  def datasets: Set[String] = _datasets.toSet

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

  private val rowCounter = db.atomicLong("/counter")

  /**
   * === Row ingest, read, delete operations ===
   */

  def ingestRows[K: TypedFieldExtractor: SortKeyHelper](dataset: Dataset,
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

    // For each row: bump row counter, insert into rows map and then index
    // NOTE: index simply points back at rows map
    val (indexMap, rowMap) = getMaps[K](dataset.name)
    for { row <- rows } {
      val rowNum = rowCounter.incrementAndGet
      rowMap.put(rowNum, row)
      val sortKey = extractor.getField(row, sortColNo)
      val oldRowNum = indexMap.put(IndexKey(partitionFunc(row), sortKey), rowNum)

      // If we are replacing an existing row, delete the old row in rowMap
      if (oldRowNum != 0) rowMap.remove(oldRowNum)
    }

    // commit the whole thing
    db.commit()

    Ingested
  }

  def readRows[K: SortKeyHelper](keyRange: KeyRange[K]): Iterator[RowReader] = {
    val (indexMap, rowMap) = getMaps[K](keyRange.dataset)
    indexMap.subMap(IndexKey(keyRange.partition, keyRange.start), IndexKey(keyRange.partition, keyRange.end))
      .keySet.iterator.map { k => rowMap.get(indexMap.get(k)) }
  }

  def removeRows[K: SortKeyHelper](dataset: Dataset, partition: PartitionKey, keys: Seq[K]): Unit = {
    val (indexMap, rowMap) = getMaps[K](dataset.name)

    // remove rows in both index as well as rowMap
    keys.foreach { k =>
      val indexKey = IndexKey(partition, k)
      rowMap.remove(indexMap.get(indexKey))
      indexMap.remove(indexKey)
    }

    // TODO: remove pending state
    db.commit()
  }

  def oldestAvailableRow[K](dataset: Dataset, skipPending: Boolean = true): Option[(PartitionKey, K)] = ???

  def markRowsAsPending[K](dataset: Dataset, partition: PartitionKey, keys: Seq[K]): Unit = ???

  def numRows(dataset: Dataset): Long = {
    val (_, rowMap) = getMaps(dataset.name)
    rowMap.size()
  }

  // private funcs
  private def getMaps[K](dataset: String) = {
    (db.treeMap[IndexKey[K], Long](s"$dataset/index0"),
     db.treeMapCreate(s"$dataset/rows")
       .keySerializer(Serializer.LONG)
       .valueSerializer(db.getDefaultSerializer())
       .valuesOutsideNodesEnable()   // Otherwise will pull out all values in a BTree Node at once
       .counterEnable()
       .makeOrGet().asInstanceOf[BTreeMap[Long, RowReader]])
  }
}

