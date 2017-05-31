package filodb.core.memstore

import org.velvia.filo._
import org.velvia.filo.{vectors => bv}
import scalaxy.loops._

import filodb.core.DatasetRef
import filodb.core.metadata.{Column, RichProjection}
import filodb.core.store.BaseColumnStore
import filodb.core.Types.PartitionKey

// partition key is separated from the other data columns, as the partition key does not need
// to be stored, but is needed for routing and other purposes... = less extraction time
// This allows a partition key to be reused across multiple records also, for efficiency :)
final case class IngestRecord(partition: SchemaRowReader, data: RowReader, offset: Long)

object IngestRecord {
  // Creates an IngestRecord from a reader spanning a single entire record, such as that from a CSV file
  def apply(proj: RichProjection, reader: RowReader, offset: Long): IngestRecord =
    IngestRecord(SchemaRoutingRowReader(reader, proj.partIndices, proj.partExtractors),
                 RoutingRowReader(reader, proj.nonPartColIndices),
                 offset)
}
/**
 * A MemStore is an in-memory ColumnStore that ingests data not in chunks but as new records, potentially
 * spread over many partitions.  It supports the BaseColumnStore API, and should support real-time reads
 * of fresh ingested data.  Being in-memory, it is designed to not retain data forever but flush completed
 * chunks to a persistent ColumnStore.
 */
trait MemStore extends BaseColumnStore {
  /**
   * Sets up a dataset for ingestion and the schema to be used when ingesting.  Once set up, the schema
   * may not be changed.  This method only succeeds if the dataset has not already been setup.
   * @throws DatasetAlreadySetup
   */
  def setup(projection: RichProjection): Unit

  /**
   * Ingests new rows, making them immediately available for reads; flushes completed chunks to persistent
   * ColumnStore; checks to see if older chunks need to be flushed from memory.
   * Should assume single writer calling this ingest method; concurrency of ingestion should be handled
   * behind the scenes by each MemStore.
   *
   * @param dataset the dataset to ingest into
   * @param rows the input rows, each one with an offset, and conforming to the schema used in setup()
   */
  def ingest(dataset: DatasetRef, rows: Seq[IngestRecord]): Unit

  /**
   * Returns the names of tags or columns that are indexed at the partition level
   */
  def indexNames(dataset: DatasetRef): Iterator[String]

  /**
   * Returns values for a given index name for a dataset
   */
  def indexValues(dataset: DatasetRef, indexName: String): Iterator[ZeroCopyUTF8String]

  /**
   * Returns the number of partitions being maintained in the memtable.
   * TODO: add shard number
   * @Return -1 if dataset not found, otherwise number of active partitions
   */
  def numPartitions(dataset: DatasetRef): Int
}

import Column.ColumnType._

object MemStore {
  /**
   * Figures out the RowReaderAppenders for each column, depending on type and whether it is a static/
   * constant column for each partition.
   */
  def getAppendables(proj: RichProjection,
                     partition: PartitionKey,
                     maxElements: Int): Array[RowReaderAppender] =
    proj.nonPartitionColumns.zipWithIndex.map { case (col, index) =>
      col.columnType match {
        case IntColumn       =>
          new IntReaderAppender(bv.IntBinaryVector.appendingVector(maxElements), index)
        case LongColumn      =>
          new LongReaderAppender(bv.LongBinaryVector.appendingVector(maxElements), index)
        case DoubleColumn    =>
          new DoubleReaderAppender(bv.DoubleVector.appendingVector(maxElements), index)
        case TimestampColumn =>
          new LongReaderAppender(bv.LongBinaryVector.appendingVector(maxElements), index)
        case other: Column.ColumnType => ???
      }
    }.toArray
}