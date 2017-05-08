package filodb.core.memstore

import org.velvia.filo.{IntReaderAppender, LongReaderAppender, DoubleReaderAppender, ConstAppender}
import org.velvia.filo.{vectors => bv, RowReaderAppender, RowReader, ZeroCopyUTF8String}

import filodb.core.DatasetRef
import filodb.core.metadata.{Column, RichProjection}
import filodb.core.store.BaseColumnStore
import filodb.core.Types.PartitionKey

final case class RowWithOffset(row: RowReader, offset: Long)

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
  def ingest(dataset: DatasetRef, rows: Seq[RowWithOffset]): Unit

  /**
   * Returns the names of tags or columns that are indexed at the partition level
   */
  def indexNames(dataset: DatasetRef): Iterator[String]

  /**
   * Returns values for a given index name for a dataset
   */
  def indexValues(dataset: DatasetRef, indexName: String): Iterator[ZeroCopyUTF8String]
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
    proj.dataColumns.zipWithIndex.map { case (col, index) =>
      val partCol = proj.staticPartIndices.indexOf(index)
      if (partCol >= 0) {
        // This column is a static part of partition key. Use ConstVectorAppenders for efficiency.
        col.columnType match {
          case IntColumn       =>
            ConstAppender(new bv.IntConstAppendingVect(partition.getInt(partCol)), index)
          case LongColumn      =>
            ConstAppender(new bv.LongConstAppendingVect(partition.getLong(partCol)), index)
          case DoubleColumn    =>
            ConstAppender(new bv.DoubleConstAppendingVect(partition.getDouble(partCol)), index)
          case TimestampColumn =>
            ConstAppender(new bv.LongConstAppendingVect(partition.getLong(partCol)), index)
          case StringColumn    =>
            ConstAppender(new bv.UTF8ConstAppendingVect(partition.filoUTF8String(partCol)), index)
          case other: Column.ColumnType => ???
        }
      } else {
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
      }
    }.toArray
}