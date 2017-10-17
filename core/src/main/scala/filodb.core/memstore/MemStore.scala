package filodb.core.memstore

import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.Observable
import org.velvia.filo._
import org.velvia.filo.{vectors => bv}

import filodb.core.DatasetRef
import filodb.core.metadata.{Column, Dataset}
import filodb.core.store.ChunkSource

case object ShardAlreadySetup extends Exception

/**
 * A MemStore is an in-memory ChunkSource that ingests data not in chunks but as new records, potentially
 * spread over many partitions.  It supports the ChunkSource API, and should support real-time reads
 * of fresh ingested data.  Being in-memory, it is designed to not retain data forever but flush completed
 * chunks to a persistent ChunkSink.
 *
 * A MemStore contains shards of data for one or more datasets, with optimized ingestion pipeline for
 * each shard.
 */
trait MemStore extends ChunkSource {
  /**
   * Sets up one shard of a dataset for ingestion and the schema to be used when ingesting.
   * Once set up, the schema may not be changed.  The schema should be the same for all shards.
   * This method only succeeds if the dataset and shard has not already been setup.
   * @throws ShardAlreadySetup
   */
  def setup(dataset: Dataset, shard: Int): Unit

  /**
   * Ingests new rows, making them immediately available for reads
   * NOTE: this method is not intended to be the main ingestion method, just used for testing.
   *       Instead the more reactive ingestStream method should be used.
   *
   * @param dataset the dataset to ingest into
   * @param shard shard number to ingest into
   * @param rows the input rows, each one with an offset, and conforming to the schema used in setup()
   */
  def ingest(dataset: DatasetRef, shard: Int, rows: Seq[IngestRecord]): Unit

  /**
   * Sets up a shard of a dataset to continuously ingest new sets of records from a stream.
   * The records are immediately available for reads from that shard of the memstore.
   * Errors during ingestion are handled by the errHandler.
   * NOTE: does not check that existing streams are not already writing to this store.  That needs to be
   * handled by an upper layer.  Multiple stream ingestion is not guaranteed to be thread safe, a single
   * stream is safe for now.
   *
   * @param dataset the dataset to ingest into
   * @param shard shard number to ingest into
   * @param stream the stream of new records, with schema conforming to that used in setup()
   * @param errHandler this is called when an ingestion error occurs
   * @return a CancelableFuture for cancelling the stream subscription, which should be done on teardown
   */
  def ingestStream(dataset: DatasetRef, shard: Int, stream: Observable[Seq[IngestRecord]])
                  (errHandler: Throwable => Unit)
                  (implicit sched: Scheduler): CancelableFuture[Unit]

  /**
   * Returns the names of tags or columns that are indexed at the partition level, across
   * all shards on this node
   * @return an index name and shard number
   */
  def indexNames(dataset: DatasetRef): Iterator[(String, Int)]

  /**
   * Returns values for a given index name for a dataset and shard
   */
  def indexValues(dataset: DatasetRef, shard: Int, indexName: String): Iterator[ZeroCopyUTF8String]

  /**
   * Returns the number of partitions being maintained in the memtable for a given shard
   * @return -1 if dataset not found, otherwise number of active partitions
   */
  def numPartitions(dataset: DatasetRef, shard: Int): Int

  /**
   * Number of total rows ingested for that shard
   */
  def numRowsIngested(dataset: DatasetRef, shard: Int): Long

  def numRowsIngested(dataset: DatasetRef): Long =
    activeShards(dataset).map(s => numRowsIngested(dataset, s)).sum

  /**
   * The active shards for a given dataset
   */
  def activeShards(dataset: DatasetRef): Seq[Int]

  /**
   * WARNING: truncates all the data in the memstore for the given dataset, and also the data
   *          in any underlying storage system too.
   */
  def truncate(dataset: DatasetRef): Unit

  /**
   * Resets the state of the MemStore. Usually used for testing.
   */
  def reset(): Unit
  def shutdown(): Unit
}

import Column.ColumnType._

object MemStore {
  /**
   * Figures out the RowReaderAppenders for each column, depending on type and whether it is a static/
   * constant column for each partition.
   */
  def getAppendables(dataset: Dataset,
                     maxElements: Int): Array[RowReaderAppender] =
    dataset.dataColumns.zipWithIndex.map { case (col, index) =>
      col.columnType match {
        case IntColumn       =>
          new IntReaderAppender(bv.IntBinaryVector.appendingVector(maxElements), index)
        case LongColumn      =>
          new LongReaderAppender(bv.LongBinaryVector.appendingVector(maxElements), index)
        case DoubleColumn    =>
          new DoubleReaderAppender(bv.DoubleVector.appendingVector(maxElements), index)
        case TimestampColumn =>
          new LongReaderAppender(bv.LongBinaryVector.appendingVector(maxElements), index)
        case StringColumn    =>
          new StringReaderAppender(bv.UTF8Vector.appendingVector(maxElements), index)
        case other: Column.ColumnType => ???
      }
    }.toArray
}