package filodb.core.memstore

import filodb.core.metadata.{Column, Dataset}
import filodb.core.store.{ChunkSink, ChunkSource}
import filodb.core.{DatasetRef, ErrorResponse}
import filodb.memory.MemFactory
import filodb.memory.format.{vectors => bv, _}

import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.Observable

case object ShardAlreadySetup extends Exception
final case class DatasetAlreadySetup(dataset: DatasetRef) extends Exception(s"Dataset $dataset already setup")

sealed trait DataOrCommand
final case class SomeData(records: Seq[IngestRecord]) extends DataOrCommand
final case class FlushCommand(groupNum: Int) extends DataOrCommand

final case class FlushGroup(shard: Int, groupNum: Int, flushWatermark: Long)

final case class FlushError(err: ErrorResponse) extends Exception(s"Flush error $err")


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
  def sink: ChunkSink

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
   * Flushes to the ChunkSink are initiated by a potentially independent stream, the flushStream, which emits
   *   flush events of a specific subgroup of a shard.
   * NOTE: does not check that existing streams are not already writing to this store.  That needs to be
   * handled by an upper layer.  Multiple stream ingestion is not guaranteed to be thread safe, a single
   * stream is safe for now.
   *
   * @param dataset the dataset to ingest into
   * @param shard shard number to ingest into
   * @param stream the stream of new records, with schema conforming to that used in setup()
   * @param flushStream the stream of FlushCommands for regular flushing of chunks to ChunkSink
   * @param errHandler this is called when an ingestion error occurs
   * @return a CancelableFuture for cancelling the stream subscription, which should be done on teardown
   *        the Future completes when both stream and flushStream ends.  It is up to the caller to ensure this.
   */
  def ingestStream(dataset: DatasetRef,
                   shard: Int,
                   stream: Observable[Seq[IngestRecord]],
                   flushStream: Observable[FlushCommand] = FlushStream.empty)
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
   * Number of ingestion subgroups per shard
   */
  def numGroups: Int

  /**
   * Returns the latest offset of a given shard
   */
  def latestOffset(dataset: DatasetRef, shard: Int): Long

  /**
   * The active shards for a given dataset
   */
  def activeShards(dataset: DatasetRef): Seq[Int]

  /**
   * WARNING: truncates all the data in the memstore for the given dataset, and also the data
   *          in any underlying ChunkSink too.
   */
  def truncate(dataset: DatasetRef): Unit

  /**
   * Resets the state of the MemStore. Usually used for testing.
   */
  def reset(): Unit
  def shutdown(): Unit
}

import filodb.core.metadata.Column.ColumnType._

object MemStore {
  /**
   * Figures out the RowReaderAppenders for each column, depending on type and whether it is a static/
   * constant column for each partition.
   */
  def getAppendables(memFactory: MemFactory,
                     dataset: Dataset,
                     maxElements: Int): Array[RowReaderAppender] =
    dataset.dataColumns.zipWithIndex.map { case (col, index) =>
      col.columnType match {
      case IntColumn       =>
        new IntReaderAppender(bv.IntBinaryVector.appendingVector(memFactory, maxElements), index)
      case LongColumn      =>
        new LongReaderAppender(bv.LongBinaryVector.appendingVector(memFactory, maxElements), index)
      case DoubleColumn    =>
        new DoubleReaderAppender(bv.DoubleVector.appendingVector(memFactory, maxElements), index)
      case TimestampColumn =>
        new LongReaderAppender(bv.LongBinaryVector.appendingVector(memFactory, maxElements), index)
      case StringColumn    =>
        new StringReaderAppender(bv.UTF8Vector.appendingVector(memFactory, maxElements), index)
      case other: Column.ColumnType => ???
    }
    }.toArray
}