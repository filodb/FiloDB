package filodb.core.memstore

import scala.concurrent.Future

import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.Observable

import filodb.core.{DatasetRef, ErrorResponse, Response}
import filodb.core.binaryrecord2.RecordContainer
import filodb.core.metadata.{Column, Dataset}
import filodb.core.metadata.Column.ColumnType._
import filodb.core.query.ColumnFilter
import filodb.core.store.{ChunkSource, ColumnStore, MetaStore, StoreConfig}
import filodb.memory.MemFactory
import filodb.memory.format.{vectors => bv, _}

final case class ShardAlreadySetup(dataset: DatasetRef, shard: Int) extends
    Exception(s"Dataset $dataset shard $shard already setup")

sealed trait DataOrCommand
// Typically one RecordContainer is a single Kafka message, a container with multiple BinaryRecords
final case class SomeData(records: RecordContainer, offset: Long) extends DataOrCommand
final case class IndexData(timeBucket: Int, segment: Int, records: RecordContainer) extends DataOrCommand
final case class FlushCommand(groupNum: Int) extends DataOrCommand
final case class FlushIndexTimeBuckets(timeBucket: Int)

final case class FlushGroup(shard: Int, groupNum: Int, flushWatermark: Long, diskTimeToLiveSeconds: Int,
                            flushTimeBuckets: Option[FlushIndexTimeBuckets])

final case class FlushError(err: ErrorResponse) extends Exception(s"Flush error $err")


/**
 * A MemStore is an in-memory ChunkSource that ingests data not in chunks but as new records, potentially
 * spread over many partitions.  It supports the high-level ChunkSource API, and should support real-time reads
 * of fresh ingested data.  Being in-memory, it is designed to not retain data forever but flush completed
 * chunks to a persistent ChunkSink.
 *
 * A MemStore contains shards of data for one or more datasets, with optimized ingestion pipeline for
 * each shard.
 */
trait MemStore extends ChunkSource {
  /**
    * Persistent column store. Ingested data will eventually be poured into this sink for persistence, and
    * read from this store on demand as needed for recovery purposes.
    */
  def store: ColumnStore
  def metastore: MetaStore

  /**
   * Sets up one shard of a dataset for ingestion and the schema to be used when ingesting.
   * Once set up, the schema may not be changed.  The schema should be the same for all shards.
   * This method only succeeds if the dataset and shard has not already been setup.
   * @param storeConf the store configuration for that dataset.  Each dataset may have a different mem config.
   *                  See sourceconfig.store section in conf/timeseries-dev-source.conf
   * @throws ShardAlreadySetup
   */
  def setup(dataset: Dataset, shard: Int, storeConf: StoreConfig): Unit

  /**
   * Ingests new rows, making them immediately available for reads
   * NOTE: this method is not intended to be the main ingestion method, just used for testing.
   *       Instead the more reactive ingestStream method should be used.
   *
   * @param dataset the dataset to ingest into
   * @param shard shard number to ingest into
   * @param data a RecordContainer with BinaryRecords conforming to the schema used in setup(), and offset
   */
  def ingest(dataset: DatasetRef, shard: Int, data: SomeData): Unit

  /**
   * Sets up a shard of a dataset to continuously ingest new sets of records from a stream.
   * The records are immediately available for reads from that shard of the memstore.
   * Errors during ingestion are handled by the errHandler.
   * Flushes to the ChunkSink are initiated by a potentially independent stream, the flushStream, which emits
   *   flush events of a specific subgroup of a shard.
   * NOTE: does not check that existing streams are not already writing to this store.  That needs to be
   * handled by an upper layer.  Multiple stream ingestion is not guaranteed to be thread safe, a single
   * stream is safe for now.
   * NOTE2: ingest happens on the shard's single ingestion thread, except for flushes which are scheduled on the
   * passed in flushSched
   *
   * @param dataset the dataset to ingest into
   * @param shard shard number to ingest into
   * @param stream the stream of SomeData() with records conforming to dataset ingestion schema
   * @param flushSched the Scheduler to use to schedule flush tasks
   * @param flushStream the stream of FlushCommands for regular flushing of chunks to ChunkSink
   * @param diskTimeToLiveSeconds the time for chunks in this stream to live on disk (Cassandra)
   * @return a CancelableFuture for cancelling the stream subscription, which should be done on teardown
   *        the Future completes when both stream and flushStream ends.  It is up to the caller to ensure this.
   */
  def ingestStream(dataset: DatasetRef,
                   shard: Int,
                   stream: Observable[SomeData],
                   flushSched: Scheduler,
                   flushStream: Observable[FlushCommand] = FlushStream.empty,
                   diskTimeToLiveSeconds: Int = 259200): CancelableFuture[Unit]


  def recoverIndex(dataset: DatasetRef, shard: Int): Future[Unit]

  /**
   * Sets up streaming recovery of a shard from ingest records.  This is a separate API for several reasons:
   * 1. No flushing occurs during recovery.  We are recovering the write buffers before they get flushed.
   * 2. Ingested records that have an offset below the group watermark in checkpoints are skipped. They should have
   *    been flushed already.
   * 3. This returns an Observable of offsets that are read in, at roughly every "reportingInterval" offsets.  This
   *    is used for reporting recovery progress and to know when to end the recovery stream.
   *
   * This allows a MemStore to implement a more efficient recovery stream.  Some assumptions are made:
   * - The stream should restart from the min(checkpoints)
   * - The caller is responsible for subscribing the resulting stream, ending it, and handling errors
   *
   * @param dataset the dataset to ingest/recover into
   * @param shard shard number to ingest/recover into
   * @param stream the stream of SomeData() with records conforming to dataset ingestion schema.
   *               It should restart from the min(checkpoints)
   * @param checkpoints the write checkpoints for each subgroup, a Map(subgroup# -> checkpoint).  Records for that
   *                    subgroup with an offset below the checkpoint will be skipped, since they have been persisted.
   * @param reportingInterval the interval at which the latest offsets ingested will be sent back
   * @return an Observable of the latest ingested offsets.  Caller is responsible for subscribing and ending the stream
   */
  def recoverStream(dataset: DatasetRef,
                    shard: Int,
                    stream: Observable[SomeData],
                    checkpoints: Map[Int, Long],
                    reportingInterval: Long): Observable[Long]

  /**
   * Returns the names of tags or columns that are indexed at the partition level, across
   * all shards on this node
   * @return an index name and shard number
   */
  def indexNames(dataset: DatasetRef): Iterator[(String, Int)]

  /**
   * Returns values for a given index name (and # of series for each) for a dataset and shard,
   * in order of decreasing frequency/# of series per item.
   * @param topK the number of top items to return
   */
  def indexValues(dataset: DatasetRef, shard: Int, indexName: String, topK: Int = 100): Seq[TermInfo]

  /**
    * Returns the values of a given index name for the matching Column Filters
    * that are indexed at the partition level, on the given
    * shard on this node.
    * @return an Iterator for the index values
    */
  def indexValuesWithFilters(dataset: DatasetRef, shard: Int, filters: Seq[ColumnFilter],
                             indexName: String, end: Long, start: Long, limit: Int): Iterator[ZeroCopyUTF8String]

  /**
    * Returns the indexed TimeSeriesPartitions matching the column filters,
    * on the given shard on this node.
    * @return an Iterator for the TimeSeriesPartition
    */
  def partKeysWithFilters(dataset: DatasetRef, shard: Int, filters: Seq[ColumnFilter],
                          end: Long, start: Long, limit: Int): Iterator[TimeSeriesPartition]

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
   * @return Success, or some ErrorResponse
   */
  def truncate(dataset: DatasetRef): Future[Response]

  /**
   * Resets the state of the MemStore. Usually used for testing.
   */
  def reset(): Unit

  /**
   * Shuts down the MemStore and releases all previously allocated memory
   */
  def shutdown(): Unit
}

object MemStore {
  // TODO: make the max string vector size configurable.
  val MaxUTF8VectorSize = 8192

  /**
   * Figures out the AppendableVectors for each column, depending on type and whether it is a static/
   * constant column for each partition.
   */
  def getAppendables(memFactory: MemFactory,
                     dataset: Dataset,
                     maxElements: Int): Array[BinaryAppendableVector[_]] =
    dataset.dataColumns.zipWithIndex.map { case (col, index) =>
      col.columnType match {
        // Time series data doesn't really need the NA/null functionality, so use more optimal vectors
        // to save memory and CPU
        case IntColumn       => bv.IntBinaryVector.appendingVectorNoNA(memFactory, maxElements)
        case LongColumn      => bv.LongBinaryVector.appendingVectorNoNA(memFactory, maxElements)
        case DoubleColumn    => bv.DoubleVector.appendingVectorNoNA(memFactory, maxElements)
        case TimestampColumn => bv.LongBinaryVector.timestampVector(memFactory, maxElements)
        case StringColumn    => bv.UTF8Vector.appendingVector(memFactory, maxElements, MaxUTF8VectorSize)
        case other: Column.ColumnType => ???
      }
    }.toArray
}