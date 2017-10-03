package filodb.core.store

import monix.reactive.Observable
import org.velvia.filo.FiloVector

import filodb.core.Types.PartitionKey
import filodb.core.query.ChunkSetReader

/**
 * An abstraction for a single partition of data, which can be scanned for chunks or readers
 *
 */
trait FiloPartition {
  def binPartition: PartitionKey
  // Use this instead of binPartition.toString as it is cached and potentially expensive
  lazy val stringPartition = binPartition.toString

  def numChunks: Int
  def latestChunkLen: Int
  def shard: Int

  /**
   * Streams back ChunkSetReaders from this Partition as an observable of readers by chunkID
   * @param method the ChunkScanMethod determining which subset of chunks to read from the partition
   * @param positions an array of the column positions according to projection.dataColumns, ie 0 for the first
   *                  column, up to projection.dataColumns.length - 1
   */
  def streamReaders(method: ChunkScanMethod, positions: Array[Int]): Observable[ChunkSetReader] =
    Observable.fromIterator(readers(method, positions))

  def readers(method: ChunkScanMethod, positions: Array[Int]): Iterator[ChunkSetReader]

  // Optimized access to the latest vectors, should be very fast (for streaming)
  def lastVectors: Array[FiloVector[_]]
}
