package filodb.core.query

import monix.reactive.Observable

import filodb.core.store.ChunkSetInfo
import filodb.core.Types.PartitionKey

/**
 * An abstraction for a single partition of data, which can be scanned for chunks or readers
 */
trait FiloPartition {
  import ChunkSetInfo._

  def binPartition: PartitionKey

  def numChunks: Int
  def latestChunkLen: Int

  /**
   * Streams back ChunkSetReaders from this Partition as an observable of readers by chunkID
   * @param infosSkips ChunkSetInfos and skips, as returned by one of the index search methods
   * @param positions an array of the column positions according to projection.dataColumns, ie 0 for the first
   *                  column, up to projection.dataColumns.length - 1
   */
  def streamReaders(infosSkips: InfosSkipsIt, positions: Array[Int]): Observable[ChunkSetReader] =
    Observable.fromIterator(readers(infosSkips, positions))

  def readers(infosSkips: InfosSkipsIt, positions: Array[Int]): Iterator[ChunkSetReader]
}
