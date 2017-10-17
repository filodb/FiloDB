package filodb.core.store

import monix.reactive.Observable
import org.velvia.filo.FiloVector

import filodb.core.metadata.Dataset
import filodb.core.query.ChunkSetReader
import filodb.core.Types.PartitionKey

/**
 * An abstraction for a single partition of data, which can be scanned for chunks or readers
 *
 */
trait FiloPartition {
  def dataset: Dataset
  def binPartition: PartitionKey
  // Use this instead of binPartition.toString as it is cached and potentially expensive
  lazy val stringPartition = binPartition.toString

  def numChunks: Int
  def latestChunkLen: Int
  def shard: Int

  private final val partitionVectors = new Array[FiloVector[_]](dataset.partitionColumns.length)

  /**
   * Returns a ConstVector for the partition column ID.  They are kept in an array for fast access.
   * Basically partition columns are not stored in memory/on disk as chunks to save space and I/O.
   * If a user queries one, then create a ConstVector on demand and return it
   * @param columnID the partition column ID to return
   */
  def constPartitionVector(columnID: Int): FiloVector[_] = {
    require(Dataset.isPartitionID(columnID), s"Column ID $columnID is not a partition column")
    // scalastyle:off null
    val partVectPos = columnID - Dataset.PartColStartIndex
    val partVect = partitionVectors(partVectPos)
    if (partVect == null) {
      // Take advantage of the fact that a ConstVector internally does not care how long it is.  So just
      // make it as long as possible to accommodate all cases  :)
      val constVect = dataset.partitionColumns(partVectPos).extractor.
                        constVector(binPartition, partVectPos, Int.MaxValue)
      partitionVectors(partVectPos) = constVect
      constVect
    } else { partVect }
    // scalastyle:on null
  }

  /**
   * Streams back ChunkSetReaders from this Partition as an observable of readers by chunkID
   * @param method the ChunkScanMethod determining which subset of chunks to read from the partition
   * @param columnIds an array of the column IDs to read from.  Most of the time this corresponds to
   *                  the position within data columns.
   */
  def streamReaders(method: ChunkScanMethod, columnIds: Array[Int]): Observable[ChunkSetReader] =
    Observable.fromIterator(readers(method, columnIds))

  def readers(method: ChunkScanMethod, columnIds: Array[Int]): Iterator[ChunkSetReader]

  // Optimized access to the latest vectors, should be very fast (for streaming)
  def lastVectors: Array[FiloVector[_]]
}
