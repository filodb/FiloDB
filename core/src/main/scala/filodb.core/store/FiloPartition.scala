package filodb.core.store

import monix.reactive.Observable

import filodb.core.metadata.{Column, Dataset}
import filodb.core.query.ChunkSetReader
import filodb.memory.format.{FiloConstVector, FiloVector, UnsafeUtils}

/**
 * An abstraction for a single partition of data, which can be scanned for chunks or readers
 *
 */
trait FiloPartition {
  def dataset: Dataset
  def partKeyBase: Array[Byte]
  def partKeyOffset: Long

  // Use this instead of binPartition.toString as it is cached and potentially expensive
  lazy val stringPartition = dataset.partKeySchema.stringify(partKeyBase, partKeyOffset)

  // Returns binary partition key as a new, copied byte array
  def partKeyBytes: Array[Byte] = dataset.partKeySchema.asByteArray(partKeyBase, partKeyOffset)

  def numChunks: Int

  /**
   * The length or # of data samples of the set of appending or ingesting chunks.  0 if there are no
   * appendable chunks or write buffers.
   */
  def appendingChunkLen: Int

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
      // val constVect = dataset.partitionColumns(partVectPos).extractor.
      //                   constVector(binPartition, partVectPos, Int.MaxValue)
      val constVect = dataset.partitionColumns(partVectPos).columnType match {
        case Column.ColumnType.StringColumn =>
          // NOTE: this should be @deprecated soon as ZCUTF8String is obsoleted
          val zcUtf8 = dataset.partKeySchema.asZCUTF8Str(partKeyBase, partKeyOffset, partVectPos)
          new FiloConstVector(zcUtf8, Int.MaxValue)
        case Column.ColumnType.IntColumn =>
          new FiloConstVector(dataset.partKeySchema.getInt(partKeyBase, partKeyOffset, partVectPos), Int.MaxValue)
        case Column.ColumnType.LongColumn =>
          new FiloConstVector(dataset.partKeySchema.getLong(partKeyBase, partKeyOffset, partVectPos), Int.MaxValue)
        case Column.ColumnType.DoubleColumn =>
          new FiloConstVector(dataset.partKeySchema.getDouble(partKeyBase, partKeyOffset, partVectPos), Int.MaxValue)
        case other: Column.ColumnType => ???
      }
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

  /**
   * Two FiloPartitions are equal if they have the same partition key.  This test is used in various
   * data structures.
   */
  override def hashCode: Int = dataset.partKeySchema.partitionHash(partKeyBase, partKeyOffset)

  override def equals(other: Any): Boolean = other match {
    case UnsafeUtils.ZeroPointer => false
    case f: FiloPartition => dataset.partKeySchema.equals(partKeyBase, partKeyOffset, f.partKeyBase, f.partKeyOffset)
    case o: Any           => false
  }
}
