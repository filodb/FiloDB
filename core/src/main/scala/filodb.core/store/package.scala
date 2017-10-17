package filodb.core

import java.nio.ByteBuffer

import com.github.rholder.fauxflake.IdGenerators
import monix.eval.Task
import net.jpountz.lz4.{LZ4Compressor, LZ4Factory, LZ4FastDecompressor}
import org.scalactic._
import org.velvia.filo.RowReader

import filodb.core.Types._
import filodb.core.metadata.{Dataset, InvalidFunctionSpec}
import filodb.core.SingleKeyTypes.Long64HighBit
import filodb.core.query._

package object store {
  val compressor = new ThreadLocal[LZ4Compressor]()
  val decompressor = new ThreadLocal[LZ4FastDecompressor]()

  val machineIdLong = IdGenerators.newSnowflakeIdGenerator.generateId(1)
  val machineId1024 = (machineIdLong.asLong >> 12) & (0x03ff)
  val msBitOffset   = 21
  val machIdBitOffset = 11
  val baseNsBitOffset = 9   // 2 ** 9 = 512
  val nanoBitMask     = Math.pow(2, machIdBitOffset).toInt - 1
  val baseTimeMillis  = org.joda.time.DateTime.parse("2016-01-01T00Z").getMillis

  // Assume LZ4 compressor has state and is not thread safe.  Use ThreadLocals.
  private def getCompressor: LZ4Compressor = {
    if (Option(compressor.get).isEmpty) {
      val lz4Factory = LZ4Factory.fastestInstance()
      compressor.set(lz4Factory.fastCompressor())
    }
    compressor.get
  }

  private def getDecompressor: LZ4FastDecompressor = {
    if (Option(decompressor.get).isEmpty) {
      val lz4Factory = LZ4Factory.fastestInstance()
      decompressor.set(lz4Factory.fastDecompressor())
    }
    decompressor.get
  }

  /**
   * Compresses bytes in the original ByteBuffer into a new ByteBuffer.  Will write a 4-byte header
   * containing the compressed length plus the compressed bytes.
   * @param orig the original data ByteBuffer
   * @param offset the offset into the target ByteBuffer to write the header + compresed bytes
   * @return a ByteBuffer containing the header + compressed bytes at offset, with position set to 0
   */
  def compress(orig: ByteBuffer, offset: Int = 0): ByteBuffer = {
    // Fastest decompression method is when giving size of original bytes, so store that as first 4 bytes
    val compressedBytes = getCompressor.compress(orig.array)
    val newBuf = ByteBuffer.allocate(offset + 4 + compressedBytes.size)
    newBuf.position(offset)
    newBuf.putInt(orig.capacity)
    newBuf.put(compressedBytes)
    newBuf.position(0)
    newBuf
  }

  /**
   * Decompresses the compressed bytes into a new ByteBuffer
   * @param compressed the ByteBuffer containing the header + compressed bytes at the current position
   * @param offset the offset into the ByteBuffer where header bytes start
   */
  def decompress(compressed: ByteBuffer, offset: Int = 0): ByteBuffer = {
    val origLength = compressed.getInt(offset)
    ByteBuffer.wrap(getDecompressor.decompress(compressed.array, offset + 4, origLength))
  }

  /**
   * 64-bit TimeUUID function designed specifically for generating unique ChunkIDs.  Chunks take a while
   * to encode so rarely would you be generating more than a few thousand chunks per second.  Format:
   * bits 63-21 (43 bits):  milliseconds since Jan 1, 2016 - enough for 278.7 years or through 2294
   * bits 20-11 (10 bits):  SnowFlake-style machine ID from FauxFlake library
   * bits 10-0  (11 bits):  nanosecond time in 512-ns increments.
   *
   * Bit 63 is inverted to allow for easy comparisons using standard signed Long math.
   *
   * The TimeUUID function generally increases in time but successive calls are not guaranteed to be strictly
   * increasing, but if called greater than 512ns apart should be unique.
   */
  def timeUUID64: Long = {
    ((System.currentTimeMillis - baseTimeMillis) << msBitOffset) |
    (machineId1024 << machIdBitOffset) |
    ((System.nanoTime >> baseNsBitOffset) & nanoBitMask) ^
    Long64HighBit
  }

  /**
   * Adds a few useful methods to ChunkSource
   */
  implicit class RichChunkSource(source: ChunkSource) {
    import ChunkSetReader._
    import Iterators._

    /**
     * Performs aggregations on the ChunkSource chunks.
     * @param dataset the Dataset to read from
     * @param columnIDs the set of column IDs to read back.  Order determines the order of columns read back
     *                in each row.  These are the IDs from the Column instances.
     */
    def aggregate(dataset: Dataset,
                  query: QuerySpec,
                  partMethod: PartitionScanMethod,
                  chunkMethod: ChunkScanMethod = AllChunkScan): Task[Aggregate[_]] Or InvalidFunctionSpec = {
      for { aggregator <- query.aggregateFunc.validate(query.column, dataset.timestampColumn.map(_.name),
                                                       chunkMethod, query.aggregateArgs, dataset)
            combiner   <- query.combinerFunc.validate(aggregator, query.combinerArgs) }
      yield {
        val aggStream = source.scanPartitions(dataset, partMethod)
                          .map { partition => aggregator.aggPartition(chunkMethod, partition) }
        combiner.fold(aggStream)
      }
    }

    /**
     * Scans chunks from a dataset.  ScanMethod params determines what gets scanned.
     *
     * @param dataset the Dataset to read from
     * @param columnIDs the set of column IDs to read back.  Order determines the order of columns read back
     *                in each row.  These are the IDs from the Column instances.
     * @param partMethod which partitions to scan
     * @param chunkMethod which chunks within a partition to scan
     * @return An iterator over ChunkSetReaders
     */
    def scanChunks(dataset: Dataset,
                   columnIDs: Seq[ColumnId],
                   partMethod: PartitionScanMethod,
                   chunkMethod: ChunkScanMethod = AllChunkScan): Iterator[ChunkSetReader] =
      source.readChunks(dataset, columnIDs, partMethod, chunkMethod).toIterator()

    /**
     * Scans over chunks, just like scanChunks, but returns an iterator of RowReader
     * for all of those row-oriented applications.  Contains a high performance iterator
     * implementation, probably faster than trying to do it yourself.  :)
     *
     * @param chunkReaderMap A function to transform the incoming ChunkSetReader => ChunkSetReader.
     *                       Mainly used by the Spark DataSource to adapt FiloVectors for Spark.
     * @param readerFactory  A function Array[FiloVector] => RowReader
     */
    def scanRows(dataset: Dataset,
                 columnIDs: Seq[ColumnId],
                 partMethod: PartitionScanMethod,
                 chunkMethod: ChunkScanMethod = AllChunkScan,
                 chunkReaderMap: ChunkSetReader => ChunkSetReader = { x => x },
                 readerFactory: RowReaderFactory = DefaultReaderFactory): Iterator[RowReader] = {
      val chunkIt = scanChunks(dataset, columnIDs, partMethod, chunkMethod)
      if (chunkIt.hasNext) {
        // TODO: fork this kind of code into a macro, called fastFlatMap.
        // That's what we really need...  :-p
        new Iterator[RowReader] {
          final def getNextRowIt: Iterator[RowReader] = {
            val chunkReader = chunkReaderMap(chunkIt.next)
            chunkReader.rowIterator(readerFactory)
          }

          var rowIt: Iterator[RowReader] = getNextRowIt

          final def hasNext: Boolean = {
            var _hasNext = rowIt.hasNext
            while (!_hasNext) {
              if (chunkIt.hasNext) {
                rowIt = getNextRowIt
                _hasNext = rowIt.hasNext
              } else {
                // all done. No more chunks.
                return false
              }
            }
            _hasNext
          }

          final def next: RowReader = rowIt.next.asInstanceOf[RowReader]
        }
      } else {
        Iterator.empty
      }
    }
  }
}