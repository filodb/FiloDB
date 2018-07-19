package filodb.core

import java.nio.ByteBuffer

import com.github.rholder.fauxflake.IdGenerators
import net.jpountz.lz4.{LZ4Compressor, LZ4Factory, LZ4FastDecompressor}

import filodb.core.Types._
import filodb.core.metadata.Dataset
import filodb.core.SingleKeyTypes.Long64HighBit
import filodb.memory.format.RowReader

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
    val arayBytes = orig.hasArray match {
      case true => orig.array
      case false =>
        val bytes = new Array[Byte](orig.limit)
        orig.position(0)
        orig.get(bytes)
        bytes
    }
    val compressedBytes = getCompressor.compress(arayBytes)
    val newBuf = ByteBuffer.allocate(offset + 4 + compressedBytes.size)
    newBuf.position(offset)
    newBuf.putInt(orig.capacity)
    newBuf.put(compressedBytes)
    newBuf.position(0)
    newBuf
  }

  // Like above, but just returns array byte without needing to do another copy
  def compress(orig: Array[Byte]): Array[Byte] = getCompressor.compress(orig)

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
    import Iterators._

    /**
     * Convenience method to scan/iterate over all rows of given selection of source data.  You must iterate
     * through all the elements.
     *
     * @param dataset the Dataset to read from
     * @param columnIDs the set of column IDs to read back.  Order determines the order of columns read back
     *                in each row.  These are the IDs from the Column instances.
     */
    def scanRows(dataset: Dataset,
                 columnIDs: Seq[ColumnId],
                 partMethod: PartitionScanMethod,
                 chunkMethod: ChunkScanMethod = AllChunkScan): Iterator[RowReader] =
      source.scanPartitions(dataset, columnIDs, partMethod, chunkMethod)
            .toIterator()
            .flatMap(_.timeRangeRows(chunkMethod, columnIDs.toArray))
  }
}