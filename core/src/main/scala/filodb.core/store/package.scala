package filodb.core

import java.nio.ByteBuffer

import com.github.rholder.fauxflake.IdGenerators
import net.jpountz.lz4.{LZ4Compressor, LZ4Factory, LZ4FastDecompressor}

import filodb.core.Types._
import filodb.core.metadata.Dataset
import filodb.core.SingleKeyTypes.Long64HighBit
import filodb.memory.format.{RowReader, UnsafeUtils}

package object store {
  val compressor = new ThreadLocal[LZ4Compressor]()
  val decompressor = new ThreadLocal[LZ4FastDecompressor]()

  val machineIdLong = IdGenerators.newSnowflakeIdGenerator.generateId(1)
  val machineId1024 = (machineIdLong.asLong >> 12) & (0x03ff)
  val msBitOffset   = 21
  val machIdBitOffset = 11
  val baseNsBitOffset = 9   // 2 ** 9 = 512
  val nanoBitMask     = Math.pow(2, machIdBitOffset).toInt - 1
  val lowerBitsMask   = Math.pow(2, msBitOffset).toInt - 1
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
   * Compresses bytes in the original ByteBuffer into a new ByteBuffer.
   * ByteBuffer is assumed to be a BinaryRegionLarge containing a 4-byte length header
   *   (eg BinaryVector, RecordContainer, BinaryRecordV2)
   * The new ByteBuffer conists of the original 4-byte length header with bit 31 set
   * (since length cannot be negative) and the compressed bytes following.
   * @param orig the original data ByteBuffer
   * @param offset the offset into the target ByteBuffer to write the header + compresed bytes
   * @return a ByteBuffer containing the (length + bit31set) + compressed bytes
   */
  def compress(orig: ByteBuffer): ByteBuffer = {
    // Fastest decompression method is when giving size of original bytes
    val arayBytes = orig.hasArray match {
      case true => orig.array
      case false =>
        val bytes = new Array[Byte](orig.limit)
        orig.position(0)
        orig.get(bytes)
        bytes
    }
    val origLen = UnsafeUtils.getInt(arayBytes, UnsafeUtils.arayOffset)
    require(origLen >= 0)
    val outBytes = new Array[Byte](getCompressor.maxCompressedLength(origLen) + 4)
    getCompressor.compress(arayBytes, 4, origLen, outBytes, 4)
    UnsafeUtils.setInt(outBytes, UnsafeUtils.arayOffset, origLen | 0x80000000)
    ByteBuffer.wrap(outBytes)
  }

  // Like above, but just returns array byte without needing to do another copy
  def compress(orig: Array[Byte]): Array[Byte] = getCompressor.compress(orig)

  /**
   * Decompresses the compressed bytes into a new ByteBuffer.
   * @param compressed the ByteBuffer containing the (length + bit31set) + compressed bytes
   */
  def decompress(compressed: ByteBuffer): ByteBuffer = {
    compressed.order(java.nio.ByteOrder.LITTLE_ENDIAN)
    val origLength = compressed.getInt(compressed.position) & 0x7fffffff   // strip off compression bit
    val decompressedBytes = new Array[Byte](origLength + 4)
    getDecompressor.decompress(compressed.array, compressed.position() + 4, decompressedBytes, 4, origLength)
    UnsafeUtils.setInt(decompressedBytes, UnsafeUtils.arayOffset, origLength)
    ByteBuffer.wrap(decompressedBytes)
  }

  /**
   * Decompresses IFF bit 31 of the 4-byte length header is set, otherwise returns original buffer
   */
  def decompressChunk(compressed: ByteBuffer): ByteBuffer = {
    compressed.get(compressed.position() + 3) match {
      case b if b < 0 => decompress(compressed)
      case b          => compressed
    }
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
   * New formulation for chunkID based on a combo of the start time for a chunk and the current time in the lower
   * bits to disambiguate two chunks which have the same start time.
   *
   * bits 63-21 (43 bits):  milliseconds since Unix Epoch (1/1/1970) - enough for 278.7 years or through 2248
   * bits 20-0  (21 bits):  The lower 21 bits of nanotime for disambiguation
   */
  @inline final def newChunkID(startTime: Long): Long = chunkID(startTime, System.nanoTime)

  @inline final def chunkID(startTime: Long, currentTime: Long): Long =
    (startTime << msBitOffset) | (currentTime & lowerBitsMask)

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
