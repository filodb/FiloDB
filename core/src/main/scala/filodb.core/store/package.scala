package filodb.core

import java.nio.ByteBuffer
import net.jpountz.lz4.{LZ4Factory, LZ4Compressor, LZ4FastDecompressor}

package object store {
  val compressor = new ThreadLocal[LZ4Compressor]()
  val decompressor = new ThreadLocal[LZ4FastDecompressor]()

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
}