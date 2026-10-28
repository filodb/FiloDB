package filodb.core.memstore

import org.apache.lucene.store.{Directory, FilterDirectory, IndexInput, IOContext}

object ByteCountingDirectory {
  // One-element array per thread. An array avoids boxing a Long on every update.
  private val threadBytes = new ThreadLocal[Array[Long]] {
    override def initialValue(): Array[Long] = new Array[Long](1)
  }

  /**
   * Total bytes that inputs of any ByteCountingDirectory read on the current thread so far.
   * The value only grows. To measure one operation, read it before and after the operation
   * and use the difference. The operation must run on a single thread.
   */
  def threadBytesRead(): Long = threadBytes.get()(0)

  private[memstore] def add(n: Long): Unit = {
    threadBytes.get()(0) += n
  }
}

/**
 * A Directory that counts the bytes that Lucene reads from its files.
 * The count is bytes requested through IndexInput. It is not disk I/O:
 * the OS page cache and read-ahead are not visible here.
 * Seeks and skipped data are not counted. A block that Lucene reads in full is counted in full.
 */
class ByteCountingDirectory(in: Directory) extends FilterDirectory(in) {
  override def openInput(name: String, context: IOContext): IndexInput =
    new ByteCountingIndexInput(s"ByteCountingIndexInput($name)", super.openInput(name, context))
}

/**
 * Delegates to `in` and adds the bytes read to the per-thread counter.
 * Only readByte and readBytes need counting for correctness: the DataInput defaults for the
 * other read methods call readByte. readShort, readInt and readLong are overridden only
 * because delegating them to `in` is faster than eight single-byte calls.
 * clone and slice return wrappers, so every IndexInput that Lucene derives is counted.
 */
// scalastyle:off
final class ByteCountingIndexInput(desc: String, in: IndexInput) extends IndexInput(desc) {
  override def close(): Unit = in.close()
  override def getFilePointer(): Long = in.getFilePointer()
  override def seek(pos: Long): Unit = in.seek(pos)
  override def length(): Long = in.length()

  override def slice(sliceDescription: String, offset: Long, length: Long): IndexInput =
    new ByteCountingIndexInput(sliceDescription, in.slice(sliceDescription, offset, length))

  override def clone(): IndexInput = new ByteCountingIndexInput(toString, in.clone())

  override def readByte(): Byte = {
    ByteCountingDirectory.add(1)
    in.readByte()
  }

  override def readBytes(b: Array[Byte], offset: Int, len: Int): Unit = {
    ByteCountingDirectory.add(len)
    in.readBytes(b, offset, len)
  }

  override def readShort(): Short = {
    ByteCountingDirectory.add(2)
    in.readShort()
  }

  override def readInt(): Int = {
    ByteCountingDirectory.add(4)
    in.readInt()
  }

  override def readLong(): Long = {
    ByteCountingDirectory.add(8)
    in.readLong()
  }
}
// scalastyle:on
