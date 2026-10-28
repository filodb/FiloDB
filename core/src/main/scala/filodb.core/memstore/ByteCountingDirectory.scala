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

  /**
   * The counter array of the current thread. An input takes it once, when it is created,
   * so that a read does not need a ThreadLocal lookup.
   */
  private[memstore] def currentCounter(): Array[Long] = threadBytes.get()
}

/**
 * A Directory that counts the bytes that Lucene reads from its files.
 * The count is bytes requested through IndexInput. It is not disk I/O:
 * the OS page cache and read-ahead are not visible here.
 * Seeks are not counted. A block that Lucene reads in full is counted in full.
 */
class ByteCountingDirectory(in: Directory) extends FilterDirectory(in) {
  override def openInput(name: String, context: IOContext): IndexInput =
    new ByteCountingIndexInput(s"ByteCountingIndexInput($name)", super.openInput(name, context),
      ByteCountingDirectory.currentCounter())
}

/**
 * Delegates to `in` and adds the bytes read to `counter`.
 * `counter` is the per-thread counter of the thread that created this input (by openInput,
 * clone or slice). Lucene makes a clone for each thread that reads, so bytes normally go to
 * the thread that reads. If an input is read on another thread, the bytes go to the thread
 * that created it.
 * Only readByte and readBytes need counting for correctness: the DataInput defaults for the
 * other read methods call readByte. readShort, readInt and readLong are overridden only
 * because delegating them to `in` is faster than eight single-byte calls.
 * clone and slice return wrappers, so every IndexInput that Lucene derives is counted.
 */
// scalastyle:off no.clone
final class ByteCountingIndexInput(desc: String, in: IndexInput, counter: Array[Long])
  extends IndexInput(desc) {
// scalastyle:on no.clone
  override def close(): Unit = in.close()
  override def getFilePointer(): Long = in.getFilePointer()
  override def seek(pos: Long): Unit = in.seek(pos)
  override def length(): Long = in.length()

  override def slice(sliceDescription: String, offset: Long, length: Long): IndexInput =
    new ByteCountingIndexInput(sliceDescription, in.slice(sliceDescription, offset, length),
      ByteCountingDirectory.currentCounter())

  override def clone(): IndexInput =
    new ByteCountingIndexInput(toString, in.clone(), ByteCountingDirectory.currentCounter())

  override def readByte(): Byte = {
    counter(0) += 1
    in.readByte()
  }

  override def readBytes(b: Array[Byte], offset: Int, len: Int): Unit = {
    counter(0) += len
    in.readBytes(b, offset, len)
  }

  override def readShort(): Short = {
    counter(0) += 2
    in.readShort()
  }

  override def readInt(): Int = {
    counter(0) += 4
    in.readInt()
  }

  override def readLong(): Long = {
    counter(0) += 8
    in.readLong()
  }
}
