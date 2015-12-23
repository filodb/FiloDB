package filodb.core.util

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ConcurrentMap}

trait BufferPool {
  def acquire(size: Int): ByteBuffer

  def release(buf: ByteBuffer): Unit
}

trait MemoryPool extends BufferPool with FiloLogging {
  val thePool = new MappedByteBufferPool(1024, true)

  def acquire(size: Int): ByteBuffer = {
    val bb = thePool.acquire(size)
    val bbSize = bb.capacity()
    metrics.debug(s"Requested size - $size. Acquired buffer with capacity $bbSize")
    bb
  }

  def release(buf: ByteBuffer): Unit = {
    val bbSize = buf.capacity()
    thePool.release(buf)
    metrics.debug(s"Released buffer with capacity $bbSize")
  }

}

// scalastyle:off
class MappedByteBufferPool(factor: Int, direct: Boolean = true) {
  private final val directBuffers: ConcurrentMap[Integer, util.Queue[ByteBuffer]]
  = new ConcurrentHashMap[Integer, util.Queue[ByteBuffer]]
  private final val heapBuffers: ConcurrentMap[Integer, util.Queue[ByteBuffer]]
  = new ConcurrentHashMap[Integer, util.Queue[ByteBuffer]]

  def acquire(size: Int): ByteBuffer = {
    val bucket: Int = bucketFor(size)
    val buffers: ConcurrentMap[Integer, util.Queue[ByteBuffer]] = buffersFor(direct)
    var result: ByteBuffer = null
    val byteBuffers: util.Queue[ByteBuffer] = buffers.get(bucket)
    if (byteBuffers != null) result = byteBuffers.poll
    if (result == null) {
      val capacity: Int = bucket * factor
      result = if (direct) allocateDirect(capacity) else allocate(capacity)
    }
    result.clear()
    result
  }

  def release(buffer: ByteBuffer): Unit = {
    if (buffer != null) {
      assert((buffer.capacity % factor) == 0)
      val bucket: Int = bucketFor(buffer.capacity)
      val buffers: ConcurrentMap[Integer, util.Queue[ByteBuffer]] = buffersFor(buffer.isDirect)
      var byteBuffers: util.Queue[ByteBuffer] = buffers.get(bucket)
      if (byteBuffers == null) {
        byteBuffers = new ConcurrentLinkedQueue[ByteBuffer]
        val existing: util.Queue[ByteBuffer] = buffers.putIfAbsent(bucket, byteBuffers)
        if (existing != null) byteBuffers = existing
      }
      buffer.clear()
      byteBuffers.offer(buffer)
    }
  }

  def clear(): Unit = {
    directBuffers.clear()
    heapBuffers.clear()
  }

  private def bucketFor(size: Int): Int = {
    var bucket: Int = size / factor
    if (size % factor > 0) {
      bucket += 1
    }
    bucket
  }

  def buffersFor(direct: Boolean): ConcurrentMap[Integer, util.Queue[ByteBuffer]] = {
    if (direct) directBuffers else heapBuffers
  }

  def allocateDirect(capacity: Int): ByteBuffer = {
    val buf: ByteBuffer = ByteBuffer.allocateDirect(capacity)
    buf.limit(0)
    buf
  }

  def allocate(capacity: Int): ByteBuffer = {
    val buf: ByteBuffer = ByteBuffer.allocate(capacity)
    buf.limit(0)
    buf
  }

}

// scalastyle:on
