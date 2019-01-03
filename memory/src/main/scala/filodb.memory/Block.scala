package filodb.memory

import java.lang.{Long => jLong}
import java.nio.ByteBuffer
import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import com.typesafe.scalalogging.StrictLogging

/*
* Useful to establish thread ownership of a buffer.
* A buffer uses marks and positions to read and write data. As such it cannot be used
* by multiple threads concurrently.
*
* This trait helps to establish ownership of a thread on a buffer.
* When a buffer is owned by a thread another thread cannot mark the buffer.
* This protects the buffer position from being modified by multiple threads.
* This is a programmer protection like in Rust except this is runtime checked
* instead of compile time.
*/
protected[memory] trait Owned extends ReusableMemory {

  protected val ref = new AtomicReference[Thread]()

  def own(): Unit = {
    val owningThread = ref.get()
    val currentThread = Thread.currentThread()
    ref.set(currentThread)
  }

  protected def checkOwnership(): Unit = {
    if (!(ref.get() == Thread.currentThread())) {
      throw new ConcurrentModificationException("Thread does not own this block")
    }
  }

}

/**
  * A listener called when a ReusableMemory is reclaimed.  The onReclaim() method is called for each piece of
  * metadata in the ReusableMemory.
  */
trait ReclaimListener {
  def onReclaim(metadata: Long, numBytes: Int): Unit
}

/**
  * A reclaimable memory which can be reclaimed and reused. Normally used to hold many BinaryVectors.
  * It also can hold pieces of metadata which are passed to a ReclaimListener when the memory is reclaimed.
  * The metadata can be used to free references and do other housekeeping.
  */
trait ReusableMemory extends StrictLogging {
  protected val _isReusable: AtomicBoolean = new AtomicBoolean(false)

  def reclaimListener: ReclaimListener

  /**
    * @return Whether this block can be reclaimed. The original owning callsite has to
    *         set this as being reusable
    */
  def canReclaim: Boolean = _isReusable.get()

  /**
    * @return The starting location of the memory
    */
  def address(): Long

  /**
    * @return The size of this memory in bytes
    */
  def capacity(): Long

  /**
    * Marks this Memory as in use.
    */
  protected[memory] def markInUse() = {
    _isReusable.set(false)
  }

  /**
    * Marks this memory as reclaimable.
    */
  def markReclaimable(): Unit = {
    _isReusable.set(true)
  }

  /**
   * Calls the reclaimListener for each piece of metadata that we own
   */
  protected def reclaimWithMetadata(): Unit

  /**
    * Marks this memory as free and calls reclaimListener for every piece of metadata.
    */
  protected def free() = {
    logger.debug(s"Reclaiming block at ${jLong.toHexString(address)}...")
    reclaimWithMetadata()
  }

  /**
    * To be called when this memory is reclaimed. In turn this will call all registered listeners.
    */
  def reclaim(forced: Boolean = false): Unit = {
    if (!canReclaim && !forced) throw new IllegalStateException("Cannot reclaim this block")
    free()
  }
}

/**
  * A block is a reusable piece of memory beginning at the address and has a capacity.
  * It is capable of holding metadata also for reclaims.
  * Normally usage of a block starts at position 0 and grows upward.  Metadata usage starts at the top and grows
  * downward.  Each piece of metadata must be < 64KB as two bytes are used to denote size at the start of each piece of
  * metadata.
  *
  * @param address
  * @param capacity
  */
class Block(val address: Long, val capacity: Long, val reclaimListener: ReclaimListener) extends Owned {
  import format.UnsafeUtils

  protected var _position: Int = 0
  protected var _metaPosition: Int = capacity.toInt

  /**
    * Marks this memory as free. Also zeroes all the bytes from the beginning address until capacity
    */
  override protected def free(): Unit = {
    super.free()
    _position = 0
  }

  def position(): Int = {
    _position
  }

  def position(newPosition: Int): Unit = {
    checkOwnership()
    _position = newPosition
  }

  def remaining(): Int = _metaPosition - _position

  /**
   * Allocates metaSize bytes for metadata storage.
   * @param metaSize the number of bytes to use for metadata storage.
   * @return the Long address of the metadata space.  The metaSize is written to the location 2 bytes before this.
   *         If there is no capacity then 0 (null) is returned.
   */
  def allocMetadata(metaSize: Short): Long =
    if (metaSize > 0 && metaSize <= (remaining() - 2)) {
      _metaPosition -= (metaSize + 2)
      val metaAddr = address + _metaPosition
      UnsafeUtils.setShort(UnsafeUtils.ZeroPointer, metaAddr, metaSize)
      metaAddr + 2
    } else {
      0
    }

  protected def reclaimWithMetadata(): Unit = {
    var metaPointer = address + _metaPosition
    while (metaPointer < (address + capacity)) {
      val metaSize = UnsafeUtils.getShort(metaPointer)
      reclaimListener.onReclaim(metaPointer + 2, metaSize)
      metaPointer += (2 + metaSize)
    }
  }

  /**
    * @param forSize the size for which to check the capacity for
    * @return Whether this block has capacity remaining to accommodate passed size of bytes.
    */
  def hasCapacity(forSize: Long): Boolean = {
    forSize <= remaining()
  }

  //debug utility method
  protected def asHexString(some: ByteBuffer): String = {
    val buf = some.duplicate()
    val byteArr = new Array[Byte](buf.remaining())
    buf.get(byteArr)
    val stringBuf = new StringBuffer()
    byteArr.foreach(b => stringBuf.append(b))
    stringBuf.toString
  }

}


