package filodb.memory

import java.nio.ByteBuffer
import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.mutable.ListBuffer

import com.kenai.jffi.MemoryIO


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
  * A listener which is called when a ReusableMemory is reclaimed
  */
trait ReclaimListener {
  protected[memory] def onReclaim(memory: ReusableMemory): Unit
}

/**
  * A reclaimable memory which can be reclaimed and reused. Has an address
  * Code which needs to be called upon reclaim should register itself using the
  * register method as a ReclaimListener. Upon reclaim the onReclaim for all the
  * registered listeners is called.
  * A buffer to which a BinaryVector can be written
  */
trait ReusableMemory {
  protected val _isFree: AtomicBoolean = new AtomicBoolean(false)

  protected def isFree = _isFree.get()

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
    _isFree.set(false)
  }

  /**
    * Marks this memory as free.
    */
  protected def free() = {
    _isFree.set(true)
  }

  protected var reclaimListeners = new ListBuffer[ReclaimListener]

  /**
    * Register a listener to get callback when this memory is reclaimed
    *
    * @param reclaimListener The listener on which this callback is called.
    */
  protected[memory] def register(reclaimListener: ReclaimListener): Unit = {
    reclaimListeners += reclaimListener
  }

  /**
    * To be called when this memory is reclaimed. In turn this will call all registered listeners.
    */
  protected[memory] def reclaim(): Unit = {
    reclaimListeners.foreach(r => r.onReclaim(this))
    free()
  }

}

/**
  * A block is a resuable piece of memory beginning at the address and has a capacity.
  *
  * @param address
  * @param capacity
  */
class Block(val address: Long, val capacity: Long) extends Owned {

  protected val internalBuffer = MemoryIO.getCheckedInstance().newDirectByteBuffer(address, capacity.toInt)

  /**
    * Marks this memory as free. Also zeroes all the bytes from the beginning address until capacity
    */
  override protected def free(): Unit = {
    super.free()
    MemoryIO.getCheckedInstance.memset(address, 0, capacity)
  }

  def position(): Int = {
    internalBuffer.position()
  }

  def position(newPosition: Int): Unit = {
    checkOwnership()
    internalBuffer.position(newPosition)
  }

  def remaining(): Int = internalBuffer.remaining()

  /**
    * @param forSize the size for which to check the capacity for
    * @return Whether this block has capacity remaining to accomodate passed size of bytes.
    */
  def hasCapacity(forSize: Long): Boolean = {
    forSize <= internalBuffer.remaining()
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


