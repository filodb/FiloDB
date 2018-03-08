package filodb.memory

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import com.kenai.jffi.MemoryIO
import com.typesafe.scalalogging.StrictLogging

import filodb.memory.format.{BinaryVector, UnsafeUtils}
import filodb.memory.format.BinaryVector.{HeaderMagic, Memory}
import filodb.memory.format.UnsafeUtils.arayOffset

/**
  * A trait which allows allocation of memory with the Filo magic header
  */
trait MemFactory {
  /**
    * Allocates memory for requested size plus 4 bytes for magic header
    *
    * @param size Request memory allocation size in bytes
    * @return Memory which has a base, offset and a length
    */
  def allocateWithMagicHeader(size: Int): Memory

  /**
    * Frees memory allocated at the passed address
    *
    * @param address The native address which represents the starting location of memory allocated
    */
  def freeMemory(address: Long): Unit

  /**
    * Allocate and make of copy of the bytes into the allocated memory
    *
    * @param bytes The bytes to be copied
    * @return The memory to which the bytes have been copied to
    */
  def copyFromBytes(bytes: Array[Byte]): ByteBuffer

  def fromBuffer(buf: ByteBuffer): Memory = {
    if (buf.hasArray) {
      (buf.array, arayOffset.toLong + buf.arrayOffset + buf.position, buf.limit - buf.position)
    } else {
      assert(buf.isDirect)
      val address = MemoryIO.getCheckedInstance.getDirectBufferAddress(buf)
      (UnsafeUtils.ZeroPointer, address + buf.position, buf.limit - buf.position)
    }
  }

  /**
   * Performs all cleanup, including freeing all allocated memory (if applicable)
   */
  def shutdown(): Unit
}

object MemFactory {
  val onHeapFactory = new ArrayBackedMemFactory()
}

/**
  * Native (off heap) memory manager, allocating using MemoryIO with every call to allocateWithMagicHeader
  * and relying on a cap to not allocate more than upperBoundSizeInBytes
  */
class NativeMemoryManager(val upperBoundSizeInBytes: Long) extends MemFactory {
  protected val usedSoFar = new AtomicLong(0)
  protected val sizeMapping = new ConcurrentHashMap[Long, Long]()

  def usedMemory: Long = usedSoFar.get()

  def availableDynMemory: Long = upperBoundSizeInBytes - usedSoFar.get()

  /**
    * Allocate and make of copy of the bytes into the allocated memory
    *
    * @param bytes The bytes to be copied
    * @return The memory to which the bytes have been copied to
    */
  override def copyFromBytes(bytes: Array[Byte]): ByteBuffer = {
    val currentSize = usedSoFar.get()
    //4 for magic header
    val size = bytes.length
    val resultantSize = currentSize + size
    if (!(resultantSize > upperBoundSizeInBytes)) {
      val address: Long = MemoryIO.getCheckedInstance().allocateMemory(size, true)
      usedSoFar.compareAndSet(currentSize, currentSize + size)
      sizeMapping.put(address, size)
      val byteBuffer = UnsafeUtils.asDirectBuffer(address, size)
      UnsafeUtils.unsafe.copyMemory(bytes, UnsafeUtils.arayOffset, UnsafeUtils.ZeroPointer, address, size)
      byteBuffer
    } else {
      val msg = s"Resultant memory size $resultantSize after allocating " +
        s"with requested size $size is greater than upper bound size $upperBoundSizeInBytes"
      throw new IndexOutOfBoundsException(msg)
    }
  }

  override def allocateWithMagicHeader(allocateSize: Int): Memory = {
    val currentSize = usedSoFar.get()
    //4 for magic header
    val size = allocateSize + 4
    val resultantSize = currentSize + size
    if (!(resultantSize > upperBoundSizeInBytes)) {
      val address: Long = MemoryIO.getCheckedInstance().allocateMemory(size, true)
      usedSoFar.compareAndSet(currentSize, currentSize + size)
      sizeMapping.put(address, size)
      UnsafeUtils.setInt(UnsafeUtils.ZeroPointer, address, HeaderMagic)
      (UnsafeUtils.ZeroPointer, address + 4, allocateSize)
    } else {
      val msg = s"Resultant memory size $resultantSize after allocating " +
        s"with requested size $size is greater than upper bound size $upperBoundSizeInBytes"
      throw new IndexOutOfBoundsException(msg)
    }
  }

  override def freeMemory(startAddress: Long): Unit = {
    //start where the header started
    val address = startAddress - 4
    val size = sizeMapping.get(address)
    if (size > 0) {
      val currentSize = usedSoFar.get()
      MemoryIO.getCheckedInstance().freeMemory(address)
      usedSoFar.compareAndSet(currentSize, currentSize - size)
      val removed = sizeMapping.remove(address)
    } else {
      val msg = s"Address $address was not allocated by this memory manager"
      throw new IllegalArgumentException(msg)
    }
  }

  protected[memory] def freeAll(): Unit = {
    sizeMapping.entrySet().asScala.foreach(entry => if (entry.getValue > 0) {
      MemoryIO.getCheckedInstance().freeMemory(entry.getKey)
    })
    sizeMapping.clear()
  }

  def shutdown(): Unit = {
    freeAll()
  }
}

/**
 * An on-heap MemFactory implemented by creating byte[]
 */
class ArrayBackedMemFactory extends MemFactory {
  /**
    * Allocates memory for requested size.
    *
    * @param size Request memory allocation size in bytes
    * @return Memory which has a base, offset and a length
    */
  override def allocateWithMagicHeader(size: Int): (Any, Long, Int) = {
    val newBytes = new Array[Byte](size + 4)
    UnsafeUtils.setInt(newBytes, UnsafeUtils.arayOffset, BinaryVector.HeaderMagic)
    (newBytes, UnsafeUtils.arayOffset + 4, size)
  }

  override def copyFromBytes(bytes: Array[Byte]): ByteBuffer = {
    val newBytes = new Array[Byte](bytes.length)
    System.arraycopy(bytes, 0, newBytes, 0, bytes.length)
    ByteBuffer.wrap(newBytes)
  }

  // Nothing to free, let heap GC take care of it  :)
  override def freeMemory(address: Long): Unit = {}
  def shutdown(): Unit = {}
}


/**
  * A MemFactory that allocates memory from Blocks obtained from the BlockManager. It
  * maintains a reference to a currentBlock which is replaced when it is full
  *
  * @param blockStore The BlockManager which is used to request more blocks when the current
  *                   block is full.
  * @param markFullBlocksAsReclaimable Immediately mark and fully used block as reclaimable.
  *                                    Typically true during on-demand paging of optimized chunks from persistent store
  */
class BlockMemFactory(blockStore: BlockManager, reclaimOrder: Option[Int],
                      markFullBlocksAsReclaimable: Boolean = false) extends MemFactory with StrictLogging {

  // tracks fully populated blocks not marked reclaimable yet (typically waiting for flush)
  val fullBlocks = ListBuffer[Block]()

  // tracks block currently being populated
  val currentBlock = new AtomicReference[Block]()

  // tracks blocks that belong to same partition and share same reclaim listener
  private val partitionGroup: ListBuffer[Block] = ListBuffer[Block]()

  currentBlock.set(blockStore.requestBlock(reclaimOrder).get)

  /**
    * Starts tracking the block references for a partition.
    * This is to aid in adding listeners to the block for the partition when
    * the block gets reclaimed
    */
  def startPartition(): Unit = {
    partitionGroup += (currentBlock.get())
  }

  /**
    * Stops tracking the blocks that a single partition is written to.
    * And registers the listener to the blocks to which this partition is written to.
    * @param reclaimListener the listener to register
    */
  def endPartition(reclaimListener: ReclaimListener): Unit = {
    partitionGroup.foreach(_.registerListener(reclaimListener))
    partitionGroup.clear()
  }

  def markUsedBlocksReclaimable(): Unit = {
    fullBlocks.foreach(_.markReclaimable())
    fullBlocks.clear()
  }

  protected def ensureCapacity(forSize: Long): Block = {
    if (!currentBlock.get().hasCapacity(forSize)) {
      if (markFullBlocksAsReclaimable) {
        currentBlock.get().markReclaimable()
      }
      fullBlocks += currentBlock.get()
      currentBlock.set(blockStore.requestBlock(reclaimOrder).get)
    }
    currentBlock.get()
  }

  /**
    * Allocates memory for requested size.
    *
    * @param allocateSize Request memory allocation size in bytes
    * @return Memory which has a base, offset and a length
    */
  override def allocateWithMagicHeader(allocateSize: Int): Memory = {
    //4 for magic header
    val size = allocateSize + 4
    val block = ensureCapacity(size)
    block.own()
    val preAllocationPosition = block.position()
    val headerAddress = block.address + preAllocationPosition
    UnsafeUtils.setInt(UnsafeUtils.ZeroPointer, headerAddress, BinaryVector.HeaderMagic)
    val postAllocationPosition = preAllocationPosition + 4 + allocateSize
    block.position(postAllocationPosition)
    (UnsafeUtils.ZeroPointer, headerAddress + 4, allocateSize)
  }

  /**
    * Chunks from recovered partitions are copied here.
    * Unlike in a flush these will happen in a multi-threaded fashion.
    * A block is  just a buffer of memory - so it cannot be concurrently used.
    * We need to lock before doing the copy. A small price to pay so that flush allocation
    * is seamless and we don't need to compact to de-fragment.
    * @param bytes The bytes to be copied
    * @return The memory to which the bytes have been copied to
    */
  override def copyFromBytes(bytes: Array[Byte]): ByteBuffer = {
    val allocateSize = bytes.length
    val block = ensureCapacity(allocateSize)
    val preAllocationPosition = block.position()
    val address = block.address + preAllocationPosition
    val byteBuffer = UnsafeUtils.asDirectBuffer(address, allocateSize)
    block.own()
    byteBuffer.put(bytes)
    val postAllocationPosition = preAllocationPosition + allocateSize
    block.position(postAllocationPosition)
    byteBuffer.flip()
    byteBuffer
  }

  /**
    * Frees memory allocated at the passed address
    *
    * @param address The native address which represents the starting location of memory allocated
    */
  override def freeMemory(address: Long): Unit = {
    throw new UnsupportedOperationException
  }

  /**
    * @return The capacity of any allocated block
    */
  def blockAllocationSize(): Long = currentBlock.get().capacity

  // We don't free memory, because many BlockHolders will share a single BlockManager, and we rely on
  // the BlockManager's own shutdown mechanism
  def shutdown(): Unit = {}
}


