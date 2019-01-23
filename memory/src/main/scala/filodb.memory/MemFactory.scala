package filodb.memory

import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.collection.mutable.ListBuffer

import com.kenai.jffi.MemoryIO
import com.typesafe.scalalogging.StrictLogging

import filodb.memory.BinaryRegion.Memory
import filodb.memory.format.UnsafeUtils

final case class OutOfOffheapMemoryException(needed: Long, have: Long) extends
  Exception(s"Out of offheap memory: Need $needed but only have $have bytes")

/**
  * A trait which allows allocation of memory with the Filo magic header
  */
trait MemFactory {
  /**
    * Allocates memory for requested size
    *
    * @param size Request memory allocation size in bytes
    * @return Memory which has a base, offset and a length
    */
  def allocate(size: Int): Memory =
    (UnsafeUtils.ZeroPointer, allocateOffheap(size), size)

  /**
   * Allocates offheap memory and returns a native 64-bit pointer
    * @param size Request memory allocation size in bytes
    * @param zero if true, zeroes out the contents of the memory first
   */
  def allocateOffheap(size: Int, zero: Boolean = false): BinaryRegion.NativePointer

  /**
    * Frees memory allocated at the passed address with allocate()
    *
    * @param address The native address which represents the starting location of memory allocated
    */
  def freeMemory(address: Long): Unit

  /**
   * Number of "free" bytes left at the moment available for allocation
   */
  def numFreeBytes: Long

  def fromBuffer(buf: ByteBuffer): Memory = {
    if (buf.hasArray) {
      (buf.array, UnsafeUtils.arayOffset.toLong + buf.arrayOffset + buf.position(), buf.limit() - buf.position())
    } else {
      assert(buf.isDirect)
      val address = MemoryIO.getCheckedInstance.getDirectBufferAddress(buf)
      (UnsafeUtils.ZeroPointer, address + buf.position(), buf.limit() - buf.position())
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
  * Synchronized to be multi-thread safe -- for example, the OffheapLFSortedIDMap will cause concurrent free/allocates
  * TODO: we don't really need freeAll(), consider not needing the map and just storing the size of allocation in
  * first four bytes.  That in fact matches what is needed for BinaryVector and BinaryRecord allocations.
  * Have an allocateOffheapWithSizeHeader which just returns the address to the size bytes  :)
  * For now we still get millions of allocations/sec with synchronized
  */
class NativeMemoryManager(val upperBoundSizeInBytes: Long) extends MemFactory {
  protected val usedSoFar = new AtomicLong(0)
  protected val sizeMapping = debox.Map.empty[Long, Int]

  def usedMemory: Long = usedSoFar.get()

  def availableDynMemory: Long = upperBoundSizeInBytes - usedSoFar.get()

  def numFreeBytes: Long = availableDynMemory

  // Allocates a native 64-bit pointer, or throws an exception if not enough space
  def allocateOffheap(size: Int, zero: Boolean = true): BinaryRegion.NativePointer = synchronized {
    val currentSize = usedSoFar.get()
    val resultantSize = currentSize + size
    if (!(resultantSize > upperBoundSizeInBytes)) {
      val address: Long = MemoryIO.getCheckedInstance().allocateMemory(size, zero)
      usedSoFar.compareAndSet(currentSize, currentSize + size)
      sizeMapping(address) = size
      address
    } else {
      throw OutOfOffheapMemoryException(size, availableDynMemory)
    }
  }

  override def freeMemory(startAddress: Long): Unit = synchronized {
    val address = startAddress
    val size = sizeMapping.getOrElse(address, -1)
    if (size >= 0) {
      val currentSize = usedSoFar.get()
      MemoryIO.getCheckedInstance().freeMemory(address)
      usedSoFar.compareAndSet(currentSize, currentSize - size)
      val removed = sizeMapping.remove(address)
    } else {
      val msg = s"Address $address was not allocated by this memory manager"
      throw new IllegalArgumentException(msg)
    }
  }

  protected[memory] def freeAll(): Unit = synchronized {
    sizeMapping.foreach { case (addr, size) =>
      MemoryIO.getCheckedInstance().freeMemory(addr)
    }
    sizeMapping.clear()
    usedSoFar.set(0)
  }

  def shutdown(): Unit = {
    freeAll()
  }
}

/**
 * An on-heap MemFactory implemented by creating byte[]
 */
class ArrayBackedMemFactory extends MemFactory {
  def numFreeBytes: Long = sys.runtime.freeMemory

  /**
    * Allocates memory for requested size.
    *
    * @param size Request memory allocation size in bytes
    * @return Memory which has a base, offset and a length
    */
  override def allocate(size: Int): Memory = {
    val newBytes = new Array[Byte](size)
    (newBytes, UnsafeUtils.arayOffset, size)
  }

  def allocateOffheap(size: Int, zero: Boolean = false): BinaryRegion.NativePointer =
    throw new UnsupportedOperationException

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
  * @param bucketTime the timebucket (timestamp) from which to allocate block(s), or None for the general list
  * @param metadataAllocSize the additional size in bytes to ensure is free for writing metadata, per chunk
  * @param markFullBlocksAsReclaimable Immediately mark and fully used block as reclaimable.
  *                                    Typically true during on-demand paging of optimized chunks from persistent store
  */
class BlockMemFactory(blockStore: BlockManager,
                      bucketTime: Option[Long],
                      metadataAllocSize: Int,
                      markFullBlocksAsReclaimable: Boolean = false) extends MemFactory with StrictLogging {
  def numFreeBytes: Long = blockStore.numFreeBlocks * blockStore.blockSizeInBytes

  // tracks fully populated blocks not marked reclaimable yet (typically waiting for flush)
  val fullBlocks = ListBuffer[Block]()

  // tracks block currently being populated
  val currentBlock = new AtomicReference[Block]()

  // tracks blocks that should share metadata
  private val metadataSpan: ListBuffer[Block] = ListBuffer[Block]()

  currentBlock.set(blockStore.requestBlock(bucketTime).get)

  /**
    * Starts tracking a span of multiple Blocks over which the same metadata should be applied.
    * An example would be chunk metadata for chunks written to potentially more than 1 block.
    */
  def startMetaSpan(): Unit = {
    metadataSpan.clear()
    metadataSpan += (currentBlock.get())
  }

  /**
    * Stops tracking the blocks that the same metadata should be applied to, and allocates and writes metadata
    * for those spanned blocks.
    * @param metadataWriter the function to write metadata to each block.  Param is the long metadata address.
    * @param metaSize the number of bytes the piece of metadata takes
    * @return the Long native address of the last metadata block written
    */
  def endMetaSpan(metadataWriter: Long => Unit, metaSize: Short): Long = {
    var metaAddr: Long = 0
    metadataSpan.foreach { blk =>
      // It is possible that the first block(s) did not have enough memory.  Don't write metadata to full blocks
      metaAddr = blk.allocMetadata(metaSize)
      if (metaAddr != 0) metadataWriter(metaAddr)
    }
    metaAddr
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
      val newBlock = blockStore.requestBlock(bucketTime).get
      currentBlock.set(newBlock)
      metadataSpan += newBlock
    }
    currentBlock.get()
  }

  /**
    * Allocates memory for requested size.
    * Also ensures that metadataAllocSize is available for metadata storage.
    *
    * @param allocateSize Request memory allocation size in bytes
    * @return Memory which has a base, offset and a length
    */
  def allocateOffheap(size: Int, zero: Boolean = false): BinaryRegion.NativePointer = {
    require(!zero, "BlockMemFactory cannot zero memory at allocation")
    val block = ensureCapacity(size + metadataAllocSize + 2)
    block.own()
    val preAllocationPosition = block.position()
    val newAddress = block.address + preAllocationPosition
    val postAllocationPosition = preAllocationPosition + size
    block.position(postAllocationPosition)
    newAddress
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


