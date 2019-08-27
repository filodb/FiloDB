package filodb.memory

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.ListBuffer

import com.kenai.jffi.MemoryIO
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon

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
   * Allocates offheap memory and returns a native 64-bit pointer, throwing
   * OutOfOffheapMemoryException if no memory is available.
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

  /**
   * Call to update (and publish) stats associated with this factory. Implementation might do nothing.
   */
  def updateStats(): Unit = {}

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
  * Synchronized to be multi-thread safe -- for example, the OffheapSortedIDMap will cause concurrent free/allocates
  * TODO: we don't really need freeAll(), consider not needing the map and just storing the size of allocation in
  * first four bytes.  That in fact matches what is needed for BinaryVector and BinaryRecord allocations.
  * Have an allocateOffheapWithSizeHeader which just returns the address to the size bytes  :)
  * For now we still get millions of allocations/sec with synchronized
  *
  * @param tags Kamon tags used by updateStats method
  */
class NativeMemoryManager(val upperBoundSizeInBytes: Long, val tags: Map[String, String] = Map.empty)
    extends MemFactory {

  val statFree    = Kamon.gauge("memstore-writebuffer-bytes-free").refine(tags)
  val statUsed    = Kamon.gauge("memstore-writebuffer-bytes-used").refine(tags)
  val statEntries = Kamon.gauge("memstore-writebuffer-entries").refine(tags)

  private val sizeMapping = debox.Map.empty[Long, Int]
  @volatile private var usedSoFar = 0L

  def usedMemory: Long = usedSoFar

  def numFreeBytes: Long = upperBoundSizeInBytes - usedSoFar

  // Allocates a native 64-bit pointer, or throws an exception if not enough space
  def allocateOffheap(size: Int, zero: Boolean = true): BinaryRegion.NativePointer = {
    var currentSize = usedSoFar

    if (currentSize + size <= upperBoundSizeInBytes) {
      // Optimistically allocate without being synchronized.
      val address: Long = MemoryIO.getCheckedInstance().allocateMemory(size, zero)

      synchronized {
        currentSize = usedSoFar
        if (currentSize + size <= upperBoundSizeInBytes) {
          // Still within the upper bound, so all is good.
          usedSoFar = currentSize + size;
          sizeMapping(address) = size
          return address
        }
      }

      // Allocated too much due to optimistic failure, so free it.
      MemoryIO.getCheckedInstance().freeMemory(address)
    }

    throw OutOfOffheapMemoryException(size, upperBoundSizeInBytes - currentSize)
  }

  override def freeMemory(address: Long): Unit = {
    synchronized {
      val size = sizeMapping.getOrElse(address, -1)
      if (size < 0) {
        val msg = s"Address $address was not allocated by this memory manager"
        throw new IllegalArgumentException(msg)
      }
      sizeMapping.remove(address)
      usedSoFar -= size
    }

    MemoryIO.getCheckedInstance().freeMemory(address)
  }

  protected[memory] def freeAll(): Unit = synchronized {
    sizeMapping.foreach { case (addr, size) =>
      MemoryIO.getCheckedInstance().freeMemory(addr)
    }
    sizeMapping.clear()
    usedSoFar = 0
  }

  override def updateStats(): Unit = {
    val used = usedSoFar
    statUsed.set(used)
    statFree.set(upperBoundSizeInBytes - used)
    statEntries.set(entries)
  }

  private def entries = synchronized {
    sizeMapping.size
  }

  def shutdown(): Unit = {
    freeAll()
  }

  override def finalize(): Unit = shutdown
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
  * @param context a set of keys/values for debugging context
  * @param markFullBlocksAsReclaimable Immediately mark and fully used block as reclaimable.
  *                                    Typically true during on-demand paging of optimized chunks from persistent store
  */
class BlockMemFactory(blockStore: BlockManager,
                      bucketTime: Option[Long],
                      metadataAllocSize: Int,
                      var context: Map[String, String],
                      markFullBlocksAsReclaimable: Boolean = false) extends MemFactory with StrictLogging {
  def numFreeBytes: Long = blockStore.numFreeBlocks * blockStore.blockSizeInBytes
  val optionSelf = Some(this)

  // tracks fully populated blocks not marked reclaimable yet (typically waiting for flush)
  val fullBlocks = ListBuffer[Block]()

  // tracks block currently being populated
  val currentBlock = new AtomicReference[Block]()

  // tracks blocks that should share metadata
  private val metadataSpan: ListBuffer[Block] = ListBuffer[Block]()

  currentBlock.set(blockStore.requestBlock(bucketTime, optionSelf).get)

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
      val newBlock = blockStore.requestBlock(bucketTime, optionSelf).get
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


