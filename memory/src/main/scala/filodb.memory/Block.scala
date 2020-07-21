package filodb.memory

import java.nio.ByteBuffer

import com.typesafe.scalalogging.StrictLogging

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
trait ReusableMemory {
  @volatile protected var _isReusable: Boolean = false

  def reclaimListener: ReclaimListener

  /**
    * @return Whether this block can be reclaimed. The original owning callsite has to
    *         set this as being reusable
    */
  def canReclaim: Boolean = _isReusable

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
    _isReusable = false
  }

  /**
    * Marks this memory as reclaimable.
    */
  def markReclaimable(): Unit = {
    _isReusable = true
  }

  /**
   * Calls the reclaimListener for each piece of metadata that we own
   */
  protected def reclaimWithMetadata(): Unit

  /**
    * Marks this memory as free and calls reclaimListener for every piece of metadata.
    */
  protected def free() = {
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

object Block extends StrictLogging {
  val _log = logger
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
class Block(val address: Long, val capacity: Long, val reclaimListener: ReclaimListener) extends ReusableMemory {
  import format.UnsafeUtils

  protected var _position: Int = 0
  protected var _metaPosition: Int = capacity.toInt

  /**
   * Keeps track of which BlockMemFactory "owns" this block.  Set when block is requested by a BMF,
   * cleared when the block is reclaimed.
   */
  var owner: Option[BlockMemFactory] = None

  def setOwner(bmf: BlockMemFactory): Unit = {
    owner = bmf.optionSelf
  }

  def clearOwner(): Unit = {
    owner = None
  }

  /**
   * Marks this block as reclaimable if unowned, or if the owner hasn't used the block in a while.
   */
  def tryMarkReclaimable(): Unit = {
    owner match {
      case None => markReclaimable
      case Some(bmf) => bmf.tryMarkReclaimable
    }
  }

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
    _position = newPosition
  }

  def remaining(): Int = _metaPosition - _position

  /**
   * Allocates metaSize bytes for metadata storage.
   * @param metaSize the number of bytes to use for metadata storage.
   * @return the Long address of the metadata space.  The metaSize is written to the location 2 bytes before this.
   *         If there is no capacity then OutOfOffheapMemoryException is thrown.
   */
  def allocMetadata(metaSize: Short): Long = {
    val rem = remaining()
    if (metaSize > 0 && metaSize <= (rem - 2)) {
      _metaPosition -= (metaSize + 2)
      val metaAddr = address + _metaPosition
      UnsafeUtils.setShort(UnsafeUtils.ZeroPointer, metaAddr, metaSize)
      metaAddr + 2
    } else {
      Block._log.error(s"Unexpected ERROR with allocMetadata.  Block info: $detailedDebugString")
      throw new OutOfOffheapMemoryException(metaSize, rem)
    }
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

  def debugString: String = f"Block @0x$address%016x canReclaim=$canReclaim remaining=$remaining " +
                            s"owner: ${owner.map(_.debugString).getOrElse("--")}"

  // Include detailed metadata debug info, enough to debug any block metadata allocation issues
  // Meta alloc overhead, plus histogram and stats on meta allocation, plus pointer info
  def detailedDebugString: String = {
    val metasizeHist = collection.mutable.HashMap[Int, Int]().withDefaultValue(0)
    var numMetas = 0

    // Walk metadata blocks and collect stats
    var metaPointer = address + _metaPosition
    while (metaPointer < (address + capacity)) {
      val metaSize = UnsafeUtils.getShort(metaPointer)
      numMetas += 1
      metasizeHist(metaSize) += 1
      metaPointer += (2 + metaSize)
    }

    debugString +
      f"\ncapacity=$capacity position=${_position}  metaPos=${_metaPosition}  gap=${_metaPosition - _position}\n" +
      s"# metadatas=$numMetas  metadata size histogram=$metasizeHist"
  }

  // debug method to set memory to specific value for testing
  private[memory] def set(value: Byte): Unit =
    UnsafeUtils.unsafe.setMemory(address, capacity, value)
}
