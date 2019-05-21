package filodb.memory.format

/**
 * A Section is a subdivision of a BinaryVector, typically used for variable-length data.  It contains a
 * fixed header with length and # elements, which lets cursors skip quickly over sections.
 *
 * The design of the data is such that a single writer updates fields but multiple readers can read.
 * Thus we have to be careful about the order in which updates are written.
 * Basically, we want the data in all fields to be consistent.
 */
final case class Section private(addr: Long) extends AnyVal {
  // not including length bytes
  final def sectionNumBytes: Int = Ptr.U16(addr).getU16

  final def numElements: Int = Ptr.U8(addr).add(2).getU8

  // Ptr to first record of section
  final def firstElem: Ptr.U8 = Ptr.U8(addr) + 4

  // The address at the end of this section's elements, based on current num bytes
  final def endAddr: Ptr.U8 = Ptr.U8(addr).add(4).add(sectionNumBytes)

  final def isComplete: Boolean = numElements > 0

  /**
   * Updates the number of bytes and elements atomically.
   * Checks that new values make sense.
   */
  final def update(addedBytes: Int, addedElements: Int): Unit = {
    require(addedBytes > 0 && addedElements > 0)
    val newNumElements = numElements + addedElements
    val newNumBytes = sectionNumBytes + addedBytes
    require(newNumElements <= 255 && newNumBytes <= 65535)
    Ptr.I32(addr).asMut.set(newNumBytes | (newNumElements << 16))
  }

  final def setNumElements(num: Int): Unit = {
    require(num >= 0 && num <= 255)
    Ptr.U8(addr).add(2).asMut.set(num)
  }
}

object Section {
  def fromPtr(addr: Ptr.U8): Section = Section(addr.addr)

  def init(sectionAddr: Ptr.U8): Section = {
    val newSect = Section(sectionAddr.addr)
    newSect.setNumElements(0)
    sectionAddr.asU16.asMut.set(0)
    newSect
  }
}

/**
 * A writer which manages sections as blobs and elements are added to it, rolling over to a new section as needed.
 */
trait SectionWriter {
  // Max # of elements per section, should be no more than 255.  Usually 64?
  def maxElementsPerSection: Int

  // Call to initialize the section writer with the address of the first section and how many bytes left
  def initSectionWriter(firstSectionAddr: Ptr.U8, remainingBytes: Int): Unit = {
    curSection = Section.init(firstSectionAddr)
    bytesLeft = remainingBytes - 4    // account for initial section header bytes
  }

  var curSection: Section = _
  var bytesLeft: Int = 0

  // Returns true if appending numBytes will start a new section
  protected def needNewSection(numBytes: Int): Boolean = {
    // Check remaining length/space.  A section must be less than 2^16 bytes long. Create new section if needed
    val newNumBytes = curSection.sectionNumBytes + numBytes
    curSection.numElements >= maxElementsPerSection || newNumBytes >= 65536
  }

  // Appends a blob, writing a 2-byte length prefix before it.
  protected def appendBlob(base: Any, offset: Long, numBytes: Int): AddResponse = {
    if (needNewSection(numBytes)) {
      if (bytesLeft >= (4 + numBytes)) {
        curSection = Section.init(curSection.endAddr)
        bytesLeft -= 4
      } else return VectorTooSmall(4 + numBytes, bytesLeft)
    }

    // Copy bytes to end address, update variables
    if (bytesLeft >= (numBytes + 2)) {
      val writeAddr = curSection.endAddr
      writeAddr.asU16.asMut.set(numBytes)
      UnsafeUtils.unsafe.copyMemory(base, offset, UnsafeUtils.ZeroPointer, (writeAddr + 2).addr, numBytes)
      bytesLeft -= (numBytes + 2)
      curSection.update(numBytes + 2, 1)
      Ack
    } else VectorTooSmall(numBytes + 2, bytesLeft)
  }
}