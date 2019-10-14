package filodb.memory.format

// Value type for section type, must be a byte
// Use of this value class helps prevent type coercion errors from using just an Int
final private[format] case class SectionType(n: Int) extends AnyVal

/**
 * A Section is a subdivision of a BinaryVector, typically used for variable-length data.  It contains a
 * fixed header with length and # elements, which lets cursors skip quickly over sections.
 *
 * The design of the data is such that a single writer updates fields but multiple readers can read.
 * Thus we have to be careful about the order in which updates are written.
 * Basically, we want the data in all fields to be consistent.
 *
 * Offset
 * +0     u16  Number of bytes in this section, following 4-byte section header
 * +2     u8   Number of elements (max 255)
 * +3     u8   Type of section
 *                0: Normal section
 *                1: Drop (for increasing counters)
 */
final case class Section private(addr: Long) extends AnyVal {
  // not including length bytes
  final def sectionNumBytes(base: Any): Int = Ptr.U16(addr).getU16(base)

  final def numElements(base: Any): Int = Ptr.U8(addr).add(2).getU8(base)

  final def sectionType(base: Any): SectionType = SectionType(Ptr.U8(addr).add(3).getU8(base))

  // Ptr to first record of section
  final def firstElem: Ptr.U8 = Ptr.U8(addr) + 4

  // The address at the end of this section's elements, based on current num bytes
  final def endAddr(base: Any): Ptr.U8 = Ptr.U8(addr).add(4).add(sectionNumBytes(base))

  final def isComplete(base: Any): Boolean = numElements(base) > 0

  /**
   * Updates the number of bytes and elements atomically.
   * Checks that new values make sense.
   */
  final def update(base: Any, addedBytes: Int, addedElements: Int): Unit = {
    require(addedBytes > 0 && addedElements > 0)
    val newNumElements = numElements(base) + addedElements
    val newNumBytes = sectionNumBytes(base) + addedBytes
    require(newNumElements <= 255 && newNumBytes <= 65535)
    Ptr.I32(addr).asMut.set(base, newNumBytes | (newNumElements << 16) | (sectionType(base).n << 24))
  }

  final def setNumElements(base: Any, num: Int): Unit = {
    require(num >= 0 && num <= 255)
    Ptr.U8(addr).add(2).asMut.set(base, num)
  }

  final def setType(base: Any, typ: SectionType): Unit = {
    Ptr.U8(addr).add(3).asMut.set(base, typ.n)
  }

  def debugString(base: Any): String = s"Section@$addr: {numBytes=${sectionNumBytes(base)}, " +
    s"len=${numElements(base)}, type=${sectionType(base)}"
}

object Section {
  def fromPtr(addr: Ptr.U8): Section = Section(addr.addr)

  def init(base: Any, sectionAddr: Ptr.U8, typ: SectionType = TypeNormal): Section = {
    val newSect = Section(sectionAddr.addr)
    newSect.setNumElements(base, 0)
    newSect.setType(base, typ)
    sectionAddr.asU16.asMut.set(base, 0)
    newSect
  }

  val TypeNormal = SectionType(0)
  val TypeDrop   = SectionType(1)
}

/**
 * A writer which manages sections as blobs and elements are added to it, rolling over to a new section as needed.
 * Each element has a 2-byte length prefix.
 */
trait SectionWriter {
  // Max # of elements per section, should be no more than 255.  Usually 64?
  def maxElementsPerSection: IntU8

  // Call to initialize the section writer with the address of the first section and how many bytes left
  def initSectionWriter(firstSectionAddr: Ptr.U8, remainingBytes: Int): Unit = {
    curSection = Section.init(UnsafeUtils.ZP, firstSectionAddr)
    bytesLeft = remainingBytes - 4    // account for initial section header bytes
  }

  var curSection: Section = _
  var bytesLeft: Int = 0

  // Returns true if appending numBytes will start a new section
  protected def needNewSection(numBytes: Int): Boolean = {
    // Check remaining length/space.  A section must be less than 2^16 bytes long. Create new section if needed
    val newNumBytes = curSection.sectionNumBytes + numBytes
    curSection.numElements >= maxElementsPerSection.n || newNumBytes >= 65536
  }

  // Appends a blob, writing a 2-byte length prefix before it.
  protected def appendBlob(base: Any, offset: Long, numBytes: Int): AddResponse = {
    if (needNewSection(numBytes)) {
      if (bytesLeft >= (4 + numBytes)) {
        curSection = Section.init(curSection.endAddr)
        bytesLeft -= 4
      } else return VectorTooSmall(4 + numBytes, bytesLeft)
    }
    addBlobInner(base, offset, numBytes)
  }

  // Appends a blob, forcing creation of a new section too
  protected def newSectionWithBlob(base: Any, offset: Long, numBytes: Int, sectType: SectionType): AddResponse = {
    if (bytesLeft >= (4 + numBytes)) {
      curSection = Section.init(curSection.endAddr, sectType)
      bytesLeft -= 4
    } else return VectorTooSmall(4 + numBytes, bytesLeft)
    addBlobInner(base, offset, numBytes)
  }

  private def addBlobInner(base: Any, offset: Long, numBytes: Int): AddResponse =
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

/**
 * Methods to help navigate elements with 2-byte length prefixes within sections.
 */
trait SectionReader {
  var curSection: Section = _
  var curElemNo = -1
  var sectStartingElemNo = 0
  var curHist: Ptr.U8 = _

  // Number of elements in the vector
  def length: Int
  def firstSectionAddr: Ptr.U8

  // "End" address, the first address beyond the last byte of this vector
  def endAddr: Ptr.U8

  // Initializes internal state when reading from a new section.  Override to add anything else needed.
  protected def setSection(sectAddr: Ptr.U8, newElemNo: Int = 0): Unit = {
    curSection = Section.fromPtr(sectAddr)
    curHist = curSection.firstElem
    curElemNo = newElemNo
    sectStartingElemNo = newElemNo
  }

  protected def initialize(): Unit =
    if (curElemNo < 0) {   // first time initialization
      setSection(firstSectionAddr)
      curElemNo = 0
    }

  // Assume that most read patterns move the "cursor" or element # forward.  Since we track the current section
  // moving forward or jumping to next section is easy.  Jumping backwards within current section is not too bad -
  // we restart at beg of current section.  Going back before current section is expensive, then we start over.
  def locate(elemNo: Int): Ptr.U8 = {
    require(elemNo >= 0 && elemNo < length, s"$elemNo is out of vector bounds [0, $length)")
    initialize()
    if (elemNo == curElemNo) {
      curHist
    } else if (elemNo > curElemNo) {
      // Jump forward to next section until we are in section containing elemNo.  BUT, don't jump beyond cur length
      while (elemNo >= (sectStartingElemNo + curSection.numElements) && curSection.endAddr.addr < endAddr.addr) {
        setSection(curSection.endAddr(base), sectStartingElemNo + curSection.numElements)
      }

      curHist = skipAhead(curHist, elemNo - curElemNo)
      curElemNo = elemNo
      curHist
    } else {  // go backwards then go forwards
      // Is it still within current section?  If so restart search at beg of section
      if (elemNo >= sectStartingElemNo) {
        curElemNo = sectStartingElemNo
        curHist = curSection.firstElem
      } else {
        // Otherwise restart search at beginning
        setSection(firstSectionBase, firstSectionAddr)
      }
      locate(elemNo)
    }
  }

  // Skips ahead numElems elements starting at startPtr and returns the new pointer.  NOTE: numElems might be 0.
  def skipAhead(startPtr: Ptr.U8, numElems: Int): Ptr.U8 = {
    require(numElems >= 0)
    var togo = numElems
    var ptr = startPtr
    while (togo > 0) {
      // FIXME dont understand why we are creating new pointer with value
      ptr += ptr.asU16.getU16(base) + 2
      togo -= 1
    }
    ptr
  }

  def iterateSections: Iterator[Section] = new Iterator[Section] {
    var curSect = Section.fromPtr(firstSectionBase, firstSectionAddr)
    final def hasNext: Boolean = curSect.endAddr.addr <= endAddr.addr
    final def next: Section = {
      val sect = curSect
      curSect = Section.fromPtr(curSect.base, curSect.endAddr)
      sect
    }
  }

  def dumpAllSections: String = iterateSections.map(_.debugString).mkString("\n")
}