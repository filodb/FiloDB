package filodb.memory.format.vectors

import java.nio.ByteBuffer

import com.typesafe.scalalogging.StrictLogging
import debox.Buffer
import org.agrona.{DirectBuffer, ExpandableArrayBuffer, MutableDirectBuffer}
import org.agrona.concurrent.UnsafeBuffer
import scalaxy.loops._

import filodb.memory.{BinaryRegion, MemFactory}
import filodb.memory.format._
import filodb.memory.format.BinaryVector.BinaryVectorPtr

/**
 * BinaryHistogram is the binary format for a histogram binary blob included in BinaryRecords and sent over the wire.
 * It fits the BinaryRegionMedium protocol.
 * Format:
 *   +0000  u16  2-byte total length of this BinaryHistogram (excluding this length)
 *   +0002  u8   1-byte combined histogram buckets and values format code
 *                  0x00   Empty/null histogram
 *                  0x03   geometric   + NibblePacked delta Long values
 *                  0x04   geometric_1 + NibblePacked delta Long values  (see [[HistogramBuckets]])
 *
 *   +0003  u16  2-byte length of Histogram bucket definition
 *   +0005  [u8] Histogram bucket definition, see [[HistogramBuckets]]
 *                  First two bytes of definition is always the number of buckets, a u16
 *   +(5+n) remaining values according to format above
 *
 *  NOTE: most of the methods below actually expect a pointer to the +2 hist bucket definition, not the length field
 */
object BinaryHistogram extends StrictLogging {
  // Pass in a buffer which includes the length bytes.  Value class - no allocations.
  case class BinHistogram(buf: DirectBuffer) extends AnyVal {
    def totalLength: Int = buf.getShort(0).toInt + 2
    def numBuckets: Int = buf.getShort(5).toInt
    def formatCode: Byte = buf.getByte(2)
    def bucketDefNumBytes: Int = buf.getShort(3).toInt
    def bucketDefOffset: Long = buf.addressOffset + 5
    def valuesIndex: Int = 2 + 3 + bucketDefNumBytes     // pointer to values bytes
    def valuesNumBytes: Int = totalLength - valuesIndex
    def valuesByteSlice: DirectBuffer = {
      UnsafeUtils.wrapDirectBuf(buf.byteArray, buf.addressOffset + valuesIndex, valuesNumBytes, valuesBuf)
      valuesBuf
    }
    override def toString: String = s"<BinHistogram: ${toHistogram}>"

    /**
     * Converts this BinHistogram to a Histogram object.  May not be the most efficient.
     * Intended for slower paths such as high level (lower # samples) aggregation and HTTP/CLI materialization
     * by clients.  Materializes/deserializes everything.
     * Ingestion ingests BinHistograms directly without conversion to Histogram first.
     */
    def toHistogram: Histogram = formatCode match {
      case HistFormat_Geometric_Delta =>
        val bucketDef = HistogramBuckets.geometric(buf.byteArray, bucketDefOffset, false)
        LongHistogram.fromPacked(bucketDef, valuesByteSlice).getOrElse(Histogram.empty)
      case HistFormat_Geometric1_Delta =>
        val bucketDef = HistogramBuckets.geometric(buf.byteArray, bucketDefOffset, true)
        LongHistogram.fromPacked(bucketDef, valuesByteSlice).getOrElse(Histogram.empty)
      case x =>
        logger.debug(s"Unrecognizable histogram format code $x, returning empty histogram")
        Histogram.empty
    }
  }

  // Thread local buffer used as read-only byte slice
  private val tlValuesBuf = new ThreadLocal[DirectBuffer]()
  def valuesBuf: DirectBuffer = tlValuesBuf.get match {
    case UnsafeUtils.ZeroPointer => val buf = new UnsafeBuffer(Array.empty[Byte])
                                    tlValuesBuf.set(buf)
                                    buf
    case b: DirectBuffer         => b
  }

  // Thread local buffer used as temp buffer for writing binary histograms
  private val tlHistBuf = new ThreadLocal[MutableDirectBuffer]()
  def histBuf: MutableDirectBuffer = tlHistBuf.get match {
    case UnsafeUtils.ZeroPointer => val buf = new ExpandableArrayBuffer(4096)
                                    tlHistBuf.set(buf)
                                    buf
    case b: MutableDirectBuffer         => b
  }

  val HistFormat_Null = 0x00.toByte
  val HistFormat_Geometric_Delta = 0x03.toByte
  val HistFormat_Geometric1_Delta = 0x04.toByte

  /**
   * Writes binary histogram with geometric bucket definition and data which is non-increasing, but will be
   * decoded as increasing.  Intended only for specific use cases when the source histogram are non increasing
   * buckets, ie each bucket has a count that is independent.
   * @param buf the buffer to write the histogram to.  Highly recommended this be an ExpandableArrayBuffer or equiv.
   *            so it can grow.
   * @return the number of bytes written, including the length prefix
   */
  def writeNonIncreasing(buckets: GeometricBuckets, values: Array[Long], buf: MutableDirectBuffer): Int = {
    require(buckets.numBuckets == values.size, s"Values array size of ${values.size} != ${buckets.numBuckets}")
    val formatCode = if (buckets.minusOne) HistFormat_Geometric1_Delta else HistFormat_Geometric_Delta

    buf.putByte(2, formatCode)
    val valuesIndex = buckets.serialize(buf, 3)
    val finalPos = NibblePack.packNonIncreasing(values, buf, valuesIndex)

    require(finalPos <= 65535, s"Histogram data is too large: $finalPos bytes needed")
    buf.putShort(0, (finalPos - 2).toShort)
    finalPos
  }

  def writeDelta(buckets: GeometricBuckets, values: Array[Long]): Int =
    writeNonIncreasing(buckets, values, histBuf)

  /**
   * Encodes binary histogram with geometric bucket definition and data which is strictly increasing and positive.
   * All histograms after ingestion are expected to be increasing.
   * Delta encoding is applied for compression.
   * @param buf the buffer to write the histogram to.  Highly recommended this be an ExpandableArrayBuffer or equiv.
   *            so it can grow.
   * @return the number of bytes written, including the length prefix
   */
  def writeDelta(buckets: GeometricBuckets, values: Array[Long], buf: MutableDirectBuffer): Int = {
    require(buckets.numBuckets == values.size, s"Values array size of ${values.size} != ${buckets.numBuckets}")
    val formatCode = if (buckets.minusOne) HistFormat_Geometric1_Delta else HistFormat_Geometric_Delta

    buf.putByte(2, formatCode)
    val valuesIndex = buckets.serialize(buf, 3)
    val finalPos = NibblePack.packDelta(values, buf, valuesIndex)

    require(finalPos <= 65535, s"Histogram data is too large: $finalPos bytes needed")
    buf.putShort(0, (finalPos - 2).toShort)
    finalPos
  }
}

object HistogramVector {
  type HistIterator = Iterator[Histogram] with TypedIterator

  val OffsetNumHistograms = 6
  val OffsetFormatCode = 8     // u8: BinHistogram format code/bucket type
  val OffsetBucketDefSize = 9  // # of bytes of bucket definition, including bucket def type
  val OffsetBucketDef  = 11    // Start of bucket definition
  val OffsetNumBuckets = 11
  // After the bucket area are regions for storing the counter values or pointers to them

  final def getNumBuckets(addr: Ptr.U8): Int = addr.add(OffsetNumBuckets).asU16.getU16

  final def getNumHistograms(addr: Ptr.U8): Int = addr.add(OffsetNumHistograms).asU16.getU16
  final def resetNumHistograms(addr: Ptr.U8): Unit = addr.add(OffsetNumHistograms).asU16.asMut.set(0)
  final def incrNumHistograms(addr: Ptr.U8): Unit =
    addr.add(OffsetNumHistograms).asU16.asMut.set(getNumHistograms(addr) + 1)

  // Note: the format code defines bucket definition format + format of each individual compressed histogram
  final def formatCode(addr: Ptr.U8): Byte = addr.add(OffsetFormatCode).getU8.toByte
  final def afterBucketDefAddr(addr: Ptr.U8): Ptr.U8 = addr + OffsetBucketDef + bucketDefNumBytes(addr)
  final def bucketDefNumBytes(addr: Ptr.U8): Int = addr.add(OffsetBucketDefSize).asU16.getU16
  final def bucketDefAddr(addr: Ptr.U8): Ptr.U8 = addr + OffsetBucketDef

  // Matches the bucket definition whose # bytes is at (base, offset)
  final def matchBucketDef(hist: BinaryHistogram.BinHistogram, addr: Ptr.U8): Boolean =
    (hist.formatCode == formatCode(addr)) &&
    (hist.bucketDefNumBytes == bucketDefNumBytes(addr)) && {
      UnsafeUtils.equate(UnsafeUtils.ZeroPointer, bucketDefAddr(addr).addr, hist.buf.byteArray, hist.bucketDefOffset,
                         hist.bucketDefNumBytes)
    }

  def appending(factory: MemFactory, maxBytes: Int): AppendableHistogramVector = {
    val addr = factory.allocateOffheap(maxBytes)
    new AppendableHistogramVector(factory, Ptr.U8(addr), maxBytes)
  }

  def apply(buffer: ByteBuffer): HistogramReader = apply(UnsafeUtils.addressFromDirectBuffer(buffer))

  import WireFormat._

  def apply(p: BinaryVectorPtr): HistogramReader = BinaryVector.vectorType(p) match {
    case x if x == WireFormat(VECTORTYPE_HISTOGRAM, SUBTYPE_H_SIMPLE) => new RowHistogramReader(Ptr.U8(p))
  }
}

/**
 * A HistogramVector appender storing compressed histogram values for less storage space.
 * This is a Section-based vector - sections of up to 64 histograms are stored at a time.
 * It stores histograms up to a maximum allowed size (since histograms are variable length)
 * Note that the bucket schema is not set until getting the first item.
 *
 * Read/Write/Lock semantics: everything is gated by the number of elements.
 * When it is 0, nothing is initialized so the reader guards against that.
 * When it is > 0, then all structures are initialized.
 */
class AppendableHistogramVector(factory: MemFactory,
                                vectPtr: Ptr.U8,
                                val maxBytes: Int) extends BinaryAppendableVector[DirectBuffer] with SectionWriter {
  import HistogramVector._
  import BinaryHistogram._

  // Initialize header
  BinaryVector.writeMajorAndSubType(addr, WireFormat.VECTORTYPE_HISTOGRAM,
                                          WireFormat.SUBTYPE_H_SIMPLE)
  reset()

  final def addr: BinaryVectorPtr = vectPtr.addr
  final def maxElementsPerSection: Int = 64

  val dispose = () => {
    // free our own memory
    factory.freeMemory(addr)
  }

  final def numBytes: Int = vectPtr.asI32.getI32 + 4
  final def length: Int = getNumHistograms(vectPtr)
  final def isAvailable(index: Int): Boolean = true
  final def isAllNA: Boolean = (length == 0)
  final def noNAs: Boolean = (length > 0)

  private def setNumBytes(len: Int): Unit = {
    require(len >= 0)
    vectPtr.asI32.asMut.set(len)
  }

  // NOTE: to eliminate allocations, re-use the DirectBuffer and keep passing the same instance to addData
  final def addData(buf: DirectBuffer): AddResponse = {
    val numItems = getNumHistograms(vectPtr)
    val h = BinHistogram(buf)
    val numBuckets = h.numBuckets
    if (numItems == 0) {
      // Copy the bucket definition and set the bucket def size
      UnsafeUtils.unsafe.copyMemory(buf.byteArray, h.bucketDefOffset,
                                    UnsafeUtils.ZeroPointer, bucketDefAddr(vectPtr).addr, h.bucketDefNumBytes)
      UnsafeUtils.setShort(addr + OffsetBucketDefSize, h.bucketDefNumBytes.toShort)
      UnsafeUtils.setByte(addr + OffsetFormatCode, h.formatCode)

      // Initialize the first section
      val firstSectPtr = afterBucketDefAddr(vectPtr)
      initSectionWriter(firstSectPtr, ((vectPtr + maxBytes).addr - firstSectPtr.addr).toInt)
    } else {
      // check the bucket schema is identical.  If not, return BucketSchemaMismatch
      if (!matchBucketDef(h, vectPtr)) return BucketSchemaMismatch
    }

    val res = appendBlob(buf.byteArray, buf.addressOffset + h.valuesIndex, h.valuesNumBytes)
    if (res == Ack) {
      // set new number of bytes first. Remember to exclude initial 4 byte length prefix
      setNumBytes(maxBytes - bytesLeft - 4)
      // Finally, increase # histograms which is the ultimate safe gate for access by readers
      incrNumHistograms(vectPtr)
    }
    res
  }

  final def addNA(): AddResponse = Ack  // TODO: Add a 0 to every appender

  def addFromReaderNoNA(reader: RowReader, col: Int): AddResponse = addData(reader.blobAsBuffer(col))
  def copyToBuffer: Buffer[DirectBuffer] = ???
  def apply(index: Int): DirectBuffer = ???

  def finishCompaction(newAddress: BinaryRegion.NativePointer): BinaryVectorPtr = newAddress

  // NOTE: do not access reader below unless this vect is nonempty.  TODO: fix this, or don't if we don't use this class
  lazy val reader: VectorDataReader = new RowHistogramReader(vectPtr)

  def reset(): Unit = {
    resetNumHistograms(vectPtr)
    setNumBytes(OffsetNumBuckets + 2)
  }

  // We don't optimize -- for now.  Histograms are already stored compressed.
  // In future, play with other optimization strategies, such as delta encoding.
}

trait HistogramReader extends VectorDataReader {
  def buckets: HistogramBuckets
  def apply(index: Int): Histogram
  def sum(start: Int, end: Int): MutableHistogram
}

/**
 * A reader for row-based Histogram vectors.  Mostly contains logic to skip around the vector to find the right
 * record pointer.
 */
class RowHistogramReader(histVect: Ptr.U8) extends HistogramReader {
  import HistogramVector._

  final def length: Int = getNumHistograms(histVect)
  val numBuckets = if (length > 0) getNumBuckets(histVect) else 0
  var curSection: Section = _
  var curElemNo = 0
  var sectStartingElemNo = 0
  var curHist: Ptr.U8 = _
  if (length > 0) setFirstSection()

  val buckets = HistogramBuckets(bucketDefAddr(histVect).add(-2), formatCode(histVect))
  val returnHist = LongHistogram(buckets, new Array[Long](buckets.numBuckets))
  val endAddr = histVect + histVect.asI32.getI32 + 4

  private def setFirstSection(): Unit = {
    curSection = Section.fromPtr(afterBucketDefAddr(histVect))
    curHist = curSection.firstElem
    curElemNo = 0
    sectStartingElemNo = 0
  }

  // Assume that most read patterns move the "cursor" or element # forward.  Since we track the current section
  // moving forward or jumping to next section is easy.  Jumping backwards within current section is not too bad -
  // we restart at beg of current section.  Going back before current section is expensive, then we start over.
  // TODO: split this out into a SectionReader trait
  private def locate(elemNo: Int): Ptr.U8 = {
    require(elemNo >= 0 && elemNo < length, s"$elemNo is out of vector bounds [0, $length)")
    if (elemNo == curElemNo) {
      curHist
    } else if (elemNo > curElemNo) {
      // Jump forward to next section until we are in section containing elemNo.  BUT, don't jump beyond cur length
      while (elemNo >= (sectStartingElemNo + curSection.numElements) && curSection.endAddr.addr < endAddr.addr) {
        curElemNo = sectStartingElemNo + curSection.numElements
        curSection = Section.fromPtr(curSection.endAddr)
        sectStartingElemNo = curElemNo
        curHist = curSection.firstElem
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
        setFirstSection()
      }
      locate(elemNo)
    }
  }

  // Skips ahead numElems elements starting at startPtr and returns the new pointer.  NOTE: numElems might be 0.
  private def skipAhead(startPtr: Ptr.U8, numElems: Int): Ptr.U8 = {
    require(numElems >= 0)
    var togo = numElems
    var ptr = startPtr
    while (togo > 0) {
      ptr += ptr.asU16.getU16 + 2
      togo -= 1
    }
    ptr
  }

  /**
   * Iterates through each histogram. Note this is expensive due to materializing the Histogram object
   * every time.  Using higher level functions such as sum is going to be a much better bet usually.
   */
  def iterate(vector: BinaryVectorPtr, startElement: Int): TypedIterator =
  new Iterator[Histogram] with TypedIterator {
    var elem = startElement
    def hasNext: Boolean = elem < getNumHistograms(histVect)
    def next: Histogram = {
      val h = apply(elem)
      elem += 1
      h
    }
  }

  def length(addr: BinaryVectorPtr): Int = length

  // WARNING: histogram returned is shared between calls, do not reuse!
  final def apply(index: Int): Histogram = {
    require(length > 0)
    val histPtr = locate(index)
    val histLen = histPtr.asU16.getU16
    val buf = BinaryHistogram.valuesBuf
    buf.wrap(histPtr.add(2).addr, histLen)
    NibblePack.unpackToSink(buf, NibblePack.DeltaSink(returnHist.values), numBuckets)
    returnHist
  }

  // sum_over_time returning a Histogram with sums for each bucket.  Start and end are inclusive row numbers
  // NOTE: for now this is just a dumb implementation that decompresses each histogram fully
  final def sum(start: Int, end: Int): MutableHistogram = {
    require(length > 0 && start >= 0 && end < length)
    val summedHist = MutableHistogram.empty(buckets)
    for { i <- start to end optimized } {
      summedHist.add(apply(i).asInstanceOf[HistogramWithBuckets])
    }
    summedHist
  }
}