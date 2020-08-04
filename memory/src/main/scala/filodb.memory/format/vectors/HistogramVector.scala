package filodb.memory.format.vectors

import java.nio.ByteBuffer

import com.typesafe.scalalogging.StrictLogging
import debox.Buffer
import org.agrona.{DirectBuffer, ExpandableArrayBuffer, MutableDirectBuffer}
import org.agrona.concurrent.UnsafeBuffer
import spire.syntax.cfor._

import filodb.memory.{BinaryRegion, MemFactory}
import filodb.memory.format._
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.MemoryReader._

/**
 * BinaryHistogram is the binary format for a histogram binary blob included in BinaryRecords and sent over the wire.
 * It fits the BinaryRegionMedium protocol.
 * Format:
 *   +0000  u16  2-byte total length of this BinaryHistogram (excluding this length)
 *   +0002  u8   1-byte combined histogram buckets and values format code
 *                  0x00   Empty/null histogram
 *                  0x03   geometric   + NibblePacked delta Long values
 *                  0x04   geometric_1 + NibblePacked delta Long values  (see [[HistogramBuckets]])
 *                  0x05   custom LE/bucket values + NibblePacked delta Long values
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

    def debugStr: String = s"totalLen=$totalLength numBuckets=$numBuckets formatCode=$formatCode " +
                           s"bucketDef=$bucketDefNumBytes bytes valuesIndex=$valuesIndex values=$valuesNumBytes bytes"

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
      case HistFormat_Custom_Delta =>
        val bucketDef = HistogramBuckets.custom(buf.byteArray, bucketDefOffset - 2)
        LongHistogram.fromPacked(bucketDef, valuesByteSlice).getOrElse(Histogram.empty)
      case HistFormat_Geometric_XOR =>
        val bucketDef = HistogramBuckets.geometric(buf.byteArray, bucketDefOffset, false)
        MutableHistogram.fromPacked(bucketDef, valuesByteSlice).getOrElse(Histogram.empty)
      case HistFormat_Custom_XOR =>
        val bucketDef = HistogramBuckets.custom(buf.byteArray, bucketDefOffset - 2)
        MutableHistogram.fromPacked(bucketDef, valuesByteSlice).getOrElse(Histogram.empty)
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

  val empty2DSink = NibblePack.DeltaDiffPackSink(Array[Long](), histBuf)
  val emptySectSink = UnsafeUtils.ZeroPointer.asInstanceOf[NibblePack.DeltaSectDiffPackSink]

  val HistFormat_Null = 0x00.toByte
  val HistFormat_Geometric_Delta = 0x03.toByte
  val HistFormat_Geometric1_Delta = 0x04.toByte
  val HistFormat_Custom_Delta = 0x05.toByte
  val HistFormat_Geometric_XOR = 0x08.toByte    // Double values XOR compressed
  val HistFormat_Custom_XOR = 0x0a.toByte

  def isValidFormatCode(code: Byte): Boolean =
    (code == HistFormat_Null) || (code == HistFormat_Geometric1_Delta) || (code == HistFormat_Geometric_Delta) ||
    (code == HistFormat_Custom_Delta)

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

  def writeDelta(buckets: HistogramBuckets, values: Array[Long]): Int =
    writeDelta(buckets, values, histBuf)

  /**
   * Encodes binary histogram with integral data which is strictly nondecreasing and positive.
   * All histograms after ingestion are expected to be increasing.
   * Delta encoding is applied for compression.
   * @param buf the buffer to write the histogram to.  Highly recommended this be an ExpandableArrayBuffer or equiv.
   *            so it can grow.
   * @return the number of bytes written, including the length prefix
   */
  def writeDelta(buckets: HistogramBuckets, values: Array[Long], buf: MutableDirectBuffer): Int = {
    require(buckets.numBuckets == values.size, s"Values array size of ${values.size} != ${buckets.numBuckets}")
    val formatCode = if (buckets.numBuckets == 0) HistFormat_Null else  buckets match {
      case g: GeometricBuckets if g.minusOne => HistFormat_Geometric1_Delta
      case g: GeometricBuckets               => HistFormat_Geometric_Delta
      case c: CustomBuckets                  => HistFormat_Custom_Delta
    }

    buf.putByte(2, formatCode)
    val finalPos = if (formatCode == HistFormat_Null) { 3 }
                   else {
                     val valuesIndex = buckets.serialize(buf, 3)
                     NibblePack.packDelta(values, buf, valuesIndex)
                   }
    require(finalPos <= 65535, s"Histogram data is too large: $finalPos bytes needed")
    buf.putShort(0, (finalPos - 2).toShort)
    finalPos
  }

  /**
   * Encodes binary histogram with double data, XOR compressed with NibblePack.
   * @param buf the buffer to write the histogram to.  Highly recommended this be an ExpandableArrayBuffer or equiv.
   *            so it can grow.
   * @return the number of bytes written, including the length prefix
   */
  def writeDoubles(buckets: HistogramBuckets, values: Array[Double], buf: MutableDirectBuffer): Int = {
    require(buckets.numBuckets == values.size, s"Values array size of ${values.size} != ${buckets.numBuckets}")
    val formatCode = if (buckets.numBuckets == 0) HistFormat_Null else buckets match {
      case g: GeometricBuckets               => HistFormat_Geometric_XOR
      case c: CustomBuckets                  => HistFormat_Custom_XOR
    }

    buf.putByte(2, formatCode)
    val finalPos = if (formatCode == HistFormat_Null) { 3 }
                   else {
                     val valuesIndex = buckets.serialize(buf, 3)
                     NibblePack.packDoubles(values, buf, valuesIndex)
                   }
    require(finalPos <= 65535, s"Histogram data is too large: $finalPos bytes needed")
    buf.putShort(0, (finalPos - 2).toShort)
    finalPos
  }
}

object HistogramVector extends StrictLogging {
  type HistIterator = Iterator[Histogram] with TypedIterator

  val OffsetNumHistograms = 6
  val OffsetFormatCode = 8     // u8: BinHistogram format code/bucket type
  val OffsetBucketDefSize = 9  // # of bytes of bucket definition
  val OffsetBucketDef  = 11    // Start of bucket definition
  val OffsetNumBuckets = 11
  // After the bucket area are regions for storing the counter values or pointers to them

  val _log = logger

  final def getNumBuckets(acc: MemoryReader, addr: Ptr.U8): Int = addr.add(OffsetNumBuckets).asU16.getU16(acc)

  final def getNumHistograms(acc: MemoryReader, addr: Ptr.U8): Int = addr.add(OffsetNumHistograms).asU16.getU16(acc)
  final def resetNumHistograms(acc: MemoryAccessor, addr: Ptr.U8): Unit =
    addr.add(OffsetNumHistograms).asU16.asMut.set(acc, 0)
  final def incrNumHistograms(acc: MemoryAccessor, addr: Ptr.U8): Unit =
    addr.add(OffsetNumHistograms).asU16.asMut.set(acc, getNumHistograms(acc, addr) + 1)

  // Note: the format code defines bucket definition format + format of each individual compressed histogram
  final def formatCode(acc: MemoryReader, addr: Ptr.U8): Byte = addr.add(OffsetFormatCode).getU8(acc).toByte
  final def afterBucketDefAddr(acc: MemoryReader, addr: Ptr.U8): Ptr.U8 =
    addr + OffsetBucketDef + bucketDefNumBytes(acc, addr)
  final def bucketDefNumBytes(acc: MemoryReader, addr: Ptr.U8): Int =
    addr.add(OffsetBucketDefSize).asU16.getU16(acc)
  final def bucketDefAddr(addr: Ptr.U8): Ptr.U8 = addr + OffsetBucketDef

  // Matches the bucket definition whose # bytes is at (base, offset)
  final def matchBucketDef(hist: BinaryHistogram.BinHistogram, acc: MemoryReader, addr: Ptr.U8): Boolean =
    (hist.formatCode == formatCode(acc, addr)) &&
    (hist.bucketDefNumBytes == bucketDefNumBytes(acc, addr)) && {
      UnsafeUtils.equate(acc.base, acc.baseOffset + bucketDefAddr(addr).addr,
        hist.buf.byteArray, hist.bucketDefOffset, hist.bucketDefNumBytes)
    }

  def appending(factory: MemFactory, maxBytes: Int): AppendableHistogramVector = {
    val addr = factory.allocateOffheap(maxBytes)
    new AppendableHistogramVector(factory, Ptr.U8(addr), maxBytes)
  }

  def appending2D(factory: MemFactory, maxBytes: Int): AppendableHistogramVector = {
    val addr = factory.allocateOffheap(maxBytes)
    new Appendable2DDeltaHistVector(factory, Ptr.U8(addr), maxBytes)
  }

  def appendingSect(factory: MemFactory, maxBytes: Int): AppendableHistogramVector = {
    val addr = factory.allocateOffheap(maxBytes)
    new AppendableSectDeltaHistVector(factory, Ptr.U8(addr), maxBytes)
  }

  def apply(buffer: ByteBuffer): HistogramReader = apply(MemoryReader.fromByteBuffer(buffer), 0)

  import WireFormat._

  def apply(acc: MemoryReader, p: BinaryVectorPtr): HistogramReader = BinaryVector.vectorType(acc, p) match {
    case x if x == WireFormat(VECTORTYPE_HISTOGRAM, SUBTYPE_H_SIMPLE) => new RowHistogramReader(acc, Ptr.U8(p))
    case x if x == WireFormat(VECTORTYPE_HISTOGRAM, SUBTYPE_H_SECTDELTA) =>new SectDeltaHistogramReader(acc, Ptr.U8(p))
  }

  // Thread local buffer used as temp buffer for histogram vector encoding ONLY
  private val tlEncodingBuf = new ThreadLocal[MutableDirectBuffer]()
  private[memory] def encodingBuf: MutableDirectBuffer = tlEncodingBuf.get match {
    case UnsafeUtils.ZeroPointer => val buf = new ExpandableArrayBuffer(4096)
                                    tlEncodingBuf.set(buf)
                                    buf
    case b: MutableDirectBuffer         => b
  }
}

/**
 * A HistogramVector appender storing compressed histogram values for less storage space.
 * This is a Section-based vector - sections of up to 64 histograms are stored at a time.
 * It stores histograms up to a maximum allowed size (since histograms are variable length)
 * Note that the bucket schema is not set until getting the first item.
 * This one stores the compressed histograms as-is, with no other transformation.
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

  protected def vectSubType: Int = WireFormat.SUBTYPE_H_SIMPLE

  // Initialize header
  BinaryVector.writeMajorAndSubType(MemoryAccessor.nativePtrAccessor,
    addr, WireFormat.VECTORTYPE_HISTOGRAM, vectSubType)
  reset()

  final def addr: BinaryVectorPtr = vectPtr.addr
  def maxElementsPerSection: IntU8 = IntU8(64)

  val dispose = () => {
    // free our own memory
    factory.freeMemory(addr)
  }

  final def numBytes: Int = vectPtr.asI32.getI32(nativePtrReader) + 4
  final def length: Int = getNumHistograms(nativePtrReader, vectPtr)
  final def isAvailable(index: Int): Boolean = true
  final def isAllNA: Boolean = (length == 0)
  final def noNAs: Boolean = (length > 0)

  private def setNumBytes(len: Int): Unit = {
    require(len >= 0)
    vectPtr.asI32.asMut.set(MemoryAccessor.nativePtrAccessor, len)
  }

  // NOTE: to eliminate allocations, re-use the DirectBuffer and keep passing the same instance to addData
  final def addData(buf: DirectBuffer): AddResponse = {
    val h = BinHistogram(buf)
    // Validate it's a valid bin histogram
    if (buf.capacity < 5 || !isValidFormatCode(h.formatCode) ||
        h.formatCode == HistFormat_Null) {
      return InvalidHistogram
    }
    if (h.bucketDefNumBytes > h.totalLength) return InvalidHistogram

    val numItems = getNumHistograms(nativePtrReader, vectPtr)
    if (numItems == 0) {
      // Copy the bucket definition and set the bucket def size
      UnsafeUtils.unsafe.copyMemory(buf.byteArray, h.bucketDefOffset,
                                    UnsafeUtils.ZeroPointer, bucketDefAddr(vectPtr).addr,
                                    h.bucketDefNumBytes)
      UnsafeUtils.setShort(addr + OffsetBucketDefSize, h.bucketDefNumBytes.toShort)
      UnsafeUtils.setByte(addr + OffsetFormatCode, h.formatCode)

      // Initialize the first section
      val firstSectPtr = afterBucketDefAddr(nativePtrReader, vectPtr)
      initSectionWriter(firstSectPtr, ((vectPtr + maxBytes).addr - firstSectPtr.addr).toInt)
    } else {
      // check the bucket schema is identical.  If not, return BucketSchemaMismatch
      if (!matchBucketDef(h, nativePtrReader, vectPtr)) return BucketSchemaMismatch
    }

    val res = appendHist(buf, h, numItems)
    if (res == Ack) {
      // set new number of bytes first. Remember to exclude initial 4 byte length prefix
      setNumBytes(maxBytes - bytesLeft - 4)
      // Finally, increase # histograms which is the ultimate safe gate for access by readers
      incrNumHistograms(MemoryAccessor.nativePtrAccessor, vectPtr)
    }
    res
  }

  def debugString: String = {
    val hReader = reader.asInstanceOf[RowHistogramReader]
    s"AppendableHistogramVector(vectPtr=$vectPtr maxBytes=$maxBytes) " +
    s"numItems=${hReader.length} curSection=$curSection " +
    { if (hReader.length > 0) s"bucketScheme: ${hReader.buckets} numBuckets=${hReader.numBuckets}" else "<noSchema>" }
  }

  // Inner method to add the histogram to this vector
  protected def appendHist(buf: DirectBuffer, h: BinHistogram, numItems: Int): AddResponse = {
    appendBlob(buf.byteArray, buf.addressOffset + h.valuesIndex, h.valuesNumBytes)
  }

  final def addNA(): AddResponse = Ack  // TODO: Add a 0 to every appender

  def addFromReaderNoNA(reader: RowReader, col: Int): AddResponse = addData(reader.blobAsBuffer(col))
  def copyToBuffer: Buffer[DirectBuffer] = ???
  def apply(index: Int): DirectBuffer = ???

  def finishCompaction(newAddress: BinaryRegion.NativePointer): BinaryVectorPtr = newAddress

  // NOTE: do not access reader below unless this vect is nonempty.  TODO: fix this, or don't if we don't use this class
  lazy val reader: VectorDataReader = new RowHistogramReader(nativePtrReader, vectPtr)

  def reset(): Unit = {
    resetNumHistograms(MemoryAccessor.nativePtrAccessor, vectPtr)
    setNumBytes(OffsetNumBuckets + 2)
  }

  // We don't optimize -- for now.  Histograms are already stored compressed.
  // In future, play with other optimization strategies, such as delta encoding.
}

/**
 * An appender for Prom-style histograms that increase over time.
 * It stores deltas between successive histograms to save space, but the histograms are assumed to be always
 * increasing.  If they do not increase, then that is considered a "reset" and recorded as such for
 * counter correction during queries.
 * Great for compression but recovering original value means summing up all the diffs  :(
 */
class Appendable2DDeltaHistVector(factory: MemFactory,
                                  vectPtr: Ptr.U8,
                                  maxBytes: Int) extends AppendableHistogramVector(factory, vectPtr, maxBytes) {
  import BinaryHistogram._
  import HistogramVector._

  override def vectSubType: Int = WireFormat.SUBTYPE_H_2DDELTA
  private var repackSink = BinaryHistogram.empty2DSink

  // TODO: handle corrections correctly. :D
  override def appendHist(buf: DirectBuffer, h: BinHistogram, numItems: Int): AddResponse = {
    // Must initialize sink correctly at beg once the actual # buckets are known
    // Also, we need to write repacked diff histogram to a temporary buffer, as appendBlob needs to know the size
    // before writing.
    if (repackSink == BinaryHistogram.empty2DSink)
      repackSink = NibblePack.DeltaDiffPackSink(new Array[Long](h.numBuckets), encodingBuf)

    // Recompress hist based on delta from last hist, write to temp storage.  Note that no matter what
    // we HAVE to feed each incoming hist through the sink, to properly seed the last hist values.
    repackSink.writePos = 0
    NibblePack.unpackToSink(h.valuesByteSlice, repackSink, h.numBuckets)

    // See if we are at beginning of section.  If so, write the original histogram.  If not, repack and write diff
    if (repackSink.valueDropped) {
      repackSink.reset()
      newSectionWithBlob(buf.byteArray, buf.addressOffset + h.valuesIndex, h.valuesNumBytes, Section.TypeDrop)
    } else if (numItems == 0 || needNewSection(h.valuesNumBytes)) {
      repackSink.reset()
      appendBlob(buf.byteArray, buf.addressOffset + h.valuesIndex, h.valuesNumBytes)
    } else {
      val repackedLen = repackSink.writePos
      repackSink.reset()
      appendBlob(encodingBuf.byteArray, encodingBuf.addressOffset, repackedLen)
    }
  }

  override def reset(): Unit = {
    super.reset()
    // IMPORTANT! Reset the sink so it can create a new sink with new bucket scheme.  Otherwise there is a bug
    // where a different time series can obtain the smae vector with a stale sink.
    repackSink = BinaryHistogram.empty2DSink
  }
}

/**
 * Appender for Prom-style increasing counter histograms of fixed buckets.
 * Unlike 2DDelta, it stores deltas from the first (original) histogram of a section, so that the original
 * histogram can easily be recovered just by one add.
 */
class AppendableSectDeltaHistVector(factory: MemFactory,
                                    vectPtr: Ptr.U8,
                                    maxBytes: Int) extends AppendableHistogramVector(factory, vectPtr, maxBytes) {
  import BinaryHistogram._
  import HistogramVector._

  override def vectSubType: Int = WireFormat.SUBTYPE_H_SECTDELTA
  private var repackSink = BinaryHistogram.emptySectSink

  // Default to smaller section sizes to maximize compression
  override final def maxElementsPerSection: IntU8 = IntU8(16)

  override def appendHist(buf: DirectBuffer, h: BinHistogram, numItems: Int): AddResponse = {
    // Initial histogram: set up new sink with first=true flag / just init with # of buckets
    if (repackSink == BinaryHistogram.emptySectSink)
      repackSink = new NibblePack.DeltaSectDiffPackSink(h.numBuckets, encodingBuf)

    // Recompress hist based on original delta.  Do this for ALL histograms so drop detection works correctly
    repackSink.writePos = 0
    try {
      NibblePack.unpackToSink(h.valuesByteSlice, repackSink, h.numBuckets)
    } catch {
      case e: Exception =>
        _log.error(s"RepackError: $debugString\nh=$h " +
          s"h.debugStr=${h.debugStr}\nSink state: ${repackSink.debugString}",
                   e)
        throw e
    }

    // If need new section, append blob.  Reset state for originalDeltas as needed.
    if (repackSink.valueDropped) {
      repackSink.reset()
      repackSink.setOriginal()
      newSectionWithBlob(buf.byteArray, buf.addressOffset + h.valuesIndex, h.valuesNumBytes, Section.TypeDrop)
    } else if (numItems == 0 || needNewSection(h.valuesNumBytes)) {
      repackSink.reset()
      repackSink.setOriginal()
      appendBlob(buf.byteArray, buf.addressOffset + h.valuesIndex, h.valuesNumBytes)
    } else {
      val repackedLen = repackSink.writePos
      repackSink.reset()
      appendBlob(encodingBuf.byteArray, encodingBuf.addressOffset, repackedLen)
    }
  }

  override def reset(): Unit = {
    super.reset()
    // IMPORTANT! Reset the sink so it can create a new sink with new bucket scheme.  Otherwise there is a bug
    // where a different time series can obtain the smae vector with a stale sink.
    repackSink = BinaryHistogram.emptySectSink
  }

  override lazy val reader: VectorDataReader = new SectDeltaHistogramReader(nativePtrReader, vectPtr)
}

trait HistogramReader extends VectorDataReader {
  def acc: MemoryReader
  def buckets: HistogramBuckets
  def apply(index: Int): HistogramWithBuckets
  def sum(start: Int, end: Int): MutableHistogram
}

/**
 * A reader for row-based Histogram vectors.  Mostly contains logic to skip around the vector to find the right
 * record pointer.
 */
class RowHistogramReader(val acc: MemoryReader, histVect: Ptr.U8) extends HistogramReader with SectionReader {
  import HistogramVector._

  final def length: Int = getNumHistograms(acc, histVect)
  val numBuckets = if (length > 0) getNumBuckets(acc, histVect) else 0

  val buckets = HistogramBuckets(acc, bucketDefAddr(histVect).add(-2), formatCode(acc, histVect))
  val returnHist = LongHistogram(buckets, new Array[Long](buckets.numBuckets))
  val endAddr = histVect + histVect.asI32.getI32(acc) + 4

  def firstSectionAddr: Ptr.U8 = afterBucketDefAddr(acc, histVect)

  /**
   * Iterates through each histogram. Note this is expensive due to materializing the Histogram object
   * every time.  Using higher level functions such as sum is going to be a much better bet usually.
   */
  def iterate(accNotUsed: MemoryReader, vectorNotUsed: BinaryVectorPtr, startElement: Int): TypedIterator =
  new Iterator[Histogram] with TypedIterator {
    var elem = startElement
    def hasNext: Boolean = elem < getNumHistograms(acc, histVect)
    def next: Histogram = {
      val h = apply(elem)
      elem += 1
      h
    }
  }

  def debugString(acc: MemoryReader, vector: BinaryVectorPtr, sep: String = System.lineSeparator): String = {
    val it = iterate(acc, vector)
    val size = length(acc, vector)
    (0 to size).map(_ => it.asHistIt).mkString(sep)
  }

  def length(accNotUsed: MemoryReader, vectorNotUsed: BinaryVectorPtr): Int = length

  protected val dSink = NibblePack.DeltaSink(returnHist.values)

  // WARNING: histogram returned is shared between calls, do not reuse!
  def apply(index: Int): HistogramWithBuckets = {
    require(length > 0)
    val histPtr = locate(index)
    val histLen = histPtr.asU16.getU16(acc)
    val buf = BinaryHistogram.valuesBuf
    acc.wrapInto(buf, histPtr.add(2).addr, histLen)
    dSink.reset()
    NibblePack.unpackToSink(buf, dSink, numBuckets)
    returnHist
  }

  // sum_over_time returning a Histogram with sums for each bucket.  Start and end are inclusive row numbers
  // NOTE: for now this is just a dumb implementation that decompresses each histogram fully
  final def sum(start: Int, end: Int): MutableHistogram = {
    require(length > 0 && start >= 0 && end < length)
    val summedHist = MutableHistogram.empty(buckets)
    cforRange { start to end } { i =>
      summedHist.addNoCorrection(apply(i))
    }
    summedHist
  }
}

final case class HistogramCorrection(lastValue: LongHistogram, correction: LongHistogram) extends CorrectionMeta

trait CounterHistogramReader extends HistogramReader with CounterVectorReader {
  def correctedValue(n: Int, meta: CorrectionMeta): HistogramWithBuckets
}

/**
 * A reader for SectDelta encoded histograms, including correction/drop functionality
 */
class SectDeltaHistogramReader(acc2: MemoryReader, histVect: Ptr.U8)
      extends RowHistogramReader(acc2, histVect) with CounterHistogramReader {
  // baseHist is section base histogram; summedHist used to compute base + delta or other sums
  private val summedHist = LongHistogram.empty(buckets)
  private val baseHist = summedHist.copy
  private val baseSink = NibblePack.DeltaSink(baseHist.values)

  // override setSection: also set the base histogram for getting real value
  override def setSection(sectAddr: Ptr.U8, newElemNo: Int = 0): Unit = {
    super.setSection(sectAddr, newElemNo)
    // unpack to baseHist
    baseSink.reset()
    val buf = BinaryHistogram.valuesBuf
    acc.wrapInto(buf, curHist.add(2).addr, curHist.asU16.getU16(acc))
    NibblePack.unpackToSink(buf, baseSink, numBuckets)
  }

  override def apply(index: Int): HistogramWithBuckets = {
    require(length > 0)
    val histPtr = locate(index)

    // Just return the base histogram if we are at start of section
    if (index == sectStartingElemNo) baseHist
    else {
      val histLen = histPtr.asU16.getU16(acc)
      val buf = BinaryHistogram.valuesBuf
      acc.wrapInto(buf, histPtr.add(2).addr, histLen)
      dSink.reset()
      NibblePack.unpackToSink(buf, dSink, numBuckets)

      summedHist.populateFrom(baseHist)
      summedHist.add(returnHist)
      summedHist
    }
  }

  // TODO: optimized summing.  It's wasteful to apply the base + delta math so many times ...
  // instead add delta + base * n if possible.   However, do we care about sum performance on increasing histograms?

  def detectDropAndCorrection(accNotUsed: MemoryReader, vectorNotUsed: BinaryVectorPtr,
                              meta: CorrectionMeta): CorrectionMeta = meta match {
    case NoCorrection =>   meta    // No last value, cannot compare.  Just pass it on.
    case h @ HistogramCorrection(lastValue, correction) =>
      val firstValue = apply(0)
      // Last value is the new delta correction.  Also assume the correction is already a cloned independent thing
      if (firstValue < lastValue) {
        correction.add(lastValue)
        h
      } else { meta }
  }

  // code to go through and build a list of corrections and corresponding index values.. (dropIndex, correction)
  private lazy val corrections = {
    var index = 0
    // Step 1: build an iterator of (starting-index, section) for each section
    iterateSections.map { case (s) => val o = (index, s); index += s.numElements(acc); o }.collect {
      case (i, s) if i > 0 && s.sectionType(acc) == Section.TypeDrop =>
        (i, apply(i - 1).asInstanceOf[LongHistogram].copy)
    }.toBuffer
  }

  def dropPositions(accNotUsed: MemoryReader, vectorNotUsed: BinaryVectorPtr): debox.Buffer[Int] = {
    val res = debox.Buffer.empty[Int]
    corrections.foreach { case (dropPos, hist) =>
      res += dropPos
    }
    res
  }

  def updateCorrection(accNotUsed: MemoryReader, vectorNotUsed: BinaryVectorPtr,
                       meta: CorrectionMeta): CorrectionMeta = {
    val correction = meta match {
      case NoCorrection                 => LongHistogram.empty(buckets)
      case HistogramCorrection(_, corr) => corr
    }
    // Go through and add corrections
    corrections.foreach { case (_, corr) => correction.add(corr) }

    HistogramCorrection(apply(length - 1).asInstanceOf[LongHistogram].copy, correction)
  }

  def correctedValue(n: Int, meta: CorrectionMeta): HistogramWithBuckets = {
    // Get the raw histogram value -- COPY it as we need to modify it, and also
    // calling corrections below might modify the temporary value
    val h = apply(n).asInstanceOf[LongHistogram].copy

    // Apply any necessary corrections
    corrections.foreach { case (ci, corr) =>
      if (ci <= n) h.add(corr)
    }

    // Plus any carryover previous corrections
    meta match {
      case NoCorrection                 => h
      case HistogramCorrection(_, corr) => h.add(corr)
                                           h
    }
  }
}
