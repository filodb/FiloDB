package filodb.memory.format.vectors

import debox.Buffer
import org.agrona.DirectBuffer
import spire.syntax.cfor._

import filodb.memory.{BinaryRegion, MemFactory}
import filodb.memory.format._
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.MemoryReader._

/**
 * A HistogramVector appender specializing in Exponential Histograms storing compressed histogram values
 * for less storage space. This is a Section-based vector - sections of up to 64 histograms are stored at a time.
 * It stores histograms up to a maximum allowed size (since histograms are variable length)
 * Note that the bucket schema is stored along with each histogram.
 * This one stores the compressed histograms as-is, with no other transformation.
 *
 * Example Vector in hex:
 * 0x5C00000009130300540003001800160009100002000300FDFFFFFF01000000000000000200031E00
 * 1C000910000A001400FDFFFFFF0900000000000000FE00141111010300111800160009100002001400BC71F2FF0100000000000000020005
 *
 * 5C000000 4bytes  vector length (92 bytes length)
 * 09       1byte vector major type
 * 13       1byte vector subtype
 * 0300     2bytes num histograms in vector
 * 5400     2bytes section length: num bytes in the section following 4 byte section header (84 bytes)
 * 03       1byte  num items in section
 * 00       1byte type of section
 * 1800     2bytes record length (24 bytes length)
 * 160009100002000300FDFFFFFF0100000000000000020003 24bytes histogram
 * 1E00     2bytes record length (30 bytes length)
 * 1C000910000A001400FDFFFFFF0900000000000000FE0014111101030011 30 bytes histogram
 * 1800     2bytes record length (24 bytes length)
 * 160009100002001400BC71F2FF0100000000000000020005 24bytes histogram
 */
class AppendableExpHistogramVector(factory: MemFactory,
                                vectPtr: Ptr.U8,
                                val maxBytes: Int) extends BinaryAppendableVector[DirectBuffer] with SectionWriter {
  import HistogramVector._
  import BinaryHistogram._

  protected def vectSubType: Int = WireFormat.SUBTYPE_H_EXP_SIMPLE

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
      // Initialize the first section
      val firstSectPtr = afterNumHistograms(nativePtrReader, vectPtr)
      initSectionWriter(firstSectPtr, ((vectPtr + maxBytes).addr - firstSectPtr.addr).toInt)
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
    val hReader = reader.asInstanceOf[RowExpHistogramReader]
    s"AppendableExpHistogramVector(vectPtr=$vectPtr maxBytes=$maxBytes) " +
    s"numItems=${hReader.length} curSection=$curSection"
  }

  // Inner method to add the histogram to this vector
  protected def appendHist(buf: DirectBuffer, h: BinHistogram, numItems: Int): AddResponse = {
    appendBlob(buf.byteArray, buf.addressOffset, h.totalLength)
  }

  final def addNA(): AddResponse = Ack  // TODO: Add a 0 to every appender

  def addFromReaderNoNA(reader: RowReader, col: Int): AddResponse = addData(reader.blobAsBuffer(col))
  def copyToBuffer: Buffer[DirectBuffer] = ???
  def apply(index: Int): DirectBuffer = ???

  def finishCompaction(newAddress: BinaryRegion.NativePointer): BinaryVectorPtr = newAddress

  // NOTE: do not access reader below unless this vect is nonempty.
  lazy val reader: VectorDataReader = new RowExpHistogramReader(nativePtrReader, vectPtr)

  def reset(): Unit = {
    resetNumHistograms(MemoryAccessor.nativePtrAccessor, vectPtr)
    setNumBytes(OffsetNumBuckets + 2)
  }

}

/**
 * A reader for row-based Exp Histogram vectors.  Mostly contains logic to skip around the vector to find the right
 * record pointer.
 */
class RowExpHistogramReader(val acc: MemoryReader, val histVect: Ptr.U8) extends HistogramReader with SectionReader {
  import HistogramVector._

  final def length: Int = getNumHistograms(acc, histVect)

  val buckets = HistogramBuckets.emptyExpBuckets
  val endAddr = histVect + histVect.asI32.getI32(acc) + 4

  def firstSectionAddr: Ptr.U8 = afterNumHistograms(acc, histVect)

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
    (0 until size).map(_ => it.asHistIt.mkString(sep)).mkString(sep)
  }

  def length(acc: MemoryReader, vectorNotUsed: BinaryVectorPtr): Int = length

  private val returnHist = LongHistogram(buckets, new Array[Long](Base2ExpHistogramBuckets.maxBuckets))
  protected val dSink = NibblePack.DeltaSink(returnHist.values)

  // WARNING: histogram returned is shared between calls, do not reuse!
  def apply(index: Int): HistogramWithBuckets = {
    require(length > 0)
    val histPtr = locate(index)
    val histLen = histPtr.asU16.getU16(acc)
    val buf = BinaryHistogram.valuesBuf
    acc.wrapInto(buf, histPtr.add(2).addr, histLen)

    val binHist = BinaryHistogram.BinHistogram(buf)
    val bucketDef = returnHist.buckets.asInstanceOf[Base2ExpHistogramBuckets]
    bucketDef.scale = HistogramBuckets.otelExpScale(buf.byteArray(), binHist.bucketDefOffset)
    bucketDef.startIndexPositiveBuckets = HistogramBuckets.otelExpStartIndexPositiveBuckets(buf.byteArray(),
                                                                                            binHist.bucketDefOffset)
    bucketDef.numPositiveBuckets = HistogramBuckets.otelExpNumPositiveBuckets(buf.byteArray(), binHist.bucketDefOffset)
    dSink.reset()
    dSink.setLength(bucketDef.numBuckets)
    val buf2 = binHist.valuesByteSlice // no object allocation here, valuesBuf is reused
    NibblePack.unpackToSink(buf2, dSink, bucketDef.numBuckets)
    returnHist
  }

  // sum_over_time returning a Histogram with sums for each bucket.  Start and end are inclusive row numbers
  // NOTE: for now this is just a dumb implementation that decompresses each histogram fully
  final def sum(start: Int, end: Int): MutableHistogram = {
    require(length > 0 && start >= 0 && end < length)
    val summedHist = MutableHistogram.empty(HistogramBuckets.emptyExpBuckets)
    cforRange { start to end } { i =>
      summedHist.addNoCorrection(apply(i))
    }
    summedHist
  }
}

