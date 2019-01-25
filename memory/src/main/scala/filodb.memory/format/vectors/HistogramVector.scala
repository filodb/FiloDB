package filodb.memory.format.vectors

import debox.Buffer
import org.agrona.concurrent.UnsafeBuffer
import scalaxy.loops._

import filodb.memory.{BinaryRegion, MemFactory}
import filodb.memory.format._
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.Encodings._

/**
 * BinaryHistogram is the binary format for a histogram binary blob included in BinaryRecords and sent over the wire.
 * It fits the BinaryRegionMedium protocol.
 * Format:
 *   +0000  u16  2-byte total length of this BinaryHistogram (excluding this length)
 *   +0002  u16  2-byte length of Histogram bucket definition
 *   +0004  [u8] Histogram bucket definition, see [[HistogramBuckets]]
 *   +(4+n) u16  2-byte length of histogram values
 *   +(6+n) u8   1-byte histogram values format code:
 *                  0x00   Flat array of 64-bit values
 *                  0x01   NibblePacked - simple delta predictor (increasing Long values)
 *   +(7+n) remaining values according to format above
 *
 *  NOTE: most of the methods below actually expect a pointer to the +2 hist bucket definition, not the length field
 */
object BinaryHistogram {
  // Pass in a buffer which includes the length bytes.  Value class - no allocations.
  case class BinHistogram(buf: UnsafeBuffer) extends AnyVal {
    def totalLength: Int = buf.getShort(0).toInt + 2
    def numBuckets: Int = buf.getShort(4).toInt
    def bucketDefNumBytes: Int = buf.getShort(2).toInt
    def bucketDefOffset: Long = buf.addressOffset + 4
    def valuesIndex: Int = 2 + 2 + bucketDefNumBytes + 2
    def valuesFormatCode: Byte = buf.getByte(valuesIndex)
    def intoValuesBuf(destBuf: UnsafeBuffer): Unit =
      UnsafeUtils.wrapUnsafeBuf(buf.byteArray, buf.addressOffset + valuesIndex + 1,
                                buf.getShort(valuesIndex - 2) - 1, destBuf)
    override def toString: String = s"<BinHistogram: ${toHistogram}>"

    /**
     * Converts this BinHistogram to a Histogram object.  May not be the most efficient.
     * Intended for slower paths such as high level (lower # samples) aggregation and HTTP/CLI materialization
     * by clients.  Materializes/deserializes everything.
     * Ingestion ingests BinHistograms directly without conversion to Histogram first.
     */
    def toHistogram: Histogram = {
      val bucketDef = HistogramBuckets(buf.byteArray, bucketDefOffset, totalLength)
      valuesFormatCode match {
        case ValuesFormat_Flat => FlatBucketHistogram(bucketDef, this)
      }
    }
  }

  private val tlValuesBuf = new ThreadLocal[UnsafeBuffer]()
  def valuesBuf: UnsafeBuffer = tlValuesBuf.get match {
    case UnsafeUtils.ZeroPointer => val buf = new UnsafeBuffer(new Array[Byte](4096))
                                    tlValuesBuf.set(buf)
                                    buf
    case b: UnsafeBuffer         => b
  }

  // Thread local buffer used as temp buffer for writing binary histograms
  private val tlHistBuf = new ThreadLocal[UnsafeBuffer]()
  def histBuf: UnsafeBuffer = tlHistBuf.get match {
    case UnsafeUtils.ZeroPointer => val buf = new UnsafeBuffer(new Array[Byte](8192))
                                    tlHistBuf.set(buf)
                                    buf
    case b: UnsafeBuffer         => b
  }

  val ValuesFormat_Flat = 0x00.toByte

  case class FlatBucketValues(buf: UnsafeBuffer) extends AnyVal {
    def bucket(no: Int): Long = buf.getLong(no * 8)
  }

  private case class FlatBucketHistogram(buckets: HistogramBuckets, binHist: BinHistogram) extends Histogram {
    binHist.intoValuesBuf(valuesBuf)
    final def numBuckets: Int = buckets.numBuckets
    final def bucketTop(no: Int): Double = buckets.bucketTop(no)
    final def bucketValue(no: Int): Double = FlatBucketValues(valuesBuf).bucket(no)
    final def serialize(intoBuf: Option[UnsafeBuffer] = None): UnsafeBuffer =
      intoBuf.map { x => x.wrap(binHist.buf); x }.getOrElse(binHist.buf)
  }

  /**
   * Writes a binary histogram including the length prefix into buf
   * @return the number of bytes written, including the length prefix
   */
  def writeBinHistogram(bucketDef: Array[Byte], values: Array[Long], buf: UnsafeBuffer): Int = {
    val bytesNeeded = 2 + 2 + bucketDef.size + 2 + 1 + 8 * values.size
    require(bytesNeeded < 65535, s"Histogram data is too large: $bytesNeeded bytes needed")
    require(buf.capacity >= bytesNeeded, s"Buffer only has ${buf.capacity} bytes but we need $bytesNeeded")

    buf.putShort(0, (bytesNeeded - 2).toShort)
    buf.putShort(2, bucketDef.size.toShort)
    buf.putBytes(4, bucketDef)
    buf.putShort(4 + bucketDef.size, (1 + 8 * values.size).toShort)
    buf.putByte(6 + bucketDef.size, ValuesFormat_Flat)
    val valuesIndex = 7 + bucketDef.size
    for { b <- 0 until values.size optimized } {
      buf.putLong(valuesIndex + b * 8, values(b))
    }
    bytesNeeded
  }

  def writeBinHistogram(bucketDef: Array[Byte], values: Array[Long]): Int =
    writeBinHistogram(bucketDef, values, histBuf)
}

object HistogramVector {
  type HistIterator = Iterator[Histogram] with TypedIterator

  val OffsetNumHistograms = 6
  val OffsetBucketDefSize = 8  // # of bytes of bucket definition, including bucket def type
  val OffsetBucketDef  = 10    // Start of bucket definition
  val OffsetNumBuckets = 10
  val OffsetBucketDefType = 12  // u8: bucket definition types
  // After the bucket area are regions for storing the counter values or pointers to them

  final def getNumBuckets(addr: BinaryVectorPtr): Int = UnsafeUtils.getShort(addr + OffsetNumBuckets).toInt

  final def getNumHistograms(addr: BinaryVectorPtr): Int = UnsafeUtils.getShort(addr + OffsetNumHistograms).toInt
  final def resetNumHistograms(addr: BinaryVectorPtr): Unit =
    UnsafeUtils.setShort(addr + OffsetNumHistograms, 0)
  final def incrNumHistograms(addr: BinaryVectorPtr): Unit =
    UnsafeUtils.setShort(addr + OffsetNumHistograms, (getNumHistograms(addr) + 1).toShort)

  final def afterBucketDefAddr(addr: BinaryVectorPtr): BinaryRegion.NativePointer =
    addr + OffsetBucketDef + bucketDefNumBytes(addr)
  final def bucketDefNumBytes(addr: BinaryVectorPtr): Int = UnsafeUtils.getShort(addr + OffsetBucketDefSize).toInt
  final def bucketDefAddr(addr: BinaryVectorPtr): BinaryRegion.NativePointer = addr + OffsetBucketDef

  // Matches the bucket definition whose # bytes is at (base, offset)
  final def matchBucketDef(hist: BinaryHistogram.BinHistogram, addr: BinaryVectorPtr): Boolean =
    (hist.bucketDefNumBytes == bucketDefNumBytes(addr)) && {
      UnsafeUtils.equate(UnsafeUtils.ZeroPointer, addr + OffsetBucketDef, hist.buf.byteArray, hist.bucketDefOffset,
                         hist.bucketDefNumBytes)
    }

  // Columnar HistogramVectors composed of multiple vectors, this calculates total used size
  def columnarTotalSize(addr: BinaryVectorPtr): Int = {
    val bucketAddrPtr = afterBucketDefAddr(addr)
    val headerBytes = UnsafeUtils.getInt(addr)
    headerBytes + (0 until getNumBuckets(addr)).map { b =>
                    val bucketVectorAddr = UnsafeUtils.getLong(bucketAddrPtr + 8*b)
                    UnsafeUtils.getInt(bucketVectorAddr) + 4
                  }.sum
  }

  val ReservedBucketDefSize = 256
  def appendingColumnar(factory: MemFactory, numBuckets: Int, maxItems: Int): ColumnarAppendableHistogramVector = {
    // Really just an estimate.  TODO: if we really go with columnar, make it more accurate
    val neededBytes = OffsetBucketDef + ReservedBucketDefSize + 8 * numBuckets
    val addr = factory.allocateOffheap(neededBytes)
    new ColumnarAppendableHistogramVector(factory, addr, maxItems)
  }

  def apply(p: BinaryVectorPtr): HistogramReader =
    new ColumnarHistogramReader(p)
}

/**
 * A HistogramVector appender composed of individual primitive columns.
 * Just a POC to get started quickly and as a reference.
 * Note that the bucket schema is not set until getting the first item.
 * After the bucket definition:
 * An array [u64] of native pointers to the individual columns
 *
 * TODO: initialize num bytes and vector type stuff
 *
 * Read/Write/Lock semantics: everything is gated by the number of elements.
 * When it is 0, nothing is initialized so the reader guards against that.
 * When it is > 0, then all structures are initialized.
 */
class ColumnarAppendableHistogramVector(factory: MemFactory,
                                        val addr: BinaryVectorPtr,
                                        maxItems: Int) extends BinaryAppendableVector[UnsafeBuffer] {
  import HistogramVector._
  import BinaryHistogram._
  resetNumHistograms(addr)

  private var bucketAppenders: Option[Array[BinaryAppendableVector[Long]]] = None

  val dispose = () => {
    // first, free memory from each appender
    bucketAppenders.foreach(_.foreach(_.dispose()))
    // free our own memory
    factory.freeMemory(addr)
  }

  final def numBytes: Int = UnsafeUtils.getInt(addr) + 4
  final def maxBytes: Int = numBytes
  final def length: Int = getNumHistograms(addr)
  final def isAvailable(index: Int): Boolean = true
  final def isAllNA: Boolean = (length == 0)
  final def noNAs: Boolean = (length > 0)

  private val valueBuf = new UnsafeBuffer(Array.empty[Byte])

  // NOTE: to eliminate allocations, re-use the UnsafeBuffer and keep passing the same instance to addData
  final def addData(buf: UnsafeBuffer): AddResponse = {
    val numItems = getNumHistograms(addr)
    val h = BinHistogram(buf)
    val numBuckets = h.numBuckets
    if (numItems == 0) {
      // Copy the bucket definition and set the bucket def size
      UnsafeUtils.unsafe.copyMemory(buf.byteArray, h.bucketDefOffset,
                                    UnsafeUtils.ZeroPointer, addr + OffsetBucketDef, h.bucketDefNumBytes)
      UnsafeUtils.setShort(addr + OffsetBucketDefSize, h.bucketDefNumBytes.toShort)

      // initialize the buckets
      initBuckets(numBuckets)
    } else if (numItems >= maxItems) {
      return VectorTooSmall(0, 0)
    } else {
      // check the bucket schema is identical.  If not, return BucketSchemaMismatch
      if (!matchBucketDef(h, addr)) return BucketSchemaMismatch
    }

    // Now, iterate through the counters and add them to each individual vector
    h.intoValuesBuf(valueBuf)
    val values = FlatBucketValues(valueBuf)
    bucketAppenders.foreach { appenders =>
      for { b <- 0 until numBuckets optimized } {
        val resp = appenders(b).addData(values.bucket(b))
        require(resp == Ack)
      }
    }

    incrNumHistograms(addr)
    Ack
  }

  final def addNA(): AddResponse = Ack  // TODO: Add a 0 to every appender

  def addFromReaderNoNA(reader: RowReader, col: Int): AddResponse = addData(reader.blobAsBuffer(col))
  def copyToBuffer: Buffer[UnsafeBuffer] = ???
  def apply(index: Int): UnsafeBuffer = ???

  def finishCompaction(newAddress: BinaryRegion.NativePointer): BinaryVectorPtr = newAddress

  // NOTE: do not access reader below unless this vect is nonempty.  TODO: fix this, or don't if we don't use this class
  lazy val reader: VectorDataReader = new ColumnarHistogramReader(addr)

  def reset(): Unit = {
    bucketAppenders.foreach(_.foreach(_.dispose()))
    bucketAppenders = None
    resetNumHistograms(addr)
  }

  // Optimize each bucket's appenders, then create a new region with the same headers but pointing at the
  // optimized vectors.
  // TODO: this is NOT safe for persistence and recovery, as pointers cannot be persisted or recovered.
  // For us to really make persistence of this work, we would need to pursue one of these strategies:
  //  1) Change code of each LongAppendingVector to tell us how much optimized bytes take up for each bucket,
  //     then do a giant allocation including every bucket, and use relative pointers, not absolute, to point
  //     to each one;  (or possibly a different kind of allocator)
  //  2) Use BlockIDs and offsets instead of absolute pointers, and persist entire blocks.
  override def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVectorPtr = {
    val optimizedBuckets = bucketAppenders.map { appenders =>
      appenders.map(_.optimize(memFactory, hint))
    }.getOrElse(Array.empty[BinaryVectorPtr])

    val newHeaderAddr = memFactory.allocateOffheap(numBytes)
    // Copy headers including bucket def
    val bucketPtrOffset = (afterBucketDefAddr(addr) - addr).toInt
    UnsafeUtils.copy(addr, newHeaderAddr, bucketPtrOffset)

    for { b <- 0 until optimizedBuckets.size optimized } {
      UnsafeUtils.setLong(newHeaderAddr + bucketPtrOffset + 8*b, optimizedBuckets(b))
    }

    newHeaderAddr
  }

  // NOTE: allocating vectors during ingestion is a REALLY BAD idea.  For one if one runs out of memory then
  //   it will fail but ingestion into other vectors might succeed, resulting in undefined switchBuffers behaviors.
  private def initBuckets(numBuckets: Int): Unit = {
    val bucketPointersAddr = afterBucketDefAddr(addr)
    val appenders = (0 until numBuckets).map { b =>
      val appender = LongBinaryVector.appendingVectorNoNA(factory, maxItems)
      UnsafeUtils.setLong(bucketPointersAddr + 8*b, appender.addr)
      appender
    }
    bucketAppenders = Some(appenders.toArray)

    // Initialize number of bytes in this histogram header
    UnsafeUtils.setInt(addr, (bucketPointersAddr - addr).toInt + 8 * numBuckets)
  }
}

trait HistogramReader extends VectorDataReader {
  def buckets: HistogramBuckets
  def apply(index: Int): Histogram
  def sum(start: Int, end: Int): Histogram
}

class ColumnarHistogramReader(histVect: BinaryVectorPtr) extends HistogramReader {
  import HistogramVector._

  final def length: Int = getNumHistograms(histVect)
  val numBuckets = if (length > 0) getNumBuckets(histVect) else 0
  val bucketAddrs = if (length > 0) {
    val bucketAddrBase = afterBucketDefAddr(histVect)
    (0 until numBuckets).map(b => UnsafeUtils.getLong(bucketAddrBase + 8 * b)).toArray
  } else {
    Array.empty[BinaryVectorPtr]
  }
  val readers = if (length > 0) bucketAddrs.map(LongBinaryVector.apply) else Array.empty[LongVectorDataReader]

  val buckets = HistogramBuckets(bucketDefAddr(histVect), bucketDefNumBytes(histVect))
  val returnHist = MutableHistogram.empty(buckets)

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
    for { b <- 0 until numBuckets optimized } {
      returnHist.values(b) = readers(b).apply(bucketAddrs(b), index)
    }
    returnHist
  }

  // sum_over_time returning a Histogram with sums for each bucket.  Start and end are inclusive row numbers
  final def sum(start: Int, end: Int): Histogram = {
    require(length > 0)
    for { b <- 0 until numBuckets optimized } {
      returnHist.values(b) = readers(b).sum(b, start, end)
    }
    returnHist
  }
}