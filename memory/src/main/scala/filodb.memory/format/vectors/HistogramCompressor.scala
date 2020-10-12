package filodb.memory.format.vectors

import scala.io.Source

import org.agrona.{ExpandableArrayBuffer, ExpandableDirectByteBuffer}
import org.agrona.concurrent.UnsafeBuffer
import spire.syntax.cfor._

import filodb.memory.NativeMemoryManager
import filodb.memory.format._
import filodb.memory.format.MemoryReader._

//scalastyle:off
trait CompressorAnalyzer {
  def inputFile: String
  def numSamples: Int

  val bucketDef = HistogramBuckets.binaryBuckets64

  // TODO: switch to PageAlignedMemManager so the memory can be constantly recycled
  val memFactory = new NativeMemoryManager(500 * 1024 * 1024)

  var binHistBytesSum = 0
  var binHistBytesMax = 0
  var numRecords = 0
  var numChunks = 0

  var encodedTotal = 0
  var writeBufferTotal = 0
  var samplesEncoded = 0

  lazy val histograms = Source.fromFile(inputFile).getLines
                         .take(numSamples)
                         .map { line =>
                           val buckets = line.split(",").map(_.trim.toLong)
                           LongHistogram(bucketDef, buckets)
                         }

  // returns (writebuffer size, encoded size) given # timestamp samples and interval in seconds
  def timestampVector(numSamples: Int, interval: Int): (Int, Int) = {
    val tsAppender = LongBinaryVector.appendingVectorNoNA(memFactory, numSamples)
    val startTime = System.currentTimeMillis
    (0 until numSamples).foreach { i =>
      tsAppender.addData(startTime + i * interval * 1000)
    }
    val writeBufSize = tsAppender.numBytes
    val optimized = tsAppender.optimize(memFactory)
    val encodedSize = BinaryVector.totalBytes(nativePtrReader, optimized)
    (writeBufSize, encodedSize)
  }

  def analyze(): Unit = {
    // Dump out overall aggregates
    val avgEncoded = encodedTotal.toDouble / numChunks
    val avgWriteBuf = writeBufferTotal.toDouble / numChunks
    val histPerChunk = samplesEncoded.toDouble / numChunks
    println(s"Total number of chunks: $numChunks")
    println(s"Samples encoded: $samplesEncoded")
    println(s"Histograms per chunk: $histPerChunk")

    val normAvgEncoded = avgEncoded * 300 / histPerChunk
    val normAvgWriteBuf = avgWriteBuf * 300 / histPerChunk
    println(s"Average encoded chunk size: $avgEncoded (normalized to 300 histograms: $normAvgEncoded)")
    println(s"Average write buffer size:  $avgWriteBuf (normalized to 300 histograms: $normAvgWriteBuf)")
    println(s"Compression ratio: ${avgWriteBuf / avgEncoded}")
    println(s"Average binHistogram size: ${binHistBytesSum / numRecords.toDouble}")
    println(s"Max binHistogram size: $binHistBytesMax bytes")
  }
}

// Input is a file with one line per histogram, bucket values are comma separated
// This app is designed to measure histogram compression ratios based on real world histogram data
object HistogramCompressor extends App with CompressorAnalyzer {
  if (args.length < 1) {
    println("Usage: sbt memory/run <histogram-file> <numsamples>")
    println("Tests chunk compression, not runtime or performance, against real-world increasing bucket histogram files")
    sys.exit(0)
  }
  val inputFile = args(0)
  val numSamples = if (args.length > 1) args(1).toInt else 6000

  val inputBuffer = new ExpandableArrayBuffer(4096)

  val maxBytes = 60 * 300   // Maximum allowable histogram writebuffer size
  val appender = HistogramVector.appendingSect(memFactory, maxBytes)

  histograms.foreach { h =>
    val buf = h.serialize()
    val histSize = (buf.getShort(0) & 0x0ffff) + 2
    numRecords += 1
    binHistBytesMax = Math.max(binHistBytesMax, histSize)
    binHistBytesSum += histSize

    // Ingest into our histogram column
    appender.addData(buf) match {
      case Ack =>    // data added, no problem
      case VectorTooSmall(_, _) =>   // not enough space.   Encode, aggregate, and add to a new appender
        // Optimize and get optimized size, dump out, aggregate
        val writeBufSize = appender.numBytes
        val optimized = appender.optimize(memFactory)
        val encodedSize = BinaryVector.totalBytes(nativePtrReader, optimized)
        val (tsBufSize, tsEncodedSize) = timestampVector(appender.length, 10)

        encodedTotal += encodedSize + tsEncodedSize
        writeBufferTotal += writeBufSize + tsBufSize
        numChunks += 1
        samplesEncoded += appender.length

        appender.reset()
        // Add back the input that did not fit into appender again
        appender.addData(buf)
      case other =>
        println(s"Warning: response $other from appender.addData")
    }
  }

  analyze()
}

/**
 * Intended to measure ingestion performance, rather than size.  Thus, pre-compressed BinaryHistograms are stored
 * in memory and repeatedly ingested again and again, including chunk encoding.
 * In between loops, the encoded chunks are freed and ingestion state is reset.
 * Each line of the file is CSV, no headers, and is expected to be Prom style (ie increasing bucket to bucket
 * and increasing over time).
 */
object HistogramIngestBench extends App with CompressorAnalyzer {
  if (args.length < 1) {
    println("Usage: sbt memory/run <histogram-file> <numsamples>")
    println("Tests histogram ingestion performance against a file of real data")
    sys.exit(0)
  }
  val inputFile = args(0)
  val numSamples = Int.MaxValue
  val numLoops = 1000

  // Maybe for the first run, just delta encode the histogram itself, forget about the HistogramBuckets for now.
  // Create lot lot compressed histograms
  val increasingBuf = new ExpandableDirectByteBuffer()
  var lastPos = 0
  val increasingHistPos = histograms.map { h =>
    lastPos = NibblePack.packDelta(h.values, increasingBuf, lastPos)
    lastPos
  }.toArray

  val numHist = increasingHistPos.size

  println(s"Finished reading and compressing $numHist histograms, now running $numLoops iterations of 2D Delta...")

  // Now, use DeltaDiffPackSink to recompress to deltas from initial inputs
  val outBuf = new ExpandableDirectByteBuffer()
  val ddsink = NibblePack.DeltaDiffPackSink(new Array[Long](bucketDef.numBuckets), outBuf)
  val ddSlice = new UnsafeBuffer(increasingBuf, 0, 0)

  val start = System.currentTimeMillis
  for { _ <- 0 until numLoops } {
    ddsink.writePos = 0
    java.util.Arrays.fill(ddsink.lastHistDeltas, 0)
    var lastPos = 0
    cforRange { 0 until numHist } { i =>
      ddSlice.wrap(increasingBuf, lastPos, increasingHistPos(i) - lastPos)
      val res = NibblePack.unpackToSink(ddSlice, ddsink, bucketDef.numBuckets)
      require(res == NibblePack.Ok)
      lastPos = increasingHistPos(i)
      ddsink.reset()
    }
  }

  val millisElapsed = System.currentTimeMillis - start
  val rate = numHist.toLong * numLoops * 1000 / millisElapsed
  println(s"${numHist * numLoops} encoded in $millisElapsed ms = $rate histograms/sec")
}

// Like HistogramCompressor but uses individual Long writebuffers like Prometheus (time series per bucket)
object PromCompressor extends App with CompressorAnalyzer {
  if (args.length < 1) {
    println("Usage: sbt memory/run <histogram-file> <numsamples>")
    println("Tests chunk compression, not runtime or performance, against real-world increasing bucket histogram files")
    sys.exit(0)
  }
  val inputFile = args(0)
  val numSamples = if (args.length > 1) args(1).toInt else 6000

  val appenders = (0 until bucketDef.numBuckets).map { _ =>
    LongBinaryVector.appendingVectorNoNA(memFactory, 300)
  }.toArray

  val (tsBufSize, tsEncodedSize) = timestampVector(300, 10)

  histograms.foreach { h =>
    numRecords += 1
    cforRange { 0 until bucketDef.numBuckets } { b =>
      appenders(b).addData(h.values(b)) match {
        case Ack =>    // data added, no problem
        case VectorTooSmall(_, _) =>   // not enough space.   Encode, aggregate, and add to a new appender
          // Optimize and get optimized size, dump out, aggregate
          val writeBufSize = appenders.map(_.numBytes).sum
          val optimized = appenders.map(_.optimize(memFactory))
          val encodedSize = optimized.map(v => BinaryVector.totalBytes(nativePtrReader, v)).sum
          println(s" WriteBuffer size: ${writeBufSize}\t\tEncoded size: $encodedSize")
          encodedTotal += encodedSize + bucketDef.numBuckets * tsEncodedSize
          writeBufferTotal += writeBufSize + bucketDef.numBuckets * tsBufSize
          numChunks += 1
          samplesEncoded += 300

          appenders.foreach(_.reset())
          // Add back the input that did not fit into appender again
          appenders(b).addData(h.values(b))
        case other =>
          println(s"Warning: response $other from appender.addData")
      }
    }
  }

  analyze()
}
