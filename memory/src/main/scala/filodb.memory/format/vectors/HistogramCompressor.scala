package filodb.memory.format.vectors

import scala.io.Source

import org.agrona.ExpandableArrayBuffer

import filodb.memory.NativeMemoryManager
import filodb.memory.format._

//scalastyle:off
// Input is a file with one line per histogram, bucket values are comma separated
// This app is designed to measure histogram compression ratios based on real world histogram data
object HistogramCompressor extends App {
  if (args.length < 1) {
    println("Usage: sbt memory/run <histogram-file> <numsamples>")
    println("Tests chunk compression, not runtime or performance, against real-world increasing bucket histogram files")
    sys.exit(0)
  }
  val inputFile = args(0)
  var numChunks = 0
  val numSamples = if (args.length > 1) args(1).toInt else 6000

  // TODO: switch to PageAlignedMemManager so the memory can be constantly recycled
  val memFactory = new NativeMemoryManager(500 * 1024 * 1024)
  val inputBuffer = new ExpandableArrayBuffer(4096)

  val maxBytes = 60 * 300   // Maximum allowable histogram writebuffer size
  val appender = HistogramVector.appending(memFactory, maxBytes)
  val bucketDef = HistogramBuckets.binaryBuckets64

  var binHistBytesSum = 0
  var binHistBytesMax = 0
  var numRecords = 0

  var encodedTotal = 0
  var writeBufferTotal = 0
  var samplesEncoded = 0

  val histograms = Source.fromFile(inputFile).getLines
                         .take(numSamples)
                         .map { line =>
                           val buckets = line.split(",").map(_.trim.toLong)
                           LongHistogram(bucketDef, buckets)
                         }

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
        val encodedSize = BinaryVector.totalBytes(optimized)
        println(s" WriteBuffer size: ${writeBufSize}\t\tEncoded size: $encodedSize")
        encodedTotal += encodedSize
        writeBufferTotal += writeBufSize
        numChunks += 1
        samplesEncoded += appender.length

        appender.reset()
        // Add back the input that did not fit into appender again
        appender.addData(buf)
      case other =>
        println(s"Warning: response $other from appender.addData")
    }
  }

  // Encode final chunk?  Or just forget it, because it will mess up size statistics?

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