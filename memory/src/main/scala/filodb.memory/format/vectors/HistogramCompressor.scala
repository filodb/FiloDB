package filodb.memory.format.vectors

import scala.io.Source

import org.agrona.concurrent.UnsafeBuffer

import filodb.memory.NativeMemoryManager

//scalastyle:off
// Input is a file with one line per histogram, bucket values are comma separated
// This app is designed to measure histogram compression ratios based on real world histogram data
object HistogramCompressor extends App {
  if (args.length < 1) {
    println("Usage: sbt memory/run <histogram-file> <numHistogramsPerChunk>")
    println("Tests chunk compression, not runtime or performance, against real-world increasing bucket histogram files")
    sys.exit(0)
  }
  val inputFile = args(0)
  val chunkLength = if (args.length > 1) args(1).toInt else 300
  val numChunks = 20

  val memFactory = new NativeMemoryManager(500 * 1024 * 1024)
  val inputBuffer = new UnsafeBuffer(new Array[Byte](8192))

  val appender = HistogramVector.appendingColumnar(memFactory, 64, chunkLength)
  val bucketDef = HistogramBuckets.binaryBuckets64Bytes

  var binHistBytesSum = 0
  var binHistBytesMax = 0
  var numRecords = 0

  var encodedTotal = 0
  var writeBufferTotal = 0

  Source.fromFile(inputFile).getLines
        .take(numChunks * chunkLength)
        .grouped(chunkLength).foreach { chunkLines =>
    // Ingest each histogram, parse and create a BinaryHistogram, then ingest into our histogram column
    chunkLines.foreach { line =>
      val buckets = line.split(",").map(_.trim.toLong)
      val histSize = BinaryHistogram.writeBinHistogram(bucketDef, buckets, inputBuffer)
      numRecords += 1
      binHistBytesMax = Math.max(binHistBytesMax, histSize)
      binHistBytesSum += histSize

      appender.addData(inputBuffer)
    }

    // Optimize and get optimized size, dump out, aggregate
    val writeBufSize = HistogramVector.columnarTotalSize(appender.addr)
    val optimized = appender.optimize(memFactory)
    val encodedSize = HistogramVector.columnarTotalSize(optimized)
    println(s" WriteBuffer size: ${writeBufSize}\t\tEncoded size: $encodedSize")
    encodedTotal += encodedSize
    writeBufferTotal += writeBufSize
  }

  // Dump out overall aggregates
  val avgEncoded = encodedTotal.toDouble / numChunks
  val avgWriteBuf = writeBufferTotal.toDouble / numChunks
  println(s"Average encoded chunk size: $avgEncoded")
  println(s"Average write buffer size:  $avgWriteBuf")
  println(s"Compression ratio: ${avgWriteBuf / avgEncoded}")
  println(s"Average binHistogram size: ${binHistBytesSum / numRecords.toDouble}")
  println(s"Max binHistogram size: $binHistBytesMax bytes")
}