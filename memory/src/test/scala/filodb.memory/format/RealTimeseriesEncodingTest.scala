package filodb.memory.format

import scala.io

import vectors._

class RealTimeseriesEncodingTest extends NativeVectorTest {
  val samples = io.Source.fromURL(getClass.getResource("/timeseries_samples.txt"))
                  .getLines.map(_.split(' '))
                  .map(ArrayStringRowReader).toSeq
  val timestampAppender = LongBinaryVector.appendingVectorNoNA(memFactory, samples.length)
  val valuesAppender = DoubleVector.appendingVectorNoNA(memFactory, samples.length)
  samples.foreach { reader =>
    timestampAppender.addData(reader.getLong(0))
    valuesAppender.addData(reader.getDouble(1))
  }

  // should compress into ~2 bytes per sample, give a little bit of slack
  val TargetBytesPerSample = 3.2

  it("should encode real timeseries efficiently") {
    timestampAppender.length shouldEqual samples.length
    valuesAppender.length shouldEqual samples.length

    val timestampPtr = timestampAppender.optimize(memFactory)
    val valuesPtr = valuesAppender.optimize(memFactory)

    // Both the timestamp and values are strictly increasing (though not exactly equally).
    // Both should be encoded into DeltaDelta vectors into less integer bits to save space

    val targetNumBytes = samples.length * TargetBytesPerSample
    println(s"${samples.length} samples, target encoded size = $targetNumBytes bytes")

    val timestampBytes = BinaryVector.totalBytes(timestampPtr)
    val valuesBytes    = BinaryVector.totalBytes(valuesPtr)

    println(s"timestamp bytes = $timestampBytes, values bytes = $valuesBytes")
    println(s"ts reader = ${LongBinaryVector(timestampPtr)}  values reader = ${DoubleVector(valuesPtr)}")

    (timestampBytes + valuesBytes).toDouble should be < (targetNumBytes)
  }
}