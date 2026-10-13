package filodb.labelchurnfinder

import java.nio.charset.StandardCharsets

import org.apache.datasketches.hll._
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

case class HllSketchAgg(lgK: Int = 12, tgt: TgtHllType = TgtHllType.HLL_4)
  extends Aggregator[String, Array[Byte], Array[Byte]] with Serializable {

  // buffer zero: a fresh updatable sketch
  override def zero: Array[Byte] = new HllSketch(lgK, tgt).toCompactByteArray

  // add single input to the sketch
  override def reduce(buffer: Array[Byte], input: String): Array[Byte] = {
    if (input != null) {
      val sketch = HllSketch.heapify(buffer)
      sketch.update(input.getBytes(StandardCharsets.UTF_8))
      sketch.toCompactByteArray
    } else buffer
  }

  // merge two sketches using Union (safe/easy path)
  override def merge(b1: Array[Byte], b2: Array[Byte]): Array[Byte] = {
    val s1 = HllSketch.heapify(b1)
    val s2 = HllSketch.heapify(b2)
    val union = new Union(Math.max(lgK, Math.max(s1.getLgConfigK, s2.getLgConfigK)))
    union.update(s1)
    union.update(s2)
    union.getResult(tgt).toCompactByteArray
  }

  override def finish(reduction: Array[Byte]): Array[Byte] = {
    // Use compact representation (smaller) for storage/transfers
    reduction
  }

  // encoders
  override def bufferEncoder: Encoder[Array[Byte]] = Encoders.BINARY
  override def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY
}
