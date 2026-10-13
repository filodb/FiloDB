package filodb.labelchurnfinder

import org.apache.datasketches.hll._
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

/**
 * Merges serialized HLL sketch byte arrays produced by HllSketchAgg.
 * Used in the second phase of two-phase aggregation to combine partial
 * sketches built per salt bucket into the final per-(ws, label) sketch.
 */
case class HllSketchMergeAgg(lgK: Int = 12, tgt: TgtHllType = TgtHllType.HLL_4)
  extends Aggregator[Array[Byte], Array[Byte], Array[Byte]] with Serializable {

  override def zero: Array[Byte] = new HllSketch(lgK, tgt).toCompactByteArray

  override def reduce(buffer: Array[Byte], input: Array[Byte]): Array[Byte] = {
    if (input != null) {
      val s1 = HllSketch.heapify(buffer)
      val s2 = HllSketch.heapify(input)
      val union = new Union(Math.max(lgK, Math.max(s1.getLgConfigK, s2.getLgConfigK)))
      union.update(s1)
      union.update(s2)
      union.getResult(tgt).toCompactByteArray
    } else buffer
  }

  override def merge(b1: Array[Byte], b2: Array[Byte]): Array[Byte] = {
    val s1 = HllSketch.heapify(b1)
    val s2 = HllSketch.heapify(b2)
    val union = new Union(Math.max(lgK, Math.max(s1.getLgConfigK, s2.getLgConfigK)))
    union.update(s1)
    union.update(s2)
    union.getResult(tgt).toCompactByteArray
  }

  override def finish(reduction: Array[Byte]): Array[Byte] = reduction

  override def bufferEncoder: Encoder[Array[Byte]] = Encoders.BINARY
  override def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY
}
