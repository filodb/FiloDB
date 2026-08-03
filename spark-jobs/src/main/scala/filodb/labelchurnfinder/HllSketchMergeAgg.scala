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
  extends Aggregator[Array[Byte], HllSketch, Array[Byte]] with Serializable {

  override def zero: HllSketch = new HllSketch(lgK, tgt)

  override def reduce(buffer: HllSketch, input: Array[Byte]): HllSketch = {
    if (input != null) {
      val union = new Union(Math.max(lgK, buffer.getLgConfigK))
      union.update(buffer)
      union.update(HllSketch.heapify(input))
      union.getResult(tgt)
    } else buffer
  }

  override def merge(b1: HllSketch, b2: HllSketch): HllSketch = {
    val union = new Union(Math.max(lgK, Math.max(b1.getLgConfigK, b2.getLgConfigK)))
    union.update(b1)
    union.update(b2)
    union.getResult(tgt)
  }

  override def finish(reduction: HllSketch): Array[Byte] = reduction.toCompactByteArray

  override def bufferEncoder: Encoder[HllSketch]   = Encoders.kryo[HllSketch]
  override def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY
}
