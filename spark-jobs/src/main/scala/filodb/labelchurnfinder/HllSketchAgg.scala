package filodb.labelchurnfinder

import java.nio.charset.StandardCharsets

import org.apache.datasketches.hll._
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

case class HllSketchAgg(lgK: Int = 12, tgt: TgtHllType = TgtHllType.HLL_4)
  extends Aggregator[String, HllSketch, Array[Byte]] with Serializable {

  // buffer zero: a fresh updatable sketch
  override def zero: HllSketch = new HllSketch(lgK, tgt)

  // add single input to the sketch
  override def reduce(buffer: HllSketch, input: String): HllSketch = {
    if (input != null) {
      buffer.update(input.getBytes(StandardCharsets.UTF_8))
    }
    buffer
  }

  // merge two sketches using Union (safe/easy path)
  override def merge(b1: HllSketch, b2: HllSketch): HllSketch = {
    val union = new Union(Math.max(lgK, Math.max(b1.getLgConfigK, b2.getLgConfigK)))
    union.update(b1)
    union.update(b2)
    union.getResult(tgt)
  }

  override def finish(reduction: HllSketch): Array[Byte] = {
    // Use compact representation (smaller) for storage/transfers
    reduction.toCompactByteArray
  }

  // encoders
  override def bufferEncoder: Encoder[HllSketch] = Encoders.kryo[HllSketch]
  override def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY
}

