package filodb.core.metadata

import com.clearspring.analytics.stream.membership.BloomFilter
import filodb.core.KeyType
import scodec.bits.ByteVector


/**
 * A KeySetDigest is a summary structure which helps determine the presence of a row key in a set of row keys.
 * Examples of this digest can be a BloomFilter of row keys or simply a HashSet or row keys.
 */
trait KeySetDigest extends Serializable {

  def contains(rowKey: Any): Boolean

  def keyType: KeyType

  def toBytes: ByteVector

  def memoryInBytes(n: Int, p: Double = 0.0001): Double

}

/**
 * A KeySetDigest which uses a bloom filter to check the presence of a row key.
 */
class BloomDigest(val bloomFilter: BloomFilter, val keyType: KeyType) extends KeySetDigest {

  override def contains(rowKey: Any): Boolean = {
    bloomFilter.isPresent(keyType.toBytes(rowKey.asInstanceOf[keyType.T])._2.toArray)
  }

  override def toBytes: ByteVector = ByteVector(BloomFilter.serialize(bloomFilter))

  import java.lang.Math._

  override def memoryInBytes(n: Int, p: Double = 0.0001): Double
  = ceil((n * log(p)) / log(1.0 / pow(2.0, log(2.0))))

}


object BloomDigest {

  def apply(rowKeys: Seq[_], helper: KeyType): BloomDigest = {
    val length = rowKeys.length
    // 1 in 100 false positives
    val bloomFilter = new BloomFilter(length, 0.0001)
    rowKeys.foreach { rowKey =>
      bloomFilter.add(helper.toBytes(rowKey.asInstanceOf[helper.T])._2.toArray)
    }
    new BloomDigest(bloomFilter, helper)
  }

  def apply(digestBytes: ByteVector, helper: KeyType): BloomDigest = {
    new BloomDigest(BloomFilter.deserialize(digestBytes.toArray), helper)
  }
}
