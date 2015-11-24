package filodb.core.metadata

import com.clearspring.analytics.stream.membership.BloomFilter
import filodb.core.KeyType
import scodec.bits.ByteVector


/**
 * A KeySetDigest is a summary structure which helps determine the presence of a row key in a set of row keys.
 * Examples of this digest can be a BloomFilter of row keys or simply a HashSet or row keys.
 */
trait KeySetDigest extends Serializable{

  def contains(rowKey: Any): Boolean

  def keyType: KeyType

  def toBytes:ByteVector

}

/**
 * A KeySetDigest which uses a bloom filter to check the presence of a row key.
 */
class BloomDigest(val bloomFilter: BloomFilter, val keyType: KeyType) extends KeySetDigest {

  override def contains(rowKey: Any): Boolean = {
    bloomFilter.isPresent(keyType.toBytes(rowKey.asInstanceOf[keyType.T]).toArray)
  }

  override def toBytes: ByteVector = ByteVector(BloomFilter.serialize(bloomFilter))
}


object BloomDigest {

  def apply(rowKeys: Seq[_], helper: KeyType): BloomDigest = {
    val length = rowKeys.length
    val bloomFilter = new BloomFilter(length, length * 10)
    rowKeys.foreach { rowKey =>
      bloomFilter.add(helper.toBytes(rowKey.asInstanceOf[helper.T]).toArray)
    }
    new BloomDigest(bloomFilter, helper)
  }

  def apply(digestBytes: ByteVector, helper: KeyType): BloomDigest = {
    new BloomDigest(BloomFilter.deserialize(digestBytes.toArray), helper)
  }
}
