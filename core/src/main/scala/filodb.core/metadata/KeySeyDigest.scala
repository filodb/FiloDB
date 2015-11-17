package filodb.core.metadata

import com.clearspring.analytics.stream.membership.BloomFilter
import filodb.core.KeyType
import scodec.bits.ByteVector


/**
 * A KeySetDigest is a summary structure which helps determine the presence of a row key in a set of row keys.
 * Examples of this digest can be a BloomFilter of row keys or simply a HashSet or row keys.
 */
trait KeySetDigest {

  def contains[K: KeyType](rowKey: K): Boolean

}

/**
 * A KeySetDigest which uses a bloom filter to check the presence of a row key.
 */
class BloomDigest(bloomFilter: BloomFilter) extends KeySetDigest {

  override def contains[K: KeyType](rowKey: K): Boolean = {
    val helper = implicitly[KeyType[K]]
    bloomFilter.isPresent(helper.toBytes(rowKey).toArray)
  }
}


object BloomDigest {

  def apply[K](rowKeys: Seq[K], helper: KeyType[K]): BloomDigest = {
    val length = rowKeys.length
    val bloomFilter = new BloomFilter(length, length * 10)
    rowKeys.foreach { rowKey =>
      bloomFilter.add(helper.toBytes(rowKey).toArray)
    }
    new BloomDigest(bloomFilter)
  }

  def apply[K](digestBytes: ByteVector,helper: KeyType[K]): BloomDigest = {
    new BloomDigest(BloomFilter.deserialize(digestBytes.toArray))
  }
}
