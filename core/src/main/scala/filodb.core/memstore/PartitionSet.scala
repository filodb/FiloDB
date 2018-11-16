package filodb.core.memstore

/**
 * NOTE: This code was lifted from https://github.com/non/debox, all credit goes to the author of that repo
 * for this code other than a few mods specific to our ingest record/partition key stuff.
 *
 * This is a customized version of debox's very fast Set of TSPartitions.  What we needed were custom methods
 * for membership based not on a TSPartition, but on the partition key of incoming ingest BinaryRecords, to
 * match against the partition key of TSPartitions.   Unfortunately the original Set in debox is final, so we
 * have to copy code.  Also, BinaryRecord v2's do not have any Java object instances, so regular hashing and equals
 * methods do not work.
 *
 * We could have used a hack for Set of FiloPartition and wrapped incoming records with a FiloPartition and used
 * the hashCode and equals method of that, but it would result in more allocations, plus getting the hashCode and equals
 * to work properly between different types of BinaryRecords (ingest vs partition) is tricky.
 */

//scalastyle:off

import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.{specialized => sp}

import spire.syntax.all._

import filodb.core.binaryrecord2.{RecordComparator, RecordSchema}
import filodb.core.store.FiloPartition
import filodb.memory.BinaryRegionLarge

/**
 * Set is a mutable hash set, with open addressing and double hashing.
 *
 * Set provides constant-time membership tests, and amortized
 * constant-time addition and removal. One underlying array stores
 * items, and another tracks which buckets are used and defined.
 *
 * When the type A is known (or the caller is specialized on A),
 * Set[A] will store the values in an unboxed array.
 */
final class PartitionSet(as: Array[FiloPartition], bs: Array[Byte], n: Int, u: Int,
                         ingestSchema: RecordSchema, comp: RecordComparator) extends Serializable { lhs =>
  // set machinery
  var items: Array[FiloPartition] = as      // slots for items
  var buckets: Array[Byte] = bs // buckets track defined/used slots
  var len: Int = n              // number of defined slots
  var used: Int = u             // number of used slots (used >= len)

  // hashing internals
  var mask: Int = buckets.length - 1             // size-1, used for hashing
  var limit: Int = (buckets.length * 0.65).toInt // point at which we should grow

  /**
   * Check if two Sets are equal.
   *
   * Equal means the sets have the same type (which is checked
   * using the ClassTag instances) and the same contents.
   *
   * Comparing Sets with any of Scala's collection types will
   * return false.
   *
   * On average this is an O(n) operation. In some cases a false
   * result can be returned more quickly.
   */
  override def equals(that: Any): Boolean =
    that match {
      case that: PartitionSet =>
        if (size != that.size) return false
        forall(that.apply)
      case _ =>
        false
    }

  /**
   * Hash the contents of the set to an Int value.
   *
   * By xor'ing all the set's values together, we can be sure that
   * sets with the same contents will have the same hashCode
   * regardless of the order those elements appear.
   *
   * This is an O(n) operation.
   */
  override def hashCode: Int = fold(0xdeadd065)(_ ^ _.##)

  /**
   * Return a string representation of the contents of the set.
   *
   * This is an O(n) operation.
   */
  override def toString: String = {
    val sb = new StringBuilder
    sb.append("Set(")
    var i = 0
    while (i < buckets.length && buckets(i) != 3) i += 1
    if (i < buckets.length) {
      sb.append(items(i).toString)
      i += 1
    }
    while (i < buckets.length) {
      if (buckets(i) == 3) {
        sb.append(", ")
        sb.append(items(i).toString)
      }
      i += 1
    }
    sb.append(")")
    sb.toString
  }

  /**
   * Return the size of this Set as an Int.
   *
   * Since Sets use arrays, their size is limited to what a 32-bit
   * signed integer can represent.
   *
   * This is an O(1) operation.
   */
  final def size: Int = len

  /**
   * Return true if the Set is empty, false otherwise.
   *
   * This is an O(1) operation.
   */
  final def isEmpty: Boolean = len == 0

  /**
   * Return true if the Set is non-empty, false otherwise.
   *
   * This is an O(1) operation.
   */
  final def nonEmpty: Boolean = len > 0

  /**
   * Return whether the item is found in the Set or not.
   *
   * On average, this is an O(1) operation; the (unlikely) worst-case
   * is O(n).
   */
  final def apply(item: FiloPartition): Boolean = {
    @inline @tailrec def loop(i: Int, perturbation: Int): Boolean = {
      val j = i & mask
      val status = buckets(j)
      if (status == 0) {
        false
      } else if (status == 3 && items(j) == item) {
        true
      } else {
        loop((i << 2) + i + perturbation + 1, perturbation >> 5)
      }
    }
    val i = item.## & 0x7fffffff
    loop(i, i)
  }

  /**
   * THIS is the custom method we are adding, used during ingestion.
   * Use custom hashcode and comparison to quickly find if the partition exists for that ingestion BinaryRecord
   * if it does then return it.
   * If it does not exist, then call the method to return a new TSPartition object and add it
   * Returns the existing or newly added TSPartition object either way
   *
   * @param ingestBase the Base/byte[] pointing to the ingestion BinaryRecord
   * @param ingestOffset the offset/address of the ingestion BinaryRecord
   * @param addFunc a no-arg function to create the FiloPartition if it cannot be found
   */
  final def getOrAddWithIngestBR(ingestBase: Any, ingestOffset: Long,
                                 addFunc: => FiloPartition): FiloPartition = {
    getWithIngestBR(ingestBase, ingestOffset) match {
      case null =>
        val newItem = addFunc
        if (newItem != null) add(newItem)
        newItem
      case existing => existing
    }
  }

  /**
   * Returns the partition that matches the partition key in an ingest record, or NULL if it doesn't exist
   */
  final def getWithIngestBR(ingestBase: Any, ingestOffset: Long): FiloPartition = {
    @inline @tailrec def loop(i: Int, perturbation: Int): FiloPartition = {
      val j = i & mask
      val status = buckets(j)
      if (status == 0) {
        null
      } else if (status == 3 && comp.partitionMatch(ingestBase, ingestOffset, null, items(j).partKeyOffset)) {
        items(j)
      } else {
        loop((i << 2) + i + perturbation + 1, perturbation >> 5)
      }
    }
    val i = ingestSchema.partitionHash(ingestBase, ingestOffset) & 0x7fffffff
    loop(i, i)
  }

  /**
   * Searches for and returns Some(tsPartition) if a partition exists with a key matching the passed in
   * partition key BinaryRecord.  Otherwise, None is returned.
   */
  final def getWithPartKeyBR(partBase: Any, partOffset: Long): Option[FiloPartition] = {
    @inline @tailrec def loop(i: Int, perturbation: Int): Option[FiloPartition] = {
      val j = i & mask
      val status = buckets(j)
      if (status == 0) {
        None
      } else if (status == 3 &&
                 BinaryRegionLarge.equals(partBase, partOffset, items(j).partKeyBase, items(j).partKeyOffset)) {
        Some(items(j))
      } else {
        loop((i << 2) + i + perturbation + 1, perturbation >> 5)
      }
    }
    val i = comp.partitionKeySchema.partitionHash(partBase, partOffset) & 0x7fffffff
    loop(i, i)
  }

  /**
   * Make a (shallow) copy of this set.
   *
   * This method creates a copy of the set with the same
   * structure. However, the actual elements will not be copied.
   *
   * This is an O(n) operation.
   */
  final def copy(): PartitionSet = new PartitionSet(items.clone, buckets.clone, len, used, ingestSchema, comp)

  /**
   * Clears the set's internal state.
   *
   * After calling this method, the set's state is identical to that
   * obtained by calling Set.empty[A].
   *
   * The previous arrays are not retained, and will become available
   * for garbage collection. This method returns a null of type
   * Unit to trigger specialization without allocating an actual
   * instance.
   *
   * This is an O(1) operation, but may generate a lot of garbage if
   * the set was previously large.
   */
  final def clear(): Unit = { absorb(PartitionSet.empty(ingestSchema, comp)) }

  /**
   * Aborb the given set's contents into this set.
   *
   * This method does not copy the other set's contents. Thus, this
   * should only be used when there are no saved references to the
   * other set. It is private, and exists primarily to simplify the
   * implementation of certain methods.
   *
   * This is an O(1) operation, although it can potentially generate a
   * lot of garbage (if the set was previously large).
   */
  private[this] def absorb(that: PartitionSet): Unit = {
    items = that.items
    buckets = that.buckets
    len = that.len
    used = that.used
    mask = that.mask
    limit = that.limit
  }

  /**
   * Synonym for +=.
   */
  final def add(item: FiloPartition): Boolean = this += item

  /**
   * Add item to the set.
   *
   * Returns whether or not the item was added. If item was already in
   * the set, this method will do nothing and return false.
   *
   * On average, this is an amortized O(1) operation; the worst-case
   * is O(n), which will occur when the set must be resized.
   */
  def +=(item: FiloPartition): Boolean = {
    @inline @tailrec def loop(i: Int, perturbation: Int): Boolean = {
      val j = i & mask
      val status = buckets(j)
      if (status == 3) {
        if (items(j) == item)
          false
        else
          loop((i << 2) + i + perturbation + 1, perturbation >> 5)
      } else if (status == 2 && apply(item)) {
        false
      } else {
        items(j) = item
        buckets(j) = 3
        len += 1
        if (status == 0) {
          used += 1
          if (used > limit) grow()
        }
        true
      }
    }
    val i = item.## & 0x7fffffff
    loop(i, i)
  }

  /**
   * Add every item in items to the set.
   *
   * This is an O(n) operation, where n is the size of items.
   */
  def ++=(items: Iterable[FiloPartition]): Unit =
    items.foreach(this += _)

  /**
   * Add every item in items to the set.
   *
   * This is an O(n) operation, where n is the size of items.
   */
  def ++=(arr: Array[FiloPartition]): Unit =
    cfor(0)(_ < arr.length, _ + 1) { this += arr(_) }

  /**
   * Synonym for -=.
   */
  def remove(item: FiloPartition): Boolean = this -= item

  /**
   * Remove an item from the set.
   *
   * Returns whether the item was originally in the set or not.
   *
   * This is an amortized O(1) operation.
   */
  final def -=(item: FiloPartition): Boolean = {
    @inline @tailrec def loop(i: Int, perturbation: Int): Boolean = {
      val j = i & mask
      val status = buckets(j)
      if (status == 3 && items(j) == item) {
        buckets(j) = 2
        len -= 1
        true
      } else if (status == 0) {
        false
      } else {
        loop((i << 2) + i + perturbation + 1, perturbation >> 5)
      }
    }
    val i = item.## & 0x7fffffff
    loop(i, i)
  }

  /**
   * Set/unset the value of item in the set.
   *
   * Like += and -= this is an amortized O(1) operation.
   */
  final def update(item: FiloPartition, b: Boolean) =
    if (b) this += item else this -= item

  /**
   * Loop over the set's contents, appying f to each element.
   *
   * There is no guaranteed order that the set's elements will be
   * traversed in, so use of foreach should not rely on a particular
   * order.
   *
   * This is an O(n) operation, where n is the length of the buffer.
   */
  def foreach(f: FiloPartition => Unit): Unit =
    cfor(0)(_ < buckets.length, _ + 1) { i =>
      if (buckets(i) == 3) f(items(i))
    }

  /**
   * Translate this Set into another Set using the given function f.
   *
   * Note that the resulting set may be smaller than this set, if f is
   * not a one-to-one function (an injection).
   *
   * This is an O(n) operation, where n is the size of the set.
   */
  def map[@sp(Short, Char, Int, Float, Long, Double, AnyRef) B: ClassTag](f: FiloPartition => B): debox.Set[B] = {
    val out = debox.Set.ofSize[B](len)
    cfor(0)(_ < buckets.length, _ + 1) { i =>
      if (buckets(i) == 3) out.add(f(items(i)))
    }
    if (out.size < len / 3) out.compact
    out
  }

  /**
   * Fold the set's values into a single value, using the provided
   * initial state and combining function f.
   *
   * Like foreach, fold makes no guarantees about the order that
   * elements will be reached.
   *
   * This is an O(n) operation, where n is the size of the set.
   */
  def fold[@sp(Int, Long, Double, AnyRef) B](init: B)(f: (B, FiloPartition) => B): B = {
    var result = init
    cfor(0)(_ < buckets.length, _ + 1) { i =>
      if (buckets(i) == 3) result = f(result, items(i))
    }
    result
  }

  /**
   * Grow the underlying array to best accommodate the set's size.
   *
   * To preserve hashing access speed, the set's size should never be
   * more than 66% of the underlying array's size. When this size is
   * reached, the set needs to be updated (using this method) to have a
   * larger array.
   *
   * The underlying array's size must always be a multiple of 2, which
   * means this method grows the array's size by 2x (or 4x if the set
   * is very small). This doubling helps amortize the cost of
   * resizing, since as the set gets larger growth will happen less
   * frequently. This method returns a null of type Unit to
   * trigger specialization without allocating an actual instance.
   *
   * Growing is an O(n) operation, where n is the set's size.
   */
  final def grow(): Unit = {
    val next = buckets.length * (if (buckets.length < 10000) 4 else 2)
    val set = PartitionSet.ofAllocatedSize(next, ingestSchema, comp)
    cfor(0)(_ < buckets.length, _ + 1) { i =>
      if (buckets(i) == 3) set += items(i)
    }
    absorb(set)
  }

  /**
   * Compacts the set's internal arrays to reduce memory usage.
   *
   * This operation should be used if a set has been shrunk
   * (e.g. through --=) and is not likely to grow again.
   *
   * This method will shrink the set to the smallest possible size
   * that allows it to be <66% full. It returns a null of type
   * Unit to trigger specialization without allocating an actual
   * instance.
   *
   * This is an O(n) operation, where n it the set's size.
   */
  final def compact(): Unit = {
    val set = PartitionSet.ofSize(len, ingestSchema, comp)
    cfor(0)(_ < buckets.length, _ + 1) { i =>
      if (buckets(i) == 3) set += items(i)
    }
    absorb(set)
  }

  // For a lot of the following methods, there are size tests to try
  // to make sure we're looping over the smaller of the two
  // sets. Some things to keep in mind:
  //
  // 1. System.arraycopy is faster than a loop.
  // 2. We want to avoid copying a large object and then shrinking it.
  // 3. & and | are symmetric (but -- is not). They all require a copy.
  // 4. &=, |=, and --= are not symmetric (they modify the lhs).
  //
  // So where possible we'd like to be looping over a smaller set,
  // doing membership tests against a larger set.

  /**
   * Union this set with the rhs set.
   *
   * This has the effect of adding all members of rhs to lhs.
   *
   * This is an O(n) operation, where n is rhs.size.
   */
  def |=(rhs: PartitionSet): Unit =
    if (lhs.size >= rhs.size) {
      cfor(0)(_ < rhs.buckets.length, _ + 1) { i =>
        if (rhs.buckets(i) == 3) lhs += rhs.items(i)
      }
    } else {
      val out = rhs.copy
      out |= lhs
      lhs.absorb(out)
    }

  /**
   * Synonym for |.
   */
  def union(rhs: PartitionSet): PartitionSet = lhs | rhs

  /**
   * Return new set which is the union of lhs and rhs.
   *
   * The new set will contain all members of lhs and rhs.
   *
   * This is an O(m max n) operation, where m and n are the sizes of
   * the sets.
   */
  def |(rhs: PartitionSet): PartitionSet =
    if (lhs.size >= rhs.size) {
      val out = lhs.copy
      out |= rhs
      out
    } else {
      val out = rhs.copy
      out |= lhs
      out
    }

  /**
   * Remove any member of this which is not in rhs.
   *
   * This is an O(m min n) operation, where m and n are the sizes of
   * the lhs and rhs sets.
   */
  def &=(rhs: PartitionSet): Unit =
    if (lhs.size <= rhs.size) {
      cfor(0)(_ < buckets.length, _ + 1) { i =>
        if (buckets(i) == 3 && !rhs(items(i))) {
          buckets(i) = 2
          len -= 1
        }
      }
    } else {
      val out = rhs.copy
      out &= lhs
      lhs.absorb(out)
    }

  /**
   * Synonym for &.
   */
  def intersection(rhs: PartitionSet): PartitionSet = this & rhs

  /**
   * Intersect this set with the rhs set.
   *
   * This has the effect of removing any item not in rhs.
   *
   * This is an O(m min n) operation, where m and n are the sizes of
   * the lhs and rhs sets.
   */
  def &(rhs: PartitionSet): PartitionSet =
    if (lhs.size <= rhs.size) {
      val out = lhs.copy
      out &= rhs
      out
    } else {
      val out = rhs.copy
      out &= lhs
      out
    }

  /**
   * Remove members of rhs from the set.
   *
   * This operation is an O(m min n) operation, where m and n are the
   * sizes of the lhs and rhs sets.
   */
  def --=(rhs: PartitionSet): Unit =
    if (lhs.size >= rhs.size) {
      cfor(0)(_ < rhs.buckets.length, _ + 1) { i =>
        if (rhs.buckets(i) == 3) lhs -= rhs.items(i)
      }
    } else {
      cfor(0)(_ < buckets.length, _ + 1) { i =>
        if (buckets(i) == 3 && rhs(items(i))) {
          buckets(i) = 2
          len -= 1
        }
      }
    }

  /**
   * This is a synonym for --.
   */
  def difference(rhs: PartitionSet): PartitionSet = lhs -- rhs

  /**
   * Create a new set with the elements of lhs that are not in rhs.
   *
   * This is an O(n) operation, where n is the size of the set.
   */
  def --(rhs: PartitionSet): PartitionSet = {
    val out = lhs.copy
    out --= rhs
    out
  }

  /**
   * Count how many elements of the set satisfy the predicate p.
   *
   * This is an O(n) operation, where n is the size of the set.
   */
  def count(p: FiloPartition => Boolean): Int =
    fold(0)((n, a) => if (p(a)) n + 1 else n)

  /**
   * Determine if every member of the set satisfies the predicate p.
   *
   * This is an O(n) operation, where n is the size of the
   * set. However, it will return as soon as a false result is
   * obtained.
   */
  def forall(p: FiloPartition => Boolean): Boolean = {
    cfor(0)(_ < buckets.length, _ + 1) { i =>
      if (buckets(i) == 3 && !p(items(i))) return false
    }
    true
  }

  /**
   * Determine if any member of the set satisfies the predicate p.
   *
   * This is an O(n) operation, where n is the size of the
   * set. However, it will return as soon as a true result is
   * obtained.
   */
  def exists(p: FiloPartition => Boolean): Boolean = {
    cfor(0)(_ < buckets.length, _ + 1) { i =>
      if (buckets(i) == 3 && p(items(i))) return true
    }
    false
  }

  /**
   * Find a member of the set that satisfies the predicate p.
   *
   * The method returns Some(item) if item satisfies p, and None if
   * none of set's elements satisfy p. Since Set is not ordered, if
   * multiple elements satisfy the predicate there is no guarantee
   * which one wil be found.
   *
   * This is an O(n) operation, where n is the size of the
   * set. However, it will return as soon as a member satisfying the
   * predicate is found.
   */
  def find(p: FiloPartition => Boolean): Option[FiloPartition] = {
    cfor(0)(_ < buckets.length, _ + 1) { i =>
      if (buckets(i) == 3) {
        val a = items(i)
        if (p(a)) return Some(a)
      }
    }
    None
  }

  /**
   * Create a new set containing all the members of this set that
   * satisfy p.
   *
   * This is an O(n) operation, where n is the size of the set.
   */
  def findAll(p: FiloPartition => Boolean): PartitionSet = {
    val out = PartitionSet.empty(lhs.ingestSchema, lhs.comp)
    cfor(0)(_ < buckets.length, _ + 1) { i =>
      if (buckets(i) == 3 && p(items(i))) out += items(i)
    }
    out
  }

  /**
   * Remove any member of the set that does not satisfy p.
   *
   * After this method, all membrers of the set will satisfy p.
   *
   * This is an O(n) operation, where n is the size of the set.
   */
  def filterSelf(p: FiloPartition => Boolean): Unit =
    cfor(0)(_ < buckets.length, _ + 1) { i =>
      if (buckets(i) == 3 && !p(items(i))) {
        buckets(i) = 2
        len -= 1
      }
    }

  /**
   * Return an iterator over this set's contents.
   *
   * This method does not do any copying or locking. Thus, if the set
   * is modified while the iterator is "live" the results will be
   * undefined and probably bad. Also, since sets are not ordered,
   * there is no guarantee elements will be returned in a particular
   * order.
   *
   * Use this.copy.iterator to get a "clean" iterator if needed.
   *
   * Creating the iterator is an O(1) operation.
   */
  def iterator(): Iterator[FiloPartition] = {
    var i = 0
    while (i < buckets.length && buckets(i) != 3) i += 1
    new Iterator[FiloPartition] {
      var index = i
      def hasNext: Boolean = index < buckets.length
      def next: FiloPartition = {
        val item = items(index)
        index += 1
        while (index < buckets.length && buckets(index) != 3) index += 1
        item
      }
    }
  }

  /**
   * Wrap this set in an Iterable[A] instance.
   *
   * This method exists as a cheap way to get compatibility with Scala
   * collections without copying/conversion. Note that since Scala
   * collections are not specialized, using this iterable will box
   * values as they are accessed (although the underlying set will
   * still be unboxed).
   *
   * Like iterator, this method directly wraps the set. Thus, you
   * should not mutate the set while using the resulting iterable, or
   * risk corruption and undefined behavior.
   *
   * To get a "safe" value that is compatible with Scala collections,
   * consider using toScalaSet.
   *
   * Creating the Iterable[A] instance is an O(1) operation.
   */
  def toIterable(): Iterable[FiloPartition] =
    new Iterable[FiloPartition] {
      override def size: Int = lhs.size
      def iterator: Iterator[FiloPartition] = lhs.iterator
      override def foreach[U](f: FiloPartition => U): Unit = lhs.foreach(a => f(a))
    }

  /**
   * Create an immutable instance of scala's PartitionSet.
   *
   * This method copies the elements into a new instance which is
   * compatible with Scala's collections and PartitionSet type.
   *
   * This is an O(n) operation, where n is the size of the set.
   */
  def toScalaSet(): scala.collection.immutable.Set[FiloPartition] =
    iterator.toSet
}


object PartitionSet {
  /**
   * Allocate an empty Set.
   */
  def empty(ingestSchema: RecordSchema, comp: RecordComparator): PartitionSet =
    new PartitionSet(new Array[FiloPartition](8), new Array[Byte](8), 0, 0, ingestSchema, comp)

  /**
   * Allocate an empty Set, capable of holding n items without
   * resizing itself.
   *
   * This method is useful if you know you'll be adding a large number
   * of elements in advance and you want to save a few resizes.
   */
  def ofSize(n: Int, ingestSchema: RecordSchema, comp: RecordComparator): PartitionSet =
    ofAllocatedSize(n / 2 * 3, ingestSchema, comp)

  /**
   * Allocate an empty Set, with underlying storage of size n.
   *
   * This method is useful if you know exactly how big you want the
   * underlying array to be. In most cases ofSize() is probably what
   * you want instead.
   */
  private def ofAllocatedSize(n: Int, ingestSchema: RecordSchema, comp: RecordComparator): PartitionSet = {
    val sz = debox.Util.nextPowerOfTwo(n) match {
      case n if n < 0 => throw debox.DeboxOverflowError(n)
      case 0 => 8
      case n => n
    }
    new PartitionSet(new Array[FiloPartition](sz), new Array[Byte](sz), 0, 0, ingestSchema, comp)
  }
}
