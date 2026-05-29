package filodb.core.binaryrecord2

import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.StrictLogging
import org.agrona.DirectBuffer
import spire.syntax.cfor._

import filodb.core.binaryrecord2.RecordSchema.schemaID
import filodb.core.metadata.{Column, DatasetOptions, PartitionSchema, Schema, Schemas}
import filodb.core.metadata.Column.ColumnType.{DoubleColumn, LongColumn, MapColumn, StringColumn}
import filodb.core.query.ColumnInfo
import filodb.memory._
import filodb.memory.format.{RowReader, SeqRowReader, UnsafeUtils, ZeroCopyUTF8String => ZCUTF8}
import filodb.memory.format.vectors.Histogram


// scalastyle:off number.of.methods
/**
 * A RecordBuilder allocates fixed size containers and builds BinaryRecords within them.
 * The size of the container should be much larger than the average size of a record for efficiency.
 * Many BinaryRecords are built within one container.
 * This is very much a mutable, stateful class and should be run within a single thread or stream of execution.
 * It is NOT multi-thread safe.
 * The idea is to use one RecordBuilder per context/stream/thread. The context should make sense; as the list of
 * containers can then be 1) sent over the wire, with no further transformations needed, 2) obtained and maybe freed
 *
 * @param memFactory the MemFactory used to allocate containers for building BinaryRecords in
 * @param containerSize the size of each container
 * @param reuseOneContainer if true, resets the container when we run out of container space.  Designed for scenario
 *                   where one copies the BinaryRecord somewhere else every time, and allocation is minimized by
 *                   reusing the same container over and over.
 */
class RecordBuilder(memFactory: MemFactory,
                    containerSize: Int = RecordBuilder.DefaultContainerSize,
                    reuseOneContainer: Boolean = false) extends RecordBuilderBase with StrictLogging {
  import RecordBuilder._
  import UnsafeUtils._
  require(containerSize >= RecordBuilder.MinContainerSize, s"RecordBuilder.containerSize < minimum")

  private val containers = new collection.mutable.ArrayBuffer[RecordContainer]

  if (reuseOneContainer) newContainer()

  /**
   * Override to return a different clock, intended when running tests.
   */
  def currentTimeMillis: Long = System.currentTimeMillis()

  // Reset last container and all pointers
  def reset(skipTime: Boolean = false): Unit = if (containers.nonEmpty) {
    resetContainerPointers()
    if (!skipTime) containers.last.updateTimestamp(currentTimeMillis)
    fieldNo = -1
    mapOffset = -1L
    recHash = -1
  }

  // Only reset the container offsets, but not the fieldNo, mapOffset, recHash
  private def resetContainerPointers(): Unit = {
    curRecordOffset = containers.last.offset + ContainerHeaderLen
    curRecEndOffset = curRecordOffset
    containers.last.updateLengthWithOffset(curRecordOffset)
    curBase = containers.last.base
  }


  import Column.ColumnType._


  /**
   * Used only internally by RecordComparator etc. to shortcut create a new BR by copying bytes from an existing BR.
   * Namely, from an ingestion record (schema, fixed area) to a partition key only record.
   * You BETTER know what you are doing.
   */
  private[binaryrecord2] def copyFixedAreasFrom(base: Any, offset: Long, fixedOffset: Int, numBytes: Int): Unit = {
    require(curRecEndOffset == curRecordOffset, s"Illegal state: $curRecEndOffset != $curRecordOffset")
    requireBytes(numBytes + 6)

    // write length header, copy bytes, and update RecEndOffset
    setInt(curBase, curRecordOffset, numBytes + 2)
    UnsafeUtils.setShort(curBase, curRecordOffset + 4, UnsafeUtils.getShort(base, offset + 4))
    UnsafeUtils.unsafe.copyMemory(base, offset + fixedOffset, curBase, curRecordOffset + 6, numBytes)
    curRecEndOffset = curRecordOffset + numBytes + 6
  }

  // Extend current variable area with stuff from somewhere else
  private[binaryrecord2] def copyVarAreasFrom(base: Any, offset: Long, numBytes: Int): Unit = {
    requireBytes(numBytes)
    UnsafeUtils.unsafe.copyMemory(base, offset, curBase, curRecEndOffset, numBytes)
    // Increase length of current BR.  Then bump curRecEndOffset so we are consistent
    setInt(curBase, curRecordOffset, getInt(curBase, curRecordOffset) + numBytes)
    curRecEndOffset += numBytes
  }

  private[binaryrecord2] def adjustFieldOffset(fieldNo: Int, adjustment: Int): Unit = {
    val offset = curRecordOffset + schema.fieldOffset(fieldNo)
    UnsafeUtils.setInt(curBase, offset, UnsafeUtils.getInt(curBase, offset) + adjustment)
  }

  // resets or empties current container.  Only used for testing.  Also ensures there's at least one container
  private[filodb] def resetCurrent(): Unit = {
    if (containers.isEmpty) requireBytes(100)
    curBase = currentContainer.get.base
    curRecordOffset = currentContainer.get.offset + 4
    currentContainer.get.updateLengthWithOffset(curRecordOffset)
    curRecEndOffset = curRecordOffset
  }

  /**
   * Returns Some(container) reference to the current RecordContainer or None if there is no container
   */
  def currentContainer: Option[RecordContainer] = containers.lastOption

  /**
   * Returns the list of all current containers
   */
  def allContainers: Seq[RecordContainer] = containers.toSeq

  def lastContainer: RecordContainer = containers.last

  // Used for debugging...  throws exception if there is no data.  Be careful here.
  def curContainerBase: Any = currentContainer.get.base


  /**
   * Returns all the full containers other than currentContainer as byte arrays.
   * Assuming all of the containers except the last one is full,
   * calls array() on all the non-last container excluding the currentContainer.
   * The memFactory needs to be an on heap one otherwise UnsupportedOperationException will be thrown.
   * The sequence of byte arrays can be for example sent to Kafka as a sequence of messages - one message
   * per byte array.
   * @param reset if true, clears out all the containers other than lastContainer.
   *              Allows a producer of containers to obtain the
   *              byte arrays for sending somewhere else, while clearing containers for the next batch.
   */
  def nonCurrentContainerBytes(reset: Boolean = false): Seq[Array[Byte]] = {
    val bytes = allContainers.dropRight(1).map(_.array)
    if (reset) removeAndFreeContainers(containers.size - 1)
    bytes
  }

  /**
   * Returns the containers as byte arrays.  Assuming all of the containers except the last one is full,
   * calls array() on the non-last container and trimmedArray() on the last one.
   * The memFactory needs to be an on heap one otherwise UnsupportedOperationException will be thrown.
   * The sequence of byte arrays can be for example sent to Kafka as a sequence of messages - one message
   * per byte array.
   * @param reset if true, clears out all the containers EXCEPT the last one.  Pointers to the last container
   *              are simply reset, which avoids an extra container buffer allocation.
   */
  def optimalContainerBytes(reset: Boolean = false): Seq[Array[Byte]] = {
    val bytes = allContainers.dropRight(1).map(_.array) ++
      allContainers.takeRight(1).filterNot(_.isEmpty).map(_.trimmedArray)
    if (reset) {
      removeAndFreeContainers(containers.size - 1)
      this.reset()
    }
    bytes
  }

  /**
   * Remove the first numContainers containers and release the memory they took up.
   * If no more containers are left, then everything will be reset.
   * @param numContainers the # of containers to remove
   */
  def removeAndFreeContainers(numContainers: Int): Unit = if (numContainers > 0) {
    require(numContainers <= containers.length)
    if (numContainers == containers.length) reset()
    containers.take(numContainers).foreach { c => memFactory.freeMemory(c.offset) }
    containers.remove(0, numContainers)
  }

  /**
   * Returns the number of free bytes in the current container, or 0 if container is not initialized
   */
  def containerRemaining: Long = maxOffset - curRecEndOffset

  protected def requireBytes(numBytes: Int): Unit =
    // if container is none, allocate a new one, make sure it has enough space, reset length, update offsets
    if (containers.isEmpty) {
      newContainer()
      // if we don't have enough space left, get a new container and move existing BR being written into new space
    } else if (curRecEndOffset + numBytes > maxOffset) {
      val oldBase = curBase
      val recordNumBytes = curRecEndOffset - curRecordOffset
      val oldOffset = curRecordOffset
      if (reuseOneContainer) resetContainerPointers() else newContainer()
      logger.trace(s"Moving $recordNumBytes bytes from end of old container to new container")
      require((containerSize - ContainerHeaderLen) > (recordNumBytes + numBytes),
        s"The intermediate or final result is too big. containerSize=$containerSize, numBytes=$numBytes," +
          s" recordNumBytes=$recordNumBytes, ContainerHeaderLen=$ContainerHeaderLen, " +
          s"For queries, please try to add more query filters or time range.")
      unsafe.copyMemory(oldBase, oldOffset, curBase, curRecordOffset, recordNumBytes)
      if (mapOffset != -1L) mapOffset = curRecordOffset + (mapOffset - oldOffset)
      curRecEndOffset = curRecordOffset + recordNumBytes
    }

  private[filodb] def newContainer(): Unit = {
    val (newBase, newOff, _) = memFactory.allocate(containerSize)
    val container = new RecordContainer(newBase, newOff, containerSize)
    containers += container
    logger.trace(s"Creating new RecordContainer with $containerSize bytes using $memFactory")
    curBase = newBase
    curRecordOffset = newOff + ContainerHeaderLen
    curRecEndOffset = curRecordOffset
    container.updateLengthWithOffset(curRecordOffset)
    container.writeVersionWord()
    container.updateTimestamp(currentTimeMillis)
    maxOffset = newOff + containerSize
  }

  def postEndRecord(): Unit = {
    // Update container length.  This is atomic so it is updated only when the record is complete.
    val lastContainer = containers.last
    lastContainer.updateLengthWithOffset(curRecEndOffset)
    lastContainer.numRecords += 1
  }

}

object RecordBuilder {
  val DefaultContainerSize = 256 * 1024
  val MinContainerSize = 2048
  val HASH_INIT = 7

  // Please do not change this.  It should only be changed with a change in BinaryRecord and/or RecordContainer
  // format, and only then REALLY carefully.
  val Version = 1
  val ContainerHeaderLen = 16
  val EmptyNumBytes = ContainerHeaderLen - 4

  val stringPairComparator = new java.util.Comparator[(String, String)] {
    def compare(pair1: (String, String), pair2: (String, String)): Int = pair1._1 compare pair2._1
  }

  /**
    * Make is a convenience factory method to access from java.
    */
  def make(memFactory: MemFactory,
           containerSize: Int = RecordBuilder.DefaultContainerSize): RecordBuilder = {
    new RecordBuilder(memFactory, containerSize)
  }

  /**
    * == Auxiliary functions to compute hashes. ==
    */

  import filodb.core._

  val keyHashCache = concurrentCache[String, Int](1000)

  /**
    * Sorts an incoming list of key-value pairs and then computes a hash value
    * for each pair.  The output can be fed into the combineHash methods to produce an overall hash.
    * NOTE: we use XXHash, it gives a MUCH higher quality hash than the default String hashCode.
    * @param pairs an unsorted list of key-value pairs.  Will be mutated and sorted.
    */
  final def sortAndComputeHashes(pairs: java.util.ArrayList[(String, String)]): Array[Int] = {
    pairs.sort(stringPairComparator)
    val hashes = new Array[Int](pairs.size)
    cforRange { 0 until pairs.size } { i =>
      val (k, v) = pairs.get(i)
      // This is not very efficient, we have to convert String to bytes first to get the hash
      // TODO: work on different API which is far more efficient and saves memory allocation
      val valBytes = v.getBytes(StandardCharsets.UTF_8)
      val keyHash = keyHashCache.getOrElseUpdate(k, { key =>
        val keyBytes = key.getBytes(StandardCharsets.UTF_8)
        BinaryRegion.hasher32.hash(keyBytes, 0, keyBytes.size, BinaryRegion.Seed)
      })
      hashes(i) = combineHash(keyHash, BinaryRegion.hasher32.hash(valBytes, 0, valBytes.size, BinaryRegion.Seed))
    }
    hashes
  }

  // NOTE: I've tried many different hash combiners, but nothing tried (including Murmur3) seem any better than
  // XXHash + the simple formula below.
  @inline
  final def combineHash(hash1: Int, hash2: Int): Int = 31 * hash1 + hash2

  /**
    * Combines the hashes from sortAndComputeHashes, excluding certain keys, into an overall hash value.
    * @param sortedPairs sorted pairs of byte key values, from sortAndComputeHashes
    * @param hashes the output from sortAndComputeHashes
    * @param excludeKeys set of String keys to exclude
    */
  final def combineHashExcluding(sortedPairs: java.util.ArrayList[(String, String)],
                                 hashes: Array[Int],
                                 excludeKeys: Set[String]): Int = {
    var hash = 7
    cforRange { 0 until sortedPairs.size } { i =>
      if (!(excludeKeys contains sortedPairs.get(i)._1))
        hash = combineHash(hash, hashes(i))
    }
    hash
  }

  /**
   * Computes a shard key hash from the metric name and the values of the non-metric shard key columns. If a
   * target-schema is defined and it doesn't include metric, then metric will be omitted from ShardKeyHash.
   *
   * @param shardKeyValues the non-metric shard key values (such as the job/exporter/app), sorted in order of
   *        the key name.  For example, it should be Seq(exporter, job).
   * @param metric the metric value to use in the calculation.
   * @param targetSchema labels that identify the resource-type of the source. Only these labels are used to
   *        determine partition hash.
   */
  final def shardKeyHash(shardKeyValues: Seq[Array[Byte]], metric: Array[Byte],
                         includeMetric: Boolean): Int = {
    var hash = 7
    shardKeyValues.foreach { value => hash = combineHash(hash, BinaryRegion.hash32(value)) }
    if (includeMetric)
      hash = combineHash(hash, BinaryRegion.hash32(metric))
    hash
  }

  // If targetSchema has metric label, include metric to calculate ShardKeyHash. Otherwise omit it.
  final def shardKeyHash(shardKeyValues: Seq[String],
                         metricShardkeyColName: String,
                         metric: String,
                         targetSchema: Seq[String] = Seq.empty): Int = {
    val includeMetric = targetSchema.isEmpty || targetSchema.contains(metricShardkeyColName)
    shardKeyHash(shardKeyValues.map(_.getBytes(StandardCharsets.UTF_8)),
                metric.getBytes(StandardCharsets.UTF_8),
                includeMetric)
  }

  /**
   * Calculate partition key hash from non-shard-key columns. This is used for calculating the ingestionShard.
   * If a target-schema is provided, use shardKey labels plus the labels configured in target-schema.
   * @param nonShardKeyLabelPair non-shard-key label pair
   * @param targetSchema target-schema list of sorted labels that uniquely identify the source of data and used
   *                     exclusively for determining target ingestion shard.
   * @param metricShardkey metric shardKey (e.g __name__)
   * @param metric metric name
   * @return
   */
  final def partitionKeyHash(nonShardKeyLabelPair: Map[String, String],
                             shardKeyLabelPair: Map[String, String],
                             targetSchema: Seq[String],
                             metricShardkey: String,
                             metric: String): Int = {
    var hash = 7
    val labelPairs = nonShardKeyLabelPair ++ shardKeyLabelPair + (metricShardkey -> metric)
    val tags: Set[String] = labelPairs.keySet
    val nonMetricShardKeys = shardKeyLabelPair - metricShardkey
    val implicitTargetSchema = nonMetricShardKeys.keySet ++ targetSchema
    val useTargetSchema = targetSchema.nonEmpty && implicitTargetSchema.diff(tags).isEmpty
    val shardingLabels = if (useTargetSchema) {
      implicitTargetSchema.toStream.sorted.map(labelPairs(_))
    } else nonShardKeyLabelPair.values  // NOTE: avoiding a sort here to preserve legacy logic
    shardingLabels.foreach { v => {
        hash = RecordBuilder
          .combineHash(hash, BinaryRegion.hash32(v.getBytes(StandardCharsets.UTF_8)))
      }
    }
    hash
  }

  /**
    * Removes the ignoreShardKeyColumnSuffixes from LabelPair as configured in DataSet.
    *
    * Few metric types like Histogram, Summary exposes multiple time
    * series for the same metric during a scrape by appending suffixes _bucket,_sum,_count
    *
    * In order to ingest all these multiple time series of a single metric to the
    * same shard, we have to trim the suffixes while calculating shardKeyHash.
    *
    * @param options - DatasetOptions
    * @param shardKeyColName  - ShardKey label name as String
    * @param shardKeyColValue - ShardKey label value as String
    * @return - Label value after removing the suffix
    */
  final def trimShardColumn(options: DatasetOptions, shardKeyColName: String, shardKeyColValue: String): String = {
    options.ignoreShardKeyColumnSuffixes.get(shardKeyColName) match {
      case Some(trimMetricSuffixColumn) => trimMetricSuffixColumn.find(shardKeyColValue.endsWith) match {
                                            case Some(s)  => shardKeyColValue.dropRight(s.length)
                                            case _        => shardKeyColValue
                                           }
      case _                            => shardKeyColValue
    }
  }

  /**
    * mutate dataschema of the partitionKey for downsampling, only when downsample dataschema is different
    * than raw schema (e.g. Guages)
    */
  final def updateSchema(partKeyBase: Any, partKeyOffset: Long, schema: Schema): Unit = {
    UnsafeUtils.setShort(partKeyBase, partKeyOffset + 4, schema.schemaHash.toShort)
  }

  /**
    * Build a partkey from the source partkey and change the downsample schema.
    * Useful during downsampling as dataschema may differ.
    */
  final def buildDownsamplePartKey(pkBytes: Array[Byte], schemas: Schemas): Option[Array[Byte]] = {
    val rawSchema = schemas(schemaID(pkBytes, UnsafeUtils.arayOffset))
    rawSchema.downsample.map { downSch =>
      val dsPkeyBytes = pkBytes.clone
      updateSchema(dsPkeyBytes, UnsafeUtils.arayOffset, downSch)
      dsPkeyBytes
    }
  }
}
