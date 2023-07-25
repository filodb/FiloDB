package filodb.core.memstore

import java.io.File
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util
import java.util.{Base64, PriorityQueue}
import java.util.regex.Pattern

import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.github.benmanes.caffeine.cache.{Caffeine, LoadingCache}
import com.googlecode.javaewah.{EWAHCompressedBitmap, IntIterator}
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.metric.MeasurementUnit
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.document.Field.Store
import org.apache.lucene.facet.{FacetsCollector, FacetsConfig}
import org.apache.lucene.facet.sortedset.{SortedSetDocValuesFacetCounts, SortedSetDocValuesFacetField}
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState
import org.apache.lucene.index._
import org.apache.lucene.search._
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.store.{MMapDirectory, NIOFSDirectory}
import org.apache.lucene.util.{BytesRef, InfoStream}
import org.apache.lucene.util.automaton.RegExp
import spire.syntax.cfor._

import filodb.core.{concurrentCache, DatasetRef}
import filodb.core.Types.PartitionKey
import filodb.core.binaryrecord2.MapItemConsumer
import filodb.core.memstore.ratelimit.CardinalityTracker
import filodb.core.metadata.Column.ColumnType.{MapColumn, StringColumn}
import filodb.core.metadata.PartitionSchema
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.query.Filter._
import filodb.memory.{BinaryRegionLarge, UTF8StringMedium, UTF8StringShort}
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String => UTF8Str}

object PartKeyLuceneIndex {
  final val PART_ID = "__partId__"
  final val START_TIME = "__startTime__"
  final val END_TIME = "__endTime__"
  final val PART_KEY = "__partKey__"
  final val LABEL_LIST = s"__labelList__"
  final val FACET_FIELD_PREFIX = "$facet_"
  final val LABEL_LIST_FACET = FACET_FIELD_PREFIX + LABEL_LIST

  final val ignoreIndexNames = HashSet(START_TIME, PART_KEY, END_TIME, PART_ID)

  val MAX_STR_INTERN_ENTRIES = 10000
  val MAX_TERMS_TO_ITERATE = 10000
  val FACET_FIELD_MAX_LEN = 1000

  val NOT_FOUND = -1

  def bytesRefToUnsafeOffset(bytesRefOffset: Int): Int = bytesRefOffset + UnsafeUtils.arayOffset

  def unsafeOffsetToBytesRefOffset(offset: Long): Int = offset.toInt - UnsafeUtils.arayOffset

  def partKeyBytesRef(partKeyBase: Array[Byte], partKeyOffset: Long): BytesRef = {
    new BytesRef(partKeyBase, unsafeOffsetToBytesRefOffset(partKeyOffset),
      BinaryRegionLarge.numBytes(partKeyBase, partKeyOffset))
  }

  def defaultTempDir(ref: DatasetRef, shardNum: Int): File = {
    val baseDir = new File(System.getProperty("java.io.tmpdir"))
    val baseName = s"partKeyIndex-$ref-$shardNum-${System.currentTimeMillis()}-"
    val tempDir = new File(baseDir, baseName)
    tempDir
  }

  private def createTempDir(ref: DatasetRef, shardNum: Int): File = {
    val tempDir = defaultTempDir(ref, shardNum)
    tempDir.mkdir()
    tempDir
  }

  def partKeyByteRefToSHA256Digest(bytes: Array[Byte], offset: Int, length: Int): String = {
    import java.security.MessageDigest
    val md: MessageDigest = MessageDigest.getInstance("SHA-256")
    md.update(bytes, offset, length)
    val strDigest = Base64.getEncoder.encodeToString(md.digest())
    strDigest
  }

   private def removeRegexTailDollarSign(regex: String): String = {
    // avoid unnecessary calculation when $ is not present at the end of the regex.
    if (regex.nonEmpty && regex.last == '$' && Pattern.matches("""^(|.*[^\\])(\\\\)*\$$""", regex)) {
      // (|.*[^\\]) means either empty or a sequence end with a character not \.
      // (\\\\)* means any number of \\.
      // remove the last $ if it is not \$.
      // $ at locations other than the end will not be removed.
      regex.substring(0, regex.length - 1)
    } else {
      regex
    }
  }

  /**
   * Remove leading anchor &#94; and ending anchor $.
   *
   * @param regex the orignal regex string.
   * @return the regex string without anchors.
   */
  def removeRegexAnchors(regex: String): String = {
    removeRegexTailDollarSign(regex.stripPrefix("^"))
  }
}

final case class TermInfo(term: UTF8Str, freq: Int)
final case class PartKeyLuceneIndexRecord(partKey: Array[Byte], startTime: Long, endTime: Long)

// scalastyle:off number.of.methods
class PartKeyLuceneIndex(ref: DatasetRef,
                         schema: PartitionSchema,
                         facetEnabledAllLabels: Boolean,
                         facetEnabledShardKeyLabels: Boolean,
                         shardNum: Int,
                         retentionMillis: Long, // only used to calculate fallback startTime
                         diskLocation: Option[File] = None,
                         val lifecycleManager: Option[IndexMetadataStore] = None,
                         useMemoryMappedImpl: Boolean = true
                        ) extends StrictLogging {

  import PartKeyLuceneIndex._

  val startTimeLookupLatency = Kamon.histogram("index-startTimes-for-odp-lookup-latency",
    MeasurementUnit.time.nanoseconds)
    .withTag("dataset", ref.dataset)
    .withTag("shard", shardNum)

  val queryIndexLookupLatency = Kamon.histogram("index-partition-lookup-latency",
    MeasurementUnit.time.nanoseconds)
    .withTag("dataset", ref.dataset)
    .withTag("shard", shardNum)

  val partIdFromPartKeyLookupLatency = Kamon.histogram("index-ingestion-partId-lookup-latency",
    MeasurementUnit.time.nanoseconds)
    .withTag("dataset", ref.dataset)
    .withTag("shard", shardNum)

  val labelValuesQueryLatency = Kamon.histogram("index-label-values-query-latency",
    MeasurementUnit.time.nanoseconds)
    .withTag("dataset", ref.dataset)
    .withTag("shard", shardNum)

  val readerStateCacheHitRate = Kamon.gauge("index-reader-state-cache-hit-rate")
    .withTag("dataset", ref.dataset)
    .withTag("shard", shardNum)

  private val numPartColumns = schema.columns.length


  val indexDiskLocation = diskLocation.map(new File(_, ref.dataset + File.separator + shardNum))
    .getOrElse(createTempDir(ref, shardNum)).toPath

  // If index rebuild is triggered or the state is Building, simply clean up the index directory and start
  // index rebuild
  if (
    lifecycleManager.forall(_.shouldTriggerRebuild(ref, shardNum))
  ) {
    logger.info(s"Cleaning up indexDirectory=$indexDiskLocation for  dataset=$ref, shard=$shardNum")
    deleteRecursively(indexDiskLocation.toFile) match {
      case Success(_) => // Notify the handler that the directory is now empty
        logger.info(s"Cleaned directory for dataset=$ref, shard=$shardNum and index directory=$indexDiskLocation")
        notifyLifecycleListener(IndexState.Empty, System.currentTimeMillis)

      case Failure(t) => // Update index state as TriggerRebuild again and rethrow the exception
        logger.warn(s"Exception while deleting directory for dataset=$ref, shard=$shardNum " +
          s"and index directory=$indexDiskLocation with stack trace", t)
        notifyLifecycleListener(IndexState.TriggerRebuild, System.currentTimeMillis)
        throw new IllegalStateException("Unable to clean up index directory", t)
    }
  }
  //else {
  // TODO here we assume there is non-empty index which we need to validate
  //}

  def  notifyLifecycleListener(state: IndexState.Value, time: Long): Unit =
    lifecycleManager.foreach(_.updateState(ref, shardNum, state, time))


  val fsDirectory = if (useMemoryMappedImpl)
    new MMapDirectory(indexDiskLocation)
  else
    new NIOFSDirectory(indexDiskLocation)

  private val analyzer = new StandardAnalyzer()

  logger.info(s"Created lucene index for dataset=$ref shard=$shardNum facetEnabledAllLabels=$facetEnabledAllLabels " +
    s"facetEnabledShardKeyLabels=$facetEnabledShardKeyLabels at $indexDiskLocation")

  private def createIndexWriterConfig(): IndexWriterConfig = {
    val config = new IndexWriterConfig(analyzer)
    config.setInfoStream(new LuceneMetricsRouter(ref, shardNum))
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
    val endTimeSort = new Sort(new SortField(END_TIME, SortField.Type.LONG),
      new SortField(START_TIME, SortField.Type.LONG))
    config.setIndexSort(endTimeSort)
  }

  private val indexWriter =
    try {
      new IndexWriterPlus(fsDirectory, createIndexWriterConfig(), ref, shardNum)
    } catch {
      case e: Exception =>
        // If an exception is thrown here there is something wrong with the index or the directory
        // We will attempt once by cleaning the directory and try instantiating the index again
        logger.warn(s"Index for dataset:${ref.dataset} and shard: $shardNum possibly corrupt," +
          s"index directory will be cleaned up and index rebuilt", e)
        deleteRecursively(indexDiskLocation.toFile) match {
          case Success(_)       => // Notify the handler that the directory is now empty
            logger.info(s"Cleaned directory for dataset=$ref," +
              s"shard=$shardNum and index directory=$indexDiskLocation")
            notifyLifecycleListener(IndexState.Empty, System.currentTimeMillis)
          case Failure(t)       => logger.warn(s"Exception while deleting directory for dataset=$ref," +
            s"shard=$shardNum and index directory=$indexDiskLocation with stack trace", t)
            // If we still see failure, set the TriggerRebuild and rethrow the exception
            notifyLifecycleListener(IndexState.TriggerRebuild, System.currentTimeMillis)
            throw new IllegalStateException("Unable to clean up index directory", t)
        }
        // Retry again after cleaning up the index directory, if it fails again, something needs to be looked into.
        new IndexWriterPlus(fsDirectory, createIndexWriterConfig(), ref, shardNum)
    }

  private val utf8ToStrCache = concurrentCache[UTF8Str, String](PartKeyLuceneIndex.MAX_STR_INTERN_ENTRIES)

  //scalastyle:off
  private val searcherManager = new SearcherManager(indexWriter, null)
  //scalastyle:on

  //start this thread to flush the segments and refresh the searcher every specific time period
  private var flushThread: ControlledRealTimeReopenThread[IndexSearcher] = _

  private def facetEnabledForLabel(label: String) = {
    facetEnabledAllLabels || (facetEnabledShardKeyLabels && schema.options.shardKeyColumns.contains(label))
  }

  private def deleteRecursively(f: File, deleteRoot: Boolean = false): Try[Boolean] = {
    val subDirDeletion: Try[Boolean] =
      if (f.isDirectory)
        f.listFiles match {
          case xs: Array[File] if xs != null  && !xs.isEmpty  =>
            val subDirDeletions: Array[Try[Boolean]] = xs map (f => deleteRecursively(f, true))
              subDirDeletions reduce((reduced, thisOne) => {
                thisOne match {
                  // Ensures even if one Right(_) is found, thr response will be Right(Throwable)
                  case Success(_) if reduced == Success(true)    => thisOne
                  case Failure(_)                                => thisOne
                  case _                                         => reduced
                }
              })
          case _                               => Success(true)
        }
      else
        Success(true)

    subDirDeletion match  {
      case Success(_)        =>
        if (deleteRoot) {
          if (f.delete()) Success(true) else Failure(new IOException(s"Unable to delete $f"))
        } else Success(true)
      case right @ Failure(_)  => right
    }

  }

  case class ReusableLuceneDocument() {

    var facetsConfig: FacetsConfig = _

    val document = new Document()
    private[memstore] val partIdField = new StringField(PART_ID, "0", Store.NO)
    private val partIdDv = new NumericDocValuesField(PART_ID, 0)
    private val partKeyDv = new BinaryDocValuesField(PART_KEY, new BytesRef())
    private val startTimeField = new LongPoint(START_TIME, 0L)
    private val startTimeDv = new NumericDocValuesField(START_TIME, 0L)
    private val endTimeField = new LongPoint(END_TIME, 0L)
    private val endTimeDv = new NumericDocValuesField(END_TIME, 0L)
    private val otherFields = mutable.WeakHashMap[String, StringField]() // weak so that it is GCed when unused

    private val fieldNames = new ArrayBuffer[String]()

    def addField(name: String, value: String): Unit = {
      addFacet(name, value, false)
      val field = otherFields.getOrElseUpdate(name, new StringField(name, "", Store.NO))
      field.setStringValue(value)
      document.add(field)
      fieldNames += name
    }

    private[PartKeyLuceneIndex] def addFacet(name: String, value: String, always: Boolean) : Unit = {
      // Use PartKeyIndexBenchmark to measure indexing performance before changing this
      if (name.nonEmpty && value.nonEmpty &&
        (always || facetEnabledForLabel(name)) &&
        value.length < FACET_FIELD_MAX_LEN) {
        facetsConfig.setRequireDimensionDrillDown(name, false)
        facetsConfig.setIndexFieldName(name, FACET_FIELD_PREFIX + name)
        document.add(new SortedSetDocValuesFacetField(name, value))
      }
    }

    def allFieldsAdded(): Unit = {
      // this special field is to fetch label names associated with query filter quickly
      // we do it by adding a facet of sorted labelNames to each document
      // NOTE: one could have added multi-field document, but query api was non-stratghtforward.
      // Improve in next iteration.
      val fieldNamesStr = fieldNames.sorted.mkString(",")
      addFacet(LABEL_LIST, fieldNamesStr, true)
    }

    def reset(partId: Int, documentId: String, partKey: BytesRef, startTime: Long, endTime: Long): Document = {

      document.clear()
      fieldNames.clear()
      facetsConfig = new FacetsConfig

      if(partId > -1) {
        partIdDv.setLongValue(partId)
        document.add(partIdDv)
      }
      partIdField.setStringValue(documentId)

      startTimeField.setLongValue(startTime)
      startTimeDv.setLongValue(startTime)
      endTimeField.setLongValue(endTime)
      endTimeDv.setLongValue(endTime)
      partKeyDv.setBytesValue(partKey)

      document.add(partIdField)
      document.add(partKeyDv)
      document.add(startTimeField)
      document.add(startTimeDv)
      document.add(endTimeField)
      document.add(endTimeDv)
      document
    }
  }



  private val luceneDocument = new ThreadLocal[ReusableLuceneDocument]() {
    override def initialValue(): ReusableLuceneDocument = new ReusableLuceneDocument
  }

  private val mapConsumer = new MapItemConsumer {
    def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
      import filodb.core._
      val key = utf8ToStrCache.getOrElseUpdate(new UTF8Str(keyBase, keyOffset + 1,
        UTF8StringShort.numBytes(keyBase, keyOffset)),
        _.toString)
      val value = new String(valueBase.asInstanceOf[Array[Byte]],
        unsafeOffsetToBytesRefOffset(valueOffset + 2), // add 2 to move past numBytes
        UTF8StringMedium.numBytes(valueBase, valueOffset), StandardCharsets.UTF_8)
      addIndexedField(key, value)
    }
  }

  /**
   * Map of partKey column to the logic for indexing the column (aka Indexer).
   * Optimization to avoid match logic while iterating through each column of the partKey
   */
  private final val indexers = schema.columns.zipWithIndex.map { case (c, pos) =>
    c.columnType match {
      case StringColumn => new Indexer {
        val colName = UTF8Str(c.name)
        def fromPartKey(base: Any, offset: Long, partIndex: Int): Unit = {
          val strOffset = schema.binSchema.blobOffset(base, offset, pos)
          val numBytes = schema.binSchema.blobNumBytes(base, offset, pos)
          val value = new String(base.asInstanceOf[Array[Byte]], strOffset.toInt - UnsafeUtils.arayOffset,
            numBytes, StandardCharsets.UTF_8)
          addIndexedField(colName.toString, value)
        }
        def getNamesValues(key: PartitionKey): Seq[(UTF8Str, UTF8Str)] = ??? // not used
      }
      case MapColumn => new Indexer {
        def fromPartKey(base: Any, offset: Long, partIndex: Int): Unit = {
          schema.binSchema.consumeMapItems(base, offset, pos, mapConsumer)
        }
        def getNamesValues(key: PartitionKey): Seq[(UTF8Str, UTF8Str)] = ??? // not used
      }
      case other: Any =>
        logger.warn(s"Column $c has type that cannot be indexed and will be ignored right now")
        NoOpIndexer
    }
  }.toArray


  def getCurrentIndexState(): (IndexState.Value, Option[Long]) =
    lifecycleManager.map(_.currentState(this.ref, this.shardNum)).getOrElse((IndexState.Empty, None))

  def reset(): Unit = {
    indexWriter.deleteAll()
    commit()
  }

  def startFlushThread(flushDelayMinSeconds: Int, flushDelayMaxSeconds: Int): Unit = {

    flushThread = new ControlledRealTimeReopenThread(indexWriter,
      searcherManager,
      flushDelayMaxSeconds,
      flushDelayMinSeconds)
    flushThread.start()
    logger.info(s"Started flush thread for lucene index on dataset=$ref shard=$shardNum")
  }

  /**
   * Find partitions that ended ingesting before a given timestamp. Used to identify partitions that can be purged.
   * @return matching partIds
   */
  def partIdsEndedBefore(endedBefore: Long): debox.Buffer[Int] = {
    val collector = new PartIdCollector()
    val deleteQuery = LongPoint.newRangeQuery(PartKeyLuceneIndex.END_TIME, 0, endedBefore)

    withNewSearcher(s => s.search(deleteQuery, collector))
    collector.result
  }

  /**
   * Method to delete documents from index that ended before the provided end time
   *
   * @param endedBefore the cutoff timestamp. All documents with time <= this time will be removed
   * @param returnApproxDeletedCount a boolean flag that requests the return value to be an approximate count of the
   *                                 documents that got deleted, if value is set to false, 0 is returned
   */
  def removePartitionsEndedBefore(endedBefore: Long, returnApproxDeletedCount: Boolean = true): Int = {
    val deleteQuery = LongPoint.newRangeQuery(PartKeyLuceneIndex.END_TIME, 0, endedBefore)
    // SInce delete does not return the deleted document count, we query to get the count that match the filter criteria
    // and then delete the documents
    val approxDeletedCount = if (returnApproxDeletedCount) {
      val searcher = searcherManager.acquire()
      try {
          searcher.count(deleteQuery)
      } finally {
        searcherManager.release(searcher)
      }
    } else
      0
    indexWriter.deleteDocuments(deleteQuery)
    approxDeletedCount

  }

  private def withNewSearcher(func: IndexSearcher => Unit): Unit = {
    val s = searcherManager.acquire()
    try {
      func(s)
    } finally {
      searcherManager.release(s)
    }
  }

  /**
   * Delete partitions with given partIds
   */
  def removePartKeys(partIds: debox.Buffer[Int]): Unit = {
    if (!partIds.isEmpty) {
      val terms = new util.ArrayList[BytesRef]()
      cforRange { 0 until partIds.length } { i =>
        terms.add(new BytesRef(partIds(i).toString.getBytes(StandardCharsets.UTF_8)))
      }
      indexWriter.deleteDocuments(new TermInSetQuery(PART_ID, terms))
    }
  }

  def indexRamBytes: Long = indexWriter.ramBytesUsed()

  /**
   * Number of documents in flushed index, excludes tombstones for deletes
   */
  def indexNumEntries: Long = indexWriter.getDocStats().numDocs

  /**
   * Number of documents in flushed index, includes tombstones for deletes
   */
  def indexNumEntriesWithTombstones: Long = indexWriter.getDocStats().maxDoc

  def closeIndex(): Unit = {
    logger.info(s"Closing index on dataset=$ref shard=$shardNum")
    if (flushThread != UnsafeUtils.ZeroPointer) flushThread.close()
    indexWriter.close()
  }

  // DefaultSortedSetDocValuesReaderState (per label/reader) is expensive to create, so cache & reuse across queries.
  // Different cache for shard keys and non-shard-keys since it is evicted differently.
  // TODO cache config is currently hardcoded - make configuration properties if really deemed necessary
  private val readerStateCacheShardKeys: LoadingCache[(IndexReader, String), DefaultSortedSetDocValuesReaderState] =
  Caffeine.newBuilder()
    .maximumSize(100)
    .recordStats()
    .build((key: (IndexReader, String)) => {
      new DefaultSortedSetDocValuesReaderState(key._1, FACET_FIELD_PREFIX + key._2)
    })
  private val readerStateCacheNonShardKeys: LoadingCache[(IndexReader, String), DefaultSortedSetDocValuesReaderState] =
    Caffeine.newBuilder()
      .maximumSize(200)
      .recordStats()
      .build((key: (IndexReader, String)) => {
        new DefaultSortedSetDocValuesReaderState(key._1, FACET_FIELD_PREFIX + key._2)
      })

  def labelNamesEfficient(colFilters: Seq[ColumnFilter], startTime: Long, endTime: Long): Seq[String] = {
    val labelSets = labelValuesEfficient(colFilters, startTime, endTime, LABEL_LIST)
    val labels = mutable.HashSet[String]()
    labelSets.foreach { labelSet =>
      labelSet.split(",").foreach(l => labels += l)
    }
    labels.toSeq
  }

  def labelValuesEfficient(colFilters: Seq[ColumnFilter], startTime: Long, endTime: Long,
                           colName: String, limit: Int = 100): Seq[String] = {
    require(facetEnabledForLabel(colName),
      s"Faceting not enabled for label $colName; labelValuesEfficient should not have been called")

    val readerStateCache = if (schema.options.shardKeyColumns.contains(colName)) readerStateCacheShardKeys
    else readerStateCacheNonShardKeys

    val labelValues = mutable.ArrayBuffer[String]()
    val start = System.nanoTime()
    withNewSearcher { searcher =>
      val reader = searcher.getIndexReader
      try {
        val state = readerStateCache.get((reader, colName))
        val fc = new FacetsCollector() {
          override def scoreMode() = ScoreMode.COMPLETE_NO_SCORES
        }
        val query = colFiltersToQuery(colFilters, startTime, endTime)
        searcher.search(query, fc)
        val facets = new SortedSetDocValuesFacetCounts(state, fc)
        val result = facets.getTopChildren(limit, colName)
        if (result != null && result.labelValues != null) {
          result.labelValues.foreach { lv =>
            labelValues += lv.label
          }
        }
      } catch {
        case e: IllegalArgumentException =>
          // If this exception is seen, then we have not seen the label. Return empty result.
          if (!e.getMessage.contains("was not indexed"))
            logger.warn(s"Got an exception when doing label-values filters=$colFilters colName=$colName", e)
      }
    }
    labelValuesQueryLatency.record(System.nanoTime() - start)
    readerStateCacheHitRate.withTag("label", "shardKey").update(readerStateCacheShardKeys.stats().hitRate())
    readerStateCacheHitRate.withTag("label", "other").update(readerStateCacheNonShardKeys.stats().hitRate())
    labelValues
  }

  /**
   * Fetch values/terms for a specific column/key/field, in order from most frequent on down.
   * Note that it iterates through all docs up to a certain limit only, so if there are too many terms
   * it will not report an accurate top k in exchange for not running too long.
   * @param fieldName the name of the column/field/key to get terms for
   * @param topK the number of top k results to fetch
   */
  def indexValues(fieldName: String, topK: Int = 100): Seq[TermInfo] = {
    // FIXME this API returns duplicate values because same value can be present in multiple lucene segments

    val freqOrder = Ordering.by[TermInfo, Int](_.freq)
    val topkResults = new PriorityQueue[TermInfo](topK, freqOrder)

    withNewSearcher { searcher =>
      val indexReader = searcher.getIndexReader
      val segments = indexReader.leaves()
      var termsRead = 0
      segments.asScala.foreach { segment =>
        val terms = segment.reader().terms(fieldName)

        //scalastyle:off
        if (terms != null) {
          val termsEnum = terms.iterator()
          var nextVal: BytesRef = termsEnum.next()
          while (nextVal != null && termsRead < MAX_TERMS_TO_ITERATE) {
            //scalastyle:on
            val valu = BytesRef.deepCopyOf(nextVal) // copy is needed since lucene uses a mutable cursor underneath
            val ret = new UTF8Str(valu.bytes, bytesRefToUnsafeOffset(valu.offset), valu.length)
            val freq = termsEnum.docFreq()
            if (topkResults.size < topK) {
              topkResults.add(TermInfo(ret, freq))
            }
            else if (topkResults.peek.freq < freq) {
              topkResults.remove()
              topkResults.add(TermInfo(ret, freq))
            }
            nextVal = termsEnum.next()
            termsRead += 1
          }
        }
      }
    }
    topkResults.toArray(new Array[TermInfo](0)).sortBy(-_.freq).toSeq
  }

  def indexNames(limit: Int): Seq[String] = {

    var ret: Seq[String] = Nil
    withNewSearcher { searcher =>
      val indexReader = searcher.getIndexReader
      val segments = indexReader.leaves()
      val iter = segments.asScala.iterator.flatMap { segment =>
        segment.reader().getFieldInfos.asScala.toIterator.map(_.name)
      }.filterNot { n => ignoreIndexNames.contains(n) || n.startsWith(FACET_FIELD_PREFIX) }
      ret = iter.take(limit).toSeq
    }
    ret
  }

  private def addIndexedField(labelName: String, value: String): Unit = {
    luceneDocument.get().addField(labelName, value)
  }

  def addPartKey(partKeyOnHeapBytes: Array[Byte],
                 partId: Int,
                 startTime: Long,
                 endTime: Long = Long.MaxValue,
                 partKeyBytesRefOffset: Int = 0)
                (partKeyNumBytes: Int = partKeyOnHeapBytes.length,
                 documentId: String = partId.toString): Unit = {
    makeDocument(partKeyOnHeapBytes, partKeyBytesRefOffset, partKeyNumBytes, partId, documentId, startTime, endTime)
    logger.debug(s"Adding document ${partKeyString(documentId, partKeyOnHeapBytes, partKeyBytesRefOffset)} " +
      s"with startTime=$startTime endTime=$endTime into dataset=$ref shard=$shardNum")
    val doc = luceneDocument.get()
    val docToAdd = doc.facetsConfig.build(doc.document)
    indexWriter.addDocument(docToAdd)
  }

  def upsertPartKey(partKeyOnHeapBytes: Array[Byte],
                    partId: Int,
                    startTime: Long,
                    endTime: Long = Long.MaxValue,
                    partKeyBytesRefOffset: Int = 0)
                   (partKeyNumBytes: Int = partKeyOnHeapBytes.length,
                    documentId: String = partId.toString): Unit = {
    makeDocument(partKeyOnHeapBytes, partKeyBytesRefOffset, partKeyNumBytes, partId, documentId, startTime, endTime)
    logger.debug(s"Upserting document ${partKeyString(documentId, partKeyOnHeapBytes, partKeyBytesRefOffset)} " +
      s"with startTime=$startTime endTime=$endTime into dataset=$ref shard=$shardNum")
    val doc = luceneDocument.get()
    val docToAdd = doc.facetsConfig.build(doc.document)
    val term = new Term(PART_ID, documentId)
    indexWriter.updateDocument(term, docToAdd)
  }


  private def partKeyString(docId: String,
                            partKeyOnHeapBytes: Array[Byte],
                            partKeyBytesRefOffset: Int = 0): String = {
    val partHash = schema.binSchema.partitionHash(partKeyOnHeapBytes, bytesRefToUnsafeOffset(partKeyBytesRefOffset))
    //scalastyle:off
    s"shard=$shardNum partId=$docId partHash=$partHash [${
      TimeSeriesPartition
        .partKeyString(schema, partKeyOnHeapBytes, bytesRefToUnsafeOffset(partKeyBytesRefOffset))
    }]"
    //scalastyle:on
  }

  private def makeDocument(partKeyOnHeapBytes: Array[Byte],
                           partKeyBytesRefOffset: Int,
                           partKeyNumBytes: Int,
                           partId: Int, documentId: String, startTime: Long, endTime: Long): Document = {
    val partKeyBytesRef = new BytesRef(partKeyOnHeapBytes, partKeyBytesRefOffset, partKeyNumBytes)
    luceneDocument.get().reset(partId, documentId, partKeyBytesRef, startTime, endTime)

    // If configured and enabled, Multi-column facets will be created on "partition-schema" columns
    createMultiColumnFacets(partKeyOnHeapBytes, partKeyBytesRefOffset)

    cforRange { 0 until numPartColumns } { i =>
      indexers(i).fromPartKey(partKeyOnHeapBytes, bytesRefToUnsafeOffset(partKeyBytesRefOffset), partId)
    }
    luceneDocument.get().allFieldsAdded()
    luceneDocument.get().document
  }

  private final val emptyStr = ""
  // Multi-column facets to be created on "partition-schema" columns
  private def createMultiColumnFacets(partKeyOnHeapBytes: Array[Byte], partKeyBytesRefOffset: Int): Unit = {
    schema.options.multiColumnFacets.foreach(facetCols => {
      facetCols match {
        case (name, cols) => {
          val concatFacetValue = cols.map { col =>
            val colInfoOpt = schema.columnIdxLookup.get(col)
            colInfoOpt match {
              case Some((columnInfo, pos)) if columnInfo.columnType == StringColumn =>
                val base = partKeyOnHeapBytes
                val offset = bytesRefToUnsafeOffset(partKeyBytesRefOffset)
                val strOffset = schema.binSchema.blobOffset(base, offset, pos)
                val numBytes = schema.binSchema.blobNumBytes(base, offset, pos)
                new String(base, strOffset.toInt - UnsafeUtils.arayOffset,
                  numBytes, StandardCharsets.UTF_8)
              case _ => emptyStr
            }
          }.mkString("\u03C0")
          luceneDocument.get().addFacet(name, concatFacetValue, false)
        }
      }
    })
  }

  /**
   * Called when TSPartition needs to be created when on-demand-paging from a
   * partId that does not exist on heap
   */
  def partKeyFromPartId(partId: Int): Option[BytesRef] = {
    val collector = new SinglePartKeyCollector()
    withNewSearcher(s => s.search(new TermQuery(new Term(PART_ID, partId.toString)), collector) )
    Option(collector.singleResult)
  }

  /**
   * Called when a document is updated with new endTime
   */
  def startTimeFromPartId(partId: Int): Long = {
    val collector = new NumericDocValueCollector(PartKeyLuceneIndex.START_TIME)
    withNewSearcher(s => s.search(new TermQuery(new Term(PART_ID, partId.toString)), collector))
    collector.singleResult
  }

  /**
   * Called when a document is updated with new endTime
   */
  def startTimeFromPartIds(partIds: Iterator[Int]): debox.Map[Int, Long] = {

    val startExecute = System.nanoTime()
    val span = Kamon.currentSpan()
    val collector = new PartIdStartTimeCollector()
    val terms = new util.ArrayList[BytesRef]()
    partIds.foreach { pId =>
      terms.add(new BytesRef(pId.toString.getBytes(StandardCharsets.UTF_8)))
    }
    // dont use BooleanQuery which will hit the 1024 term limit. Instead use TermInSetQuery which is
    // more efficient within Lucene
    withNewSearcher(s => s.search(new TermInSetQuery(PART_ID, terms), collector))
    span.tag(s"num-partitions-to-page", terms.size())
    val latency = System.nanoTime - startExecute
    span.mark(s"index-startTimes-for-odp-lookup-latency=${latency}ns")
    startTimeLookupLatency.record(latency)
    collector.startTimes
  }

  def commit(): Long = indexWriter.commit()

  /**
   * Called when a document is updated with new endTime
   */
  def endTimeFromPartId(partId: Int): Long = {
    val collector = new NumericDocValueCollector(PartKeyLuceneIndex.END_TIME)
    withNewSearcher(s => s.search(new TermQuery(new Term(PART_ID, partId.toString)), collector))
    collector.singleResult
  }

  /**
   * Query top-k partIds matching a range of endTimes, in ascending order of endTime
   *
   * Note: This uses a collector that uses a PriorityQueue underneath covers.
   * O(k) heap memory will be used.
   */
  def partIdsOrderedByEndTime(topk: Int,
                              fromEndTime: Long = 0,
                              toEndTime: Long = Long.MaxValue): EWAHCompressedBitmap = {
    val coll = new TopKPartIdsCollector(topk)
    withNewSearcher(s => s.search(LongPoint.newRangeQuery(END_TIME, fromEndTime, toEndTime), coll))
    coll.topKPartIDsBitmap()
  }

  def foreachPartKeyStillIngesting(func: (Int, BytesRef) => Unit): Int = {
    val coll = new ActionCollector(func)
    withNewSearcher(s => s.search(LongPoint.newExactQuery(END_TIME, Long.MaxValue), coll))
    coll.numHits
  }


  def foreachPartKeyMatchingFilter(columnFilters: Seq[ColumnFilter],
                                   startTime: Long,
                                   endTime: Long, func: (BytesRef) => Unit): Int = {
    val coll = new PartKeyActionCollector(func)
    searchFromFilters(columnFilters, startTime, endTime, coll)
    coll.numHits
  }

  def updatePartKeyWithEndTime(partKeyOnHeapBytes: Array[Byte],
                               partId: Int,
                               endTime: Long = Long.MaxValue,
                               partKeyBytesRefOffset: Int = 0)
                              (partKeyNumBytes: Int = partKeyOnHeapBytes.length,
                               documentId: String = partId.toString): Unit = {
    var startTime = startTimeFromPartId(partId) // look up index for old start time
    if (startTime == NOT_FOUND) {
      startTime = System.currentTimeMillis() - retentionMillis
      logger.warn(s"Could not find in Lucene startTime for partId=$partId in dataset=$ref. Using " +
        s"$startTime instead.", new IllegalStateException()) // assume this time series started retention period ago
    }
    makeDocument(partKeyOnHeapBytes, partKeyBytesRefOffset, partKeyNumBytes,
      partId, documentId, startTime, endTime)

    val doc = luceneDocument.get()
    logger.debug(s"Updating document ${partKeyString(documentId, partKeyOnHeapBytes, partKeyBytesRefOffset)} " +
                 s"with startTime=$startTime endTime=$endTime into dataset=$ref shard=$shardNum")
    val docToAdd = doc.facetsConfig.build(doc.document)
    indexWriter.updateDocument(new Term(PART_ID, partId.toString), docToAdd)
  }

  /**
   * Refresh readers with updates to index. May be expensive - use carefully.
   * @return
   */
  def refreshReadersBlocking(): Unit = {
    searcherManager.maybeRefreshBlocking()
    logger.info(s"Refreshed index searchers to make reads consistent for dataset=$ref shard=$shardNum")
  }

  //scalastyle:off method.length
  private def leafFilter(column: String, filter: Filter): Query = {
    filter match {
      case EqualsRegex(value) =>
        val regex = removeRegexAnchors(value.toString)
        if(regex.r.unapplySeq("").isDefined) {
          // Check if the given regex matches the empty string, if yes, then do not consider this label
          val booleanQuery = new BooleanQuery.Builder
          val allDocs = new MatchAllDocsQuery
          booleanQuery.add(allDocs, Occur.FILTER).build()
        } else {
          if (regex.nonEmpty) new RegexpQuery(new Term(column, regex), RegExp.NONE)
          else leafFilter(column, NotEqualsRegex(".+")) // value="" means the label is absent or has an empty value.
        }
      case NotEqualsRegex(value) =>
        val term = new Term(column, removeRegexAnchors(value.toString))
        val allDocs = new MatchAllDocsQuery
        val booleanQuery = new BooleanQuery.Builder
        booleanQuery.add(allDocs, Occur.FILTER)
        booleanQuery.add(new RegexpQuery(term, RegExp.NONE), Occur.MUST_NOT)
        booleanQuery.build()
      case Equals(value) =>
        if (value.toString.nonEmpty) new TermQuery(new Term(column, value.toString))
        else leafFilter(column, NotEqualsRegex(".+"))  // value="" means the label is absent or has an empty value.
      case NotEquals(value) =>
        val str = value.toString
        val term = new Term(column, str)
        val booleanQuery = new BooleanQuery.Builder
        str.isEmpty match {
          case true =>
            val termAll = new Term(column, ".*")
            booleanQuery.add(new RegexpQuery(termAll, RegExp.NONE), Occur.FILTER)
          case false =>
            val allDocs = new MatchAllDocsQuery
            booleanQuery.add(allDocs, Occur.FILTER)
        }
        booleanQuery.add(new TermQuery(term), Occur.MUST_NOT)
        booleanQuery.build()

      case In(values) =>
        if (values.size < 2)
          throw new IllegalArgumentException("In filter should have atleast 2 values")
        val booleanQuery = new BooleanQuery.Builder
        values.foreach { value =>
          booleanQuery.add(new TermQuery(new Term(column, value.toString)), Occur.SHOULD)
        }
        booleanQuery.build()
      case And(lhs, rhs) =>
        val andQuery = new BooleanQuery.Builder
        andQuery.add(leafFilter(column, lhs), Occur.FILTER)
        andQuery.add(leafFilter(column, rhs), Occur.FILTER)
        andQuery.build()
      case _ => throw new UnsupportedOperationException
    }
  }
  //scalastyle:on method.length
  def partIdsFromFilters(columnFilters: Seq[ColumnFilter],
                         startTime: Long,
                         endTime: Long): debox.Buffer[Int] = {
    val collector = new PartIdCollector() // passing zero for unlimited results
    searchFromFilters(columnFilters, startTime, endTime, collector)
    collector.result
  }

  def singlePartKeyFromFilters(columnFilters: Seq[ColumnFilter],
                               startTime: Long,
                               endTime: Long): Option[Array[Byte]] = {

    val collector = new SinglePartKeyCollector() // passing zero for unlimited results
    searchFromFilters(columnFilters, startTime, endTime, collector)
    val pkBytesRef = collector.singleResult
    if (pkBytesRef == null)
      None
    else Some(util.Arrays.copyOfRange(pkBytesRef.bytes, pkBytesRef.offset, pkBytesRef.offset + pkBytesRef.length))

  }

  def labelNamesFromFilters(columnFilters: Seq[ColumnFilter],
                            startTime: Long,
                            endTime: Long): Int = {
    val partIdCollector = new SinglePartIdCollector
    searchFromFilters(columnFilters, startTime, endTime, partIdCollector)
    partIdCollector.singleResult
  }

  def partKeyRecordsFromFilters(columnFilters: Seq[ColumnFilter],
                                startTime: Long,
                                endTime: Long): Seq[PartKeyLuceneIndexRecord] = {
    val collector = new PartKeyRecordCollector()
    searchFromFilters(columnFilters, startTime, endTime, collector)
    collector.records
  }


  private def searchFromFilters(columnFilters: Seq[ColumnFilter],
                                startTime: Long,
                                endTime: Long,
                                collector: Collector): Unit = {

    val startExecute = System.nanoTime()
    val span = Kamon.currentSpan()
    val query: BooleanQuery = colFiltersToQuery(columnFilters, startTime, endTime)
    logger.debug(s"Querying dataset=$ref shard=$shardNum partKeyIndex with: $query")
    withNewSearcher(s => s.search(query, collector))
    val latency = System.nanoTime - startExecute
    span.mark(s"index-partition-lookup-latency=${latency}ns")
    queryIndexLookupLatency.record(latency)
  }

  private def colFiltersToQuery(columnFilters: Seq[ColumnFilter], startTime: PartitionKey, endTime: PartitionKey) = {
    val booleanQuery = new BooleanQuery.Builder
    columnFilters.foreach { filter =>
      val q = leafFilter(filter.column, filter.filter)
      booleanQuery.add(q, Occur.FILTER)
    }
    booleanQuery.add(LongPoint.newRangeQuery(START_TIME, 0, endTime), Occur.FILTER)
    booleanQuery.add(LongPoint.newRangeQuery(END_TIME, startTime, Long.MaxValue), Occur.FILTER)
    val query = booleanQuery.build()
    query
  }

  def partIdFromPartKeySlow(partKeyBase: Any,
                            partKeyOffset: Long): Option[Int] = {

    val columnFilters = schema.binSchema.toStringPairs(partKeyBase, partKeyOffset)
      .map { pair => ColumnFilter(pair._1, Filter.Equals(pair._2)) }

    val startExecute = System.nanoTime()
    val booleanQuery = new BooleanQuery.Builder
    columnFilters.foreach { filter =>
      val q = leafFilter(filter.column, filter.filter)
      booleanQuery.add(q, Occur.FILTER)
    }
    val query = booleanQuery.build()
    logger.debug(s"Querying dataset=$ref shard=$shardNum partKeyIndex with: $query")
    var chosenPartId: Option[Int] = None
    def handleMatch(partId: Int, candidate: BytesRef): Unit = {
      // we need an equals check because there can potentially be another partKey with additional tags
      if (schema.binSchema.equals(partKeyBase, partKeyOffset,
        candidate.bytes, PartKeyLuceneIndex.bytesRefToUnsafeOffset(candidate.offset))) {
        logger.debug(s"There is already a partId=$partId assigned for " +
          s"${schema.binSchema.stringify(partKeyBase, partKeyOffset)} in" +
          s" dataset=$ref shard=$shardNum")
        chosenPartId = chosenPartId.orElse(Some(partId))
      }
    }
    val collector = new ActionCollector(handleMatch)
    withNewSearcher(s => s.search(query, collector))
    partIdFromPartKeyLookupLatency.record(System.nanoTime - startExecute)
    chosenPartId
  }

  /**
   * Iterate through the LuceneIndex and calculate cardinality count
   */
  def calculateCardinality(partSchema: PartitionSchema, cardTracker: CardinalityTracker): Unit = {
    val coll = new CardinalityCountBuilder(partSchema, cardTracker)
    withNewSearcher(s => s.search(new MatchAllDocsQuery(), coll))
    // IMPORTANT: making sure to flush all the data in rocksDB
    cardTracker.flushCardinalityCount()
  }
}

/**
 * In this lucene index collector, we read through the entire lucene index periodically and re-calculate
 * the cardinality count from scratch. This class iterates through each document in lucene, extracts a shard-key
 * and updates the cardinality count using the given CardinalityTracker.
 * */
class CardinalityCountBuilder(partSchema: PartitionSchema, cardTracker: CardinalityTracker)
  extends SimpleCollector with StrictLogging {

  private var partKeyDv: BinaryDocValues = _

  // gets called for each segment
  override def doSetNextReader(context: LeafReaderContext): Unit = {
    partKeyDv = context.reader().getBinaryDocValues(PartKeyLuceneIndex.PART_KEY)
  }

  // gets called for each matching document in current segment
  override def collect(doc: Int): Unit = {
    if (partKeyDv.advanceExact(doc)) {
      val binaryValue = partKeyDv.binaryValue()
      val unsafePkOffset = PartKeyLuceneIndex.bytesRefToUnsafeOffset(binaryValue.offset)
      val shardKey = partSchema.binSchema.colValues(
        binaryValue.bytes, unsafePkOffset, partSchema.options.shardKeyColumns)

      try {
        // update the cardinality count by 1, since the shardKey for each document in index is unique
        cardTracker.modifyCount(shardKey, 1, 0)
      } catch {
        case t: Throwable =>
          logger.error("exception while modifying cardinality tracker count; shardKey=" + shardKey, t)
      }
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have a partKeyDv")
    }
  }

  override def scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES
}

class NumericDocValueCollector(docValueName: String) extends SimpleCollector {

  var docValue: NumericDocValues = _
  var singleResult: Long = PartKeyLuceneIndex.NOT_FOUND

  // gets called for each segment
  override def doSetNextReader(context: LeafReaderContext): Unit = {
    docValue = context.reader().getNumericDocValues(docValueName)
  }

  // gets called for each matching document in current segment
  override def collect(doc: Int): Unit = {
    if (docValue.advanceExact(doc)) {
      singleResult = docValue.longValue()
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have a docValue")
    }
  }

  override def scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES
}

class SinglePartKeyCollector extends SimpleCollector {

  var partKeyDv: BinaryDocValues = _
  var singleResult: BytesRef = _

  // gets called for each segment
  override def doSetNextReader(context: LeafReaderContext): Unit = {
    partKeyDv = context.reader().getBinaryDocValues(PartKeyLuceneIndex.PART_KEY)
  }

  // gets called for each matching document in current segment
  override def collect(doc: Int): Unit = {
    if (partKeyDv.advanceExact(doc)) {
      singleResult = partKeyDv.binaryValue()
      // Stop further collection from this segment
      throw new CollectionTerminatedException
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have a partKeyDv")
    }
  }

  override def scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES
}

class SinglePartIdCollector extends SimpleCollector {

  var partIdDv: NumericDocValues = _
  var singleResult: Int = PartKeyLuceneIndex.NOT_FOUND

  // gets called for each segment
  override def doSetNextReader(context: LeafReaderContext): Unit = {
    partIdDv = context.reader().getNumericDocValues(PartKeyLuceneIndex.PART_ID)
  }

  // gets called for each matching document in current segment
  override def collect(doc: Int): Unit = {
    if (partIdDv.advanceExact(doc)) {
      singleResult = partIdDv.longValue().toInt
      // terminate further iteration by throwing this exception
      throw new CollectionTerminatedException
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have a partKeyDv")
    }
  }

  override def scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES
}

/**
 * A collector that takes advantage of index sorting within segments
 * to collect the top-k results matching the query.
 *
 * It uses a priority queue of size k across each segment. It early terminates
 * a segment once k elements have been added, or if all smaller values in the
 * segment have been examined.
 */
class TopKPartIdsCollector(limit: Int) extends Collector with StrictLogging {

  import PartKeyLuceneIndex._

  var endTimeDv: NumericDocValues = _
  var partIdDv: NumericDocValues = _
  val endTimeComparator = Ordering.by[(Int, Long), Long](_._2).reverse
  val topkResults = new PriorityQueue[(Int, Long)](limit, endTimeComparator)

  // gets called for each segment; need to return collector for that segment
  def getLeafCollector(context: LeafReaderContext): LeafCollector = {
    logger.trace("New segment inspected:" + context.id)
    endTimeDv = DocValues.getNumeric(context.reader, END_TIME)
    partIdDv = DocValues.getNumeric(context.reader, PART_ID)

    new LeafCollector() {
      override def setScorer(scorer: Scorable): Unit = {}
      // gets called for each matching document in the segment.
      def collect(doc: Int): Unit = {
        val partIdValue = if (partIdDv.advanceExact(doc)) {
          partIdDv.longValue().toInt
        } else throw new IllegalStateException("This shouldn't happen since every document should have a partId")
        if (endTimeDv.advanceExact(doc)) {
          val endTimeValue = endTimeDv.longValue
          if (topkResults.size < limit) {
            topkResults.add((partIdValue, endTimeValue))
          }
          else if (topkResults.peek._2 > endTimeValue) {
            topkResults.remove()
            topkResults.add((partIdValue, endTimeValue))
          }
          else { // terminate further iteration on current segment by throwing this exception
            throw new CollectionTerminatedException
          }
        } else throw new IllegalStateException("This shouldn't happen since every document should have an endTime")
      }
    }
  }

  override def scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES

  def topKPartIds(): IntIterator = {
    val result = new EWAHCompressedBitmap()
    topkResults.iterator().asScala.foreach { p => result.set(p._1) }
    result.intIterator()
  }

  def topKPartIDsBitmap(): EWAHCompressedBitmap = {
    val result = new EWAHCompressedBitmap()
    topkResults.iterator().asScala.foreach { p => result.set(p._1) }
    result
  }
}

class PartIdCollector extends SimpleCollector {
  val result: debox.Buffer[Int] = debox.Buffer.empty[Int]
  private var partIdDv: NumericDocValues = _

  override def scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES

  override def doSetNextReader(context: LeafReaderContext): Unit = {
    //set the subarray of the numeric values for all documents in the context
    partIdDv = context.reader().getNumericDocValues(PartKeyLuceneIndex.PART_ID)
  }

  override def collect(doc: Int): Unit = {
    if (partIdDv.advanceExact(doc)) {
      result += partIdDv.longValue().toInt
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have a partIdDv")
    }
  }
}

class PartIdStartTimeCollector extends SimpleCollector {
  val startTimes = debox.Map.empty[Int, Long]
  private var partIdDv: NumericDocValues = _
  private var startTimeDv: NumericDocValues = _

  override def scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES

  override def doSetNextReader(context: LeafReaderContext): Unit = {
    //set the subarray of the numeric values for all documents in the context
    partIdDv = context.reader().getNumericDocValues(PartKeyLuceneIndex.PART_ID)
    startTimeDv = context.reader().getNumericDocValues(PartKeyLuceneIndex.START_TIME)
  }

  override def collect(doc: Int): Unit = {
    if (partIdDv.advanceExact(doc) && startTimeDv.advanceExact(doc)) {
      val partId = partIdDv.longValue().toInt
      val startTime = startTimeDv.longValue()
      startTimes(partId) = startTime
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have partIdDv and startTimeDv")
    }
  }
}

class PartKeyRecordCollector extends SimpleCollector {
  val records = new ArrayBuffer[PartKeyLuceneIndexRecord]
  private var partKeyDv: BinaryDocValues = _
  private var startTimeDv: NumericDocValues = _
  private var endTimeDv: NumericDocValues = _

  override def scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES

  override def doSetNextReader(context: LeafReaderContext): Unit = {
    partKeyDv = context.reader().getBinaryDocValues(PartKeyLuceneIndex.PART_KEY)
    startTimeDv = context.reader().getNumericDocValues(PartKeyLuceneIndex.START_TIME)
    endTimeDv = context.reader().getNumericDocValues(PartKeyLuceneIndex.END_TIME)
  }

  override def collect(doc: Int): Unit = {
    if (partKeyDv.advanceExact(doc) && startTimeDv.advanceExact(doc) && endTimeDv.advanceExact(doc)) {
      val pkBytesRef = partKeyDv.binaryValue()
      // Gotcha! make copy of array because lucene reuses bytesRef for next result
      val pkBytes = util.Arrays.copyOfRange(pkBytesRef.bytes, pkBytesRef.offset, pkBytesRef.offset + pkBytesRef.length)
      records += PartKeyLuceneIndexRecord(pkBytes, startTimeDv.longValue(), endTimeDv.longValue())
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have partIdDv and startTimeDv")
    }
  }
}

class ActionCollector(action: (Int, BytesRef) => Unit) extends SimpleCollector {
  private var partIdDv: NumericDocValues = _
  private var partKeyDv: BinaryDocValues = _
  private var counter: Int = 0

  override def scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES

  override def doSetNextReader(context: LeafReaderContext): Unit = {
    partIdDv = context.reader().getNumericDocValues(PartKeyLuceneIndex.PART_ID)
    partKeyDv = context.reader().getBinaryDocValues(PartKeyLuceneIndex.PART_KEY)
  }

  override def collect(doc: Int): Unit = {
    if (partIdDv.advanceExact(doc) && partKeyDv.advanceExact(doc)) {
      val partId = partIdDv.longValue().toInt
      val partKey = partKeyDv.binaryValue()
      action(partId, partKey)
      counter += 1
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have a partIdDv && partKeyDv")
    }
  }

  def numHits: Int = counter
}

class PartKeyActionCollector(action: (BytesRef) => Unit) extends SimpleCollector {
  private var partKeyDv: BinaryDocValues = _
  private var counter: Int = 0

  override def scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES

  override def doSetNextReader(context: LeafReaderContext): Unit = {
    partKeyDv = context.reader().getBinaryDocValues(PartKeyLuceneIndex.PART_KEY)
  }

  override def collect(doc: Int): Unit = {
    if (partKeyDv.advanceExact(doc)) {
      val partKey = partKeyDv.binaryValue()
      action(partKey)
      counter += 1
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have a partKeyDv")
    }
  }

  def numHits: Int = counter
}

class LuceneMetricsRouter(ref: DatasetRef, shard: Int) extends InfoStream with StrictLogging {
  override def message(component: String, message: String): Unit = {
    logger.debug(s"dataset=$ref shard=$shard component=$component $message")
    // TODO parse string and report metrics to kamon
  }
  override def isEnabled(component: String): Boolean = true
  override def close(): Unit = {}
}
// scalastyle:on number.of.methods