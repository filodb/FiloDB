package filodb.core.memstore

import java.io.File
import java.util.PriorityQueue

import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet

import com.googlecode.javaewah.{EWAHCompressedBitmap, IntIterator}
import com.typesafe.scalalogging.StrictLogging
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.document.Field.Store
import org.apache.lucene.index._
import org.apache.lucene.search._
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.store.MMapDirectory
import org.apache.lucene.util.{BytesRef, InfoStream}
import scalaxy.loops._

import filodb.core.Types.PartitionKey
import filodb.core.binaryrecord2.MapItemConsumer
import filodb.core.concurrentCache
import filodb.core.metadata.Column.ColumnType.{MapColumn, StringColumn}
import filodb.core.metadata.Dataset
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.query.Filter._
import filodb.core.store.StoreConfig
import filodb.memory.{BinaryRegionLarge, UTF8StringMedium}
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String => UTF8Str}

object PartKeyLuceneIndex {
  final val PART_ID =    "__partId__"
  final val START_TIME = "__startTime__"
  final val END_TIME =   "__endTime__"
  final val PART_KEY =   "__partKey__"

  final val ignoreIndexNames = HashSet(START_TIME, PART_KEY, END_TIME, PART_ID)

  val MAX_STR_INTERN_ENTRIES = 10000
  val MAX_TERMS_TO_ITERATE = 10000

  val NOT_FOUND = -1

  def bytesRefToUnsafeOffset(bytesRefOffset: Int): Int = bytesRefOffset + UnsafeUtils.arayOffset

  def unsafeOffsetToBytesRefOffset(offset: Long): Int = offset.toInt - UnsafeUtils.arayOffset

  def partKeyBytesRef(partKeyBase: Array[Byte], partKeyOffset: Long): BytesRef = {
    new BytesRef(partKeyBase, unsafeOffsetToBytesRefOffset(partKeyOffset),
      BinaryRegionLarge.numBytes(partKeyBase, partKeyOffset))
  }

  private def createTempDir(dataset: Dataset, shardNum: Int): File = {
    val baseDir = new File(System.getProperty("java.io.tmpdir"))
    val baseName = s"partKeyIndex-${dataset.name}-$shardNum-${System.currentTimeMillis()}-"
    val tempDir = new File(baseDir, baseName)
    tempDir.mkdir()
    tempDir
  }
}

final case class TermInfo(term: UTF8Str, freq: Int)

class PartKeyLuceneIndex(dataset: Dataset,
                         shardNum: Int,
                         storeConfig: StoreConfig,
                         diskLocation: Option[File] = None) extends StrictLogging {

  import PartKeyLuceneIndex._

  private val numPartColumns = dataset.partitionColumns.length
  private val indexDiskLocation = diskLocation.getOrElse(createTempDir(dataset, shardNum)).toPath
  private val mMapDirectory = new MMapDirectory(indexDiskLocation)
  private val analyzer = new StandardAnalyzer()

  logger.info(s"Created lucene index for shard=$shardNum at $indexDiskLocation")

  private val config = new IndexWriterConfig(analyzer)
  config.setInfoStream(new LuceneMetricsRouter(shardNum))
  config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)

  private val endTimeSort = new Sort(new SortField(END_TIME, SortField.Type.LONG),
                                     new SortField(START_TIME, SortField.Type.LONG))
  config.setIndexSort(endTimeSort)
  private val indexWriter = new IndexWriter(mMapDirectory, config)

  private val utf8ToStrCache = concurrentCache[UTF8Str, String](PartKeyLuceneIndex.MAX_STR_INTERN_ENTRIES)

  //scalastyle:off
  private val searcherManager = new SearcherManager(indexWriter, null)
  //scalastyle:on

  //start this thread to flush the segments and refresh the searcher every specific time period
  private val flushThread = new ControlledRealTimeReopenThread(indexWriter,
                                                               searcherManager,
                                                               storeConfig.partIndexFlushMaxDelaySeconds,
                                                               storeConfig.partIndexFlushMinDelaySeconds)
  private val luceneDocument = new ThreadLocal[Document]()

  private val mapConsumer = new MapItemConsumer {
    def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
      import filodb.core._
      val key = utf8ToStrCache.getOrElseUpdate(new UTF8Str(keyBase, keyOffset + 2,
                                                           UTF8StringMedium.numBytes(keyBase, keyOffset)),
                                               _.toString)
      val value = new BytesRef(valueBase.asInstanceOf[Array[Byte]],
                               unsafeOffsetToBytesRefOffset(valueOffset + 2), // add 2 to move past numBytes
                               UTF8StringMedium.numBytes(valueBase, valueOffset))
      addIndexEntry(key, value, index)
    }
  }

  /**
    * Map of partKey column to the logic for indexing the column (aka Indexer).
    * Optimization to avoid match logic while iterating through each column of the partKey
    */
  private final val indexers = dataset.partitionColumns.zipWithIndex.map { case (c, pos) =>
    c.columnType match {
      case StringColumn => new Indexer {
        val colName = UTF8Str(c.name)
        def fromPartKey(base: Any, offset: Long, partIndex: Int): Unit = {
          val strOffset = dataset.partKeySchema.getStringOffset(base, UnsafeUtils.arayOffset, pos)
          val value = new BytesRef(base.asInstanceOf[Array[Byte]], strOffset + 2,
            UTF8StringMedium.numBytes(base, UnsafeUtils.arayOffset + strOffset))
          addIndexEntry(colName.toString, value, partIndex)
        }
        def getNamesValues(key: PartitionKey): Seq[(UTF8Str, UTF8Str)] = ??? // not used
      }
      case MapColumn => new Indexer {
        def fromPartKey(base: Any, offset: Long, partIndex: Int): Unit = {
          dataset.partKeySchema.consumeMapItems(base, offset, pos, mapConsumer)
        }
        def getNamesValues(key: PartitionKey): Seq[(UTF8Str, UTF8Str)] = ??? // not used
      }
      case other: Any =>
        logger.warn(s"Column $c has type that cannot be indexed and will be ignored right now")
        NoOpIndexer
    }
  }.toArray

  def reset(): Unit = indexWriter.deleteAll()

  def startFlushThread(): Unit = {
    flushThread.start()
    logger.info(s"Started flush thread for lucene index on shard $shardNum")
  }

  def removeEntries(prunedPartitions: EWAHCompressedBitmap): Unit = {
    val deleteQuery = IntPoint.newSetQuery(PartKeyLuceneIndex.PART_ID, prunedPartitions.toList)
    indexWriter.deleteDocuments(deleteQuery)
  }

  /**
    * Use to delete partitions that were ingesting before retention period
    * @return partIds of deleted partitions
    */
  def removePartKeysEndedBefore(endedBefore: Long): IntIterator = {
    val collector = new PartIdCollector()
    val deleteQuery = LongPoint.newRangeQuery(PartKeyLuceneIndex.END_TIME, 0, endedBefore)
    searcherManager.acquire().search(deleteQuery, collector)
    indexWriter.deleteDocuments(deleteQuery)
    collector.intIterator()
  }

  def indexRamBytes: Long = indexWriter.ramBytesUsed()

  /**
    * Number of documents in flushed index, excludes tombstones for deletes
    */
  def indexNumEntries: Long = indexWriter.numDocs()

  /**
    * Number of documents in flushed index, includes tombstones for deletes
    */
  def indexNumEntriesWithTombstones: Long = indexWriter.maxDoc()

  def closeIndex(): Unit = {
    logger.info(s"Closing index on shard ${shardNum}")
    flushThread.close()
    indexWriter.close()
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
    val searcher = searcherManager.acquire()
    val indexReader = searcher.getIndexReader
    val segments = indexReader.leaves()
    val freqOrder = Ordering.by[TermInfo, Int](_.freq)
    val topkResults = new PriorityQueue[TermInfo](topK, freqOrder)
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
    topkResults.toArray(new Array[TermInfo](0)).sortBy(-_.freq).toSeq
  }

  def indexNames: scala.Iterator[String] = {
    val searcher = searcherManager.acquire()
    val indexReader = searcher.getIndexReader
    val segments = indexReader.leaves()
    segments.asScala.iterator.flatMap { segment =>
      segment.reader().getFieldInfos.asScala.toIterator.map(_.name)
    }.filterNot { n => ignoreIndexNames.contains(n) }
  }

  private def addIndexEntry(labelName: String, value: BytesRef, partIndex: Int): Unit = {
    luceneDocument.get().add(new StringField(labelName, value, Store.NO))
  }

  def addPartKey(partKeyOnHeapBytes: Array[Byte],
                 partId: Int,
                 startTime: Long,
                 endTime: Long = Long.MaxValue,
                 partKeyBytesRefOffset: Int = 0)
                (partKeyNumBytes: Int = partKeyOnHeapBytes.length): Unit = {
    val document = makeDocument(partKeyOnHeapBytes, partKeyBytesRefOffset, partKeyNumBytes, partId, startTime, endTime)
    logger.debug(s"Adding document ${partKeyString(partId, partKeyOnHeapBytes, partKeyBytesRefOffset)} " +
                 s"with startTime=$startTime endTime=$endTime")
    indexWriter.addDocument(document)
  }

  def upsertPartKey(partKeyOnHeapBytes: Array[Byte],
                    partId: Int,
                    startTime: Long,
                    endTime: Long = Long.MaxValue,
                    partKeyBytesRefOffset: Int = 0)
                   (partKeyNumBytes: Int = partKeyOnHeapBytes.length): Unit = {
    val document = makeDocument(partKeyOnHeapBytes, partKeyBytesRefOffset, partKeyNumBytes, partId, startTime, endTime)
    logger.debug(s"Upserting document ${partKeyString(partId, partKeyOnHeapBytes, partKeyBytesRefOffset)} " +
                 s"with startTime=$startTime endTime=$endTime")
    indexWriter.updateDocument(new Term(PART_ID, partId.toString), document)
  }

  private def partKeyString(partId: Int,
                            partKeyOnHeapBytes: Array[Byte],
                            partKeyBytesRefOffset: Int = 0): String = {
    //scalastyle:off
    s"sh$shardNum-pId$partId[${
      TimeSeriesPartition
        .partKeyString(dataset, partKeyOnHeapBytes, bytesRefToUnsafeOffset(partKeyBytesRefOffset))
    }"
    //scalastyle:on
  }

  private def makeDocument(partKeyOnHeapBytes: Array[Byte],
                           partKeyBytesRefOffset: Int,
                           partKeyNumBytes: Int,
                           partId: Int, startTime: Long, endTime: Long): Document = {
    val document = new Document()
    // TODO We can use RecordSchema.toStringPairs to get the name/value pairs from partKey.
    // That is far more simpler with much of the logic abstracted out.
    // Currently there is a bit of leak in abstraction of Binary Record processing in this class.

    luceneDocument.set(document) // threadlocal since we are not able to pass the document into mapconsumer
    for { i <- 0 until numPartColumns optimized } {
      indexers(i).fromPartKey(partKeyOnHeapBytes, bytesRefToUnsafeOffset(partKeyBytesRefOffset), partId)
    }
    // partId
    document.add(new StringField(PART_ID, partId.toString, Store.NO)) // cant store as an IntPoint because of lucene bug
    document.add(new NumericDocValuesField(PART_ID, partId))
    // partKey
    val bytesRef = new BytesRef(partKeyOnHeapBytes, partKeyBytesRefOffset, partKeyNumBytes)
    document.add(new BinaryDocValuesField(PART_KEY, bytesRef))
    // startTime
    document.add(new LongPoint(START_TIME, startTime))
    document.add(new NumericDocValuesField(START_TIME, startTime))
    // endTime
    document.add(new LongPoint(END_TIME, endTime))
    document.add(new NumericDocValuesField(END_TIME, endTime))

    luceneDocument.remove()
    document
  }

  /**
    * Called when TSPartition needs to be created when on-demand-paging from a
    * partId that does not exist on heap
    */
  def partKeyFromPartId(partId: Int): Option[BytesRef] = {
    val collector = new SinglePartKeyCollector()
    searcherManager.acquire().search(new TermQuery(new Term(PART_ID, partId.toString)), collector)
    Option(collector.singleResult)
  }

  /**
    * Called when a document is updated with new endTime
    */
  def startTimeFromPartId(partId: Int): Long = {
    val collector = new NumericDocValueCollector(PartKeyLuceneIndex.START_TIME)
    searcherManager.acquire().search(new TermQuery(new Term(PART_ID, partId.toString)), collector)
    collector.singleResult
  }

  /**
    * Called when a document is updated with new endTime
    */
  def endTimeFromPartId(partId: Int): Long = {
    val collector = new NumericDocValueCollector(PartKeyLuceneIndex.END_TIME)
    searcherManager.acquire().search(new TermQuery(new Term(PART_ID, partId.toString)), collector)
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
    searcherManager.acquire().search(LongPoint.newRangeQuery(END_TIME, fromEndTime, toEndTime), coll)
    coll.topKPartIDsBitmap()
  }

  def foreachPartKeyStillIngesting(func: (Int, BytesRef) => Unit): Int = {
    val coll = new ActionCollector(func)
    searcherManager.acquire().search(LongPoint.newExactQuery(END_TIME, Long.MaxValue), coll)
    coll.numHits
  }

  def updatePartKeyWithEndTime(partKeyOnHeapBytes: Array[Byte],
                               partId: Int,
                               endTime: Long = Long.MaxValue,
                               partKeyBytesRefOffset: Int = 0)
                               (partKeyNumBytes: Int = partKeyOnHeapBytes.length): Unit = {
    var startTime = startTimeFromPartId(partId) // look up index for old start time
    if (startTime == NOT_FOUND) {
      startTime = System.currentTimeMillis() - storeConfig.demandPagedRetentionPeriod.toMillis
      logger.warn(s"Could not find in Lucene startTime for partId $partId. Using $startTime instead.",
        new IllegalStateException()) // assume this time series started retention period ago
    }
    val updatedDoc = makeDocument(partKeyOnHeapBytes, partKeyBytesRefOffset, partKeyNumBytes,
      partId, startTime, endTime)
    logger.debug(s"Updating document ${partKeyString(partId, partKeyOnHeapBytes, partKeyBytesRefOffset)} " +
                 s"with startTime=$startTime endTime=$endTime")
    indexWriter.updateDocument(new Term(PART_ID, partId.toString), updatedDoc)
  }

  /**
    * Refresh readers with updates to index. May be expensive - use carefully.
    * @return
    */
  def commitBlocking(): Unit = {
    searcherManager.maybeRefreshBlocking()
  }

  private def leafFilter(column: String, filter: Filter): Query = {
    filter match {
      case EqualsRegex(value) =>
        val term = new Term(column, value.toString)
        new RegexpQuery(term)
      case NotEqualsRegex(value) =>
        val term = new Term(column, value.toString)
        val allDocs = new MatchAllDocsQuery
        val booleanQuery = new BooleanQuery.Builder
        booleanQuery.add(allDocs, Occur.FILTER)
        booleanQuery.add(new RegexpQuery(term), Occur.MUST_NOT)
        booleanQuery.build()
      case Equals(value) =>
        val term = new Term(column, value.toString)
        new TermQuery(term)
      case NotEquals(value) =>
        val term = new Term(column, value.toString)
        val allDocs = new MatchAllDocsQuery
        val booleanQuery = new BooleanQuery.Builder
        booleanQuery.add(allDocs, Occur.FILTER)
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

  def partIdsFromFilters(columnFilters: Seq[ColumnFilter],
                         startTime: Long,
                         endTime: Long): IntIterator = {
    val booleanQuery = new BooleanQuery.Builder
    columnFilters.foreach { filter =>
      val q = leafFilter(filter.column, filter.filter)
      booleanQuery.add(q, Occur.FILTER)
    }
    booleanQuery.add(LongPoint.newRangeQuery(START_TIME, 0, endTime), Occur.FILTER)
    booleanQuery.add(LongPoint.newRangeQuery(END_TIME, startTime, Long.MaxValue), Occur.FILTER)
    val query = booleanQuery.build()
    logger.debug(s"Querying partKeyIndex with: $query")
    val searcher = searcherManager.acquire()
    val collector = new PartIdCollector() // passing zero for unlimited results
    searcher.search(query, collector)
    collector.intIterator()
  }
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

  override def needsScores(): Boolean = false
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
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have a partKeyDv")
    }
  }

  override def needsScores(): Boolean = false
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
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have a partKeyDv")
    }
  }

  override def needsScores(): Boolean = false
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
      def setScorer(scorer: Scorer): Unit = {}

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

  def needsScores(): Boolean = false

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
  val result = new EWAHCompressedBitmap()
  private var partIdDv: NumericDocValues = _

  override def needsScores(): Boolean = false

  override def doSetNextReader(context: LeafReaderContext): Unit = {
    //set the subarray of the numeric values for all documents in the context
    partIdDv = context.reader().getNumericDocValues(PartKeyLuceneIndex.PART_ID)
  }

  override def collect(doc: Int): Unit = {
    if (partIdDv.advanceExact(doc)) {
      result.set(partIdDv.longValue().toInt)
    } else {
      throw new IllegalStateException("This shouldn't happen since every document should have a partIdDv")
    }
  }

  def intIterator(): IntIterator = result.intIterator()
}

class ActionCollector(action: (Int, BytesRef) => Unit) extends SimpleCollector {
  private var partIdDv: NumericDocValues = _
  private var partKeyDv: BinaryDocValues = _
  private var counter: Int = 0

  override def needsScores(): Boolean = false

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

class LuceneMetricsRouter(shard: Int) extends InfoStream with StrictLogging {
  override def message(component: String, message: String): Unit = {
    logger.debug(s"shard=$shard component=$component $message")
    // TODO parse string and report metrics to kamon
  }
  override def isEnabled(component: String): Boolean = true
  override def close(): Unit = {}
}