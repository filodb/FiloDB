package filodb.core.memstore

import java.io.File

import scala.collection.JavaConverters._

import com.googlecode.javaewah.{EWAHCompressedBitmap, IntIterator}
import com.typesafe.scalalogging.StrictLogging
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.document.Field.Store
import org.apache.lucene.index.{IndexWriter, LeafReaderContext, NumericDocValues, Term}
import org.apache.lucene.search.{SearcherManager, _}
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.store.MMapDirectory
import org.apache.lucene.util.BytesRef
import scalaxy.loops._

import filodb.core.Types.PartitionKey
import filodb.core.binaryrecord2.MapItemConsumer
import filodb.core.metadata.Column.ColumnType.{MapColumn, StringColumn}
import filodb.core.metadata.Dataset
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.query.Filter._
import filodb.core.store.StoreConfig
import filodb.memory.{BinaryRegionLarge, UTF8StringMedium}
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String => UTF8Str}


object PartKeyLuceneIndex {
  final val PER_DOC_PART_ID = "PER_DOC_PART_ID"
  final val SEARCHABLE_PART_ID = "SEARCHABLE_PART_ID"
  val MAX_STR_INTERN_ENTRIES = 10000
}

class PartKeyLuceneIndex(dataset: Dataset, storeConfig: StoreConfig) extends StrictLogging {

  private val numPartColumns = dataset.partitionColumns.length
  private val indexDiskLocation = createTempDir.toPath
  val mMapDirectory = new MMapDirectory(indexDiskLocation)
  val analyzer = new StandardAnalyzer()

  logger.info(s"Created lucene index at $indexDiskLocation")

  import org.apache.lucene.index.IndexWriterConfig

  val config = new IndexWriterConfig(analyzer)
  val indexWriter = new IndexWriter(mMapDirectory, config)

  val stringInternCache = new StringInternCache()

  //scalastyle:off
  val searcherManager = new SearcherManager(indexWriter, null)
  //scalastyle:on

  //start this thread to flush the segments and refresh the searcher every specific time period
  val flushThread = new ControlledRealTimeReopenThread(indexWriter,
                                                       searcherManager,
                                                       storeConfig.partIndexFlushMaxDelaySeconds,
                                                       storeConfig.partIndexFlushMinDelaySeconds)
  flushThread.start()

  val partitionColumns = dataset.partitionColumns

  def reset(): Unit = indexWriter.deleteAll()

  def removeEntries(prunedPartitions: EWAHCompressedBitmap): Unit = {
    val deleteQuery = IntPoint.newSetQuery(PartKeyLuceneIndex.SEARCHABLE_PART_ID, prunedPartitions.toList)
    indexWriter.deleteDocuments(deleteQuery)
  }

  def indexBytes: Long = indexWriter.ramBytesUsed()

  def indexSize: Long = indexWriter.maxDoc()

  /**
    * Fetch values for a specific tag
    */
  def indexValues(fieldName: String): scala.Iterator[UTF8Str] = {
    val searcher = searcherManager.acquire()
    val indexReader = searcher.getIndexReader
    val segments = indexReader.leaves()
    segments.asScala.iterator.flatMap { segment =>
      val terms = segment.reader().terms(fieldName)
      //scalastyle:off
      if (terms != null) {
        new Iterator[UTF8Str] {
          val termsEnum = terms.iterator()
          var nextVal: BytesRef = termsEnum.next()
          override def hasNext: Boolean = nextVal != null
          //scalastyle:on
          override def next(): UTF8Str = {
            val valu = BytesRef.deepCopyOf(nextVal) // copy is needed since lucene uses a mutable cursor underneath
            val ret = new UTF8Str(valu.bytes, valu.offset + UnsafeUtils.arayOffset, valu.length)
            nextVal = termsEnum.next()
            ret
          }
        }
      } else {
        Iterator.empty
      }
    }
  }

  def indexNames: scala.Iterator[String] = {
    val searcher = searcherManager.acquire()
    val indexReader = searcher.getIndexReader
    val segments = indexReader.leaves()
    segments.asScala.iterator.flatMap { segment =>
      segment.reader().getFieldInfos.asScala.toIterator.map(_.name)
    }.filterNot { n => n == PartKeyLuceneIndex.PER_DOC_PART_ID || n == PartKeyLuceneIndex.SEARCHABLE_PART_ID }
  }

  var partKeyOnHeapBytes: Array[Byte] = _
  var partKeyOffHeapOffset: Long = _
  var document: Document = _

  val mapConsumer = new MapItemConsumer {
    def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
      val key = stringInternCache.intern(new UTF8Str(keyBase,
                                                     keyOffset + 2,
                                                     UTF8StringMedium.numBytes(keyBase, keyOffset)))
      val value = new BytesRef(partKeyOnHeapBytes,
                               (valueOffset - partKeyOffHeapOffset + 2).toInt,
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
          val strOffset = dataset.partKeySchema.getStringOffset(partKeyOnHeapBytes, UnsafeUtils.arayOffset, pos)
          val value = new BytesRef(partKeyOnHeapBytes,
            strOffset + 2,
            UTF8StringMedium.numBytes(partKeyOnHeapBytes, UnsafeUtils.arayOffset + strOffset))
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

  def addIndexEntry(labelName: String, value: BytesRef, partIndex: Int): Unit = {
    document.add(new StringField(labelName, value, Store.NO))
  }

  def addPartKey(base: Any, offset: Long, partIndex: Int): Unit = {
    document = new Document()
    partKeyOnHeapBytes = BinaryRegionLarge.asNewByteArray(base, offset)
    partKeyOffHeapOffset = offset
    for { i <- 0 until numPartColumns optimized } {
      indexers(i).fromPartKey(base, offset, partIndex)
    }
    document.add(new IntPoint(PartKeyLuceneIndex.SEARCHABLE_PART_ID, partIndex))
    document.add(new NumericDocValuesField(PartKeyLuceneIndex.PER_DOC_PART_ID, partIndex))
    indexWriter.addDocument(document)
  }

  /**
    * Explicit commit of index to disk - to be used for testing only.
    * @return
    */
  private[memstore] def commitBlocking() = {
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
        andQuery.add(leafFilter(column, lhs), Occur.MUST)
        andQuery.add(leafFilter(column, rhs), Occur.MUST)
        andQuery.build()
      case _ => throw new UnsupportedOperationException
    }
  }

  def parseFilters(columnFilters: Seq[ColumnFilter], itemLimit: Int = 10000): IntIterator = {
    val booleanQuery = new BooleanQuery.Builder
    columnFilters.foreach { filter =>
      val q = leafFilter(filter.column, filter.filter)
      booleanQuery.add(q, Occur.FILTER)
    }
    val searcher = searcherManager.acquire()
    val collector = new EntryCollector()
    searcher.search(booleanQuery.build(), collector)
    collector.intIterator()
  }

  private def createTempDir: File = {
    val baseDir = new File(System.getProperty("java.io.tmpdir"))
    val baseName = PartKeyLuceneIndex.PER_DOC_PART_ID + System.currentTimeMillis() + "-"
    val tempDir = new File(baseDir, baseName)
    tempDir.mkdir()
    tempDir
  }

}

class EntryCollector extends SimpleCollector {
  val values = new EWAHCompressedBitmap()
  var numericDocValues: NumericDocValues = _

  override def needsScores(): Boolean = false

  override def doSetNextReader(context: LeafReaderContext): Unit = {
    //set the subarray of the numeric values for all documents in the context
    numericDocValues = context.reader().getNumericDocValues(PartKeyLuceneIndex.PER_DOC_PART_ID)
  }

  override def collect(doc: Int): Unit = {
    //collect sends me the matched document offset
    val current = numericDocValues.advance(doc)
    //the matched docs always 'collects' in sequential order.
    //so advance should return the offset to the match
    //otherwise something wrong with the data itself
    if (current == doc) {
      values.set(numericDocValues.longValue().toInt)
    } else {
      throw new IllegalStateException()
    }
  }

  def intIterator(): IntIterator = values.intIterator()
}

class StringInternCache {

  val cache = new java.util.LinkedHashMap[UTF8Str, String](PartKeyLuceneIndex.MAX_STR_INTERN_ENTRIES + 1, .75F, true) {
    // This method is called just after a new entry has been added
    override def removeEldestEntry(eldest: java.util.Map.Entry[UTF8Str, String]): Boolean =
      size > PartKeyLuceneIndex.MAX_STR_INTERN_ENTRIES
  }

  def intern(key: UTF8Str): String = {
    var str = cache.get(key)
    //scalastyle:off
    if (str == null) str = key.toString
    //scalastyle:on
    cache.put(key, str)
    str
  }
}