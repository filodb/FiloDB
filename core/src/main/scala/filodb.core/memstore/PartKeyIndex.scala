package filodb.core.memstore

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.regex.Pattern

import scala.collection.immutable.HashSet
import scala.util.{Failure, Success}

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.metric.MeasurementUnit
import org.apache.lucene.util.BytesRef

import filodb.core.{concurrentCache, DatasetRef, Utils}
import filodb.core.Types.PartitionKey
import filodb.core.binaryrecord2.MapItemConsumer
import filodb.core.memstore.PartKeyIndexRaw.{bytesRefToUnsafeOffset, createTempDir, END_TIME, START_TIME}
import filodb.core.memstore.PartKeyLuceneIndex.unsafeOffsetToBytesRefOffset
import filodb.core.memstore.PartKeyQueryBuilder.removeRegexAnchors
import filodb.core.memstore.ratelimit.CardinalityTracker
import filodb.core.metadata.Column.ColumnType.{MapColumn, StringColumn}
import filodb.core.metadata.PartitionSchema
import filodb.core.query.{ColumnFilter, Filter, QueryUtils}
import filodb.core.query.Filter.{And, Equals, EqualsRegex, In, NotEquals, NotEqualsRegex}
import filodb.memory.{UTF8StringMedium, UTF8StringShort}
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String => UTF8Str}

object PartKeyIndexRaw {
  // NOTE: these partId fields need to be separate because Lucene 9.7.0 enforces consistent types for document
  //   field values (i.e. a field cannot have both numeric and string values). Additional details can be found
  //   here: https://github.com/apache/lucene/pull/11
  final val PART_ID_DV = "__partIdDv__"
  final val PART_ID_FIELD = "__partIdField__"
  final val START_TIME = "__startTime__"
  final val END_TIME = "__endTime__"
  final val PART_KEY = "__partKey__"
  final val LABEL_LIST = s"__labelList__"
  final val FACET_FIELD_PREFIX = "$facet_"
  final val LABEL_LIST_FACET = FACET_FIELD_PREFIX + LABEL_LIST

  final val ignoreIndexNames = HashSet(START_TIME, PART_KEY, END_TIME, PART_ID_FIELD, PART_ID_DV)

  def bytesRefToUnsafeOffset(bytesRefOffset: Int): Int = bytesRefOffset + UnsafeUtils.arayOffset

  private def defaultTempDir(ref: DatasetRef, shardNum: Int): File = {
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
}

abstract class PartKeyIndexRaw(ref: DatasetRef,
                               shardNum: Int,
                               schema: PartitionSchema,
                               diskLocation: Option[File] = None,
                               protected val lifecycleManager: Option[IndexMetadataStore] = None)
  extends StrictLogging {

  protected val startTimeLookupLatency = Kamon.histogram("index-startTimes-for-odp-lookup-latency",
      MeasurementUnit.time.nanoseconds)
    .withTag("dataset", ref.dataset)
    .withTag("shard", shardNum)

  protected val queryIndexLookupLatency = Kamon.histogram("index-partition-lookup-latency",
      MeasurementUnit.time.nanoseconds)
    .withTag("dataset", ref.dataset)
    .withTag("shard", shardNum)

  protected val indexDiskLocation = diskLocation.map(new File(_, ref.dataset + File.separator + shardNum))
    .getOrElse(createTempDir(ref, shardNum)).toPath

  // If index rebuild is triggered or the state is Building, simply clean up the index directory and start
  // index rebuild
  if (
    lifecycleManager.forall(_.shouldTriggerRebuild(ref, shardNum))
  ) {
    logger.info(s"Cleaning up indexDirectory=$indexDiskLocation for  dataset=$ref, shard=$shardNum")
    Utils.deleteRecursively(indexDiskLocation.toFile) match {
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

  protected def loadIndexData[T](ctor: () => T): T = try {
    ctor()
  } catch {
    case e: Exception =>
      // If an exception is thrown here there is something wrong with the index or the directory
      // We will attempt once by cleaning the directory and try instantiating the index again
      logger.warn(s"Index for dataset:${ref.dataset} and shard: $shardNum possibly corrupt," +
        s"index directory will be cleaned up and index rebuilt", e)
      Utils.deleteRecursively(indexDiskLocation.toFile) match {
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
      ctor()
  }


  def notifyLifecycleListener(state: IndexState.Value, time: Long): Unit =
    lifecycleManager.foreach(_.updateState(ref, shardNum, state, time))



  protected def partKeyString(docId: String,
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

  private val utf8ToStrCache = concurrentCache[UTF8Str, String](PartKeyLuceneIndex.MAX_STR_INTERN_ENTRIES)

  /**
   * Map of partKey column to the logic for indexing the column (aka Indexer).
   * Optimization to avoid match logic while iterating through each column of the partKey
   */
  protected final val indexers = schema.columns.zipWithIndex.map { case (c, pos) =>
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
        private val colName = c.name

        private val mapConsumer = new MapItemConsumer {
          def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
            import filodb.core._
            val key = utf8ToStrCache.getOrElseUpdate(new UTF8Str(keyBase, keyOffset + 1,
              UTF8StringShort.numBytes(keyBase, keyOffset)),
              _.toString)
            val value = new String(valueBase.asInstanceOf[Array[Byte]],
              unsafeOffsetToBytesRefOffset(valueOffset + 2), // add 2 to move past numBytes
              UTF8StringMedium.numBytes(valueBase, valueOffset), StandardCharsets.UTF_8)
            addIndexedMapField(colName, key, value)
          }
        }

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

  protected val numPartColumns = schema.columns.length

  private final val emptyStr = ""
  // Multi-column facets to be created on "partition-schema" columns
  protected def createMultiColumnFacets(partKeyOnHeapBytes: Array[Byte], partKeyBytesRefOffset: Int): Unit = {
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
          addMultiColumnFacet(name, concatFacetValue)
        }
      }
    })
  }


  /**
   * Add an indexed field + value to the document being prepared
   */
  protected def addIndexedField(key: String, value: String): Unit

  /**
   * Add an indexed field + value defined in a map field to the document being prepared
   */
  protected def addIndexedMapField(mapColumn: String, key: String, value: String): Unit

  /**
   * Add a facet for a computed multi-column facet
   */
  protected def addMultiColumnFacet(key: String, value: String): Unit

  /**
   * Clear the index by deleting all documents and commit
   */
  def reset(): Unit

  /**
   * Start the asynchronous thread to automatically flush
   * new writes to readers at the given min and max delays
   */
  def startFlushThread(flushDelayMinSeconds: Int, flushDelayMaxSeconds: Int): Unit

  /**
   * Find partitions that ended ingesting before a given timestamp. Used to identify partitions that can be purged.
   * @return matching partIds
   */
  def partIdsEndedBefore(endedBefore: Long): debox.Buffer[Int]

  /**
   * Method to delete documents from index that ended before the provided end time
   *
   * @param endedBefore the cutoff timestamp. All documents with time <= this time will be removed
   * @param returnApproxDeletedCount a boolean flag that requests the return value to be an approximate count of the
   *                                 documents that got deleted, if value is set to false, 0 is returned
   */
  def removePartitionsEndedBefore(endedBefore: Long, returnApproxDeletedCount: Boolean = true): Int

  /**
   * Delete partitions with given partIds
   */
  def removePartKeys(partIds: debox.Buffer[Int]): Unit

  /**
   * Memory used by index, esp for unflushed data
   */
  def indexRamBytes: Long

  /**
   * Number of documents in flushed index, excludes tombstones for deletes
   */
  def indexNumEntries: Long

  /**
   * Closes the index for read by other clients. Check for implementation if commit would be done
   * automatically.
   */
  def closeIndex(): Unit

  /**
   * Return user field/dimension names in index, except those that are created internally
   */
  def indexNames(limit: Int): Seq[String]

  /**
   * Fetch values/terms for a specific column/key/field, in order from most frequent on down.
   * Note that it iterates through all docs up to a certain limit only, so if there are too many terms
   * it will not report an accurate top k in exchange for not running too long.
   * @param fieldName the name of the column/field/key to get terms for
   * @param topK the number of top k results to fetch
   */
  def indexValues(fieldName: String, topK: Int = 100): Seq[TermInfo]

  /**
   * Use faceting to get field/index names given a column filter and time range
   */
  def labelNamesEfficient(colFilters: Seq[ColumnFilter], startTime: Long, endTime: Long): Seq[String]

  /**
   * Use faceting to get field/index values given a column filter and time range
   */
  def labelValuesEfficient(colFilters: Seq[ColumnFilter], startTime: Long, endTime: Long,
                           colName: String, limit: Int = 100): Seq[String]

  /**
   * Add new part key to index
   */
  def addPartKey(partKeyOnHeapBytes: Array[Byte],
                 partId: Int,
                 startTime: Long,
                 endTime: Long = Long.MaxValue,
                 partKeyBytesRefOffset: Int = 0)
                (partKeyNumBytes: Int = partKeyOnHeapBytes.length,
                 documentId: String = partId.toString): Unit

  /**
   * Update or create part key to index
   */
  def upsertPartKey(partKeyOnHeapBytes: Array[Byte],
                    partId: Int,
                    startTime: Long,
                    endTime: Long = Long.MaxValue,
                    partKeyBytesRefOffset: Int = 0)
                   (partKeyNumBytes: Int = partKeyOnHeapBytes.length,
                    documentId: String = partId.toString): Unit

  /**
   * Called when TSPartition needs to be created when on-demand-paging from a
   * partId that does not exist on heap
   */
  def partKeyFromPartId(partId: Int): Option[BytesRef]

  /**
   * Called when a document is updated with new endTime
   */
  def startTimeFromPartId(partId: Int): Long

  /**
   * Called when a document is updated with new endTime
   */
  def endTimeFromPartId(partId: Int): Long

  /**
   * Fetch start time for given set of partIds. Used to check if ODP is needed for
   * queries.
   */
  def startTimeFromPartIds(partIds: Iterator[Int]): debox.Map[Int, Long]

  /**
   * Commit index contents to disk
   */
  def commit(): Unit

  /**
   * Update existing part key document with new endTime.
   */
  def updatePartKeyWithEndTime(partKeyOnHeapBytes: Array[Byte],
                               partId: Int,
                               endTime: Long = Long.MaxValue,
                               partKeyBytesRefOffset: Int = 0)
                              (partKeyNumBytes: Int = partKeyOnHeapBytes.length,
                               documentId: String = partId.toString): Unit
  /**
   * Refresh readers with updates to index. May be expensive - use carefully.
   * @return
   */
  def refreshReadersBlocking(): Unit

  /**
   * Fetch list of partIds for given column filters
   */
  def partIdsFromFilters(columnFilters: Seq[ColumnFilter],
                         startTime: Long,
                         endTime: Long,
                         limit: Int = Int.MaxValue): debox.Buffer[Int]

  /**
   * Fetch list of part key records for given column filters
   */
  def partKeyRecordsFromFilters(columnFilters: Seq[ColumnFilter],
                                startTime: Long,
                                endTime: Long,
                                limit: Int = Int.MaxValue): Seq[PartKeyLuceneIndexRecord]

  /**
   * Fetch partId given partKey. This is slower since it would do an index search
   * instead of a key-lookup.
   */
  def partIdFromPartKeySlow(partKeyBase: Any,
                            partKeyOffset: Long): Option[Int]

  /**
   * Fetch one partKey matching filters
   */
  def singlePartKeyFromFilters(columnFilters: Seq[ColumnFilter],
                               startTime: Long,
                               endTime: Long): Option[Array[Byte]]

}

abstract class PartKeyIndexDownsampled(ref: DatasetRef,
                                       shardNum: Int,
                                       schema: PartitionSchema,
                                       diskLocation: Option[File] = None,
                                       lifecycleManager: Option[IndexMetadataStore] = None)
  extends PartKeyIndexRaw(ref, shardNum, schema, diskLocation, lifecycleManager) {

  def getCurrentIndexState(): (IndexState.Value, Option[Long])

  /**
   * Iterate through the LuceneIndex and calculate cardinality count
   */
  def calculateCardinality(partSchema: PartitionSchema, cardTracker: CardinalityTracker): Unit

  /**
   * Run some code for each ingesting partKey which has endTime != Long.MaxValue
   */
  def foreachPartKeyStillIngesting(func: (Int, BytesRef) => Unit): Int

  /**
   * Run some code for each partKey matchin column filter
   */
  def foreachPartKeyMatchingFilter(columnFilters: Seq[ColumnFilter],
                                   startTime: Long,
                                   endTime: Long, func: (BytesRef) => Unit): Int

}

/**
 * Base class to convert incoming FiloDB ColumnFilters into the index native
 * query format.  Uses a visitor pattern to allow the implementation to build
 * different query object patterns.
 *
 * The query language supports the following:
 * * Boolean queries (many subqueries with MUST / MUST_NOT).  These may be nested.
 * * Equals queries (term match)
 * * Regex queries (regex term match)
 * * TermIn queries (term presence in list)
 * * Prefix queries (term starts with)
 * * MatchAll queries (match all values)
 * * Range queries (start to end on a long value)
 *
 * This list may expand over time.
 *
 * The top level is always a Boolean query, even if it only has one child query.  A
 * Boolean query will never have zero child queries.  Leaf nodes are always non-Boolean
 * queries.
 *
 * For example:
 *   + represents MUST, - represents MUST_NOT
 *   Input query -> ((+Equals(Col, A) -Equals(Col, B)) +Regex(Col2, C.*))
 *
 *   The visitor call pattern would be:
 *    * visitStartBooleanQuery
 *    * visitStartBooleanQuery
 *    * visitEqualsQuery(Col, A, MUST)
 *    * visitEqualsQuery(Col, B, MUST_NOT)
 *    * visitEndBooleanQuery
 *    * visitRegexQuery(Col2, C.*, MUST)
 *    * visitEndBooleanQuery
 *
 * This class should be used instead of putting query parsing code in each
 * implementation so that common optimizations, such as query rewriting,
 * can occur in one place.
 */
abstract class PartKeyQueryBuilder {
  /**
   * Start a new boolean query
   */
  protected def visitStartBooleanQuery(): Unit

  /**
   * Indicate the current boolean query has ended
   */
  protected def visitEndBooleanQuery(): Unit

  /**
   * Add a new equals query to the current boolean query
   */
  protected def visitEqualsQuery(column: String, term: String, occur: PartKeyQueryOccur): Unit

  /**
   * Add a new Regex query to the current boolean query
   */
  protected def visitRegexQuery(column: String, pattern: String, occur: PartKeyQueryOccur): Unit

  /**
   * Add a TermsIn query to the current boolean query
   */
  protected def visitTermInQuery(column: String, terms: Seq[String], occur: PartKeyQueryOccur): Unit

  /**
   * Add a prefix query to the current boolean query
   */
  protected def visitPrefixQuery(column: String, prefix: String, occur: PartKeyQueryOccur): Unit

  /**
   * Add a match all query to the current boolean query
   */
  protected def visitMatchAllQuery(): Unit

  /**
   * Add a range match to the current boolean query
   */
  protected def visitRangeQuery(column: String, start: Long, end: Long, occur: PartKeyQueryOccur): Unit

  protected def visitQuery(columnFilters: Seq[ColumnFilter]): Unit = {
    visitStartBooleanQuery()
    columnFilters.foreach { filter =>
      visitFilter(filter.column, filter.filter)
    }
    visitEndBooleanQuery()
  }

  protected def visitQueryWithStartAndEnd(columnFilters: Seq[ColumnFilter], startTime: PartitionKey,
                                          endTime: PartitionKey): Unit = {
    visitStartBooleanQuery()
    columnFilters.foreach { filter =>
      visitFilter(filter.column, filter.filter)
    }
    visitRangeQuery(START_TIME, 0, endTime, OccurMust)
    visitRangeQuery(END_TIME, startTime, Long.MaxValue, OccurMust)
    visitEndBooleanQuery()
  }

  // scalastyle:off method.length
  private def visitFilter(column: String, filter: Filter): Unit = {
    def equalsQuery(value: String): Unit = {
      if (value.nonEmpty) visitEqualsQuery(column, value, OccurMust)
      else visitFilter(column, NotEqualsRegex(".+")) // value="" means the label is absent or has an empty value.
    }

    filter match {
      case EqualsRegex(value) =>
        val regex = removeRegexAnchors(value.toString)
        if (regex == "") {
          // if label=~"" then match empty string or label not present condition too
          visitFilter(column, NotEqualsRegex(".+"))
        } else if (regex.replaceAll("\\.\\*", "") == "") {
          // if label=~".*" then match all docs since promQL matches .* with absent label too
          visitMatchAllQuery()
        } else if (!QueryUtils.containsRegexChars(regex)) {
          // if all regex special chars absent, then treat like Equals
          equalsQuery(regex)
        } else if (QueryUtils.containsPipeOnlyRegex(regex)) {
          // if pipe is only regex special char present, then convert to IN query
          visitTermInQuery(column, regex.split('|'), OccurMust)
        } else if (regex.endsWith(".*") && regex.length > 2 &&
          !QueryUtils.containsRegexChars(regex.dropRight(2))) {
          // if suffix is .* and no regex special chars present in non-empty prefix, then use prefix query
          visitPrefixQuery(column, regex.dropRight(2), OccurMust)
        } else {
          // regular non-empty regex query
          visitRegexQuery(column, regex, OccurMust)
        }

      case NotEqualsRegex(value) =>
        val term = removeRegexAnchors(value.toString)
        visitStartBooleanQuery()
        visitMatchAllQuery()
        visitRegexQuery(column, term, OccurMustNot)
        visitEndBooleanQuery()

      case Equals(value) =>
        equalsQuery(value.toString)

      case NotEquals(value) =>
        val str = value.toString
        visitStartBooleanQuery()
        str.isEmpty match {
          case true =>
            visitRegexQuery(column, ".*", OccurMust)
          case false =>
            visitMatchAllQuery()
        }
        visitEqualsQuery(column, str, OccurMustNot)
        visitEndBooleanQuery()

      case In(values) =>
        visitTermInQuery(column, values.toArray.map(t => t.toString), OccurMust)

      case And(lhs, rhs) =>
        visitStartBooleanQuery()
        visitFilter(column, lhs)
        visitFilter(column, rhs)
        visitEndBooleanQuery()

      case _ => throw new UnsupportedOperationException
    }
  }
  //scalastyle:on method.length
}

object PartKeyQueryBuilder {

  /**
   * Remove leading anchor &#94; and ending anchor $.
   *
   * @param regex the orignal regex string.
   * @return the regex string without anchors.
   */
  def removeRegexAnchors(regex: String): String = {
    removeRegexTailDollarSign(regex.stripPrefix("^"))
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
}

/**
 * Enumeration of occur values for a query - MUST vs MUST_NOT
 */
sealed trait PartKeyQueryOccur

/**
 * Query must match
 */
case object OccurMust extends PartKeyQueryOccur

/**
 * Query must not match
 */
case object OccurMustNot extends PartKeyQueryOccur