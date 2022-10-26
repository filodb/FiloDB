package filodb.downsampler.chunk

import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable

import kamon.Kamon
import kamon.metric.MeasurementUnit
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Column.ColumnType.DoubleColumn
import filodb.core.metadata.Schemas
import filodb.core.query.ColumnFilter
import filodb.core.store.{ChunkSetInfoReader, ReadablePartition}
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.Utils._
import filodb.downsampler.chunk.BatchExporter.{DATE_REGEX_MATCHER, LABEL_REGEX_MATCHER}
import filodb.memory.format.{TypedIterator, UnsafeUtils}

case class ExportRule(allowFilterGroups: Seq[Seq[ColumnFilter]],
                      blockFilterGroups: Seq[Seq[ColumnFilter]],
                      dropLabels: Seq[String])

/**
 * All info needed to output a result Spark Row.
 */
case class ExportRowData(partKeyMap: Map[String, String],
                         timestamp: Long,
                         value: Double,
                         partitionStrings: Iterator[String],
                         dropLabels: Set[String])

object BatchExporter {
  val LABEL_REGEX_MATCHER = """\{\{(.*)\}\}""".r
  val DATE_REGEX_MATCHER = """<<(.*)>>""".r
}

/**
 * Exports a window of data to a bucket.
 */
case class BatchExporter(downsamplerSettings: DownsamplerSettings, userStartTime: Long, userEndTime: Long) {

  @transient lazy private[downsampler] val schemas = Schemas.fromConfig(downsamplerSettings.filodbConfig).get

  @transient lazy val numPartitionsExportPrepped = Kamon.counter("num-partitions-export-prepped").withoutTags()

  @transient lazy val exportPrepBatchLatency =
    Kamon.histogram("export-prep-batch-latency", MeasurementUnit.time.milliseconds).withoutTags()

  val keyToRules = downsamplerSettings.exportKeyToRules
  val exportSchema = {
    // NOTE: ArrayBuffers are sometimes used instead of Seq's because otherwise
    //   ArrayIndexOutOfBoundsExceptions occur when Spark exports a batch.
    val fields = new mutable.ArrayBuffer[StructField](3 + downsamplerSettings.exportPathSpecPairs.size)
    fields.append(
      StructField("LABELS", StringType),
      StructField("TIMESTAMP", LongType),
      StructField("VALUE", DoubleType)
    )
    // append all partitioning columns as strings
    fields.appendAll(downsamplerSettings.exportPathSpecPairs.map(f => StructField(f._1, StringType)))
    StructType(fields)
  }
  val partitionByNames = {
    val cols = new mutable.ArrayBuffer[String](downsamplerSettings.exportPathSpecPairs.size)
    cols.appendAll(downsamplerSettings.exportPathSpecPairs.map(_._1))
    cols
  }

  // One for each name; "template" because these still contain the {{}} notation to be replaced.
  var partitionByValuesTemplate: Seq[String] = Nil
  // The indices of partitionByValuesTemplate that contain {{}} notation to be replaced.
  var partitionByValuesIndicesWithTemplate: Set[Int] = Set.empty

  // Replace <<>> template notation with formatted dates.
  {
    val date = new Date(userStartTime)
    partitionByValuesTemplate = downsamplerSettings.exportPathSpecPairs.map(_._2)
      // replace the <<>> strings with dates
      .map{ pathSpecString =>
        DATE_REGEX_MATCHER.replaceAllIn(pathSpecString, matcher => {
          val dateFormatter = new SimpleDateFormat(matcher.group(1))
          dateFormatter.format(date)
        })}
    partitionByValuesIndicesWithTemplate = partitionByValuesTemplate.zipWithIndex.filter { case (label, i) =>
      LABEL_REGEX_MATCHER.findFirstIn(label).isDefined
    }.map(_._2).toSet
  }

  // Unused, but keeping here for convenience if needed later.
  private def hashToString(bytes: Array[Byte]): String = {
    MessageDigest.getInstance("SHA-256")
      .digest(bytes)
      .map("%02x".format(_))
      .mkString
  }

  /**
   * Returns an iterator of a single column of the argument partition's chunk.
   * @param iCol the index of the column to iterate.
   * @param iRowStart the index of the row to begin iteration from.
   */
  private def getChunkColIter(partition: ReadablePartition,
                              chunkReader: ChunkSetInfoReader,
                              iCol: Int,
                              iRowStart: Int): TypedIterator = {
    val tsPtr = chunkReader.vectorAddress(iCol)
    val tsAcc = chunkReader.vectorAccessor(iCol)
    partition.chunkReader(iCol, tsAcc, tsPtr).iterate(tsAcc, tsPtr, iRowStart)
  }

  /**
   * Extracts and returns an iterator of (timestamp,value) pairs from the argument chunk.
   * @param irowStart the index of the first row to include in the result.
   * @param irowEnd the index of the final row to include in the result (exclusive).
   */
  private def getTimeValuePairs(partKeyMap: Map[String, String],
                                partition: ReadablePartition,
                                chunkReader: ChunkSetInfoReader,
                                irowStart: Int,
                                irowEnd: Int): Iterator[(Long, Double)] = {
    val timestampCol = 0  // FIXME: need a more dynamic (but efficient) solution
    val columns = partition.schema.data.columns
    val valueCol = columns.indexWhere(_.name == partition.schema.data.valueColName)
    val timestampIter = getChunkColIter(partition, chunkReader, timestampCol, irowStart).asLongIt
    columns(valueCol).columnType match {
      case DoubleColumn =>
        val valueIter = getChunkColIter(partition, chunkReader, valueCol, irowStart).asDoubleIt
        (irowStart until irowEnd).iterator.map{ _ =>
          (timestampIter.next, valueIter.next)
        }
      case colType =>
        DownsamplerContext.dsLogger.warn(
          s"BatchExporter encountered unhandled ColumnType=$colType; partKeyMap=$partKeyMap")
        Iterator.empty
    }
  }

  /**
   * Returns true iff the label map matches all filters.
   */
  private def matchAllFilters(filters: Seq[ColumnFilter],
                              labels: Map[String, String]): Boolean = {
    filters.forall( filt =>
      labels.get(filt.column)
        .exists(value => filt.filter.filterFunc(value))
    )
  }

  /**
   * Returns the Spark Rows to be exported.
   */
  def getExportRows(readablePartitions: Seq[ReadablePartition]): Iterator[Row] = {
    val startMs = System.currentTimeMillis()
    val rowIter = readablePartitions
      .iterator
      .map { part =>
        // package each partition with a map of its label-value pairs and a matching export rule (if it exists)
        val partKeyMap = {
          val rawSchemaId = RecordSchema.schemaID(part.partKeyBytes, UnsafeUtils.arayOffset)
          val schema = schemas(rawSchemaId)
          schema.partKeySchema.toStringPairs(part.partKeyBytes, UnsafeUtils.arayOffset).toMap
        }
        val rule = getRuleIfShouldExport(partKeyMap)
        (part, partKeyMap, rule)
      }
      .filter { case (part, partKeyMap, rule) => rule.isDefined }
      .flatMap { case (part, partKeyMap, rule) => getExportDatas(part, partKeyMap, rule.get) }
      .map(exportDataToRow)
    val endMs = System.currentTimeMillis()
    exportPrepBatchLatency.record(endMs - startMs)
    rowIter
  }

  /**
   * Converts an ExportData to a Spark Row.
   * The result Row *must* match this.exportSchema.
   */
  private def exportDataToRow(exportData: ExportRowData): Row = {
    val dataSeq = new mutable.ArrayBuffer[Any](3 + downsamplerSettings.exportPathSpecPairs.size)
    val filteredPartKeyMap = exportData.partKeyMap.filterKeys { label =>
      // Drop unwanted labels from the exported "labels" column.
      !downsamplerSettings.exportDropLabels.contains(label) &&
      !exportData.dropLabels.contains(label)
    }
    dataSeq.append(
      "{" + filteredPartKeyMap.map { case (k, v) => s"'$k':'$v'"}.mkString(",") + "}",
      exportData.timestamp,
      exportData.value
    )
    dataSeq.appendAll(exportData.partitionStrings)
    numPartitionsExportPrepped.increment()
    Row.fromSeq(dataSeq)
  }

  /**
   * Returns the column values used to partition the data.
   * Value order should match the order of this.partitionByCols.
   */
  def getPartitionByValues(partKeyMap: Map[String, String]): Iterator[String] = {
    partitionByValuesTemplate.iterator.zipWithIndex.map{ case (value, i) =>
      if (partitionByValuesIndicesWithTemplate.contains(i)) {
        LABEL_REGEX_MATCHER.replaceAllIn(partitionByValuesTemplate(i), matcher => partKeyMap(matcher.group(1)))
      } else {
        value
      }
    }
  }

  def getRuleIfShouldExport(partKeyMap: Map[String, String]): Option[ExportRule] = {
    keyToRules.find { case (key, rules) =>
      // find the group with a key that matches these label-values
      downsamplerSettings.exportRuleKey.zipWithIndex.forall { case (label, i) =>
        partKeyMap.get(label).contains(key(i))
      }
    }.flatMap { case (key, rules) =>
      rules.takeWhile{ rule =>
        // step through rules while we still haven't matched a "block" filter
        !rule.blockFilterGroups.exists { filterGroup =>
          matchAllFilters(filterGroup, partKeyMap)
        }
      }.find { rule =>
        // stop at a rule if its "allow" filters are either empty or match the partKey
        rule.allowFilterGroups.isEmpty || rule.allowFilterGroups.exists { filterGroup =>
          matchAllFilters(filterGroup, partKeyMap)
        }
      }
    }
  }

  /**
   * Returns data about a single row to export.
   * exportDataToRow will convert these into Spark Rows that conform to this.exportSchema.
   */
  private def getExportDatas(readablePartition: ReadablePartition,
                             partKeyMap: Map[String, String],
                             rule: ExportRule): Iterator[ExportRowData] = {
    getChunkRangeIter(readablePartition, userStartTime, userEndTime).flatMap{ chunkRow =>
      getTimeValuePairs(partKeyMap, readablePartition,
        chunkRow.chunkSetInfoReader, chunkRow.istartRow, chunkRow.iendRow)
    }.map{ case (timestamp, value) =>
      val partitionByValues = getPartitionByValues(partKeyMap)
      ExportRowData(partKeyMap, timestamp, value, partitionByValues, rule.dropLabels.toSet)
    }
  }
}
