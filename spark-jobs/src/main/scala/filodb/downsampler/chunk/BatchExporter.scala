package filodb.downsampler.chunk

import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable
import scala.util.matching.Regex
import kamon.Kamon
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Column.ColumnType.{DoubleColumn, HistogramColumn}
import filodb.core.metadata.Schemas
import filodb.core.query.ColumnFilter
import filodb.core.store.{ChunkSetInfoReader, ReadablePartition}
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.Utils._
import filodb.downsampler.chunk.BatchExporter.{DATE_REGEX_MATCHER, LABEL_REGEX_MATCHER, getExportLabelValueString}
import filodb.memory.format.{TypedIterator, UnsafeUtils}
import filodb.memory.format.vectors.LongIterator


case class ExportRule(allowFilterGroups: Seq[Seq[ColumnFilter]],
                      blockFilterGroups: Seq[Seq[ColumnFilter]],
                      dropLabels: Seq[String])

/**
 * All info needed to output a result Spark Row.
 */
case class ExportRowData(partKeyMap: collection.Map[String, String],
                         labelString: String,
                         timestamp: Long,
                         value: Double,
                         partitionStrings: Iterator[String],
                         dropLabels: Set[String])

object BatchExporter {
  val LABEL_REGEX_MATCHER: Regex = """\{\{(.*)\}\}""".r
  val DATE_REGEX_MATCHER: Regex = """<<(.*)>>""".r

  /**
   * Converts a label's value to a value of an exported row's LABELS column.
   */
  def getExportLabelValueString(value: String): String = {
    value
      // escape all single- and double-quotes if they aren't already escaped
      .replaceAll("""\\(\"|\')|(\"|\')""", """\\$1$2""")
  }
}

/**
 * Exports a window of data to a bucket.
 */
case class BatchExporter(downsamplerSettings: DownsamplerSettings, userStartTime: Long, userEndTime: Long) {

  @transient lazy private[downsampler] val schemas = Schemas.fromConfig(downsamplerSettings.filodbConfig).get

  @transient lazy val numPartitionsExportPrepped = Kamon.counter("num-partitions-export-prepped").withoutTags()

  @transient lazy val numRowsExportPrepped = Kamon.counter("num-rows-export-prepped").withoutTags()

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
   * Returns true iff the label map matches all filters.
   */
  private def matchAllFilters(filters: Seq[ColumnFilter],
                              labels: collection.Map[String, String]): Boolean = {
    filters.forall( filt =>
      labels.get(filt.column)
        .exists(value => filt.filter.filterFunc(value))
    )
  }

  /**
   * Returns the Spark Rows to be exported.
   */
  def getExportRows(readablePartitions: Seq[ReadablePartition]): Iterator[Row] = {
    readablePartitions
      .iterator
      .map { part =>
        // package each partition with a map of its label-value pairs and a matching export rule (if it exists)
        // FIXME: scala immutable wrapper?
        val partKeyMap: mutable.Map[String, String] = {
          val rawSchemaId = RecordSchema.schemaID(part.partKeyBytes, UnsafeUtils.arayOffset)
          val pairs = schemas(rawSchemaId).partKeySchema.toStringPairs(part.partKeyBytes, UnsafeUtils.arayOffset)
          val pairMap = new mutable.HashMap[String, String]()
          pairMap ++= pairs
          // replace _metric_ label name with __name__
          pairMap("__name__") = pairMap("_metric_")
          pairMap.remove("_metric_")
          pairMap
        }
        val rule = getRuleIfShouldExport(partKeyMap)
        (part, partKeyMap, rule)
      }
      .filter { case (part, partKeyMap, rule) => rule.isDefined }
      .flatMap { case (part, partKeyMap, rule) =>
        getExportDatas(part, partKeyMap, rule.get)
      }
      .map { exportData =>
        numRowsExportPrepped.increment()
        exportDataToRow(exportData)
      }
  }

  /**
   * Converts an ExportData to a Spark Row.
   * The result Row *must* match this.exportSchema.
   */
  private def exportDataToRow(exportData: ExportRowData): Row = {
    val dataSeq = new mutable.ArrayBuffer[Any](3 + downsamplerSettings.exportPathSpecPairs.size)
    dataSeq.append(
      exportData.labelString,
      exportData.timestamp / 1000,
      exportData.value
    )
    dataSeq.appendAll(exportData.partitionStrings)
    Row.fromSeq(dataSeq)
  }

  /**
   * Returns the column values used to partition the data.
   * Value order should match the order of this.partitionByCols.
   */
  def getPartitionByValues(partKeyMap: collection.Map[String, String]): Iterator[String] = {
    partitionByValuesTemplate.iterator.zipWithIndex.map{ case (value, i) =>
      if (partitionByValuesIndicesWithTemplate.contains(i)) {
        LABEL_REGEX_MATCHER.replaceAllIn(partitionByValuesTemplate(i), matcher => partKeyMap(matcher.group(1)))
      } else {
        value
      }
    }
  }

  def getRuleIfShouldExport(partKeyMap: collection.Map[String, String]): Option[ExportRule] = {
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

  private def makeLabelString(labels: collection.Map[String, String]): String = {
    val inner = labels.map {case (k, v) => (k, getExportLabelValueString(v))}.map {case (k, v) =>
      if (v.contains (",") ) {
        String.format ("'%s\':\"%s\"", k, v)
      } else {
        s"'$k':'$v'"
      }
    }.mkString (",")
    s"{$inner}"
  }

  private def mergeLabelStrings(left: String, right: String): String = {
    left.substring(0, left.size - 1) + "," + right.substring(1)
  }

  // scalastyle:off method.length
  /**
   * Returns data about a single row to export.
   * exportDataToRow will convert these into Spark Rows that conform to this.exportSchema.
   */
  private def getExportDatas(partition: ReadablePartition,
                             partKeyMap: mutable.Map[String, String],
                             rule: ExportRule): Iterator[ExportRowData] = {
    // Pseudo-struct for convenience.
    case class RangeInfo(irowStart: Int,
                         irowEnd: Int,
                         timestampIter: LongIterator,
                         valueIter: TypedIterator)

    // drop all unwanted labels
    downsamplerSettings.exportDropLabels.foreach(drop => partKeyMap.remove(drop))
    rule.dropLabels.foreach(drop => partKeyMap.remove(drop))

    val timestampCol = 0  // FIXME: need a more dynamic (but efficient) solution
    val columns = partition.schema.data.columns
    val valueCol = columns.indexWhere(_.name == partition.schema.data.valueColName)
    val rangeInfoIter = getChunkRangeIter(partition, userStartTime, userEndTime).map{ chunkRange =>
      val infoReader = chunkRange.chunkSetInfoReader
      val irowStart = chunkRange.istartRow
      val irowEnd = chunkRange.iendRow
      val timestampIter = getChunkColIter(partition, infoReader, timestampCol, irowStart).asLongIt
      val valueIter = getChunkColIter(partition, infoReader, valueCol, irowStart)
      RangeInfo(irowStart, irowEnd, timestampIter, valueIter)
    }
    if (!rangeInfoIter.hasNext) {
      return Iterator.empty
    }
    val tupleIter = columns(valueCol).columnType match {
      case DoubleColumn =>
        val labelString = makeLabelString(partKeyMap)
        rangeInfoIter.flatMap{ info =>
          val doubleIter = info.valueIter.asDoubleIt
          (info.irowStart to info.irowEnd).iterator.map{ _ =>
            (partKeyMap, labelString, info.timestampIter.next, doubleIter.next)
          }
        }
      case HistogramColumn =>
        // If the histogram to export looks like:
        //     my_histo{l1=v1, l2=v2}
        // with bucket values:
        //     [<=10 = 4, <=20 = 15, <=Inf = 26]
        // then the exported time-series will look like:
        //    my_histo_bucket{l1=v1, l2=v2, le=10} 4
        //    my_histo_bucket{l1=v1, l2=v2, le=20} 15
        //    my_histo_bucket{l1=v1, l2=v2, le=+Inf} 26

        val nameWithoutBucket = partKeyMap("__name__")
        val nameWithBucket = nameWithoutBucket + "_bucket"

        // make labelString without __name__; will be replaced with _bucket-suffixed name
        partKeyMap.remove("__name__")
        val baseLabelString = makeLabelString(partKeyMap)
        partKeyMap.put("__name__", nameWithoutBucket)

        rangeInfoIter.flatMap{ info =>
          val histIter = info.valueIter.asHistIt
          (info.irowStart to info.irowEnd).iterator.flatMap{ _ =>
            val hist = histIter.next()
            val timestamp = info.timestampIter.next
            (0 until hist.numBuckets).iterator.map{ i =>
              val bucketTopString = {
                val raw = hist.bucketTop(i)
                if (raw.isPosInfinity) "+Inf" else raw.toString
              }
              val bucketMapping = Map("__name__" -> nameWithBucket, "le" -> bucketTopString)
              val bucketLabels = partKeyMap ++ bucketMapping
              val labelString = mergeLabelStrings(baseLabelString, makeLabelString(bucketMapping))
              (bucketLabels, labelString, timestamp, hist.bucketValue(i))
            }
          }
        }
      case colType =>
        DownsamplerContext.dsLogger.warn(
          s"BatchExporter encountered unhandled ColumnType=$colType; partKeyMap=$partKeyMap")
        Iterator.empty
    }

    if (tupleIter.hasNext) {
      // NOTE: this will not count each histogram bucket; the logic to do that is messy.
      numPartitionsExportPrepped.increment()
    }

    val dropLabelSet = rule.dropLabels.toSet
    tupleIter.map{ case (pkeyMapWithBucket, labelString, timestamp, value) =>
      // NOTE: partition without histogram-adjusted labels, since we want the original metric name.
      val partitionByValues = getPartitionByValues(partKeyMap)
      ExportRowData(pkeyMapWithBucket, labelString, timestamp, value, partitionByValues, dropLabelSet)
    }
  }
  // scalastyle:on method.length
}
