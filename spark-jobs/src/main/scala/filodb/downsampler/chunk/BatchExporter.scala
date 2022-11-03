package filodb.downsampler.chunk

import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable

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
import filodb.downsampler.chunk.BatchExporter.{DATE_REGEX_MATCHER, LABEL_REGEX_MATCHER}
import filodb.memory.format.{TypedIterator, UnsafeUtils}
import filodb.memory.format.vectors.{Histogram, LongIterator}

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
    readablePartitions
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

  // scalastyle:off method.length
  /**
   * Returns data about a single row to export.
   * exportDataToRow will convert these into Spark Rows that conform to this.exportSchema.
   */
  private def getExportDatas(partition: ReadablePartition,
                             partKeyMap: Map[String, String],
                             rule: ExportRule): Iterator[ExportRowData] = {
    // Pseudo-struct for convenience.
    case class RangeInfo(irowStart: Int,
                         irowEnd: Int,
                         timestampIter: LongIterator,
                         valueIter: TypedIterator)

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
    val f = columns(valueCol).columnType match {
      case DoubleColumn =>
        val it = rangeInfoIter.flatMap{ info =>
          val doubleIter = info.valueIter.asDoubleIt
          (info.irowStart until info.irowEnd).iterator.map{ _ =>
            (partKeyMap, info.timestampIter.next, doubleIter.next)
          }
        }
        if (it.hasNext) {
          numPartitionsExportPrepped.increment()
        }
        it
      case HistogramColumn =>
        // If the histogram to export looks like:
        //     my_histo{l1=v1, l2=v2}
        // with bucket values:
        //     [<=10 = 4, <=20 = 15, <=Inf = 26]
        // then the exported time-series will look like:
        //    my_histo_bucket{l1=v1, l2=v2, le=10} 4
        //    my_histo_bucket{l1=v1, l2=v2, le=20} 15
        //    my_histo_bucket{l1=v1, l2=v2, le=+Inf} 26
        val info = rangeInfoIter.next
        val value = info.valueIter.asHistIt.next
        val ts = info.timestampIter.asLongIt.next
        val lit = new LongIterator {
          override def next: Long = ts
        }
        val hit = new Iterator[Histogram] with TypedIterator {
          var isDone = false
          override def next: Histogram = {
            isDone = true
            value
          }
          override def hasNext: Boolean = !isDone
        }

        val newThing = RangeInfo(info.irowStart, info.irowStart + 1, lit, hit)
        val it =
          (Iterator.single(newThing) ++ Iterator.single(info.copy(irowStart = info.irowStart + 1)) ++ rangeInfoIter)
          .flatMap{ info =>
          val histIter = info.valueIter.asHistIt.buffered
          val bucketMetric = partKeyMap("_metric_") + "_bucket"
          (info.irowStart until info.irowEnd).iterator.flatMap{ _ =>
            val hist = histIter.next()
            (0 until hist.numBuckets).iterator.map{ i =>
              val bucketTopString = {
                val raw = hist.bucketTop(i)
                if (raw.isPosInfinity) "+Inf" else raw.toString
              }
              val bucketLabels = partKeyMap ++ Map("le" -> bucketTopString, "_metric_" -> bucketMetric)
              (bucketLabels, info.timestampIter.next, hist.bucketValue(i))
            }
          }
        }

        if (it.hasNext) {
          numPartitionsExportPrepped.increment(value.numBuckets)
        }

        it

      case colType =>
        DownsamplerContext.dsLogger.warn(
          s"BatchExporter encountered unhandled ColumnType=$colType; partKeyMap=$partKeyMap")
        Iterator.empty
    }

    f.map{ case (pkeyMapWithBucket, timestamp, value) =>
      // NOTE: partition without histogram-adjusted labels, since we want the original metric name.
      val partitionByValues = getPartitionByValues(partKeyMap)
      ExportRowData(pkeyMapWithBucket, timestamp, value, partitionByValues, rule.dropLabels.toSet)
    }
  }
  // scalastyle:on method.length
}
