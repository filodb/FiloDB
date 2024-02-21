package filodb.downsampler.chunk

import java.security.MessageDigest
import java.time.{Instant, LocalDateTime, ZoneId}

import kamon.Kamon
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType,
                                   TimestampType}
import scala.collection.mutable

import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Column.ColumnType.{DoubleColumn, HistogramColumn}
import filodb.core.metadata.Schemas
import filodb.core.query.ColumnFilter
import filodb.core.store.{ChunkSetInfoReader, ReadablePartition}
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.Utils._
import filodb.memory.format.{TypedIterator, UnsafeUtils}
import filodb.memory.format.vectors.LongIterator

case class ExportRule(allowFilterGroups: Seq[Seq[ColumnFilter]],
                      blockFilterGroups: Seq[Seq[ColumnFilter]],
                      dropLabels: Seq[String])

case class ExportTableConfig(tableName: String,
                             tablePath: String,
                             exportRules: Seq[ExportRule])
/**
 * All info needed to output a result Spark Row.
 */
case class ExportRowData(workspace: String,
                         namespace: String,
                         metric: String,
                         labels: collection.Map[String, String],
                         epoch_timestamp: Long,
                         timestamp: LocalDateTime,
                         value: Double,
                         year: Int,
                         month: Int,
                         day: Int,
                         hour: Int)

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
    val fields = new mutable.ArrayBuffer[StructField](11)
    fields.append(
      StructField("workspace", StringType),
      StructField("namespace", StringType),
      StructField("metric", StringType),
      StructField("labels", StringType),
      StructField("epoch_timestamp", LongType),
      StructField("timestamp", TimestampType),
      StructField("value", DoubleType),
      StructField("year", IntegerType),
      StructField("month", IntegerType),
      StructField("day", IntegerType),
      StructField("hour", IntegerType)
    )
    StructType(fields)
  }

  /**
   * Returns the index of a column in the export schema.
   * E.g. "foo" will return `3` if "foo" is the name of the fourth column (zero-indexed)
   *   of each exported row.
   */
  def getRowIndex(fieldName: String): Option[Int] = {
    exportSchema.zipWithIndex.find(_._1.name == fieldName).map(_._2)
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
        getExportData(part, partKeyMap, rule.get)
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
    val dataSeq = new mutable.ArrayBuffer[Any](this.exportSchema.fields.length)
    dataSeq.append(
      exportData.workspace,
      exportData.namespace,
      exportData.metric,
      exportData.labels,
      exportData.epoch_timestamp,
      exportData.timestamp,
      exportData.value,
      exportData.year,
      exportData.month,
      exportData.day,
      exportData.hour
    )
    Row.fromSeq(dataSeq)
  }

  def writeDataToIcebergTable(spark: SparkSession,
                              settings: DownsamplerSettings,
                              table: String,
                              tablePath: String,
                              rdd: RDD[Row]): Unit = {
    spark.sql(sqlCreateDatabase(settings.exportDatabase))
    spark.sql(sqlCreateTable(settings.exportCatalog, settings.exportDatabase, table, tablePath))
    // covert rdd to dataframe and write to table in append mode
    spark.createDataFrame(rdd, this.exportSchema)
      .write
      .format(settings.exportFormat)
      .mode(SaveMode.Append)
      .insertInto(s"${settings.exportDatabase}.$table")
  }

  private def sqlCreateDatabase(database: String) =
    s"""
       |CREATE DATABASE IF NOT EXISTS $database
       |""".stripMargin

  private def sqlCreateTable(catalog: String, database: String, table: String, tablePath: String) =
    s"""
       |CREATE TABLE IF NOT EXISTS $catalog.$database.$table (
       | workspace string,
       | namespace string,
       | metric string,
       | labels map<string, string>,
       | epoch_timestamp long,
       | timestamp timestamp,
       | value double,
       | year int,
       | month int,
       | day int,
       | hour int
       | )
       | USING iceberg
       | PARTITIONED BY (year, month, day, namespace, metric)
       | LOCATION $tablePath
       |""".stripMargin

  def getRuleIfShouldExport(partKeyMap: collection.Map[String, String]): Option[ExportRule] = {
    keyToRules.find { case (key, exportTableConfig) =>
      // find the group with a key that matches these label-values
      downsamplerSettings.exportRuleKey.zipWithIndex.forall { case (label, i) =>
        partKeyMap.get(label).contains(key(i))
      }
    }.flatMap { case (key, exportTableConfig) =>
      exportTableConfig.exportRules.takeWhile{ rule =>
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
  private def getExportData(partition: ReadablePartition,
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
        rangeInfoIter.flatMap{ info =>
          val doubleIter = info.valueIter.asDoubleIt
          (info.irowStart to info.irowEnd).iterator.map{ _ =>
            (partKeyMap, info.timestampIter.next, doubleIter.next)
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
              (bucketLabels, timestamp, hist.bucketValue(i))
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

    tupleIter.map{ case (labels, epoch_timestamp, value) =>
      val workspace = labels("_ws_")
      val namespace = labels("_ns_")
      val metric = labels("__name__")
      // to compute YYYY, MM, dd, hh
      // to compute readable timestamp from unix timestamp
      val timestamp = Instant.ofEpochMilli(epoch_timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime()
      val year = timestamp.getYear()
      val month = timestamp.getMonthValue()
      val day = timestamp.getDayOfMonth()
      val hour = timestamp.getHour()
      ExportRowData(workspace, namespace, metric, labels, epoch_timestamp, timestamp, value, year, month, day, hour)
    }
  }
  // scalastyle:on method.length
}
