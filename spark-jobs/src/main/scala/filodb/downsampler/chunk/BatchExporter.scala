package filodb.downsampler.chunk

import java.security.MessageDigest
import java.time.{Instant, ZoneId}

import kamon.Kamon
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType
import scala.collection.mutable

import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Column.ColumnType.{DoubleColumn, HistogramColumn}
import filodb.core.metadata.Schemas
import filodb.core.query.ColumnFilter
import filodb.core.store.{ChunkSetInfoReader, ReadablePartition}
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.Utils._
import filodb.downsampler.chunk.ExportConstants._
import filodb.memory.format.{TypedIterator, UnsafeUtils}
import filodb.memory.format.vectors.LongIterator

case class ExportRule(allowFilterGroups: Seq[Seq[ColumnFilter]],
                      blockFilterGroups: Seq[Seq[ColumnFilter]],
                      dropLabels: Seq[String])

case class ExportTableConfig(tableName: String,
                             tableSchema: StructType,
                             tablePath: String,
                             exportRules: Seq[ExportRule],
                             labelColumnMapping: Seq[(String, String, String)],
                             partitionByCols: Seq[String])

/**
 * All info needed to output a result Spark Row.
 */
case class ExportRowData(metric: String,
                         labels: collection.Map[String, String],
                         epoch_timestamp: Long,
                         // IMPORTANT: a Spark-compatible value must be used here (something
                         //   like LocalDateTime will throw exceptions).
                         timestamp: java.sql.Timestamp,
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

  @transient lazy val numRowExportPrepErrors = Kamon.counter("num-row-export-prep-errors").withoutTags()

  val keyToRules = downsamplerSettings.exportKeyToRules

  /**
   * Returns the index of a column in the export schema.
   * E.g. "foo" will return `3` if "foo" is the name of the fourth column (zero-indexed)
   * of each exported row.
   */
  def getColumnIndex(colName: String, exportTableConfig: ExportTableConfig): Option[Int] = {
    // check if fieldName is from dynamic schema
    // if yes, then get the col name for that label from dynamic schema else search for fieldName
    val dynamicColName = exportTableConfig.labelColumnMapping.find(_._1 == colName)
    val fieldName = if (dynamicColName.isEmpty) colName else dynamicColName.get._2
    exportTableConfig.tableSchema.fieldNames.zipWithIndex.find(_._1 == fieldName).map(_._2)
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
   *
   * @param iCol      the index of the column to iterate.
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
    filters.forall(filt =>
      labels.get(filt.column)
        .exists(value => filt.filter.filterFunc(value))
    )
  }

  /**
   * Returns the Spark Rows to be exported.
   */
  def getExportRows(readablePartitions: Seq[ReadablePartition], exportTableConfig: ExportTableConfig): Iterator[Row] = {
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
          pairMap(LABEL_NAME) = pairMap(LABEL_METRIC)
          pairMap.remove(LABEL_METRIC)
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
        try {
          val row = exportDataToRow(exportData, exportTableConfig)
          numRowsExportPrepped.increment()
          Some(row)
        } catch {
          case t: Throwable =>
            if (downsamplerSettings.logAllRowErrors) {
              DownsamplerContext.dsLogger.error(s"error during exportDataToRow: $exportData", t)
            }
            numRowExportPrepErrors.increment()
            None
        }
      }
      .filter(_.isDefined)
      .map(_.get)
  }

  /**
   * Converts an ExportData to a Spark Row.
   * The result Row *must* match exportSchema.
   */
  private def exportDataToRow(exportData: ExportRowData, exportTableConfig: ExportTableConfig): Row = {
    val dataSeq = new mutable.ArrayBuffer[Any](exportTableConfig.tableSchema.fields.length)
    // append all dynamic column values
    exportTableConfig.labelColumnMapping.foreach { pair =>
      val labelValue = exportData.labels.get(pair._1)
      assert(labelValue.isDefined, s"${pair._1} label was expected but not found: ${exportData.labels}")
      dataSeq.append(labelValue.get)
    }
    // append all fixed column values
    dataSeq.append(
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
                              exportTableConfig: ExportTableConfig,
                              rdd: RDD[Row]): Unit = {
    spark.sql(sqlCreateDatabase(settings.exportDatabase))
    spark.sql(sqlCreateTable(settings.exportCatalog, settings.exportDatabase, exportTableConfig))
    val partitionColNames = Seq("year", "month", "day") ++ exportTableConfig.partitionByCols ++ Seq("metric")
    var df = spark.createDataFrame(rdd, exportTableConfig.tableSchema)
    // distribution mode: none, does not request any shuffles or sort to be performed automatically by Spark.
    // Because no work is done automatically by Spark, the data must be manually sorted by partition value.
    // The data must be sorted either within each spark task, or globally within the entire dataset.
    // A global sort will minimize the number of output files.
    df = df.sortWithinPartitions(partitionColNames.head, partitionColNames.tail: _*)
    df.write
      .format(settings.exportFormat)
      .mode(SaveMode.Append)
      .options(settings.exportOptions)
      .insertInto(s"${settings.exportDatabase}.${exportTableConfig.tableName}")
  }

  private def sqlCreateDatabase(database: String) =
    s"""
       |CREATE DATABASE IF NOT EXISTS $database
       |""".stripMargin

  private def sqlCreateTable(catalog: String, database: String, exportTableConfig: ExportTableConfig) = {
    // create dynamic col names with type to append to create table statement
    val dynamicColNames = exportTableConfig.labelColumnMapping.map(pair =>
      pair._2 + " string " + pair._3).mkString(", ")
    val partitionColNames = exportTableConfig.partitionByCols.mkString(", ")
    s"""
       |CREATE TABLE IF NOT EXISTS $catalog.$database.${exportTableConfig.tableName} (
       | $dynamicColNames,
       | metric string NOT NULL,
       | labels map<string, string>,
       | epoch_timestamp long NOT NULL,
       | timestamp timestamp NOT NULL,
       | value double,
       | year int NOT NULL,
       | month int NOT NULL,
       | day int NOT NULL,
       | hour int NOT NULL
       | )
       | USING iceberg
       | PARTITIONED BY (year, month, day, $partitionColNames, metric)
       | LOCATION '${exportTableConfig.tablePath}'
       |""".stripMargin
  }

  def getRuleIfShouldExport(partKeyMap: collection.Map[String, String]): Option[ExportRule] = {
    keyToRules.find { case (key, exportTableConfig) =>
      // find the group with a key that matches these label-values
      downsamplerSettings.exportRuleKey.zipWithIndex.forall { case (label, i) =>
        partKeyMap.get(label).contains(key(i))
      }
    }.flatMap { case (key, exportTableConfig) =>
      exportTableConfig.exportRules.takeWhile { rule =>
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

    val timestampCol = 0 // FIXME: need a more dynamic (but efficient) solution
    val columns = partition.schema.data.columns
    val valueCol = columns.indexWhere(_.name == partition.schema.data.valueColName)
    val rangeInfoIter = getChunkRangeIter(partition, userStartTime, userEndTime).map { chunkRange =>
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
        rangeInfoIter.flatMap { info =>
          val doubleIter = info.valueIter.asDoubleIt
          (info.irowStart to info.irowEnd).iterator.map { _ =>
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

        val nameWithoutBucket = partKeyMap(LABEL_NAME)
        val nameWithBucket = nameWithoutBucket + "_bucket"

        // make labelString without __name__; will be replaced with _bucket-suffixed name
        partKeyMap.remove(LABEL_NAME)
        partKeyMap.put(LABEL_NAME, nameWithoutBucket)

        rangeInfoIter.flatMap { info =>
          val histIter = info.valueIter.asHistIt
          (info.irowStart to info.irowEnd).iterator.flatMap { _ =>
            val hist = histIter.next()
            val timestamp = info.timestampIter.next
            (0 until hist.numBuckets).iterator.map { i =>
              val bucketTopString = {
                val raw = hist.bucketTop(i)
                if (raw.isPosInfinity) "+Inf" else raw.toString
              }
              val bucketMapping = Map(LABEL_NAME -> nameWithBucket, LABEL_LE -> bucketTopString)
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

    tupleIter.map { case (labels, epoch_timestamp, value) =>
      val metric = labels(LABEL_NAME)
      // to compute YYYY, MM, dd, hh
      // to compute readable timestamp from unix timestamp
      val dateTime = Instant.ofEpochMilli(epoch_timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime()
      val timestamp = java.sql.Timestamp.valueOf(dateTime)
      val year = dateTime.getYear()
      val month = dateTime.getMonthValue()
      val day = dateTime.getDayOfMonth()
      val hour = dateTime.getHour()
      ExportRowData(metric, labels, epoch_timestamp, timestamp, value, year, month, day, hour)
    }
  }
  // scalastyle:on method.length
}
