package filodb.downsampler.chunk

import java.security.MessageDigest
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Column.ColumnType.DoubleColumn
import filodb.core.metadata.Schemas
import filodb.core.query.ColumnFilter
import filodb.core.store.{ChunkSetInfoReader, ReadablePartition}
import filodb.downsampler.Utils._
import filodb.downsampler.chunk.BatchExporter.{DATE_REGEX_MATCHER, LABEL_REGEX_MATCHER}
import filodb.memory.format.{TypedIterator, UnsafeUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}


import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable

case class ExportRule(key: Seq[String],
                      includeFilterGroups: Seq[Seq[ColumnFilter]],
                      excludeFilterGroups: Seq[Seq[ColumnFilter]])

object BatchExporter {
  val LABEL_REGEX_MATCHER = """\{\{(.*)\}\}""".r
  val DATE_REGEX_MATCHER = """<<(.*)>>""".r
}

/**
 * Exports a window of data to a bucket.
 */
case class BatchExporter(downsamplerSettings: DownsamplerSettings) {

  @transient lazy private[downsampler] val schemas = Schemas.fromConfig(downsamplerSettings.filodbConfig).get

  // TODO(a_theimer): probably worth building this into a trie-based structure eventually
  val rules = downsamplerSettings.exportRules
  val exportSchema = {
    val fields = new mutable.ArrayBuffer[StructField](3 + downsamplerSettings.exportPathSpecPairs.size)
    fields.append(
      StructField("labels", StringType),
      StructField("timestamp", LongType),
      StructField("value", DoubleType)
    )
    fields.appendAll(downsamplerSettings.exportPathSpecPairs.map(f => StructField(f._1, StringType)))
    StructType(fields)
  }
  val partitionByCols = {
    val cols = new mutable.ArrayBuffer[String](3 + downsamplerSettings.exportPathSpecPairs.size)
    cols.appendAll(downsamplerSettings.exportPathSpecPairs.map(_._1))
    cols
  }

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
  private def extractRows(partition: ReadablePartition,
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
        // TODO(a_theimer): this is commented out so tests pass
        // throw new IllegalArgumentException(s"unhandled ColumnType: $colType")
        Iterator.empty
    }
  }

  /**
   * Export the data to a bucket.
   * @param path the absolute path of the file to write.
   * @param bucket the export bucket.
   * @param partKeyPairs label-value pairs of the partition to export.
   * @param rows sequence of timestamp/value pairs to export.
   */
  private def export(path: Seq[String],
                     bucket: String,
                     partKeyPairs: Seq[(String, String)],
                     rows: Iterator[(Long, Double)]): Unit = {
    // scalastyle:off
    println(s"======== exporting to $bucket:${path.mkString("/")}")
    println(s"partKeyPairs: $partKeyPairs")
    rows.foreach{ case (timestamp, value) =>
      println(s"-- $timestamp: $value")
    }
    // scalastyle:on
  }

  private def matchAllFilters(filters: Seq[ColumnFilter],
                              labels: Map[String, String]): Boolean = {
    filters.forall( filt =>
      labels.get(filt.column)
        .exists(value => filt.filter.filterFunc(value))
    )
  }

  def getExportRows(readablePartitions: Seq[ReadablePartition],
                    userStartTime: Long,
                    userEndTime: Long): Iterator[Row] = {
    // TODO(a_theimer): use this space to update metrics / logs
    readablePartitions
      .iterator
      .flatMap(exportPartition(_, userStartTime, userEndTime))
      .map(presentExportRow(_))
  }

  private def presentExportRow(exportRow: ExportRow): Row = {
    val dataSeq = new mutable.ArrayBuffer[Any](3 + downsamplerSettings.exportPathSpecPairs.size)
    dataSeq.append(
      // TODO(a_theimer): don't sort every time
      exportRow.partKeyMap.toSeq.sortBy(pair => s"${pair._1}=${pair._2}").mkString(","),
      exportRow.timestamp,
      exportRow.value
    )
    dataSeq.appendAll(downsamplerSettings.exportPathSpecPairs.map(_._2))
    Row.fromSeq(dataSeq)
  }

  // scalastyle:off
  private def exportPartition(readablePartition: ReadablePartition,
                              userStartTime: Long,
                              userEndTime: Long): Iterator[ExportRow] = {

    // get the label-value pairs for this partition
    val partKeyMap = {
      val rawSchemaId = RecordSchema.schemaID(readablePartition.partKeyBytes, UnsafeUtils.arayOffset)
      val schema = schemas(rawSchemaId)
      schema.partKeySchema.toStringPairs(readablePartition.partKeyBytes, UnsafeUtils.arayOffset).toMap
    }

    val shouldExport = rules.find { rule =>
      // find the rule with a key that matches these label-values
      downsamplerSettings.exportRuleKey.zipWithIndex.forall { case (label, i) =>
        partKeyMap.get(label).contains(rule.key(i))
      }
    }.exists { rule =>
      // decide whether-or-not to export this partition
      lazy val matchAnyIncludeGroup = rule.includeFilterGroups.exists { group =>
        matchAllFilters(group, partKeyMap)
      }
      val matchAnyExcludeGroup = rule.excludeFilterGroups.exists { group =>
        matchAllFilters(group, partKeyMap)
      }
      !matchAnyExcludeGroup && (rule.includeFilterGroups.isEmpty || matchAnyIncludeGroup)
    }

    if (shouldExport) {
      // build the path to export to
      val directories = {
        val date = new Date(userEndTime)
        downsamplerSettings.exportPathSpecPairs.map(_._2)
          // replace the label {{}} strings
          .map(LABEL_REGEX_MATCHER.replaceAllIn(_, matcher => partKeyMap(matcher.group(1))))
          // replace the datetime <<>> strings
          .map{ pathSpecString =>
            DATE_REGEX_MATCHER.replaceAllIn(pathSpecString, matcher => {
              val dateFormatter = new SimpleDateFormat(matcher.group(1))
              dateFormatter.format(date)
            })}
      }

//      val fileName = hashToString(readablePartition.partKeyBytes)

      getChunkRangeIter(readablePartition, userStartTime, userEndTime).flatMap{ chunkRow =>
        extractRows(
          readablePartition, chunkRow.chunkSetInfoReader, chunkRow.istartRow, chunkRow.iendRow)
      }.map{ case (timestamp, value) =>
        ExportRow(partKeyMap, timestamp, value, directories)
      }
    } else Iterator.empty
  }
}
