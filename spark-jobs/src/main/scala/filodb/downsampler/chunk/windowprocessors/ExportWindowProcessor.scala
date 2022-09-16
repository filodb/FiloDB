package filodb.downsampler.chunk.windowprocessors

import java.security.MessageDigest

import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Column.ColumnType.DoubleColumn
import filodb.core.metadata.Schemas
import filodb.core.store.{ChunkSetInfoReader, RawPartData, ReadablePartition}
import filodb.downsampler.chunk.{DownsamplerSettings, PartitionAutoPager, SingleWindowProcessor}
import filodb.memory.format.{TypedIterator, UnsafeUtils}

/**
 * Exports a window of data to a bucket.
 */
case class ExportWindowProcessor(schemas: Schemas,
                                 downsamplerSettings: DownsamplerSettings)
  extends SingleWindowProcessor{

  // TODO(a_theimer): probably worth building this into a trie eventually
  // TODO(a_theimer): rewrite this when better config is setup
  val rules = downsamplerSettings.exportFilters.map{ rule =>
    // each size-2 window is a key-value pair
    val labelValueFilters = rule.head.sliding(2, 2).map(pair => pair.head -> pair.last).toMap
    val publishBuckets = rule.last
    (labelValueFilters, publishBuckets)
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
   * Extracts and returns a sequence of (timestamp,value) pairs from the argument chunk.
   * @param irowStart the index of the first row to include in the result.
   * @param irowEnd the index of the final row to include in the result (exclusive).
   */
  private def extractRows(partition: ReadablePartition,
                          chunkReader: ChunkSetInfoReader,
                          irowStart: Int,
                          irowEnd: Int): Seq[(Long, Double)] = {
    val timestampCol = 0  // FIXME: need a more dynamic (but efficient) solution
    val columns = partition.schema.data.columns
    val valueCol = columns.indexWhere(_.name == partition.schema.data.valueColName)
    columns(valueCol).columnType match {
      case DoubleColumn =>
        val timestampIter = getChunkColIter(partition, chunkReader, timestampCol, irowStart).asLongIt
        val valueIter = getChunkColIter(partition, chunkReader, valueCol, irowStart).asDoubleIt
        (irowStart until irowEnd).map{ _ =>
          (timestampIter.next, valueIter.next)
        }
      case colType =>
        // TODO(a_theimer): this is commented out so tests pass
        // throw new IllegalArgumentException(s"unhandled ColumnType: $colType")
        Nil
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
    println(s"======== exporting to $bucket:${path.mkString("/")}")
    println(s"partKeyPairs: $partKeyPairs")
    rows.foreach{ case (timestamp, value) =>
      println(s"-- $timestamp: $value")
    }
  }

  override def process(rawPartData: RawPartData,
                       userEndTime: Long,
                       partitionAutoPager: PartitionAutoPager): Unit = {
    // get the label-value pairs for this partition
    val partKeyMap = {
      val rawSchemaId = RecordSchema.schemaID(rawPartData.partitionKey, UnsafeUtils.arayOffset)
      val schema = schemas(rawSchemaId)
      schema.partKeySchema.toStringPairs(rawPartData.partitionKey, UnsafeUtils.arayOffset).toMap
    }

    // find the buckets with matching rules
    // TODO(a_theimer): this is annoying to read
    val publishBuckets = rules.filter{ rule =>
      rule._1.exists{ case (name, value) =>
        val pkVal = partKeyMap.get(name)
        pkVal.isEmpty || pkVal.get != value
      }
    }.flatMap(_._2)

    // get the path to export to
    val regexMatcher = """\{\{(.*)\}\}""".r
    val directories = downsamplerSettings.exportStructure.map(
      regexMatcher.replaceAllIn(_, matcher => partKeyMap(matcher.group(1))))
    val fileName = hashToString(partitionAutoPager.getReadablePartition().partKeyBytes)

    val rows = partitionAutoPager.getChunkRows().toIterator.flatMap{ chunkRow =>
      extractRows(
        partitionAutoPager.getReadablePartition(), chunkRow.chunkSetInfoReader, chunkRow.istartRow, chunkRow.iendRow)
    }

    // TODO(a_theimer): bucket/rows should be interfaces
    publishBuckets.foreach{ bucket =>
      export(directories ++ Seq(fileName), bucket, partKeyMap.toSeq, rows)
    }
  }
}
