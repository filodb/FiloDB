package filodb.downsampler.chunk.windowprocessors

import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Column.ColumnType.DoubleColumn
import filodb.core.metadata.Schemas
import filodb.core.store.{ChunkSetInfoReader, RawPartData, ReadablePartition}
import filodb.downsampler.chunk.{DownsamplerSettings, PartitionAutoPager, SingleWindowProcessor}
import filodb.memory.format.{TypedIterator, UnsafeUtils}


import java.security.MessageDigest

// scalastyle:off
case class ExportWindowProcessor(schemas: Schemas,
                                 downsamplerSettings: DownsamplerSettings) extends SingleWindowProcessor{

  private def hashToString(bytes: Array[Byte]): String = {
    MessageDigest.getInstance("SHA-256")
      .digest(bytes)
      .map("%02x".format(_))
      .mkString
  }

  private def getChunkColIter(part: ReadablePartition, cset: ChunkSetInfoReader, icol: Int, istart: Int): TypedIterator = {
    val tsPtr = cset.vectorAddress(icol)
    val tsAcc = cset.vectorAccessor(icol)
    part.chunkReader(icol, tsAcc, tsPtr).iterate(tsAcc, tsPtr, istart)
  }

  private def getData(part: ReadablePartition, cset: ChunkSetInfoReader, istart: Int, iend: Int): Seq[(Long, Double)] = {
    val timestampCol = 0
    // TODO(a_theimer): cleanup
    val valueCol = part.schema.data.columns.indexWhere(_.name == part.schema.data.valueColName)
    part.schema.data.columns(valueCol).columnType match {
      case DoubleColumn =>
        val timestampReader = getChunkColIter(part, cset, timestampCol, istart).asLongIt
        val valueReader = getChunkColIter(part, cset, valueCol, istart).asDoubleIt
        (istart until iend).map{ _ =>
          (timestampReader.next, valueReader.next)
        }
      case _ => Nil
    }
  }

  //scalastyle:off


  private def doExport(path: Seq[String], bucket: String, data: Iterator[(Long, Double)]): Unit = {
    // TODO(a_theimer)
    println(s"======== exporting to $bucket:${path.mkString("/")}")
    val dseq = data.toArray
    dseq.foreach{ case (timestamp, value) =>
      println(s"-- $timestamp: $value")
    }
  }

  // TODO(a_theimer): move somewhere more appropriate; make params more appropriate
  private def export(partitionAutoPager: PartitionAutoPager,
             filters: Seq[(String, String)],
             downsamplerSettings: DownsamplerSettings
            ): Unit = {
    // load the export rules into memory
    // TODO(a_theimer): probably worth building this into a trie eventually
    // TODO(a_theimer): rewrite this when better config is setup
    val rules = downsamplerSettings.exportFilters.map{ rule =>
      val filterSet = rule.head.sliding(2, 2).map(pair => pair.head -> pair.last).toMap
      val publishTo = rule.last
      (filterSet, publishTo)
    }
    // export for all matching rules
    val pkPairMap = filters.toMap
    val publishTo = rules.filter{ rule =>
      rule._1.exists{ case (name, value) =>
        val pkVal = pkPairMap.get(name)
        pkVal.isEmpty || pkVal.get != value
      }
    }.flatMap(_._2)
    // get the path to the time-series files
    val regexMatcher = """\{\{(.*)\}\}""".r
    val directories = downsamplerSettings.exportStructure.map(
      regexMatcher.replaceAllIn(_, matcher => pkPairMap(matcher.group(1))))

    val fileName = hashToString(partitionAutoPager.getReadablePartition().partKeyBytes)

    val data = partitionAutoPager.getChunkRows().toIterator.flatMap{ chunkRow =>
      getData(partitionAutoPager.getReadablePartition(), chunkRow.chunkSetInfoReader, chunkRow.istartRow, chunkRow.iendRow)
    }

    // TODO(a_theimer): "bucket" should be an interface with export method
    publishTo.foreach{ bucket =>
      // do the export
      doExport(directories ++ Seq(fileName), bucket, data)
    }
  }

  override def process(rawPartData: RawPartData,
                       userEndTime: Long,
                       partitionAutoPager: PartitionAutoPager): Unit = {
    // TODO(a_theimer)
    val rawSchemaId = RecordSchema.schemaID(rawPartData.partitionKey, UnsafeUtils.arayOffset)
    val schema = schemas(rawSchemaId)
    val pkPairs = schema.partKeySchema.toStringPairs(rawPartData.partitionKey, UnsafeUtils.arayOffset)
    export(partitionAutoPager, pkPairs, downsamplerSettings)
  }
}
