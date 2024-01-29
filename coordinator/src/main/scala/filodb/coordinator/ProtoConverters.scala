package filodb.coordinator

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}

import com.google.protobuf.ByteString
import com.typesafe.config.ConfigFactory

import filodb.core.downsample.{CounterDownsamplePeriodMarker, TimeDownsamplePeriodMarker}
import filodb.core.memstore.PartLookupResult
import filodb.core.metadata.{ComputedColumn, DataColumn}
import filodb.core.query._
import filodb.core.store.{AllChunkScan, InMemoryChunkScan, TimeRangeChunkScan, WriteBufferChunkScan}
import filodb.grpc.ExecPlans
import filodb.grpc.ExecPlans.ExecPlanContainer.ExecPlanCase
import filodb.grpc.ExecPlans.FuncArgs.FuncArgTypeCase
import filodb.grpc.ExecPlans.RangeVectorTransformerContainer
import filodb.query.{AggregationOperator, QueryCommand}
import filodb.query.exec._

// scalastyle:off number.of.methods
// scalastyle:off number.of.types
// scalastyle:off file.size.limit
// scalastyle:off method.length
object ProtoConverters {

  import filodb.query.ProtoConverters._

  implicit class QueryConfigToProtoConverter(qc: QueryConfig) {

    def toProto: ExecPlans.QueryConfig = {
      import JavaConverters._
      val builder = ExecPlans.QueryConfig.newBuilder()
      builder.setAskTimeoutSeconds(qc.askTimeout.toSeconds)
      builder.setStaleSampleAfterMs(qc.staleSampleAfterMs)
      builder.setMinStepMs(qc.minStepMs)
      builder.setFastReduceMaxWindows(qc.fastReduceMaxWindows)
      builder.setParser(qc.parser)
      builder.setTranslatePromToFilodbHistogram(qc.translatePromToFilodbHistogram)
      builder.setFasterRateEnabled(qc.fasterRateEnabled)
      qc.partitionName.foreach(partitionName => builder.setPartitionName(partitionName))
      qc.remoteHttpTimeoutMs.foreach(remoteHttpTimeoutMs => builder.setRemoteHttpTimeoutMs(remoteHttpTimeoutMs))
      qc.remoteHttpEndpoint.foreach(remoteHttpEndpoint => builder.setRemoteHttpEndpoint(remoteHttpEndpoint))
      qc.remoteGrpcEndpoint.foreach(remoteGrpcEndpoint => builder.setRemoteGrpcEndpoint(remoteGrpcEndpoint))
      builder.setNumRvsPerResultMessage(qc.numRvsPerResultMessage)
      builder.setEnforceResultByteLimit(qc.enforceResultByteLimit)
      builder.setAllowPartialResultsRangeQuery(qc.allowPartialResultsRangeQuery)
      builder.setAllowPartialResultsMetadataQuery(qc.allowPartialResultsMetadataQuery)
      builder.addAllGrpcPartitionsDenyList(qc.grpcPartitionsDenyList.asJava)
      qc.plannerSelector.foreach(plannerSelector => builder.setPlannerSelector(plannerSelector))
      qc.recordContainerOverrides.foreach(overrides => builder.putRecordContainerOverrides(overrides._1, overrides._2))
      builder.build()
    }
  }

  implicit class QueryConfigFromProtoConverter(qc: ExecPlans.QueryConfig) {
    def fromProto: QueryConfig = {
      import JavaConverters._
      val rcoIntegerMap : Map[String, Integer] = qc.getRecordContainerOverridesMap().asScala.toMap
      val rcoIntMap = rcoIntegerMap.map{case (key, value) => key -> value.intValue() }
      val queryConfig = QueryConfig(
        FiniteDuration(qc.getAskTimeoutSeconds(), TimeUnit.SECONDS) : FiniteDuration,
        qc.getStaleSampleAfterMs(),
        qc.getMinStepMs(),
        qc.getFastReduceMaxWindows(),
        qc.getParser(),
        qc.getTranslatePromToFilodbHistogram(),
        qc.getFasterRateEnabled(),
        if (qc.hasPartitionName) Option(qc.getPartitionName) else None,
        if (qc.hasRemoteHttpTimeoutMs) Option(qc.getRemoteHttpTimeoutMs) else None,
        if (qc.hasRemoteHttpEndpoint) Option(qc.getRemoteHttpEndpoint) else None,
        if (qc.hasRemoteGrpcEndpoint) Option(qc.getRemoteGrpcEndpoint) else None,
        qc.getNumRvsPerResultMessage(),
        qc.getEnforceResultByteLimit(),
        qc.getAllowPartialResultsRangeQuery(),
        qc.getAllowPartialResultsMetadataQuery(),
        qc.getGrpcPartitionsDenyListList().asScala.toSet,
        if (qc.hasPlannerSelector) Option(qc.getPlannerSelector()) else None,
        rcoIntMap
      )
      queryConfig
    }
  }

  // ChunkScanMethod
  implicit class ChunkScanMethodToProtoConverter(csm: filodb.core.store.ChunkScanMethod) {
    def toProto: ExecPlans.ChunkScanMethod = {
      val builder = ExecPlans.ChunkScanMethod.newBuilder()
      builder.setStartTime(csm.startTime)
      builder.setEndTime(csm.endTime)
      csm match {
        case trcs: TimeRangeChunkScan => builder.setMethod(
          filodb.grpc.ExecPlans.ChunkScanType.TIME_RANGE_CHUNK_SCAN
        )
        case acs: filodb.core.store.AllChunkScan.type => builder.setMethod(
          filodb.grpc.ExecPlans.ChunkScanType.ALL_CHUNKS_SCAN
        )
        case imcs: filodb.core.store.InMemoryChunkScan.type => builder.setMethod(
          filodb.grpc.ExecPlans.ChunkScanType.IN_MEMORY_CHUNK_SCAN
        )
        case wbcs: filodb.core.store.WriteBufferChunkScan.type => builder.setMethod(
          filodb.grpc.ExecPlans.ChunkScanType.WRITE_BUFFER_CHUNK_SCAN
        )
      }
      builder.build()
    }
  }

  implicit class ChunkScanMethodFromProtoConverter(cs: ExecPlans.ChunkScanMethod) {
    def fromProto: filodb.core.store.ChunkScanMethod = {
     cs.getMethod match {
       case ExecPlans.ChunkScanType.IN_MEMORY_CHUNK_SCAN =>
         InMemoryChunkScan
       case ExecPlans.ChunkScanType.ALL_CHUNKS_SCAN =>
         AllChunkScan
       case ExecPlans.ChunkScanType.WRITE_BUFFER_CHUNK_SCAN =>
         WriteBufferChunkScan
       case ExecPlans.ChunkScanType.TIME_RANGE_CHUNK_SCAN =>
         TimeRangeChunkScan(cs.getStartTime, cs.getEndTime)
       case _ => throw new IllegalArgumentException(s"Unexpected ChunkScanMethod ${cs.getMethod}")
     }
    }
  }

  // TypedFieldExtractor
  implicit class TypedFieldExtractorToProtoConverter(
    tfe: filodb.memory.format.RowReader.TypedFieldExtractor[_]
  ) {
    def toProto: ExecPlans.TypedFieldExtractor = {
      tfe match {
        case filodb.memory.format.RowReader.IntFieldExtractor => ExecPlans.TypedFieldExtractor.INT
        case filodb.memory.format.RowReader.FloatFieldExtractor => ExecPlans.TypedFieldExtractor.FLOAT
        case filodb.memory.format.RowReader.HistogramExtractor => ExecPlans.TypedFieldExtractor.HISTOGRAM
        case filodb.memory.format.RowReader.DateTimeFieldExtractor => ExecPlans.TypedFieldExtractor.DATE_TIME
        case filodb.memory.format.RowReader.DoubleFieldExtractor => ExecPlans.TypedFieldExtractor.DOUBLE
        case filodb.memory.format.RowReader.TimestampFieldExtractor => ExecPlans.TypedFieldExtractor.TIMESTAMP
        case filodb.memory.format.RowReader.UTF8StringFieldExtractor => ExecPlans.TypedFieldExtractor.UTF8_STRING
        case filodb.memory.format.RowReader.LongFieldExtractor => ExecPlans.TypedFieldExtractor.LONG
        case filodb.memory.format.RowReader.BooleanFieldExtractor => ExecPlans.TypedFieldExtractor.BOOLEAN
        case filodb.memory.format.RowReader.StringFieldExtractor => ExecPlans.TypedFieldExtractor.STRING
        case filodb.memory.format.RowReader.ObjectFieldExtractor(_) =>
          throw new IllegalArgumentException("Cannot serialize object extractors")
        case we: filodb.memory.format.RowReader.WrappedExtractor[_, _] =>
          throw new IllegalArgumentException("Cannot serialize wrapped extractors")
      }
    }
  }

  implicit class TypedFieldExtractorFromProtoConverter(dc: ExecPlans.TypedFieldExtractor) {
    def fromProto: filodb.memory.format.RowReader.TypedFieldExtractor[_]= {
      dc match {
        case ExecPlans.TypedFieldExtractor.INT => filodb.memory.format.RowReader.IntFieldExtractor
        case ExecPlans.TypedFieldExtractor.FLOAT => filodb.memory.format.RowReader.FloatFieldExtractor
        case ExecPlans.TypedFieldExtractor.HISTOGRAM => filodb.memory.format.RowReader.HistogramExtractor
        case ExecPlans.TypedFieldExtractor.DATE_TIME => filodb.memory.format.RowReader.DateTimeFieldExtractor
        case ExecPlans.TypedFieldExtractor.DOUBLE => filodb.memory.format.RowReader.DoubleFieldExtractor
        case ExecPlans.TypedFieldExtractor.TIMESTAMP => filodb.memory.format.RowReader.TimestampFieldExtractor
        case ExecPlans.TypedFieldExtractor.UTF8_STRING => filodb.memory.format.RowReader.UTF8StringFieldExtractor
        case ExecPlans.TypedFieldExtractor.LONG => filodb.memory.format.RowReader.LongFieldExtractor
        case ExecPlans.TypedFieldExtractor.BOOLEAN => filodb.memory.format.RowReader.BooleanFieldExtractor
        case ExecPlans.TypedFieldExtractor.STRING => filodb.memory.format.RowReader.StringFieldExtractor
        case ExecPlans.TypedFieldExtractor.OBJECT =>
          throw new IllegalArgumentException("Cannot deserialize ObjectExtractor")
        case ExecPlans.TypedFieldExtractor.WRAPPED =>
          throw new IllegalArgumentException("Cannot deserialize WrappedExtractor")
        case ExecPlans.TypedFieldExtractor.UNRECOGNIZED =>
          throw new IllegalArgumentException("Unrecognized Extractor")
      }
    }
  }

  // ComputedColumn
  implicit class ComputedColumnToProtoConverter(cc: filodb.core.metadata.ComputedColumn) {
    def toProto: ExecPlans.ComputedColumn = {

      val builder = ExecPlans.ComputedColumn.newBuilder()
      builder.setId(cc.id)
      builder.setExpr(cc.expr)
      builder.setDataset(cc.dataset)
      builder.setColumnType(cc.columnType.toProto)
      cc.sourceIndices.foreach( i => builder.addSourceIndices(i))
      builder.setExtractor(cc.extractor.toProto)
      builder.build()
    }
  }

  implicit class ComputedColumnFromProtoConverter(cc: ExecPlans.ComputedColumn) {
    def fromProto: filodb.core.metadata.Column = {
      //val params = ConfigFactory.parseMap(dc.getParamsMap)
      import JavaConverters._
      val indicesJava : java.util.List[Integer]= cc.getSourceIndicesList()
      val sourceIndices : Seq[Int]= indicesJava.asScala.toSeq.map(i => i.intValue())
      //source
      ComputedColumn(
        cc.getId,
        cc.getExpr,
        cc.getDataset,
        cc.getColumnType.fromProto,
        sourceIndices,
        cc.getExtractor.fromProto
      )
    }
  }

  // DataColumn
  implicit class DataColumnToProtoConverter(dc: filodb.core.metadata.DataColumn) {
    def toProto: ExecPlans.DataColumn = {
      val builder = ExecPlans.DataColumn.newBuilder()
      builder.setId(dc.id)
      builder.setName(dc.name)
      builder.setColumnType(dc.columnType.toProto)
      val params = dc.params.entrySet().stream().forEach( e => {
        builder.putParams(e.getKey, e.getValue.render())
      })
      builder.build()
    }
  }

  implicit class DataColumnFromProtoConverter(dc: ExecPlans.DataColumn) {
    def fromProto: filodb.core.metadata.Column = {
      val params = ConfigFactory.parseMap(dc.getParamsMap)
      DataColumn(
        dc.getId,
        dc.getName,
        dc.getColumnType.fromProto,
        params
      )
    }
  }

  // Column
  implicit class ColumnToProtoConverter(c: filodb.core.metadata.Column) {
    def toProto: ExecPlans.ColumnContainer = {
      val builder = ExecPlans.ColumnContainer.newBuilder()
      c match {
        case dc: DataColumn => builder.setDataColumn(dc.toProto)
        case cc: ComputedColumn => builder.setComputedColumn(cc.toProto)
      }
      builder.build()
    }
  }

  implicit class ColumnFromProtoConverter(c: ExecPlans.ColumnContainer) {
    def fromProto: filodb.core.metadata.Column = {
      c.getColumnCase match {
        case ExecPlans.ColumnContainer.ColumnCase.DATACOLUMN =>
            c.getDataColumn.fromProto
        case ExecPlans.ColumnContainer.ColumnCase.COMPUTEDCOLUMN =>
            c.getComputedColumn.fromProto
        case _ => throw new IllegalArgumentException(s"Unexpected Column ${c.getColumnCase}")
      }
    }
  }

  // RepeatedString
  implicit class RepeatedStringToProtoConverter(c: Seq[String]) {
    def toProto: ExecPlans.RepeatedString = {
      val builder = ExecPlans.RepeatedString.newBuilder()
      c.foreach(s => builder.addStrings(s))
      builder.build()
    }
  }

  implicit class RepeatedStringFromProtoConverter(c: ExecPlans.RepeatedString) {
    def fromProto: Seq[String] = {
      import JavaConverters._
      c.getStringsList.asScala.toSeq
    }
  }

  // StringTuple
  implicit class StringTupleToProtoConverter(st: (String, String)) {
    def toProto: ExecPlans.StringTuple = {
      val builder = ExecPlans.StringTuple.newBuilder()
      builder.setFieldOne(st._1)
      builder.setFieldTwo(st._2)
      builder.build()
    }
  }

  implicit class StringTupleFromProtoConverter(st: ExecPlans.StringTuple) {
    def fromProto: (String, String) = {
      (st.getFieldOne, st.getFieldTwo)
    }
  }

  // DatasetOptions
  implicit class DatasetOptionsToProtoConverter(dso: filodb.core.metadata.DatasetOptions) {
    def toProto: ExecPlans.DatasetOptions = {
      val builder = ExecPlans.DatasetOptions.newBuilder()
      dso.shardKeyColumns.foreach(c => builder.addShardKeyColumns(c))
      builder.setMetricColumn(dso.metricColumn)
      builder.setHasDownsampledData(dso.hasDownsampledData)
      dso.ignoreShardKeyColumnSuffixes.foreach(kv => builder.putIgnoreShardKeyColumnSuffixes(kv._1, kv._2.toProto))
      dso.ignoreTagsOnPartitionKeyHash.foreach(tag => builder.addIgnoreTagsOnPartitionKeyHash(tag))
      dso.copyTags.foreach(tt => builder.addCopyTags(tt.toProto))
      dso.multiColumnFacets.foreach(kv => builder.putMultiColumFacets(kv._1, kv._2.toProto))
      builder.build()
    }
  }

  implicit class DatasetOptionsFromProtoConverter(dso: ExecPlans.DatasetOptions) {
    def fromProto: filodb.core.metadata.DatasetOptions = {
      import JavaConverters._
      filodb.core.metadata.DatasetOptions(
        dso.getShardKeyColumnsList.asScala.toSeq,
        dso.getMetricColumn,
        dso.getHasDownsampledData,
        dso.getIgnoreShardKeyColumnSuffixesMap.asScala.mapValues(rs => rs.fromProto).toMap,
        dso.getIgnoreTagsOnPartitionKeyHashList.asScala.toSeq,
        dso.getCopyTagsList.asScala.map(t => t.fromProto),
        dso.getMultiColumFacetsMap.asScala.mapValues(rs => rs.fromProto).toMap
      )
    }
  }

  // PartitionSchema
  implicit class PartitionSchemaToProtoConverter(ps: filodb.core.metadata.PartitionSchema) {
    def toProto: ExecPlans.PartitionSchema = {
      val builder = ExecPlans.PartitionSchema.newBuilder()
      ps.columns.foreach(c => builder.addColumns(c.toProto))
      ps.predefinedKeys.foreach(k => builder.addPredefinedKeys(k))
      builder.setOptions(ps.options.toProto)
      builder.build()
    }
  }

  implicit class PartitionSchemaFromProtoConverter(ps: ExecPlans.PartitionSchema) {
    def fromProto: filodb.core.metadata.PartitionSchema = {
      import JavaConverters._
      filodb.core.metadata.PartitionSchema(
        ps.getColumnsList().asScala.toSeq.map(c => c.fromProto),
        ps.getPredefinedKeysList().asScala.toSeq,
        ps.getOptions.fromProto
      )
    }
  }

  def getChunkDownsampler(d : filodb.core.downsample.ChunkDownsampler) : ExecPlans.ChunkDownsampler = {
    val builder = ExecPlans.ChunkDownsampler.newBuilder()
    d.inputColIds.foreach(id => builder.addInputColIds(id))
    builder.build()
  }

  def getInputColIds(cd : ExecPlans.ChunkDownsampler) : Seq[Int] = {
    import JavaConverters._
    cd.getInputColIdsList.asScala.toSeq.map(i => i.intValue())
  }

  // TimeDownsampler
  implicit class TimeDownsamplerToProtoConverter(td: filodb.core.downsample.TimeDownsampler) {
    def toProto: ExecPlans.TimeDownsampler = {
      val builder = ExecPlans.TimeDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class TimeDownsamplerFromProtoConverter(td: ExecPlans.TimeDownsampler) {
    def fromProto: filodb.core.downsample.TimeDownsampler = {
      import JavaConverters._
      filodb.core.downsample.TimeDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // LastValueHDownsampler
  implicit class LastValueHDownsamplerToProtoConverter(td: filodb.core.downsample.LastValueHDownsampler) {
    def toProto: ExecPlans.LastValueHDownsampler = {
      val builder = ExecPlans.LastValueHDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class LastValueHDownsamplerFromProtoConverter(td: ExecPlans.LastValueHDownsampler) {
    def fromProto: filodb.core.downsample.LastValueHDownsampler = {
      import JavaConverters._
      filodb.core.downsample.LastValueHDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // HistSumDownsampler
  implicit class HistSumDownsamplerToProtoConverter(td: filodb.core.downsample.HistSumDownsampler) {
    def toProto: ExecPlans.HistSumDownsampler = {
      val builder = ExecPlans.HistSumDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class HistSumDownsamplerFromProtoConverter(td: ExecPlans.HistSumDownsampler) {
    def fromProto: filodb.core.downsample.HistSumDownsampler = {
      import JavaConverters._
      filodb.core.downsample.HistSumDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // AvgDownsampler
  implicit class AvgDownsampleroProtoConverter(td: filodb.core.downsample.AvgDownsampler) {
    def toProto: ExecPlans.AvgDownsampler = {
      val builder = ExecPlans.AvgDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class AvgDownsamplerFromProtoConverter(td: ExecPlans.AvgDownsampler) {
    def fromProto: filodb.core.downsample.AvgDownsampler = {
      import JavaConverters._
      filodb.core.downsample.AvgDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // AvgScDownsampler
  implicit class AvgScDownsamplerToProtoConverter(td: filodb.core.downsample.AvgScDownsampler) {
    def toProto: ExecPlans.AvgScDownsampler = {
      val builder = ExecPlans.AvgScDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class AvgScDownsamplerFromProtoConverter(td: ExecPlans.AvgScDownsampler) {
    def fromProto: filodb.core.downsample.AvgScDownsampler = {
      import JavaConverters._
      filodb.core.downsample.AvgScDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // LastValueDDownsampler
  implicit class LastValueDDownsamplerToProtoConverter(td: filodb.core.downsample.LastValueDDownsampler) {
    def toProto: ExecPlans.LastValueDDownsampler = {
      val builder = ExecPlans.LastValueDDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class LastValueDDownsamplerFromProtoConverter(td: ExecPlans.LastValueDDownsampler) {
    def fromProto: filodb.core.downsample.LastValueDDownsampler = {
      import JavaConverters._
      filodb.core.downsample.LastValueDDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // MinDownsampler
  implicit class MinDownsamplerToProtoConverter(td: filodb.core.downsample.MinDownsampler) {
    def toProto: ExecPlans.MinDownsampler= {
      val builder = ExecPlans.MinDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class MinDownsamplerFromProtoConverter(td: ExecPlans.MinDownsampler) {
    def fromProto: filodb.core.downsample.MinDownsampler = {
      import JavaConverters._
      filodb.core.downsample.MinDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // SumDownsampler
  implicit class SumDownsamplerToProtoConverter(td: filodb.core.downsample.SumDownsampler) {
    def toProto: ExecPlans.SumDownsampler = {
      val builder = ExecPlans.SumDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class SumDownsamplerFromProtoConverter(td: ExecPlans.SumDownsampler) {
    def fromProto: filodb.core.downsample.SumDownsampler = {
      import JavaConverters._
      filodb.core.downsample.SumDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // AvgAcDownsampler
  implicit class AvgAcDownsamplerToProtoConverter(td: filodb.core.downsample.AvgAcDownsampler) {
    def toProto: ExecPlans.AvgAcDownsampler = {
      val builder = ExecPlans.AvgAcDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class AvgAcDownsamplerFromProtoConverter(td: ExecPlans.AvgAcDownsampler) {
    def fromProto: filodb.core.downsample.AvgAcDownsampler = {
      import JavaConverters._
      filodb.core.downsample.AvgAcDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // MaxDownsampler
  implicit class MaxDownsampleroProtoConverter(td: filodb.core.downsample.MaxDownsampler) {
    def toProto: ExecPlans.MaxDownsampler = {
      val builder = ExecPlans.MaxDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class MaxDownsamplerFromProtoConverter(td: ExecPlans.MaxDownsampler) {
    def fromProto: filodb.core.downsample.MaxDownsampler = {
      import JavaConverters._
      filodb.core.downsample.MaxDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // CountDownsampler
  implicit class CountDownsamplerToProtoConverter(td: filodb.core.downsample.CountDownsampler) {
    def toProto: ExecPlans.CountDownsampler = {
      val builder = ExecPlans.CountDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class CountDownsamplerFromProtoConverter(td: ExecPlans.CountDownsampler) {
    def fromProto: filodb.core.downsample.CountDownsampler = {
      import JavaConverters._
      filodb.core.downsample.CountDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // ChunkDownsamplerContainer
  implicit class ChunkDownsamplerToProtoConverter(cd: filodb.core.downsample.ChunkDownsampler) {
    def toProto: ExecPlans.ChunkDownsamplerContainer = {
      val builder = ExecPlans.ChunkDownsamplerContainer.newBuilder()
      cd match {
        case td : filodb.core.downsample.TimeDownsampler => builder.setTimeDownsampler(td.toProto)
        case lvhd : filodb.core.downsample.LastValueHDownsampler => builder.setLastValueHDownampler(lvhd.toProto)
        case hsd : filodb.core.downsample.HistSumDownsampler => builder.setHistSumDownsampler(hsd.toProto)
        case ad : filodb.core.downsample.AvgDownsampler => builder.setAvgDownsampler(ad.toProto)
        case avgSc : filodb.core.downsample.AvgScDownsampler => builder.setAvgScDownsampler(avgSc.toProto)
        case lvdd : filodb.core.downsample.LastValueDDownsampler => builder.setLastValueDDownsampler(lvdd.toProto)
        case md : filodb.core.downsample.MinDownsampler => builder.setMinDownsampler(md.toProto)
        case sd : filodb.core.downsample.SumDownsampler => builder.setSumDownsampler(sd.toProto)
        case aad : filodb.core.downsample.AvgAcDownsampler => builder.setAvgAcDownsampler(aad.toProto)
        case md : filodb.core.downsample.MaxDownsampler => builder.setMaxDownsampler(md.toProto)
        case cd : filodb.core.downsample.CountDownsampler => builder.setCountDownsampler(cd.toProto)
      }
      builder.build()
    }
  }

  implicit class ChunkDownsamplerFromProtoConverter(cd: ExecPlans.ChunkDownsamplerContainer) {
    def fromProto: filodb.core.downsample.ChunkDownsampler = {
      import ExecPlans.ChunkDownsamplerContainer.ChunkDownsamplerCase
      val inputColIds = getInputColIds(cd.getTimeDownsampler.getChunkDownsampler)
      cd.getChunkDownsamplerCase match {
        case ChunkDownsamplerCase.TIMEDOWNSAMPLER => filodb.core.downsample.TimeDownsampler(inputColIds)
        case ChunkDownsamplerCase.LASTVALUEHDOWNAMPLER => filodb.core.downsample.LastValueHDownsampler(inputColIds)
        case ChunkDownsamplerCase.HISTSUMDOWNSAMPLER => filodb.core.downsample.HistSumDownsampler(inputColIds)
        case ChunkDownsamplerCase.AVGDOWNSAMPLER => filodb.core.downsample.AvgDownsampler(inputColIds)
        case ChunkDownsamplerCase.AVGSCDOWNSAMPLER => filodb.core.downsample.AvgScDownsampler(inputColIds)
        case ChunkDownsamplerCase.LASTVALUEDDOWNSAMPLER => filodb.core.downsample.LastValueDDownsampler(inputColIds)
        case ChunkDownsamplerCase.MINDOWNSAMPLER => filodb.core.downsample.MinDownsampler(inputColIds)
        case ChunkDownsamplerCase.SUMDOWNSAMPLER => filodb.core.downsample.SumDownsampler(inputColIds)
        case ChunkDownsamplerCase.AVGACDOWNSAMPLER => filodb.core.downsample.AvgAcDownsampler(inputColIds)
        case ChunkDownsamplerCase.MAXDOWNSAMPLER => filodb.core.downsample.MaxDownsampler(inputColIds)
        case ChunkDownsamplerCase.COUNTDOWNSAMPLER => filodb.core.downsample.CountDownsampler(inputColIds)
        case ChunkDownsamplerCase.CHUNKDOWNSAMPLER_NOT_SET => throw new IllegalArgumentException(
          "Invalid ChunkDownsampler"
        )
      }
    }
  }

  def getDownsamplePeriodMarkerProto(
    dpm: filodb.core.downsample.DownsamplePeriodMarker
  ): ExecPlans.DownsamplePeriodMarker = {
    val builder = ExecPlans.DownsamplePeriodMarker.newBuilder()
    builder.setInputColId(dpm.inputColId)
    builder.build()
  }

  // CounterDownsamplePeriodMarkerContainer
  implicit class CounterDownsamplePeriodMarkerContainerToProtoConverter(
    cdpmc: filodb.core.downsample.CounterDownsamplePeriodMarker
  ) {
    def toProto: ExecPlans.CounterDownsamplePeriodMarker = {
      val builder = ExecPlans.CounterDownsamplePeriodMarker.newBuilder()
      builder.setDownsamplePeriodMarker(getDownsamplePeriodMarkerProto(cdpmc))
      builder.build()
    }
  }

  implicit class CounterDownsamplePeriodMarkerFromProtoConverter(cdspm: ExecPlans.CounterDownsamplePeriodMarker) {
    def fromProto: filodb.core.downsample.CounterDownsamplePeriodMarker = {
      new CounterDownsamplePeriodMarker(
        cdspm.getDownsamplePeriodMarker.getInputColId
      )
    }
  }

  // TimeDownsamplePeriodMarkerContainer
  implicit class TimeDownsamplePeriodMarkerContainerToProtoConverter(
    tdpmc: filodb.core.downsample.TimeDownsamplePeriodMarker
  ) {
    def toProto: ExecPlans.TimeDownsamplePeriodMarker = {
      val builder = ExecPlans.TimeDownsamplePeriodMarker.newBuilder()
      builder.setDownsamplePeriodMarker(getDownsamplePeriodMarkerProto(tdpmc))
      builder.build()
    }
  }

  implicit class TimeDownsamplePeriodMarkerFromProtoConverter(tdspm: ExecPlans.TimeDownsamplePeriodMarker) {
    def fromProto: filodb.core.downsample.TimeDownsamplePeriodMarker = {
      new TimeDownsamplePeriodMarker(
        tdspm.getDownsamplePeriodMarker.getInputColId
      )
    }
  }

  // DownsamplePeriodMarkerContainer
  implicit class DownsamplePeriodMarkerContainerToProtoConverter(dpm: filodb.core.downsample.DownsamplePeriodMarker) {
    def toProto: ExecPlans.DownsamplePeriodMarkerContainer = {
      val builder = ExecPlans.DownsamplePeriodMarkerContainer.newBuilder()
      dpm match {
        case cdpm : CounterDownsamplePeriodMarker => builder.setCounterDownsamplePeriodMarker(cdpm.toProto)
        case tdpm : TimeDownsamplePeriodMarker => builder.setTimeDownsamplePeriodMarker(tdpm.toProto)
      }
      builder.build()
    }
  }

  implicit class DownsamplePeriodMarkerContainerFromProtoConverter(dpmc: ExecPlans.DownsamplePeriodMarkerContainer) {
    def fromProto: filodb.core.downsample.DownsamplePeriodMarker = {
      import ExecPlans.DownsamplePeriodMarkerContainer.DownsamplePeriodMarkerCase
      dpmc.getDownsamplePeriodMarkerCase match {
        case DownsamplePeriodMarkerCase.TIMEDOWNSAMPLEPERIODMARKER => new TimeDownsamplePeriodMarker(
          dpmc.getTimeDownsamplePeriodMarker.getDownsamplePeriodMarker.getInputColId
        )
        case DownsamplePeriodMarkerCase.COUNTERDOWNSAMPLEPERIODMARKER => new CounterDownsamplePeriodMarker(
          dpmc.getTimeDownsamplePeriodMarker.getDownsamplePeriodMarker.getInputColId
        )
        case DownsamplePeriodMarkerCase.DOWNSAMPLEPERIODMARKER_NOT_SET => throw new IllegalArgumentException(
          "Invalid DownsamplePeriodMarker"
        )
      }
    }
  }

  // DataSchema
  implicit class DataSchemaToProtoConverter(ds: filodb.core.metadata.DataSchema) {
    def toProto: ExecPlans.DataSchema = {
      val builder = ExecPlans.DataSchema.newBuilder()
      builder.setName(ds.name)
      ds.columns.foreach(c => builder.addColumns(c.toProto))
      ds.downsamplers.foreach(d => builder.addDownsamplers(d.toProto))
      builder.setHash(ds.hash)
      builder.setValueColumn(ds.hash)
      ds.downsampleSchema.foreach(schema => builder.setDownsampleSchema(schema))
      builder.setDownsamplePeriodMarker(ds.downsamplePeriodMarker.toProto)
      builder.build()
    }
  }

  implicit class DataSchemaFromProtoConverter(ds: ExecPlans.DataSchema) {
    def fromProto: filodb.core.metadata.DataSchema = {
      import JavaConverters._
      val columns : Seq[filodb.core.metadata.Column] = ds.getColumnsList.asScala.map(c => c.fromProto)
      val downsamplers = ds.getDownsamplersList.asScala.map(d => d.fromProto)
      val downsampleSchema = if (ds.hasDownsampleSchema) {Option(ds.getDownsampleSchema)} else {None}
      filodb.core.metadata.DataSchema(
        ds.getName,
        columns,
        downsamplers,
        ds.getHash,
        ds.getValueColumn,
        downsampleSchema,
        ds.getDownsamplePeriodMarker.fromProto
      )
    }
  }

  // Schema
  implicit class SchemaToProtoConverter(s: filodb.core.metadata.Schema) {
    def toProto: ExecPlans.Schema = {
      val builder = ExecPlans.Schema.newBuilder()
      builder.setPartition(s.partition.toProto)
      builder.setData(s.data.toProto)
      s.downsample.foreach(s => builder.setDownsample(s.toProto))
      builder.build()
    }
  }

  implicit class SchemaFromProtoConverter(s: ExecPlans.Schema) {
    def fromProto: filodb.core.metadata.Schema = {
      val downsample = if (s.hasDownsample) {Option(s.getDownsample.fromProto)} else {None}
      filodb.core.metadata.Schema(s.getPartition.fromProto, s.getData.fromProto, downsample)
    }
  }

  // PartKeyLuceneIndexRecord
  implicit class PartKeyLuceneIndexRecordToProtoConverter(pklir: filodb.core.memstore.PartKeyLuceneIndexRecord) {
    def toProto: ExecPlans.PartKeyLuceneIndexRecord = {
      val builder = ExecPlans.PartKeyLuceneIndexRecord.newBuilder()
      builder.setPartKey(ByteString.copyFrom(pklir.partKey))
      builder.setStartTime(pklir.startTime)
      builder.setEndTime(pklir.endTime)
      builder.build()
    }
  }

  implicit class PartKeyLuceneIndexRecordFromProtoConverter(pklir: ExecPlans.PartKeyLuceneIndexRecord) {
    def fromProto: filodb.core.memstore.PartKeyLuceneIndexRecord = {
      filodb.core.memstore.PartKeyLuceneIndexRecord(pklir.getPartKey.toByteArray, pklir.getStartTime, pklir.getEndTime)
    }
  }

  // PartLookupResult
  implicit class PartLookupResultToProtoConverter(plr: PartLookupResult) {
    def toProto: ExecPlans.PartLookupResult = {
      val builder = ExecPlans.PartLookupResult.newBuilder()
      builder.setShard(plr.shard)
      builder.setChunkMethod(plr.chunkMethod.toProto)
      plr.partsInMemory.foreach(p => builder.addPartsInMemory(p))
      plr.firstSchemaId.foreach(fsi => builder.setFirstSchemaID(fsi))
      plr.partIdsMemTimeGap.foreach((k, v) => builder.putPartIdsMemTimeGap(k, v))
      plr.partIdsNotInMemory.foreach(i => builder.addPartIdsNotInMemory(i))
      plr.pkRecords.foreach(pklir => builder.addPkRecords(pklir.toProto))
      builder.setDataBytesScannedCtr(plr.dataBytesScannedCtr.longValue())
      builder.build()
    }
  }

  implicit class PartLookupResultFromProtoConverter(plr: ExecPlans.PartLookupResult) {
    def fromProto: PartLookupResult = {
      import JavaConverters._
      val psim = plr.getPartsInMemoryList.asScala.map(intgr => intgr.intValue())
      val partsInMemory = debox.Buffer.fromIterable(psim)
      val firstSchemaId = if (plr.hasFirstSchemaID) Option(plr.getFirstSchemaID) else None
      val pimtg = plr.getPartIdsMemTimeGapMap.asScala.map{case (key, value) => (key.intValue(), value.longValue())}
      val partIdsMemTimeGap = debox.Map.fromIterable(pimtg)
      val pinim = plr.getPartIdsNotInMemoryList.asScala.map(intgr => intgr.intValue())
      val partIdsNotInMemory = debox.Buffer.fromIterable(pinim)
      val pkRecords = plr.getPkRecordsList.asScala.map(pklir => pklir.fromProto)
      PartLookupResult(
        plr.getShard,
        plr.getChunkMethod.fromProto,
        partsInMemory,
        firstSchemaId,
        partIdsMemTimeGap,
        partIdsNotInMemory,
        pkRecords,
        new java.util.concurrent.atomic.AtomicLong(plr.getDataBytesScannedCtr)
      )
    }
  }

  // BinaryOperator
  implicit class BinaryOperatorToProtoConverter(bo: filodb.query.BinaryOperator) {
    // scalastyle:off cyclomatic.complexity
    def toProto: ExecPlans.BinaryOperator = {
      val operator = bo match {
        case filodb.query.BinaryOperator.SUB      => ExecPlans.BinaryOperator.SUB
        case filodb.query.BinaryOperator.ADD      => ExecPlans.BinaryOperator.ADD
        case filodb.query.BinaryOperator.MUL      => ExecPlans.BinaryOperator.MUL
        case filodb.query.BinaryOperator.MOD      => ExecPlans.BinaryOperator.MOD
        case filodb.query.BinaryOperator.DIV      => ExecPlans.BinaryOperator.DIV
        case filodb.query.BinaryOperator.POW      => ExecPlans.BinaryOperator.POW
        case filodb.query.BinaryOperator.LAND     => ExecPlans.BinaryOperator.LAND
        case filodb.query.BinaryOperator.LOR      => ExecPlans.BinaryOperator.LOR
        case filodb.query.BinaryOperator.LUnless  => ExecPlans.BinaryOperator.LUNLESS
        case filodb.query.BinaryOperator.EQL      => ExecPlans.BinaryOperator.EQL
        case filodb.query.BinaryOperator.NEQ      => ExecPlans.BinaryOperator.NEQ
        case filodb.query.BinaryOperator.LTE      => ExecPlans.BinaryOperator.LTE
        case filodb.query.BinaryOperator.LSS      => ExecPlans.BinaryOperator.LSS
        case filodb.query.BinaryOperator.GTE      => ExecPlans.BinaryOperator.GTE
        case filodb.query.BinaryOperator.GTR      => ExecPlans.BinaryOperator.GTR
        case filodb.query.BinaryOperator.EQL_BOOL => ExecPlans.BinaryOperator.EQL_BOOL
        case filodb.query.BinaryOperator.NEQ_BOOL => ExecPlans.BinaryOperator.NEQ_BOOL
        case filodb.query.BinaryOperator.LTE_BOOL => ExecPlans.BinaryOperator.LTE_BOOL
        case filodb.query.BinaryOperator.LSS_BOOL => ExecPlans.BinaryOperator.LSS_BOOL
        case filodb.query.BinaryOperator.GTE_BOOL => ExecPlans.BinaryOperator.GTE_BOOL
        case filodb.query.BinaryOperator.GTR_BOOL => ExecPlans.BinaryOperator.GTR_BOOL
        case filodb.query.BinaryOperator.EQLRegex => ExecPlans.BinaryOperator.EQL_REGEX
        case filodb.query.BinaryOperator.NEQRegex => ExecPlans.BinaryOperator.NEQ_REGEX
      }
      operator
    }
  }

  implicit class BinaryOperatorFromProtoConverter(bo: ExecPlans.BinaryOperator) {
    def fromProto: filodb.query.BinaryOperator = {
      val operator : filodb.query.BinaryOperator = bo match {
        case ExecPlans.BinaryOperator.SUB => filodb.query.BinaryOperator.SUB
        case ExecPlans.BinaryOperator.ADD => filodb.query.BinaryOperator.ADD
        case ExecPlans.BinaryOperator.MUL => filodb.query.BinaryOperator.MUL
        case ExecPlans.BinaryOperator.MOD => filodb.query.BinaryOperator.MOD
        case ExecPlans.BinaryOperator.DIV => filodb.query.BinaryOperator.DIV
        case ExecPlans.BinaryOperator.POW => filodb.query.BinaryOperator.POW
        case ExecPlans.BinaryOperator.LAND => filodb.query.BinaryOperator.LAND
        case ExecPlans.BinaryOperator.LOR => filodb.query.BinaryOperator.LOR
        case ExecPlans.BinaryOperator.LUNLESS => filodb.query.BinaryOperator.LUnless
        case ExecPlans.BinaryOperator.EQL => filodb.query.BinaryOperator.EQL
        case ExecPlans.BinaryOperator.NEQ => filodb.query.BinaryOperator.NEQ
        case ExecPlans.BinaryOperator.LTE => filodb.query.BinaryOperator.LTE
        case ExecPlans.BinaryOperator.LSS => filodb.query.BinaryOperator.LSS
        case ExecPlans.BinaryOperator.GTE => filodb.query.BinaryOperator.GTE
        case ExecPlans.BinaryOperator.GTR => filodb.query.BinaryOperator.GTR
        case ExecPlans.BinaryOperator.EQL_BOOL => filodb.query.BinaryOperator.EQL_BOOL
        case ExecPlans.BinaryOperator.NEQ_BOOL => filodb.query.BinaryOperator.NEQ_BOOL
        case ExecPlans.BinaryOperator.LTE_BOOL => filodb.query.BinaryOperator.LTE_BOOL
        case ExecPlans.BinaryOperator.LSS_BOOL => filodb.query.BinaryOperator.LSS_BOOL
        case ExecPlans.BinaryOperator.GTE_BOOL => filodb.query.BinaryOperator.GTE_BOOL
        case ExecPlans.BinaryOperator.GTR_BOOL => filodb.query.BinaryOperator.GTR_BOOL
        case ExecPlans.BinaryOperator.EQL_REGEX => filodb.query.BinaryOperator.EQLRegex
        case ExecPlans.BinaryOperator.NEQ_REGEX => filodb.query.BinaryOperator.NEQRegex
        case ExecPlans.BinaryOperator.UNRECOGNIZED => throw new IllegalArgumentException("Unrecognized binary operator")
      }
      operator
    }
    // scalastyle:on cyclomatic.complexity
  }

  // Cardinality
  implicit class CardinalityToProtoConverter(c: filodb.query.Cardinality) {
    def toProto: ExecPlans.Cardinality = {
      val operator = c match {
        case filodb.query.Cardinality.OneToOne => ExecPlans.Cardinality.ONE_TO_ONE
        case filodb.query.Cardinality.OneToMany => ExecPlans.Cardinality.ONE_TO_MANY
        case filodb.query.Cardinality.ManyToOne => ExecPlans.Cardinality.MANY_TO_ONE
        case filodb.query.Cardinality.ManyToMany => ExecPlans.Cardinality.MANY_TO_MANY
      }
      operator
    }
  }

  implicit class CardinalityFromProtoConverter(c: ExecPlans.Cardinality) {
    def fromProto: filodb.query.Cardinality = {
      val cardinality: filodb.query.Cardinality = c match {
        case ExecPlans.Cardinality.ONE_TO_ONE => filodb.query.Cardinality.OneToOne
        case ExecPlans.Cardinality.ONE_TO_MANY => filodb.query.Cardinality.OneToMany
        case ExecPlans.Cardinality.MANY_TO_ONE => filodb.query.Cardinality.ManyToOne
        case ExecPlans.Cardinality.MANY_TO_MANY => filodb.query.Cardinality.ManyToMany
        case ExecPlans.Cardinality.UNRECOGNIZED => throw new IllegalArgumentException("Unrecognized cardinality")
      }
      cardinality
    }
  }


  // ScalarFunctionId
  implicit class ScalarFunctionIdToProtoConverter(f: filodb.query.ScalarFunctionId) {
    //noinspection ScalaStyle
    def toProto: ExecPlans.ScalarFunctionId = {
      val function = f match {
        case filodb.query.ScalarFunctionId.Scalar => ExecPlans.ScalarFunctionId.SCALAR_FI
        case filodb.query.ScalarFunctionId.Time => ExecPlans.ScalarFunctionId.TIME_FI
        case filodb.query.ScalarFunctionId.DaysInMonth => ExecPlans.ScalarFunctionId.DAYS_IN_MONTH_FI
        case filodb.query.ScalarFunctionId.DayOfMonth => ExecPlans.ScalarFunctionId.DAY_OF_MONTH_FI
        case filodb.query.ScalarFunctionId.DayOfWeek => ExecPlans.ScalarFunctionId.DAY_OF_WEEK_FI
        case filodb.query.ScalarFunctionId.Hour => ExecPlans.ScalarFunctionId.HOUR_FI
        case filodb.query.ScalarFunctionId.Minute => ExecPlans.ScalarFunctionId.MINUTE_FI
        case filodb.query.ScalarFunctionId.Month => ExecPlans.ScalarFunctionId.MONTH_FI
        case filodb.query.ScalarFunctionId.Year => ExecPlans.ScalarFunctionId.YEAR_FI
      }
      function
    }
  }

  implicit class ScalarFunctionIdFromProtoConverter(f: ExecPlans.ScalarFunctionId) {
    //noinspection ScalaStyle
    def fromProto: filodb.query.ScalarFunctionId = {
      val function: filodb.query.ScalarFunctionId = f match {
        case ExecPlans.ScalarFunctionId.SCALAR_FI => filodb.query.ScalarFunctionId.Scalar
        case ExecPlans.ScalarFunctionId.TIME_FI => filodb.query.ScalarFunctionId.Time
        case ExecPlans.ScalarFunctionId.DAYS_IN_MONTH_FI => filodb.query.ScalarFunctionId.DaysInMonth
        case ExecPlans.ScalarFunctionId.DAY_OF_MONTH_FI => filodb.query.ScalarFunctionId.DayOfMonth
        case ExecPlans.ScalarFunctionId.DAY_OF_WEEK_FI => filodb.query.ScalarFunctionId.DayOfWeek
        case ExecPlans.ScalarFunctionId.HOUR_FI => filodb.query.ScalarFunctionId.Hour
        case ExecPlans.ScalarFunctionId.MINUTE_FI => filodb.query.ScalarFunctionId.Minute
        case ExecPlans.ScalarFunctionId.MONTH_FI => filodb.query.ScalarFunctionId.Month
        case ExecPlans.ScalarFunctionId.YEAR_FI => filodb.query.ScalarFunctionId.Year
        case ExecPlans.ScalarFunctionId.UNRECOGNIZED =>
          throw new IllegalArgumentException("Unrecognized scala function")
      }
      function
    }
  }

  // InstantFunctionId
  implicit class InstantFunctionIdToProtoConverter(f: filodb.query.InstantFunctionId) {
    // scalastyle:off cyclomatic.complexity
    def toProto: ExecPlans.InstantFunctionId = {
      val function = f match {
        case filodb.query.InstantFunctionId.Abs => ExecPlans.InstantFunctionId.ABS
        case filodb.query.InstantFunctionId.Ceil => ExecPlans.InstantFunctionId.CEIL
        case filodb.query.InstantFunctionId.ClampMax => ExecPlans.InstantFunctionId.CLAMP_MAX
        case filodb.query.InstantFunctionId.ClampMin => ExecPlans.InstantFunctionId.CLAMP_MIN
        case filodb.query.InstantFunctionId.Exp => ExecPlans.InstantFunctionId.EXP
        case filodb.query.InstantFunctionId.Floor => ExecPlans.InstantFunctionId.FLOOR
        case filodb.query.InstantFunctionId.HistogramQuantile => ExecPlans.InstantFunctionId.HISTOGRAM_QUANTILE
        case filodb.query.InstantFunctionId.HistogramMaxQuantile => ExecPlans.InstantFunctionId.HISTOGRAM_MAX_QUANTILE
        case filodb.query.InstantFunctionId.HistogramBucket => ExecPlans.InstantFunctionId.HISTOGRAM_BUCKET
        case filodb.query.InstantFunctionId.Ln => ExecPlans.InstantFunctionId.LN
        case filodb.query.InstantFunctionId.Log10 => ExecPlans.InstantFunctionId.LOG10
        case filodb.query.InstantFunctionId.Log2 => ExecPlans.InstantFunctionId.LOG2
        case filodb.query.InstantFunctionId.Round => ExecPlans.InstantFunctionId.ROUND
        case filodb.query.InstantFunctionId.Sgn => ExecPlans.InstantFunctionId.SGN
        case filodb.query.InstantFunctionId.Sqrt => ExecPlans.InstantFunctionId.SQRT
        case filodb.query.InstantFunctionId.DaysInMonth => ExecPlans.InstantFunctionId.DAYS_IN_MONTH
        case filodb.query.InstantFunctionId.DayOfMonth => ExecPlans.InstantFunctionId.DAY_OF_MONTH
        case filodb.query.InstantFunctionId.DayOfWeek => ExecPlans.InstantFunctionId.DAY_OF_WEEK
        case filodb.query.InstantFunctionId.Hour => ExecPlans.InstantFunctionId.HOUR
        case filodb.query.InstantFunctionId.Minute => ExecPlans.InstantFunctionId.MINUTE
        case filodb.query.InstantFunctionId.Month => ExecPlans.InstantFunctionId.MONTH
        case filodb.query.InstantFunctionId.Year => ExecPlans.InstantFunctionId.YEAR
        case filodb.query.InstantFunctionId.OrVectorDouble => ExecPlans.InstantFunctionId.OR_VECTOR_DOUBLE
      }
      function
    }
  }

  implicit class InstantFunctionIdFromProtoConverter(f: ExecPlans.InstantFunctionId) {
    def fromProto: filodb.query.InstantFunctionId = {
      val function: filodb.query.InstantFunctionId  = f match {
        case ExecPlans.InstantFunctionId.ABS => filodb.query.InstantFunctionId.Abs
        case ExecPlans.InstantFunctionId.CEIL => filodb.query.InstantFunctionId.Ceil
        case ExecPlans.InstantFunctionId.CLAMP_MAX => filodb.query.InstantFunctionId.ClampMax
        case ExecPlans.InstantFunctionId.CLAMP_MIN => filodb.query.InstantFunctionId.ClampMin
        case ExecPlans.InstantFunctionId.EXP => filodb.query.InstantFunctionId.Exp
        case ExecPlans.InstantFunctionId.FLOOR => filodb.query.InstantFunctionId.Floor
        case ExecPlans.InstantFunctionId.HISTOGRAM_QUANTILE => filodb.query.InstantFunctionId.HistogramQuantile
        case ExecPlans.InstantFunctionId.HISTOGRAM_MAX_QUANTILE => filodb.query.InstantFunctionId.HistogramMaxQuantile
        case ExecPlans.InstantFunctionId.HISTOGRAM_BUCKET => filodb.query.InstantFunctionId.HistogramBucket
        case ExecPlans.InstantFunctionId.LN => filodb.query.InstantFunctionId.Ln
        case ExecPlans.InstantFunctionId.LOG10 => filodb.query.InstantFunctionId.Log10
        case ExecPlans.InstantFunctionId.LOG2 => filodb.query.InstantFunctionId.Log2
        case ExecPlans.InstantFunctionId.ROUND => filodb.query.InstantFunctionId.Round
        case ExecPlans.InstantFunctionId.SGN => filodb.query.InstantFunctionId.Sgn
        case ExecPlans.InstantFunctionId.SQRT => filodb.query.InstantFunctionId.Sqrt
        case ExecPlans.InstantFunctionId.DAYS_IN_MONTH => filodb.query.InstantFunctionId.DaysInMonth
        case ExecPlans.InstantFunctionId.DAY_OF_MONTH => filodb.query.InstantFunctionId.DayOfMonth
        case ExecPlans.InstantFunctionId.DAY_OF_WEEK => filodb.query.InstantFunctionId.DayOfWeek
        case ExecPlans.InstantFunctionId.HOUR => filodb.query.InstantFunctionId.Hour
        case ExecPlans.InstantFunctionId.MINUTE => filodb.query.InstantFunctionId.Minute
        case ExecPlans.InstantFunctionId.MONTH => filodb.query.InstantFunctionId.Month
        case ExecPlans.InstantFunctionId.YEAR => filodb.query.InstantFunctionId.Year
        case ExecPlans.InstantFunctionId.OR_VECTOR_DOUBLE => filodb.query.InstantFunctionId.OrVectorDouble
        case ExecPlans.InstantFunctionId.UNRECOGNIZED =>
          throw new IllegalArgumentException("Unrecognized scala function")
      }
      function
    }
  }

  // InternalRangeFunction
  implicit class InternalRangeFunctionToProtoConverter(f: InternalRangeFunction) {
    def toProto: ExecPlans.InternalRangeFunction = {
      val function = f match {
        case InternalRangeFunction.AvgOverTime => ExecPlans.InternalRangeFunction.AVG_OVER_TIME
        case InternalRangeFunction.Changes => ExecPlans.InternalRangeFunction.CHANGES
        case InternalRangeFunction.CountOverTime => ExecPlans.InternalRangeFunction.COUNT_OVER_TIME
        case InternalRangeFunction.Delta => ExecPlans.InternalRangeFunction.DELTA
        case InternalRangeFunction.Deriv => ExecPlans.InternalRangeFunction.DERIV
        case InternalRangeFunction.HoltWinters => ExecPlans.InternalRangeFunction.HOLT_WINTERS
        case InternalRangeFunction.ZScore => ExecPlans.InternalRangeFunction.ZSCORE
        case InternalRangeFunction.Idelta => ExecPlans.InternalRangeFunction.IDELTA
        case InternalRangeFunction.Increase => ExecPlans.InternalRangeFunction.INCREASE
        case InternalRangeFunction.Irate => ExecPlans.InternalRangeFunction.IRATE
        case InternalRangeFunction.MaxOverTime => ExecPlans.InternalRangeFunction.MAX_OVER_TIME
        case InternalRangeFunction.MinOverTime => ExecPlans.InternalRangeFunction.MIN_OVER_TIME
        case InternalRangeFunction.PredictLinear => ExecPlans.InternalRangeFunction.PREDICT_LINEAR
        case InternalRangeFunction.QuantileOverTime => ExecPlans.InternalRangeFunction.QUANTILE_OVER_TIME
        case InternalRangeFunction.Rate => ExecPlans.InternalRangeFunction.RATE
        case InternalRangeFunction.Resets => ExecPlans.InternalRangeFunction.RESETS
        case InternalRangeFunction.StdDevOverTime => ExecPlans.InternalRangeFunction.STD_DEV_OVER_TIME
        case InternalRangeFunction.StdVarOverTime => ExecPlans.InternalRangeFunction.STD_VAR_OVER_TIME
        case InternalRangeFunction.SumOverTime => ExecPlans.InternalRangeFunction.SUM_OVER_TIME
        case InternalRangeFunction.Last => ExecPlans.InternalRangeFunction.LAST
        case InternalRangeFunction.LastOverTime => ExecPlans.InternalRangeFunction.LAST_OVER_TIME
        case InternalRangeFunction.AvgWithSumAndCountOverTime =>
          ExecPlans.InternalRangeFunction.AVG_WITH_SUM_AND_COUNT_OVER_TIME
        case InternalRangeFunction.SumAndMaxOverTime => ExecPlans.InternalRangeFunction.SUM_AND_MAX_OVER_TIME
        case InternalRangeFunction.LastSampleHistMax => ExecPlans.InternalRangeFunction.LAST_SAMPLE_HIST_MAX
        case InternalRangeFunction.Timestamp => ExecPlans.InternalRangeFunction.TIME_STAMP
        case InternalRangeFunction.AbsentOverTime => ExecPlans.InternalRangeFunction.ABSENT_OVER_TIME
        case InternalRangeFunction.PresentOverTime => ExecPlans.InternalRangeFunction.PRESENT_OVER_TIME
      }
      function
    }
  }

  implicit class InternalRangeFunctionFromProtoConverter(f: ExecPlans.InternalRangeFunction) {
    def fromProto: InternalRangeFunction = {
      val function: InternalRangeFunction = f match {
        case ExecPlans.InternalRangeFunction.AVG_OVER_TIME => InternalRangeFunction.AvgOverTime
        case ExecPlans.InternalRangeFunction.CHANGES => InternalRangeFunction.Changes
        case ExecPlans.InternalRangeFunction.COUNT_OVER_TIME => InternalRangeFunction.CountOverTime
        case ExecPlans.InternalRangeFunction.DELTA => InternalRangeFunction.Delta
        case ExecPlans.InternalRangeFunction.DERIV => InternalRangeFunction.Deriv
        case ExecPlans.InternalRangeFunction.HOLT_WINTERS => InternalRangeFunction.HoltWinters
        case ExecPlans.InternalRangeFunction.ZSCORE => InternalRangeFunction.ZScore
        case ExecPlans.InternalRangeFunction.IDELTA => InternalRangeFunction.Idelta
        case ExecPlans.InternalRangeFunction.INCREASE => InternalRangeFunction.Increase
        case ExecPlans.InternalRangeFunction.IRATE => InternalRangeFunction.Irate
        case ExecPlans.InternalRangeFunction.MAX_OVER_TIME => InternalRangeFunction.MaxOverTime
        case ExecPlans.InternalRangeFunction.MIN_OVER_TIME => InternalRangeFunction.MinOverTime
        case ExecPlans.InternalRangeFunction.PREDICT_LINEAR => InternalRangeFunction.PredictLinear
        case ExecPlans.InternalRangeFunction.QUANTILE_OVER_TIME => InternalRangeFunction.QuantileOverTime
        case ExecPlans.InternalRangeFunction.RATE => InternalRangeFunction.Rate
        case ExecPlans.InternalRangeFunction.RESETS => InternalRangeFunction.Resets
        case ExecPlans.InternalRangeFunction.STD_DEV_OVER_TIME => InternalRangeFunction.StdDevOverTime
        case ExecPlans.InternalRangeFunction.STD_VAR_OVER_TIME => InternalRangeFunction.StdVarOverTime
        case ExecPlans.InternalRangeFunction.SUM_OVER_TIME => InternalRangeFunction.SumAndMaxOverTime
        case ExecPlans.InternalRangeFunction.LAST => InternalRangeFunction.Last
        case ExecPlans.InternalRangeFunction.LAST_OVER_TIME => InternalRangeFunction.LastOverTime
        case ExecPlans.InternalRangeFunction.AVG_WITH_SUM_AND_COUNT_OVER_TIME =>
          InternalRangeFunction.AvgWithSumAndCountOverTime
        case ExecPlans.InternalRangeFunction.SUM_AND_MAX_OVER_TIME => InternalRangeFunction.SumAndMaxOverTime
        case ExecPlans.InternalRangeFunction.LAST_SAMPLE_HIST_MAX => InternalRangeFunction.LastSampleHistMax
        case ExecPlans.InternalRangeFunction.TIME_STAMP => InternalRangeFunction.Timestamp
        case ExecPlans.InternalRangeFunction.ABSENT_OVER_TIME => InternalRangeFunction.AbsentOverTime
        case ExecPlans.InternalRangeFunction.PRESENT_OVER_TIME => InternalRangeFunction.PresentOverTime
        case ExecPlans.InternalRangeFunction.UNRECOGNIZED =>
          throw new IllegalArgumentException(s"Unrecognized InternalRangeFunction ${f}")
      }
      function
    }
    // scalastyle:on cyclomatic.complexity
  }

  // SortFunctionId
  implicit class SortFunctionIdToProtoConverter(f: filodb.query.SortFunctionId) {
    //noinspection ScalaStyle
    def toProto: ExecPlans.SortFunctionId = {
      val function = f match {
        case filodb.query.SortFunctionId.Sort => ExecPlans.SortFunctionId.SORT
        case filodb.query.SortFunctionId.SortDesc => ExecPlans.SortFunctionId.SORT_DESC
      }
      function
    }
  }

  implicit class SortFunctionIdFromProtoConverter(f: ExecPlans.SortFunctionId) {
    //noinspection ScalaStyle
    def fromProto: filodb.query.SortFunctionId = {
      val function: filodb.query.SortFunctionId = f match {
        case ExecPlans.SortFunctionId.SORT => filodb.query.SortFunctionId.Sort
        case ExecPlans.SortFunctionId.SORT_DESC => filodb.query.SortFunctionId.SortDesc
        case ExecPlans.SortFunctionId.UNRECOGNIZED =>
          throw new IllegalArgumentException(s"Unrecognized SortFunctionId ${f}")
      }
      function
    }
  }

  // MiscellaneousFunctionId
  implicit class MiscellaneousFunctionIdToProtoConverter(f: filodb.query.MiscellaneousFunctionId) {
    def toProto: ExecPlans.MiscellaneousFunctionId = {
      val function = f match {
        case filodb.query.MiscellaneousFunctionId.LabelReplace => ExecPlans.MiscellaneousFunctionId.LABEL_REPLACE
        case filodb.query.MiscellaneousFunctionId.LabelJoin => ExecPlans.MiscellaneousFunctionId.LABEL_JOIN
        case filodb.query.MiscellaneousFunctionId.HistToPromVectors =>
          ExecPlans.MiscellaneousFunctionId.HIST_TO_PROM_VECTORS
      }
      function
    }
  }

  implicit class MiscellaneousFunctionIdFromProtoConverter(f: ExecPlans.MiscellaneousFunctionId) {
    def fromProto: filodb.query.MiscellaneousFunctionId = {
      val function: filodb.query.MiscellaneousFunctionId = f match {
        case ExecPlans.MiscellaneousFunctionId.LABEL_REPLACE => filodb.query.MiscellaneousFunctionId.LabelReplace
        case ExecPlans.MiscellaneousFunctionId.LABEL_JOIN => filodb.query.MiscellaneousFunctionId.LabelJoin
        case ExecPlans.MiscellaneousFunctionId.HIST_TO_PROM_VECTORS =>
          filodb.query.MiscellaneousFunctionId.HistToPromVectors
        case ExecPlans.MiscellaneousFunctionId.UNRECOGNIZED =>
          throw new IllegalArgumentException(s"Unrecognized MiscellaneousFunctionId ${f}")
      }
      function
    }
  }

  // Filters section
  def getProtoFilter(f: Filter): ExecPlans.Filter = {
    val builder = ExecPlans.Filter.newBuilder()
    builder.setOperatorString(f.operatorString)
    //TODO why is value ANY???
    //need to enumerate what can be in ANY, most likely
    // number integer/float/string?
    f.valuesStrings.foreach(vs => builder.addValueStrings(vs.asInstanceOf[String]))
    builder.build()
  }

  // FilterEquals
  implicit class FilterEqualsToProtoConverter(fe: Filter.Equals) {
    def toProto: ExecPlans.FilterEquals = {
      val builder = ExecPlans.FilterEquals.newBuilder()
      builder.setFilter(getProtoFilter(fe))
      builder.build()
    }
  }

  implicit class FilterEqualsFromProtoConverter(fe: ExecPlans.FilterEquals) {
    def fromProto: Filter.Equals = {
      Filter.Equals(
        fe.getFilter.getValueStringsList.get(0)
      )
    }
  }

  // FilterNotEquals
  implicit class FilterNotEqualsToProtoConverter(fne: Filter.NotEquals) {
    def toProto: ExecPlans.FilterNotEquals = {
      val builder = ExecPlans.FilterNotEquals.newBuilder()
      builder.setFilter(getProtoFilter(fne))
      builder.build()
    }
  }

  implicit class FilterNotEqualsFromProtoConverter(fne: ExecPlans.FilterNotEquals) {
    def fromProto: Filter.NotEquals = {
      Filter.NotEquals(
        fne.getFilter.getValueStringsList.get(0)
      )
    }
  }

  // FilterEqualsRegex
  implicit class FilterEqualsRegexToProtoConverter(fe: Filter.EqualsRegex) {
    def toProto: ExecPlans.FilterEqualsRegex = {
      val builder = ExecPlans.FilterEqualsRegex.newBuilder()
      builder.setFilter(getProtoFilter(fe))
      builder.build()
    }
  }

  implicit class FilterEqualsRegexFromProtoConverter(fe: ExecPlans.FilterEqualsRegex) {
    def fromProto: Filter.EqualsRegex = {
      Filter.EqualsRegex(
        fe.getFilter.getValueStringsList.get(0)
      )
    }
  }

  // FilterNotEqualsRegex
  implicit class FilterNotEqualsRegexToProtoConverter(fner: Filter.NotEqualsRegex) {
    def toProto: ExecPlans.FilterNotEqualsRegex = {
      val builder = ExecPlans.FilterNotEqualsRegex.newBuilder()
      builder.setFilter(getProtoFilter(fner))
      builder.build()
    }
  }

  implicit class FilterNotEqualsRegexFromProtoConverter(fner: ExecPlans.FilterNotEqualsRegex) {
    def fromProto: Filter.NotEqualsRegex = {
      Filter.NotEqualsRegex(
        fner.getFilter.getValueStringsList.get(0)
      )
    }
  }

  // FilterIn
  implicit class FilterInToProtoConverter(fi: Filter.In) {
    def toProto: ExecPlans.FilterIn = {
      val builder = ExecPlans.FilterIn.newBuilder()
      builder.setFilter(getProtoFilter(fi))
      builder.build()
    }
  }

  implicit class FilterInFromProtoConverter(fi: ExecPlans.FilterIn) {
    def fromProto: Filter.In = {
      import JavaConverters._
      Filter.In(
        fi.getFilter.getValueStringsList.asScala.toSet
      )
    }
  }

  // FilterAnd
  implicit class FilterAndToProtoConverter(fa: Filter.And) {
    def toProto: ExecPlans.FilterAnd = {
      val builder = ExecPlans.FilterAnd.newBuilder()
      builder.setFilter(getProtoFilter(fa))
      builder.setLeft(fa.left.toProto)
      builder.setRight(fa.right.toProto)
      builder.build()
    }
  }

  implicit class FilterAndFromProtoConverter(fi: ExecPlans.FilterAnd) {
    def fromProto: Filter.And = {
      val left = fi.getLeft.fromProto
      val right = fi.getRight.fromProto
      Filter.And(left, right)
    }
  }

  implicit class FilterToProtoConverter(f: Filter) {
    def toProto(): ExecPlans.FilterContainer = {
      val builder = ExecPlans.FilterContainer.newBuilder()
      f match {
        case fe: Filter.Equals => builder.setFilterEquals(fe.toProto)
        case fne: Filter.NotEquals => builder.setFilterNotEquals(fne.toProto)
        case fer: Filter.EqualsRegex => builder.setFilterEqualsRegex(fer.toProto)
        case fner: Filter.NotEqualsRegex => builder.setFilterNotEqualsRegex(fner.toProto)
        case fi: Filter.In => builder.setFilterIn(fi.toProto)
        case fa: Filter.And => builder.setFilterAnd(fa.toProto)
      }
      builder.build()
    }
  }

  implicit class FilterContainerFromProtoConverter(fc: ExecPlans.FilterContainer) {
    def fromProto: Filter = {
      import ExecPlans.FilterContainer.FilterCase
      fc.getFilterCase match {
        case FilterCase.FILTEREQUALS => fc.getFilterEquals.fromProto
        case FilterCase.FILTERNOTEQUALS => fc.getFilterNotEquals.fromProto
        case FilterCase.FILTEREQUALSREGEX => fc.getFilterEqualsRegex.fromProto
        case FilterCase.FILTERNOTEQUALSREGEX => fc.getFilterNotEqualsRegex.fromProto
        case FilterCase.FILTERIN => fc.getFilterIn.fromProto
        case FilterCase.FILTERAND => fc.getFilterAnd.fromProto
        case FilterCase.FILTER_NOT_SET => throw new IllegalArgumentException("Filter is not set")
      }
    }
  }

  // ColumnFilter
  implicit class ColumnFilterToProtoConverter(cf: ColumnFilter) {
    def toProto: ExecPlans.ColumnFilter = {
      val builder = ExecPlans.ColumnFilter.newBuilder()
      builder.setColumn(cf.column)
      builder.setFilter(cf.filter.toProto())
      builder.build()
    }
  }

  implicit class ColumnFilterFromProtoConverter(cf: ExecPlans.ColumnFilter) {
    def fromProto: ColumnFilter = {
      ColumnFilter(cf.getColumn, cf.getFilter.fromProto)
    }
  }

  // QueryContext
  implicit class QueryContextToProtoConverter(qc: QueryContext) {
    def toProto: ExecPlans.QueryContext = {
      val builder = ExecPlans.QueryContext.newBuilder()
      builder.setOrigQueryParams(qc.origQueryParams.toProto)
      builder.setQueryId(qc.queryId)
      builder.setSubmitTime(qc.submitTime)
      builder.setPlannerParams(qc.plannerParams.toProto)
      val javaTraceInfoMap = JavaConverters.mapAsJavaMap(qc.traceInfo)
      builder.putAllTraceInfo(javaTraceInfoMap)
      builder.build()
    }
  }

  implicit class QueryContextFromProtoConverter(qcProto: ExecPlans.QueryContext) {
    def fromProto: QueryContext = {
      val originalQueryParams = qcProto.getOrigQueryParams().fromProto
      val plannerParams = qcProto.getPlannerParams.fromProto
      import scala.collection.JavaConverters._
      val traceInfo = qcProto.getTraceInfoMap.asScala.toMap
      val qc = QueryContext(
        originalQueryParams,
        qcProto.getQueryId,
        qcProto.getSubmitTime,
        plannerParams,
        traceInfo
      )
      qc
    }
  }


  implicit class PlanDispatcherToProtoConverter(pd: filodb.query.exec.PlanDispatcher) {

    def toProto: ExecPlans.PlanDispatcher = {
      val builder = ExecPlans.PlanDispatcher.newBuilder()
      builder.setClusterName(pd.clusterName)
      builder.setIsLocalCall(pd.isLocalCall)
      builder.build()
    }

    def toPlanDispatcherContainer : ExecPlans.PlanDispatcherContainer = {
      val builder = ExecPlans.PlanDispatcherContainer.newBuilder()
      pd match {
        case apd: ActorPlanDispatcher => builder.setActorPlanDispatcher(apd.toProto)
        case ippd: InProcessPlanDispatcher => builder.setInProcessPlanDispatcher(ippd.toProto)
        case _ => throw new IllegalArgumentException(s"Unexpected PlanDispatcher subclass ${pd.getClass.getName}")
      }
      builder.build()

    }
  }

  implicit class ActorPlanDispatcherToProtoConverter(apd: filodb.coordinator.ActorPlanDispatcher) {
    def toProto(): ExecPlans.ActorPlanDispatcher = {
      val builder = ExecPlans.ActorPlanDispatcher.newBuilder()
      builder.setPlanDispatcher(apd.asInstanceOf[filodb.query.exec.PlanDispatcher].toProto)
      builder.setActorPath(apd.target.path.toSerializationFormat)
      builder.build()
    }
  }

  implicit class ActorPlanDispatcherFromProtoConverter(apd: ExecPlans.ActorPlanDispatcher) {
    def fromProto: ActorPlanDispatcher = {
      // target: ActorRef, clusterName: String
      val timeout = akka.util.Timeout(10L, TimeUnit.SECONDS);
      val f = ActorSystemHolder.system.actorSelection(apd.getActorPath).resolveOne()(timeout);
      // HERE we wait for 60 seconds for the actorref to be resolved:
      // 1) too long?
      // 2) should it be done somehow in parallel/asynchronously?
      val ar: akka.actor.ActorRef = Await.result(f, Duration(60L, TimeUnit.SECONDS))
      val dispatcher = ActorPlanDispatcher(
        ar, apd.getPlanDispatcher.getClusterName
      )
      dispatcher
    }
  }

  implicit class InProcessPlanDispatcherToProtoConverter(ippd: filodb.query.exec.InProcessPlanDispatcher) {
    def toProto(): ExecPlans.InProcessPlanDispatcher = {
      val builder = ExecPlans.InProcessPlanDispatcher.newBuilder()
      //builder.setPlanDispatcher(ippd.asInstanceOf[filodb.query.exec.PlanDispatcher].toProto)
      builder.setQueryConfig(ippd.queryConfig.toProto)
      builder.build()
    }
  }

  implicit class InProcessPlanDispatcherFromProtoConverter(apd: ExecPlans.InProcessPlanDispatcher) {
    def fromProto: InProcessPlanDispatcher = {
      InProcessPlanDispatcher(apd.getQueryConfig.fromProto)
    }
  }

  implicit class DatasetRefToProtoConverter(dr: filodb.core.DatasetRef) {
    def toProto(): ExecPlans.DatasetRef = {
      val builder = ExecPlans.DatasetRef.newBuilder()
      builder.setDataset(dr.dataset)
      builder.clearDatabase()
      dr.database.foreach(db =>builder.setDatabase(db))
      builder.build()
    }
  }

  implicit class DatasetRefFromProtoConverter(dr: ExecPlans.DatasetRef) {
    def fromProto(): filodb.core.DatasetRef = {
      val database = if (dr.hasDatabase) Some(dr.getDatabase) else None
      val datasetRef = filodb.core.DatasetRef(dr.getDataset, database)
      datasetRef
    }
  }

  implicit class QueryCommandToProtoConverter(qc: QueryCommand) {
    def toProto : ExecPlans.QueryCommand = {
      val builder = ExecPlans.QueryCommand.newBuilder()
      builder.setSubmitTime(qc.submitTime)
      builder.setDatasetRef(qc.dataset.toProto)
      builder.build()
    }
  }

  implicit class AggregationOperatorToProtoConverter(ao : filodb.query.AggregationOperator) {
    def toProto : ExecPlans.AggregationOperator = {
      ao match {
        case AggregationOperator.TopK => ExecPlans.AggregationOperator.TOP_K
        case AggregationOperator.CountValues => ExecPlans.AggregationOperator.COUNT_VALUES
        case AggregationOperator.Count => ExecPlans.AggregationOperator.COUNT
        case AggregationOperator.Group => ExecPlans.AggregationOperator.GROUP
        case AggregationOperator.BottomK => ExecPlans.AggregationOperator.BOTTOM_K
        case AggregationOperator.Min => ExecPlans.AggregationOperator.MIN
        case AggregationOperator.Avg => ExecPlans.AggregationOperator.AVG
        case AggregationOperator.Sum => ExecPlans.AggregationOperator.SUM
        case AggregationOperator.Stddev => ExecPlans.AggregationOperator.STDDEV
        case AggregationOperator.Stdvar => ExecPlans.AggregationOperator.STDVAR
        case AggregationOperator.Quantile => ExecPlans.AggregationOperator.QUANTILE
        case AggregationOperator.Max => ExecPlans.AggregationOperator.MAX
      }
    }
  }

  implicit class AggregationOperatorFromProtoConverter(ao: ExecPlans.AggregationOperator) {
    def fromProto: AggregationOperator = {
      ao match {
        case ExecPlans.AggregationOperator.TOP_K => AggregationOperator.TopK
        case ExecPlans.AggregationOperator.COUNT_VALUES => AggregationOperator.CountValues
        case ExecPlans.AggregationOperator.COUNT => AggregationOperator.Count
        case ExecPlans.AggregationOperator.GROUP => AggregationOperator.Group
        case ExecPlans.AggregationOperator.BOTTOM_K => AggregationOperator.BottomK
        case ExecPlans.AggregationOperator.MIN => AggregationOperator.Min
        case ExecPlans.AggregationOperator.AVG => AggregationOperator.Avg
        case ExecPlans.AggregationOperator.SUM => AggregationOperator.Sum
        case ExecPlans.AggregationOperator.STDDEV => AggregationOperator.Stddev
        case ExecPlans.AggregationOperator.STDVAR => AggregationOperator.Stdvar
        case ExecPlans.AggregationOperator.QUANTILE => AggregationOperator.Quantile
        case ExecPlans.AggregationOperator.MAX => AggregationOperator.Max
        case _ => throw new IllegalArgumentException("Unknown aggregation operator")
      }
    }
  }

  def getAggregateParameter(ap: Any): ExecPlans.AggregateParameter = {
    val builder = ExecPlans.AggregateParameter.newBuilder()
    ap match {
      case l: Long => builder.setLongParameter(l)
      case i: Int => builder.setIntParameter(i)
      case d: Double => builder.setDoubleParameter(d)
      case s: String => builder.setStringParameter(s)
      case _ => throw new IllegalArgumentException(s"Unexpected aggregate parameter ${ap}")
    }
    builder.build()
  }

  implicit class AggregateParameterFromProto(ap: ExecPlans.AggregateParameter) {
    def fromProto() : Any = {
      ap.getAggregateParameterCase match {
        case ExecPlans.AggregateParameter.AggregateParameterCase.LONGPARAMETER => ap.getLongParameter
        case ExecPlans.AggregateParameter.AggregateParameterCase.INTPARAMETER => ap.getIntParameter
        case ExecPlans.AggregateParameter.AggregateParameterCase.DOUBLEPARAMETER => ap.getDoubleParameter
        case ExecPlans.AggregateParameter.AggregateParameterCase.STRINGPARAMETER => ap.getStringParameter
        case ExecPlans.AggregateParameter.AggregateParameterCase.AGGREGATEPARAMETER_NOT_SET =>
          throw new IllegalArgumentException("aggregate parameter is not set")
      }
    }
  }

  // AggregateClauseType
  implicit class AggregateClauseTypeToProto(act: filodb.query.AggregateClause.ClauseType.Value) {
    def toProto : ExecPlans.AggregateClauseType = {
      act match {
        case filodb.query.AggregateClause.ClauseType.By => ExecPlans.AggregateClauseType.BY
        case filodb.query.AggregateClause.ClauseType.Without => ExecPlans.AggregateClauseType.WITHOUT
      }
    }
  }

  implicit class AggregateClauseTypeFromProto(act: ExecPlans.AggregateClauseType) {
    def fromProto(): filodb.query.AggregateClause.ClauseType.Value = {
      act match  {
        case ExecPlans.AggregateClauseType.BY => filodb.query.AggregateClause.ClauseType.By
        case ExecPlans.AggregateClauseType.WITHOUT => filodb.query.AggregateClause.ClauseType.Without
        case ExecPlans.AggregateClauseType.UNRECOGNIZED =>
          throw new IllegalArgumentException("Unrecognized aggregate clause type")
      }
    }
  }

  implicit class AggregateClauseToProto(ac : filodb.query.AggregateClause) {
    def toProto : ExecPlans.AggregateClause = {
      val builder = ExecPlans.AggregateClause.newBuilder()
      builder.setClauseType(ac.clauseType.toProto)
      ac.labels.foreach(l => builder.addLabels(l))
      builder.build()
    }
  }

  implicit class AggregateClauseFromProto(ac: ExecPlans.AggregateClause) {
    def fromProto(): filodb.query.AggregateClause = {
      import JavaConverters._
      filodb.query.AggregateClause(
        ac.getClauseType.fromProto,
        ac.getLabelsList.asScala
      )
    }
  }

  implicit class ExecPlanFuncArgsToProtoConverter(epfs : ExecPlanFuncArgs) {
    def toProto() : ExecPlans.ExecPlanFuncArgs = {
      val builder = ExecPlans.ExecPlanFuncArgs.newBuilder()
      builder.setExecPlan(epfs.execPlan.toExecPlanContainerProto)
      builder.setTimeStepParams(epfs.timeStepParams.toProto)
      builder.build()
    }
  }

  implicit class ExecPlanFuncArgsFromProtoConverter(epfa: ExecPlans.ExecPlanFuncArgs) {
    def fromProto() : ExecPlanFuncArgs = {
      ExecPlanFuncArgs(
        epfa.getExecPlan.fromProto,
        epfa.getTimeStepParams.fromProto
      )
    }
  }

  implicit class TimeFuncArgsToProtoConverter(tfa : TimeFuncArgs) {
    def toProto() : ExecPlans.TimeFuncArgs = {
      val builder = ExecPlans.TimeFuncArgs.newBuilder()
      builder.setTimeStepParms(tfa.timeStepParams.toProto)
      builder.build()
    }
  }

  implicit class TimeFuncArgsFromProtoConverter(tfa: ExecPlans.TimeFuncArgs) {
    def fromProto(): TimeFuncArgs = {
      TimeFuncArgs(
        tfa.getTimeStepParms.fromProto
      )
    }
  }

  implicit class StaticFuncArgsToProtoConverter(sfa: StaticFuncArgs) {
    def toProto(): ExecPlans.StaticFuncArgs = {
      val builder = ExecPlans.StaticFuncArgs.newBuilder()
      builder.setScalar(sfa.scalar)
      builder.setTimeStepParams(sfa.timeStepParams.toProto)
      builder.build()
    }
  }

  implicit class StaticFuncArgsFromProtoConverter(sfa: ExecPlans.StaticFuncArgs) {
    def fromProto(): StaticFuncArgs = {
      StaticFuncArgs(
        sfa.getScalar,
        sfa.getTimeStepParams.fromProto
      )
    }
  }

  implicit class FuncArgToProto(fa : FuncArgs) {
    def toProto : ExecPlans.FuncArgs = {
      val builder = ExecPlans.FuncArgs.newBuilder()
      fa match {
        case epfa : ExecPlanFuncArgs => {
          val epfaProto : ExecPlans.ExecPlanFuncArgs  = epfa.toProto
          builder.setExecPlanFuncArgs(epfaProto)
        }
        case tfa : TimeFuncArgs => {
          val tfaProto : ExecPlans.TimeFuncArgs = tfa.toProto
          builder.setTimeFuncArgs(tfa.toProto)
        }
        case sfa : StaticFuncArgs => {
          val sfaProto : ExecPlans.StaticFuncArgs = sfa.toProto
          builder.setStaticFuncArgs(sfaProto)
        }
      }
      builder.build()
    }
  }

  implicit class FuncArgsFromProtoConverter(fa: ExecPlans.FuncArgs) {
    def fromProto : FuncArgs = {
      fa.getFuncArgTypeCase match {
        case FuncArgTypeCase.EXECPLANFUNCARGS => fa.getExecPlanFuncArgs.fromProto
        case FuncArgTypeCase.TIMEFUNCARGS => fa.getTimeFuncArgs.fromProto
        case FuncArgTypeCase.STATICFUNCARGS => fa.getStaticFuncArgs.fromProto
        case FuncArgTypeCase.FUNCARGTYPE_NOT_SET => throw new IllegalArgumentException("invalid function argument")
      }
    }
  }

  //************************
  // RangeVectorTransformers
  //************************
  // StitchRvsMapper
  implicit class StitchRvsMapperToProtoConverter(srm: StitchRvsMapper) {
    def toProto : ExecPlans.StitchRvsMapper = {
      val builder = ExecPlans.StitchRvsMapper.newBuilder()
      builder.build()
    }
  }

  implicit class StitchRvsMapperFromProtoConverter(srm: ExecPlans.StitchRvsMapper) {
    def fromProto : StitchRvsMapper = {
      val outputRvRange = if (srm.hasOutputRvRange) {
        Option(srm.getOutputRvRange().fromProto)
      } else {
        None
      }
      StitchRvsMapper(outputRvRange)
    }
  }

//  def getAggregateParameter(parameter : Any) : ExecPlans.AggregateParameter = {
//    val builder = ExecPlans.AggregateParameter.newBuilder()
//    parameter match {
//      case i: Int => builder.setIntParameter(i)
//      case l: Long => builder.setLongParameter(l)
//      case d: Double => builder.setDoubleParameter(d)
//      case s: String => builder.setStringParameter(s)
//    }
//    builder.build()
//  }
//
//  def getAggregateParametersFromProto(params: java.util.List[filodb.grpc.ExecPlans.AggregateParameter]): Seq[Any] = {
//    import JavaConverters._
//    val parameters: Seq[Any] = params.asScala.map(ap => {
//      ap.getAggregateParameterCase match {
//        case AggregateParameterCase.LONGPARAMETER => ap.getLongParameter
//        case AggregateParameterCase.INTPARAMETER => ap.getIntParameter
//        case AggregateParameterCase.DOUBLEPARAMETER => ap.getDoubleParameter
//        case AggregateParameterCase.STRINGPARAMETER => ap.getStringParameter
//        case _ => throw new IllegalArgumentException(s"Unexpected aggregate parameter ${ap}")
//      }
//    })
//    parameters
//  }

  //AggregateMapReduce
  implicit class AggregateMapReduceToProtoConverter(amr : AggregateMapReduce) {
    def toProto: ExecPlans.AggregateMapReduce = {
      val builder = ExecPlans.AggregateMapReduce.newBuilder()
      builder.setAggrOp(amr.aggrOp.toProto)
      amr.aggrParams.foreach(p => builder.addAggrParams(getAggregateParameter(p)))
      amr.clauseOpt.foreach(cp => builder.setClauseOpt(cp.toProto))
      amr.funcParams.foreach(fp => builder.addFuncParams(fp.toProto))
      builder.build()
    }
  }

  implicit class AggregateMapReduceFromProtoConverter(amr: ExecPlans.AggregateMapReduce) {
    def fromProto: AggregateMapReduce = {
      import JavaConverters._
      AggregateMapReduce(
        amr.getAggrOp.fromProto,
        amr.getAggrParamsList().asScala.toSeq.map(ap => ap.fromProto()),
        if (amr.hasClauseOpt) Option(amr.getClauseOpt.fromProto()) else None,
        amr.getFuncParamsList.asScala.map(fa => fa.fromProto)
      )
    }
  }

  //HistToPromSeriesMapper
  implicit class HistToPromSeriesMapperToProtoConverter(htpsm : HistToPromSeriesMapper) {
    def toProto: ExecPlans.HistToPromSeriesMapper = {
      val builder = ExecPlans.HistToPromSeriesMapper.newBuilder()
      builder.setSch(htpsm.sch.toProto)
      builder.build()
    }
  }

  implicit class HistToPromSeriesMapperFromProtoConverter(htpsm: ExecPlans.HistToPromSeriesMapper) {
    def fromProto: HistToPromSeriesMapper = {
      HistToPromSeriesMapper(htpsm.getSch.fromProto)
    }
  }

  //LabelCardinalityPresenter
  implicit class LabelCardinalityPresenterToProtoConverter(lcp : LabelCardinalityPresenter) {
    def toProto: ExecPlans.LabelCardinalityPresenter = {
      val builder = ExecPlans.LabelCardinalityPresenter.newBuilder()
      lcp.funcParams.foreach(fp => builder.addFuncParams(fp.toProto))
      builder.build()
    }
  }

  implicit class LabelCardinalityPresenterFromProtoConverter(lcp: ExecPlans.LabelCardinalityPresenter) {
    def fromProto: LabelCardinalityPresenter = {
      import JavaConverters._
      val funcParams = lcp.getFuncParamsList.asScala.map(fa => fa.fromProto)
      new LabelCardinalityPresenter(funcParams)
    }
  }

  //HistogramQuantileMapper
  implicit class HistogramQuantileMapperToProtoConverter(hqm: HistogramQuantileMapper) {
    def toProto: ExecPlans.HistogramQuantileMapper = {
      val builder = ExecPlans.HistogramQuantileMapper.newBuilder()
      hqm.funcParams.foreach(fp => builder.addFuncParams(fp.toProto))
      builder.build()
    }
  }

  implicit class HistogramQuantileMapperFromProtoConverter(hqm: ExecPlans.HistogramQuantileMapper) {
    def fromProto: HistogramQuantileMapper = {
      import JavaConverters._
      val funcParams = hqm.getFuncParamsList.asScala.map(fa => fa.fromProto)
      HistogramQuantileMapper(funcParams)
    }
  }

  //InstantVectorFunctionMapper
  implicit class InstantVectorFunctionMapperToProtoConverter(ivfm : InstantVectorFunctionMapper) {
    def toProto: ExecPlans.InstantVectorFunctionMapper = {
      val builder = ExecPlans.InstantVectorFunctionMapper.newBuilder()
      builder.setFunction(ivfm.function.toProto)
      ivfm.funcParams.foreach(fa => builder.addFuncParams(fa.toProto))
      builder.build()
    }
  }

  implicit class InstantVectorFunctionMapperFromProtoConverter(ivfm: ExecPlans.InstantVectorFunctionMapper) {
    def fromProto: InstantVectorFunctionMapper = {
      import JavaConverters._
      val funcParams = ivfm.getFuncParamsList.asScala.toSeq.map(fp => fp.fromProto)
      InstantVectorFunctionMapper(
        ivfm.getFunction.fromProto,
        funcParams
      )
    }
  }

  // PeriodicSamples
  implicit class PeriodicSamplesMapperToProtoConverter(psm : PeriodicSamplesMapper) {
    def toProto: ExecPlans.PeriodicSamplesMapper = {
      val builder = ExecPlans.PeriodicSamplesMapper.newBuilder()
      builder.setStartMs(psm.startMs)
      builder.setStepMs(psm.stepMs)
      builder.setEndMs(psm.endMs)
      psm.window.foreach(w => builder.setWindow(w))
      psm.functionId.foreach(fi => builder.setFunctionId(fi.toProto))
      builder.setQueryContext(psm.queryContext.toProto)
      builder.setStepMultipleNotationUsed(psm.stepMultipleNotationUsed)
      psm.funcParams.foreach(fp => builder.addFuncParams(fp.toProto))
      psm.offsetMs.foreach(o => builder.setOffsetMs(o))
      builder.setRawSource(psm.rawSource)
      builder.setLeftInclusiveWindow(psm.leftInclusiveWindow)
      builder.build()
    }
  }

  implicit class PeriodicSamplesMapperFromProtoConverter(psm: ExecPlans.PeriodicSamplesMapper) {
    def fromProto: PeriodicSamplesMapper = {
      import JavaConverters._
      val window = if (psm.hasWindow) Option(psm.getWindow) else None
      val functionId = if (psm.hasFunctionId) Option(psm.getFunctionId.fromProto) else None
      val funcParams = psm.getFuncParamsList.asScala.toSeq.map(fa => fa.fromProto)
      val offsetMs = if (psm.hasOffsetMs) Option(psm.getOffsetMs) else None
      PeriodicSamplesMapper(
        psm.getStartMs,
        psm.getStepMs,
        psm.getEndMs,
        window,
        functionId,
        psm.getQueryContext.fromProto,
        psm.getStepMultipleNotationUsed,
        funcParams,
        offsetMs,
        psm.getRawSource,
        psm.getLeftInclusiveWindow
      )
    }
  }

  // SortFunctionMapper
  implicit class SortFunctionMapperToProtoConverter(sfm : SortFunctionMapper) {
    def toProto: ExecPlans.SortFunctionMapper = {
      val builder = ExecPlans.SortFunctionMapper.newBuilder()
      builder.setFunction(sfm.function.toProto)
      builder.build()
    }
  }

  implicit class SortFunctionMapperFromProtoConverter(srm: ExecPlans.SortFunctionMapper) {
    def fromProto: SortFunctionMapper = {
      SortFunctionMapper(srm.getFunction.fromProto)
    }
  }

  // MiscellaneousFunction
  implicit class MiscellaneousFunctionMapperToProtoConverter(mfm : MiscellaneousFunctionMapper) {
    def toProto: ExecPlans.MiscellaneousFunctionMapper = {
      val builder = ExecPlans.MiscellaneousFunctionMapper.newBuilder()
      builder.setFunction(mfm.function.toProto)
      mfm.funcStringParam.foreach(fsp => builder.addFuncStringParam(fsp))
      mfm.funcParams.foreach(fa => builder.addFuncParams(fa.toProto))
      builder.build()
    }
  }

  implicit class MiscellaneousFunctionMapperFromProtoConverter(mfm: ExecPlans.MiscellaneousFunctionMapper) {
    def fromProto: MiscellaneousFunctionMapper = {
      import JavaConverters._
      MiscellaneousFunctionMapper(
        mfm.getFunction.fromProto,
        mfm.getFuncStringParamList.asScala.toSeq,
        mfm.getFuncParamsList.asScala.toSeq.map(fa => fa.fromProto)
      )
    }
  }

  // LimitFunctionMapper
  implicit class LimitFunctionMapperToProtoConverter(lfm : LimitFunctionMapper) {
    def toProto: ExecPlans.LimitFunctionMapper = {
      val builder = ExecPlans.LimitFunctionMapper.newBuilder()
      builder.setLimitToApply(lfm.limitToApply)
      builder.build()
    }
  }

  implicit class LimitFunctionMapperFromProtoConverter(lfm: ExecPlans.LimitFunctionMapper) {
    def fromProto: LimitFunctionMapper = {
      LimitFunctionMapper(lfm.getLimitToApply)
    }
  }

  // ScalarOperationMapper
  implicit class ScalarOperationMapperToProtoConverter(som : ScalarOperationMapper) {
    def toProto: ExecPlans.ScalarOperationMapper = {
      val builder = ExecPlans.ScalarOperationMapper.newBuilder()
      builder.setOperator(som.operator.toProto)
      builder.setScalarOnLhs(som.scalarOnLhs)
      som.funcParams.foreach(fa => builder.addFuncParams(fa.toProto))
      builder.build()
    }
  }

  implicit class ScalarOperationMapperFromProtoConverter(som: ExecPlans.ScalarOperationMapper) {
    def fromProto: ScalarOperationMapper = {
      import JavaConverters._
      ScalarOperationMapper(
        som.getOperator.fromProto,
        som.getScalarOnLhs,
        som.getFuncParamsList.asScala.toSeq.map(fa => fa.fromProto)
      )
    }
  }

  // ScalarFunctionMapper
  implicit class ScalarFunctionMapperToProtoConverter(sfm: ScalarFunctionMapper) {
    def toProto: ExecPlans.ScalarFunctionMapper = {
      val builder = ExecPlans.ScalarFunctionMapper.newBuilder()
      builder.setFunction(sfm.function.toProto)
      builder.setTimeStepParams(sfm.timeStepParams.toProto)
      builder.build()
    }
  }

  implicit class ScalarFunctionMapperFromProtoConverter(sfm: ExecPlans.ScalarFunctionMapper) {
    def fromProto: ScalarFunctionMapper = {
      ScalarFunctionMapper(
        sfm.getFunction.fromProto,
        sfm.getTimeStepParams.fromProto
      )
    }
  }

  // VectorFunctionMapper
  implicit class VectorFunctionMapperToProtoConverter(vfm: VectorFunctionMapper) {
    def toProto: ExecPlans.VectorFunctionMapper = {
      val builder = ExecPlans.VectorFunctionMapper.newBuilder()
      builder.build()
    }
  }

  implicit class VectorFunctionMapperFromProtoConverter(srm: ExecPlans.VectorFunctionMapper) {
    def fromProto: VectorFunctionMapper = {
      VectorFunctionMapper()
    }
  }

  // AggregatePresenter
  implicit class AggregatePresenterToProtoConverter(ap : AggregatePresenter) {
    def toProto: ExecPlans.AggregatePresenter = {
      val builder = ExecPlans.AggregatePresenter.newBuilder()
      builder.setAggrOp(ap.aggrOp.toProto)
      ap.aggrParams.foreach(aggrParam => builder.addAggrParams(getAggregateParameter(aggrParam)))
      builder.setRangeParams(ap.rangeParams.toProto)
      ap.funcParams.foreach(fa => builder.addFuncParams(fa.toProto))
      builder.build()
    }
  }

  implicit class AggregatePresenterFromProtoConverter(ap: ExecPlans.AggregatePresenter) {
    def fromProto: AggregatePresenter = {
      import JavaConverters._
      AggregatePresenter(
        ap.getAggrOp.fromProto,
        ap.getAggrParamsList.asScala.toSeq.map(aggrParam => aggrParam.fromProto()),
        ap.getRangeParams.fromProto,
        ap.getFuncParamsList.asScala.toSeq.map(fp => fp.fromProto)
      )
    }
  }

  // AbsentFunctionMapper
  implicit class AbsentFunctionMapperToProtoConverter(afm: AbsentFunctionMapper) {
    def toProto: ExecPlans.AbsentFunctionMapper = {
      val builder = ExecPlans.AbsentFunctionMapper.newBuilder()
      afm.columnFilter.foreach(cf => builder.addColumnFilter(cf.toProto))
      builder.setRangeParams(afm.rangeParams.toProto)
      builder.setMetricColumn(afm.metricColumn)
      builder.build()
    }
  }

  implicit class AbsentFunctionMapperFromProtoConverter(afm: ExecPlans.AbsentFunctionMapper) {
    def fromProto: AbsentFunctionMapper = {
      import JavaConverters._
      AbsentFunctionMapper(
        afm.getColumnFilterList.asScala.toSeq.map(cf => cf.fromProto),
        afm.getRangeParams.fromProto,
        afm.getMetricColumn
      )
    }
  }


  implicit class LeafExecPlanToProtoConverter(lep: LeafExecPlan) {

    def toProto(): ExecPlans.LeafExecPlan = {
      val builder = ExecPlans.LeafExecPlan.newBuilder()
      builder.setExecPlan(lep.asInstanceOf[filodb.query.exec.ExecPlan].toProto)
      builder.setSubmitTime(lep.submitTime)
      builder.build()
    }
  }


  implicit class NonLeafExecPlanToProtoConverter(mspe: filodb.query.exec.NonLeafExecPlan) {

    //import collection.JavaConverters._

    def toProto(): ExecPlans.NonLeafExecPlan = {
      val builder = ExecPlans.NonLeafExecPlan.newBuilder()
      builder.setExecPlan(mspe.asInstanceOf[filodb.query.exec.ExecPlan].toProto)
      mspe.children.foreach(ep => builder.addChildren(ep.toExecPlanContainerProto))


      builder.build()
    }
  }

  //
  //
  // Non Leaf Plans
  //
  //

  // LabelCardinalityReduceExec
  implicit class LabelCardinalityReduceExecToProtoConverter(lcre : LabelCardinalityReduceExec) {
    def toProto(): ExecPlans.LabelCardinalityReduceExec = {
      val builder = ExecPlans.LabelCardinalityReduceExec.newBuilder()
      builder.setNonLeafExecPlan(lcre.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class LabelCardinalityReduceExecFromProtoConverter(lcre: ExecPlans.LabelCardinalityReduceExec) {
    def fromProto(): LabelCardinalityReduceExec = {
      import JavaConverters._
      val execPlan = lcre.getNonLeafExecPlan().getExecPlan()
      val queryContext: QueryContext = execPlan.getQueryContext().fromProto
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[ExecPlans.ExecPlanContainer] = lcre.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto)

      LabelCardinalityReduceExec(
        queryContext,
        planDispatcher,
        children
      )
    }
  }

  // MultiPartitionDistConcatExec
  implicit class MultiPartitionDistConcatExecToProtoConverter(mpdce: filodb.query.exec.MultiPartitionDistConcatExec) {
    def toProto(): ExecPlans.MultiPartitionDistConcatExec = {
      val builder = ExecPlans.MultiPartitionDistConcatExec.newBuilder()
      builder.setNonLeafExecPlan(mpdce.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class MultiPartitionDistConcatExecFromProtoConverter(mpdce: ExecPlans.MultiPartitionDistConcatExec) {
    def fromProto(): filodb.query.exec.MultiPartitionDistConcatExec = {
      import scala.collection.JavaConverters._
      val execPlan = mpdce.getNonLeafExecPlan().getExecPlan()
      val queryContext: QueryContext = execPlan.getQueryContext().fromProto
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[ExecPlans.ExecPlanContainer] = mpdce.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto)
      filodb.query.exec.MultiPartitionDistConcatExec(queryContext, planDispatcher, children)
    }
  }

  // LocalPartitionDistConcatExec
  implicit class LocalPartitionDistConcatExecToProtoConverter(mspe: filodb.query.exec.LocalPartitionDistConcatExec) {
    def toProto(): ExecPlans.LocalPartitionDistConcatExec = {
      val builder = ExecPlans.LocalPartitionDistConcatExec.newBuilder()
      builder.setNonLeafExecPlan(mspe.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class LocalPartitionDistConcatExecFromProtoConverter(lpdce: ExecPlans.LocalPartitionDistConcatExec) {
    def fromProto(): LocalPartitionDistConcatExec = {
      import scala.collection.JavaConverters._
      val execPlan = lpdce.getNonLeafExecPlan().getExecPlan()
      val queryContext : QueryContext = execPlan.getQueryContext().fromProto
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[ExecPlans.ExecPlanContainer] = lpdce.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto)
      LocalPartitionDistConcatExec(queryContext, planDispatcher, children)
    }
  }

  // SplitLocalPartitionDistConcatExec
  implicit class SplitLocalPartitionDistConcatExecToProtoConverter(
    mspe: filodb.query.exec.SplitLocalPartitionDistConcatExec
  ) {
    def toProto(): ExecPlans.SplitLocalPartitionDistConcatExec = {
      val builder = ExecPlans.SplitLocalPartitionDistConcatExec.newBuilder()
      builder.setNonLeafExecPlan(mspe.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class SplitLocalPartitionDistConcatExecFromProtoConverter(
    slpdce: ExecPlans.SplitLocalPartitionDistConcatExec
  ) {
    def fromProto(): filodb.query.exec.SplitLocalPartitionDistConcatExec = {
      import scala.collection.JavaConverters._
      val execPlan = slpdce.getNonLeafExecPlan().getExecPlan()
      val queryContext: QueryContext = execPlan.getQueryContext().fromProto
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[ExecPlans.ExecPlanContainer] = slpdce.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto)
      SplitLocalPartitionDistConcatExec(queryContext, planDispatcher, children, None)
    }
  }

  // LocalPartitionReduceAggregateExec
  implicit class LocalPartitionReduceAggregateExecToProtoConverter(lpra: LocalPartitionReduceAggregateExec) {
    def toProto(): ExecPlans.LocalPartitionReduceAggregateExec = {
      val builder = ExecPlans.LocalPartitionReduceAggregateExec.newBuilder()
      builder.setNonLeafExecPlan(lpra.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.setAggrOp(lpra.aggrOp.toProto)
      lpra.aggrParams.foreach(ap => builder.addAggrParams(getAggregateParameter(ap)))
      builder.build()
    }
  }

  implicit class LocalPartitionReduceAggregateExecFromProtoConverter(
    lcre: ExecPlans.LocalPartitionReduceAggregateExec
  ) {
    def fromProto(): LocalPartitionReduceAggregateExec = {
      import JavaConverters._
      val execPlan = lcre.getNonLeafExecPlan().getExecPlan()
      val queryContext: QueryContext = execPlan.getQueryContext().fromProto
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[ExecPlans.ExecPlanContainer] = lcre.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto)

      import JavaConverters._
      LocalPartitionReduceAggregateExec(
        queryContext: QueryContext,
        dispatcher: PlanDispatcher,
        children: Seq[ExecPlan],
        lcre.getAggrOp.fromProto,
        lcre.getAggrParamsList.asScala.toSeq.map(ap => ap.fromProto)
      )
    }
  }

  // MultiPartitionReduceAggregateExec
  implicit class MultiPartitionReduceAggregateExecToProtoConverter(mprae: MultiPartitionReduceAggregateExec) {
    def toProto(): ExecPlans.MultiPartitionReduceAggregateExec = {
      val builder = ExecPlans.MultiPartitionReduceAggregateExec.newBuilder()
      builder.setNonLeafExecPlan(mprae.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.setAggrOp(mprae.aggrOp.toProto)
      mprae.aggrParams.foreach(ap => builder.addAggrParams(getAggregateParameter(ap)))
      builder.build()
    }
  }

  implicit class MultiPartitionReduceAggregateExecFromProtoConverter(
    mprae: ExecPlans.MultiPartitionReduceAggregateExec
  ) {
    def fromProto(): MultiPartitionReduceAggregateExec = {
      import JavaConverters._

      val execPlan = mprae.getNonLeafExecPlan().getExecPlan()
      val queryContext: QueryContext = execPlan.getQueryContext().fromProto
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[ExecPlans.ExecPlanContainer] = mprae.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto)

      MultiPartitionReduceAggregateExec(
        queryContext: QueryContext,
        dispatcher: PlanDispatcher,
        children: Seq[ExecPlan],
        mprae.getAggrOp.fromProto,
        mprae.getAggrParamsList.asScala.toSeq.map(ap => ap.fromProto)
      )
    }
  }

  // BinaryJoinExec
  implicit class BinaryJoinExecToProtoConverter(bje: BinaryJoinExec) {
    def toProto(): ExecPlans.BinaryJoinExec = {
      val builder = ExecPlans.BinaryJoinExec.newBuilder()
      builder.setNonLeafExecPlan(bje.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      bje.lhs.foreach(ep => builder.addLhs(ep.toExecPlanContainerProto))
      bje.rhs.foreach(ep => builder.addRhs(ep.toExecPlanContainerProto))
      builder.setBinaryOp(bje.binaryOp.toProto)
      builder.setCardinality(bje.cardinality.toProto)
      builder.build()
      bje.on.foreach(on => builder.addOn(on))
      bje.ignoring.foreach(ignoring => builder.addIgnoring(ignoring))
      bje.include.foreach(include => builder.addIgnoring(include))
      builder.setMetricColumn(bje.metricColumn)
      bje.outputRvRange.foreach(orr => builder.setOutputRvRange(orr.toProto))
      builder.build()
    }
  }

  implicit class BinaryJoinExecFromProtoConverter(bje: ExecPlans.BinaryJoinExec) {
    def fromProto(): BinaryJoinExec = {
      import JavaConverters._
      val execPlan = bje.getNonLeafExecPlan().getExecPlan()
      val queryContext: QueryContext = execPlan.getQueryContext().fromProto
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val lhs: Seq[filodb.query.exec.ExecPlan] =
        bje.getLhsList.asScala.toSeq.map(c => c.fromProto)
      val rhs: Seq[filodb.query.exec.ExecPlan] =
        bje.getRhsList.asScala.toSeq.map(c => c.fromProto)
      val on = bje.getOnList.asScala.toSeq
      val ignoring = bje.getIgnoringList.asScala.toSeq
      val include = bje.getIncludeList.asScala.toSeq
      val outputRvRange =
        if (bje.hasOutputRvRange) Option(bje.getOutputRvRange.fromProto) else None

      BinaryJoinExec(
        queryContext: QueryContext,
        dispatcher: PlanDispatcher,
        lhs: Seq[ExecPlan],
        rhs: Seq[ExecPlan],
        bje.getBinaryOp.fromProto,
        bje.getCardinality.fromProto,
        on,
        ignoring,
        include,
        bje.getMetricColumn,
        outputRvRange: Option[RvRange]
      )
    }
  }

  // TsCardReduceExec
  implicit class TsCardReduceExecToProtoConverter(tcre: TsCardReduceExec) {
    def toProto(): ExecPlans.TsCardReduceExec = {
      val builder = ExecPlans.TsCardReduceExec.newBuilder()
      builder.setNonLeafExecPlan(tcre.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class TsCardReduceExecFromProtoConverter(tcre: ExecPlans.TsCardReduceExec) {
    def fromProto(): TsCardReduceExec = {
      import JavaConverters._
      val execPlan = tcre.getNonLeafExecPlan().getExecPlan()
      val queryContext: QueryContext = execPlan.getQueryContext().fromProto
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[ExecPlans.ExecPlanContainer] = tcre.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto)
      TsCardReduceExec(
        queryContext,
        dispatcher,
        children
      )
    }
  }

  // StitchRvsExec
  implicit class StitchRvsExecToProtoConverter(sre: StitchRvsExec) {
    def toProto(): ExecPlans.StitchRvsExec = {
      val builder = ExecPlans.StitchRvsExec.newBuilder()
      builder.setNonLeafExecPlan(sre.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      sre.outputRvRange.foreach(orr => builder.setOutputRvRange(orr.toProto))
      builder.build()
    }
  }

  implicit class StitchRvsExecFromProtoConverter(sre: ExecPlans.StitchRvsExec) {
    def fromProto(): StitchRvsExec = {
      import JavaConverters._
      val execPlan = sre.getNonLeafExecPlan().getExecPlan()
      val queryContext: QueryContext = execPlan.getQueryContext().fromProto
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[ExecPlans.ExecPlanContainer] = sre.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto)
      val outputRvRange =
        if (sre.hasOutputRvRange) Option(sre.getOutputRvRange.fromProto) else None
      StitchRvsExec(
        queryContext,
        dispatcher,
        outputRvRange,
        children
      )
    }
  }

  // SetOperatorExec
  implicit class SetOperatorExecToProtoConverter(soe: SetOperatorExec) {
    def toProto(): ExecPlans.SetOperatorExec = {
      val builder = ExecPlans.SetOperatorExec.newBuilder()
      builder.setNonLeafExecPlan(soe.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      soe.lhs.foreach(ep => builder.addLhs(ep.toExecPlanContainerProto))
      soe.rhs.foreach(ep => builder.addRhs(ep.toExecPlanContainerProto))
      builder.setBinaryOp(soe.binaryOp.toProto)
      builder.build()
      soe.on.foreach(on => builder.addOn(on))
      soe.ignoring.foreach(ignoring => builder.addIgnoring(ignoring))
      builder.setMetricColumn(soe.metricColumn)
      soe.outputRvRange.foreach(orr => builder.setOutputRvRange(orr.toProto))

      builder.build()
    }
  }

  implicit class SetOperatorExecFromProtoConverter(soe: ExecPlans.SetOperatorExec) {
    def fromProto(): SetOperatorExec = {
      import JavaConverters._
      val execPlan = soe.getNonLeafExecPlan().getExecPlan()
      val queryContext: QueryContext = execPlan.getQueryContext().fromProto
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[ExecPlans.ExecPlanContainer] = soe.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto)
      val lhs: Seq[filodb.query.exec.ExecPlan] =
        soe.getLhsList.asScala.toSeq.map(c => c.fromProto)
      val rhs: Seq[filodb.query.exec.ExecPlan] =
        soe.getRhsList.asScala.toSeq.map(c => c.fromProto)
      val on = soe.getOnList.asScala.toSeq
      val ignoring = soe.getIgnoringList.asScala.toSeq
      val outputRvRange =
        if (soe.hasOutputRvRange) Option(soe.getOutputRvRange.fromProto) else None


      SetOperatorExec(
        queryContext: QueryContext,
        dispatcher: PlanDispatcher,
        lhs: Seq[ExecPlan],
        rhs: Seq[ExecPlan],
        soe.getBinaryOp.fromProto,
        on: Seq[String],
        ignoring: Seq[String],
        soe.getMetricColumn,
        outputRvRange: Option[RvRange]
      )
    }
  }

  // LabelValuesDistConcatExec
  implicit class LabelValuesDistConcatExecToProtoConverter(lvdce: LabelValuesDistConcatExec) {
    def toProto(): ExecPlans.LabelValuesDistConcatExec = {
      val builder = ExecPlans.LabelValuesDistConcatExec.newBuilder()
      builder.setNonLeafExecPlan(lvdce.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class LabelValuesDistConcatExecFromProtoConverter(lvdce: ExecPlans.LabelValuesDistConcatExec) {
    def fromProto(): LabelValuesDistConcatExec = {
      import JavaConverters._
      val execPlan = lvdce.getNonLeafExecPlan().getExecPlan()
      val queryContext: QueryContext = execPlan.getQueryContext().fromProto
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[ExecPlans.ExecPlanContainer] = lvdce.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto)
      LabelValuesDistConcatExec(
        queryContext,
        dispatcher,
        children
      )
    }
  }

  // PartKeysDistConcatExec
  implicit class PartKeysDistConcatExecToProtoConverter(pkdce: PartKeysDistConcatExec) {
    def toProto(): ExecPlans.PartKeysDistConcatExec = {
      val builder = ExecPlans.PartKeysDistConcatExec.newBuilder()
      builder.setNonLeafExecPlan(pkdce.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class PartKeysDistConcatExecFromProtoConverter(pkdce: ExecPlans.PartKeysDistConcatExec) {
    def fromProto(): PartKeysDistConcatExec = {
      import JavaConverters._
      val execPlan = pkdce.getNonLeafExecPlan().getExecPlan()
      val queryContext: QueryContext = execPlan.getQueryContext().fromProto
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[ExecPlans.ExecPlanContainer] = pkdce.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto)
      PartKeysDistConcatExec(
        queryContext,
        dispatcher,
        children
      )
    }
  }

  // LabelNamesDistConcatExec
  implicit class LabelNamesDistConcatExecToProtoConverter(lndce: LabelNamesDistConcatExec) {
    def toProto(): ExecPlans.LabelNamesDistConcatExec = {
      val builder = ExecPlans.LabelNamesDistConcatExec.newBuilder()
      builder.setNonLeafExecPlan(lndce.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class LabelNamesDistConcatExecFromProtoConverter(lndce: ExecPlans.LabelNamesDistConcatExec) {
    def fromProto(): LabelNamesDistConcatExec = {
      import JavaConverters._
      val execPlan = lndce.getNonLeafExecPlan().getExecPlan()
      val queryContext: QueryContext = execPlan.getQueryContext().fromProto
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[ExecPlans.ExecPlanContainer] = lndce.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto)
      LabelNamesDistConcatExec(
        queryContext: QueryContext,
        dispatcher: PlanDispatcher,
        children: Seq[ExecPlan])
    }
  }

  //
  //
  // Leaf Plans
  //
  //

  // LabelNamesExec
  implicit class LabelNamesExecToProtoConverter(lne: filodb.query.exec.LabelNamesExec) {
    def toProto(): ExecPlans.LabelNamesExec = {
      val builder = ExecPlans.LabelNamesExec.newBuilder()
      builder.setLeafExecPlan(lne.asInstanceOf[filodb.query.exec.LeafExecPlan].toProto)
      builder.setShard(lne.shard)
      lne.filters.foreach(f => builder.addFilters(f.toProto))
      builder.setStartMs(lne.startMs)
      builder.setEndMs(lne.endMs)
      builder.build()
    }
  }

  implicit class LabelNamesExecFromProtoConverter(lne: ExecPlans.LabelNamesExec) {
    def fromProto(): filodb.query.exec.LabelNamesExec = {
      import scala.collection.JavaConverters._
      val execPlan = lne.getLeafExecPlan().getExecPlan()
      val queryContext: QueryContext = execPlan.getQueryContext().fromProto
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val filters = lne.getFiltersList.asScala.toSeq.map(
        f => f.fromProto
      )
      val datasetRef = execPlan.getQueryCommand.getDatasetRef.fromProto()
      filodb.query.exec.LabelNamesExec(
        queryContext,
        planDispatcher,
        datasetRef,
        lne.getShard,
        filters,
        lne.getStartMs,
        lne.getEndMs
      )
    }
  }

  // EmptyResultExec
  implicit class EmptyResultExecToProtoConverter(lne: filodb.query.exec.EmptyResultExec) {
    def toProto(): ExecPlans.EmptyResultExec = {
      val builder = ExecPlans.EmptyResultExec.newBuilder()
      builder.setLeafExecPlan(lne.asInstanceOf[filodb.query.exec.LeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class EmptyResultExecFromProtoConverter(lne: ExecPlans.EmptyResultExec) {
    def fromProto(): filodb.query.exec.EmptyResultExec = {
      val execPlan = lne.getLeafExecPlan().getExecPlan()
      val queryContext: QueryContext = execPlan.getQueryContext().fromProto
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val datasetRef = execPlan.getQueryCommand.getDatasetRef.fromProto()
      val inProcessPlanDispatcher = planDispatcher match {
        case ippd : InProcessPlanDispatcher => ippd
      }
      filodb.query.exec.EmptyResultExec(
        queryContext,
        datasetRef,
        inProcessPlanDispatcher
      )
    }
  }

  // PartKeysExec
  implicit class PartKeysExecToProtoConverter(pke: filodb.query.exec.PartKeysExec) {
    def toProto(): ExecPlans.PartKeysExec = {
      val builder = ExecPlans.PartKeysExec.newBuilder()
      builder.setLeafExecPlan(pke.asInstanceOf[filodb.query.exec.LeafExecPlan].toProto)
      builder.setShard(pke.shard)
      pke.filters.foreach(f => builder.addFilters(f.toProto))
      builder.setFetchFirstLastSampleTimes(pke.fetchFirstLastSampleTimes)
      builder.setStart(pke.start)
      builder.setEnd(pke.end)
      builder.build()
    }
  }

  implicit class PartKeysExecFromProtoConverter(pke: ExecPlans.PartKeysExec) {
    def fromProto(): filodb.query.exec.PartKeysExec = {
      import scala.collection.JavaConverters._
      val execPlan = pke.getLeafExecPlan().getExecPlan()
      val queryContext: QueryContext = execPlan.getQueryContext().fromProto
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val filters = pke.getFiltersList.asScala.toSeq.map(
        f => f.fromProto
      )
      val datasetRef = execPlan.getQueryCommand.getDatasetRef.fromProto()
      filodb.query.exec.PartKeysExec(
        queryContext,
        planDispatcher,
        datasetRef,
        pke.getShard,
        filters,
        pke.getFetchFirstLastSampleTimes,
        pke.getStart,
        pke.getEnd
      )
    }
  }

  // LabelValuesExec
  implicit class LabelValuesExecToProtoConverter(lve: filodb.query.exec.LabelValuesExec) {
    def toProto(): ExecPlans.LabelValuesExec = {
      val builder = ExecPlans.LabelValuesExec.newBuilder()
      builder.setLeafExecPlan(lve.asInstanceOf[filodb.query.exec.LeafExecPlan].toProto)
      builder.setShard(lve.shard)
      lve.filters.foreach(f => builder.addFilters(f.toProto))
      lve.columns.foreach(c => builder.addColumns(c))
      builder.setStartMs(lve.startMs)
      builder.setEndMs(lve.endMs)
      builder.build()
    }
  }

  implicit class LabelValuesExecFromProtoConverter(lve: ExecPlans.LabelValuesExec) {
    def fromProto(): filodb.query.exec.LabelValuesExec = {
      import scala.collection.JavaConverters._
      val execPlan = lve.getLeafExecPlan().getExecPlan()
      val queryContext: QueryContext = execPlan.getQueryContext().fromProto
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val filters = lve.getFiltersList.asScala.toSeq.map(
        f => f.fromProto
      )
      val columns = lve.getColumnsList.asScala.toSeq
      val datasetRef = execPlan.getQueryCommand.getDatasetRef.fromProto()
      filodb.query.exec.LabelValuesExec(
        queryContext,
        planDispatcher,
        datasetRef,
        lve.getShard,
        filters,
        columns,
        lve.getStartMs,
        lve.getEndMs
      )
    }
  }

  // LabelCardinalityExec
  implicit class LabelCardinalityExecToProtoConverter(lce: filodb.query.exec.LabelCardinalityExec) {
    def toProto(): ExecPlans.LabelCardinalityExec = {
      val builder = ExecPlans.LabelCardinalityExec.newBuilder()
      builder.setLeafExecPlan(lce.asInstanceOf[filodb.query.exec.LeafExecPlan].toProto)
      builder.setShard(lce.shard)
      lce.filters.foreach(f => builder.addFilters(f.toProto))
      builder.setStartMs(lce.startMs)
      builder.setEndMs(lce.endMs)
      builder.build()
    }
  }

  implicit class LabelCardinalityExecFromProtoConverter(lce: ExecPlans.LabelCardinalityExec) {
    def fromProto(): filodb.query.exec.LabelCardinalityExec = {
      import scala.collection.JavaConverters._
      val execPlan = lce.getLeafExecPlan().getExecPlan()
      val queryContext: QueryContext = execPlan.getQueryContext().fromProto
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val filters = lce.getFiltersList.asScala.toSeq.map(
        f => f.fromProto
      )
      val datasetRef = execPlan.getQueryCommand.getDatasetRef.fromProto()
      filodb.query.exec.LabelCardinalityExec(
        queryContext,
        planDispatcher,
        datasetRef,
        lce.getShard,
        filters,
        lce.getStartMs,
        lce.getEndMs
      )
    }
  }

  // SelectChunkInfosExec
  implicit class SelectChunkInfosExecToProtoConverter(scie: SelectChunkInfosExec) {

    //import collection.JavaConverters._

    def toProto(): ExecPlans.SelectChunkInfosExec = {
      val builder = ExecPlans.SelectChunkInfosExec.newBuilder()
      builder.setLeafExecPlan(scie.asInstanceOf[LeafExecPlan].toProto)
      builder.setShard(scie.shard)
      scie.filters.foreach(f => builder.addFilters(f.toProto))
      builder.setChunkScanMethod(scie.chunkMethod.toProto)
      builder.clearSchema()
      scie.schema.foreach(s => builder.setSchema(s))
      builder.clearColName()
      scie.colName.foreach(cn => builder.setColName(cn))
      builder.build()
    }
  }

  implicit class SelectChunkInfosExecFromProtoConverter(scie: ExecPlans.SelectChunkInfosExec) {
    def fromProto: SelectChunkInfosExec = {
      val ep = scie.getLeafExecPlan.getExecPlan
      val queryContext = ep.getQueryContext.fromProto
      val dispatcher = ep.getDispatcher.fromProto
      val datasetRef = scie.getLeafExecPlan.getExecPlan.getQueryCommand.getDatasetRef.fromProto
      import scala.collection.JavaConverters._
      val filters = scie.getFiltersList.asScala.toSeq.map(
        f => f.fromProto
      )
      val chunkMethod = scie.getChunkScanMethod.fromProto
      val schema = if (scie.hasSchema) {
        Some(scie.getSchema)
      } else {
        None
      }
      val colName = if (scie.hasColName) {
        Some(scie.getColName)
      } else {
        None
      }
      SelectChunkInfosExec(
        queryContext, dispatcher, datasetRef,
        scie.getShard,
        filters,
        chunkMethod, schema, colName
      )
    }
  }

  // MultiSchemaPartitionsExec
  implicit class MultiSchemaPartitionsExecToProtoConverter(mspe: MultiSchemaPartitionsExec) {

    //import collection.JavaConverters._

    def toProto(): ExecPlans.MultiSchemaPartitionsExec = {
      val builder = ExecPlans.MultiSchemaPartitionsExec.newBuilder()
      builder.setLeafExecPlan(mspe.asInstanceOf[LeafExecPlan].toProto)
      builder.setShard(mspe.shard)
      mspe.filters.foreach(f => builder.addFilters(f.toProto))
      builder.setChunkMethod(mspe.chunkMethod.toProto)
      builder.setMetricColumn(mspe.metricColumn)
      builder.clearSchema()
      mspe.schema.foreach(s => builder.setSchema(s))
      builder.clearColName()
      mspe.colName.foreach(cn => builder.setColName(cn))
      //builder.set

      builder.build()
    }
  }

  implicit class MultiSchemaPartitionsExecFromProtoConverter(mspe: ExecPlans.MultiSchemaPartitionsExec) {
    def fromProto: MultiSchemaPartitionsExec = {
      val ep = mspe.getLeafExecPlan.getExecPlan
      val queryContext = ep.getQueryContext.fromProto
      val dispatcher = ep.getDispatcher.fromProto
      val datasetRef = mspe.getLeafExecPlan.getExecPlan.getQueryCommand.getDatasetRef.fromProto
      import scala.collection.JavaConverters._
      val filters = mspe.getFiltersList.asScala.toSeq.map(
        f => f.fromProto
      )
      val chunkMethod = mspe.getChunkMethod.fromProto
      val schema = if (mspe.hasSchema) {
        Some(mspe.getSchema)
      } else {
        None
      }
      val colName = if (mspe.hasColName) {
        Some(mspe.getColName)
      } else {
        None
      }
      MultiSchemaPartitionsExec(
        queryContext, dispatcher, datasetRef,
        mspe.getShard,
        filters,
        chunkMethod, mspe.getMetricColumn, schema, colName
      )
    }
  }

  // ScalarBinaryOperationExec
  implicit class ScalarBinaryOperationExecToProtoConverter(sboe: ScalarBinaryOperationExec) {

    //import collection.JavaConverters._

    def toProto(): ExecPlans.ScalarBinaryOperationExec = {
      val builder = ExecPlans.ScalarBinaryOperationExec.newBuilder()
      builder.setLeafExecPlan(sboe.asInstanceOf[LeafExecPlan].toProto)
      builder.setParams(sboe.params.toProto)
      sboe.lhs match {
        case d : Left[Double, ScalarBinaryOperationExec] => builder.setDoubleValueLhs(d.value)
        case sboe : Right[Double, ScalarBinaryOperationExec] => {
          builder.setScalarBinaryOperationExecLhs(sboe.value.toProto)
        }
      }
      sboe.rhs match {
        case d: Left[Double, ScalarBinaryOperationExec] => builder.setDoubleValueRhs(d.value)
        case sboe: Right[Double, ScalarBinaryOperationExec] => {
          builder.setScalarBinaryOperationExecRhs(sboe.value.toProto)
        }
      }
      builder.setOperator(sboe.operator.toProto)
      builder.build()
    }
  }

  implicit class ScalarBinaryOperationExecFromProtoConverter(sboe: ExecPlans.ScalarBinaryOperationExec) {
    def fromProto: ScalarBinaryOperationExec = {
      val ep = sboe.getLeafExecPlan.getExecPlan
      val queryContext = ep.getQueryContext.fromProto
      val dispatcher = ep.getDispatcher.fromProto
      val datasetRef = sboe.getLeafExecPlan.getExecPlan.getQueryCommand.getDatasetRef.fromProto
      val lhs = sboe.getLhsCase match {
        case ExecPlans.ScalarBinaryOperationExec.LhsCase.DOUBLEVALUELHS => Left(sboe.getDoubleValueLhs)
        case ExecPlans.ScalarBinaryOperationExec.LhsCase.SCALARBINARYOPERATIONEXECLHS=>
          Right(sboe.getScalarBinaryOperationExecLhs.fromProto)
        case ExecPlans.ScalarBinaryOperationExec.LhsCase.LHS_NOT_SET =>
          throw new IllegalArgumentException("invalid lhs")
      }
      val rhs = sboe.getRhsCase match {
        case ExecPlans.ScalarBinaryOperationExec.RhsCase.DOUBLEVALUERHS => Left(sboe.getDoubleValueRhs)
        case ExecPlans.ScalarBinaryOperationExec.RhsCase.SCALARBINARYOPERATIONEXECRHS =>
          Right(sboe.getScalarBinaryOperationExecRhs.fromProto)
        case ExecPlans.ScalarBinaryOperationExec.RhsCase.RHS_NOT_SET =>
          throw new IllegalArgumentException("invalid rhs")
      }
      val inProcessPlanDispatcher = dispatcher match {
        case ippd: InProcessPlanDispatcher => ippd
      }
      ScalarBinaryOperationExec(
        queryContext,
        datasetRef,
        sboe.getParams.fromProto,
        lhs: Either[Double, ScalarBinaryOperationExec],
        rhs: Either[Double, ScalarBinaryOperationExec],
        sboe.getOperator.fromProto,
        inProcessPlanDispatcher
      )
    }
  }

  // ScalarFixedDoubleExec
  implicit class ScalarFixedDoubleExecToProtoConverter(sfde: ScalarFixedDoubleExec) {
    def toProto(): ExecPlans.ScalarFixedDoubleExec = {
      val builder = ExecPlans.ScalarFixedDoubleExec.newBuilder()
      builder.setLeafExecPlan(sfde.asInstanceOf[LeafExecPlan].toProto)
      builder.setParams(sfde.params.toProto)
      builder.setValue(sfde.value)
      builder.build()
    }
  }

  implicit class ScalarFixedDoubleExecFromProtoConverter(sfde: ExecPlans.ScalarFixedDoubleExec) {
    def fromProto: ScalarFixedDoubleExec = {
      val ep = sfde.getLeafExecPlan.getExecPlan
      val queryContext = ep.getQueryContext.fromProto
      val dispatcher = ep.getDispatcher.fromProto
      val datasetRef = sfde.getLeafExecPlan.getExecPlan.getQueryCommand.getDatasetRef.fromProto
      val inProcessPlanDispatcher = dispatcher match {
        case ippd: InProcessPlanDispatcher => ippd
      }
      ScalarFixedDoubleExec(
        queryContext,
        datasetRef,
        sfde.getParams.fromProto,
        sfde.getValue,
        inProcessPlanDispatcher
      )
    }
  }

  // TsCardExec
  implicit class TsCardExecToProtoConverter(tce: TsCardExec) {
    def toProto(): ExecPlans.TsCardExec = {
      val builder = ExecPlans.TsCardExec.newBuilder()
      builder.setLeafExecPlan(tce.asInstanceOf[LeafExecPlan].toProto)
      builder.setShard(tce.shard)
      tce.shardKeyPrefix.foreach(f => builder.addShardKeyPrefix(f))
      builder.setNumGroupByFields(tce.numGroupByFields)
      builder.setClusterName(tce.clusterName)
      builder.setVersion(tce.version)
      builder.build()
    }
  }

  implicit class TsCardExecFromProtoConverter(tce: ExecPlans.TsCardExec) {
    def fromProto: TsCardExec = {
      val ep = tce.getLeafExecPlan.getExecPlan
      val queryContext = ep.getQueryContext.fromProto
      val dispatcher = ep.getDispatcher.fromProto
      val datasetRef = tce.getLeafExecPlan.getExecPlan.getQueryCommand.getDatasetRef.fromProto
      import scala.collection.JavaConverters._
      val shardKeyPrefix = tce.getShardKeyPrefixList.asScala.toSeq
      val numGroupByFields = tce.getNumGroupByFields
      TsCardExec(
        queryContext, dispatcher, datasetRef,
        tce.getShard,
        shardKeyPrefix,
        tce.getNumGroupByFields,
        tce.getClusterName,
        tce.getVersion
      )
    }
  }

  // TimeScalarGeneratorExec
  implicit class TimeScalarGeneratorExecProtoConverter(tsge: TimeScalarGeneratorExec) {
    def toProto(): ExecPlans.TimeScalarGeneratorExec = {
      val builder = ExecPlans.TimeScalarGeneratorExec.newBuilder()
      builder.setLeafExecPlan(tsge.asInstanceOf[LeafExecPlan].toProto)
      builder.setParams(tsge.params.toProto)
      builder.setFunction(tsge.function.toProto)
      builder.build()
    }
  }

  implicit class TimeScalarGeneratorExecFromProtoConverter(tsge: ExecPlans.TimeScalarGeneratorExec) {
    def fromProto: TimeScalarGeneratorExec = {
      val ep = tsge.getLeafExecPlan.getExecPlan
      val queryContext = ep.getQueryContext.fromProto
      val dispatcher = ep.getDispatcher.fromProto
      val datasetRef = tsge.getLeafExecPlan.getExecPlan.getQueryCommand.getDatasetRef.fromProto
      val inProcessPlanDispatcher = dispatcher match {
        case ippd: InProcessPlanDispatcher => ippd
      }
      TimeScalarGeneratorExec(
        queryContext,
        datasetRef,
        tsge.getParams.fromProto,
        tsge.getFunction.fromProto,
        inProcessPlanDispatcher
      )
    }
  }

  // SelectRawPartitionsExec
  implicit class SelectRawPartitionsExecToProtoConverter(srp: SelectRawPartitionsExec) {

    def toProto(): ExecPlans.SelectRawPartitionsExec = {
      val builder = ExecPlans.SelectRawPartitionsExec.newBuilder()
      builder.setLeafExecPlan(srp.asInstanceOf[LeafExecPlan].toProto)
      srp.dataSchema.map(s => builder.setDataSchema(s.toProto))
      srp.lookupRes.foreach(lr => builder.setLookupRes(lr.toProto))
      builder.setFilterSchemas(srp.filterSchemas)
      srp.colIds.foreach(colId => builder.addColIds(Integer.valueOf(colId)))
      builder.setPlanId(srp.planId)
      builder.build()
    }
  }

  implicit class SelectRawPartitionsExecFromProtoConverter(srpe: ExecPlans.SelectRawPartitionsExec) {
    def fromProto: SelectRawPartitionsExec = {
      import JavaConverters._
      val ep = srpe.getLeafExecPlan.getExecPlan
      val dataSchema = if (srpe.hasDataSchema) Option(srpe.getDataSchema.fromProto) else None
      val lookupRes = if (srpe.hasLookupRes) Option(srpe.getLookupRes.fromProto) else None
      val colIds = srpe.getColIdsList.asScala.map(intgr => intgr.intValue())
      val leafExecPlan = srpe.getLeafExecPlan
      val execPlan = leafExecPlan.getExecPlan
      val queryContext = execPlan.getQueryContext.fromProto
      val dispatcher = execPlan.getDispatcher.fromProto
      val datasetRef = leafExecPlan.getExecPlan.getQueryCommand.getDatasetRef.fromProto
      SelectRawPartitionsExec(
        queryContext, dispatcher, datasetRef,
        dataSchema,
        lookupRes,
        srpe.getFilterSchemas,
        colIds,
        srpe.getPlanId
      )
    }
  }


  implicit class ExecPlanToProtoConverter(ep: filodb.query.exec.ExecPlan) {
    def toProto: ExecPlans.ExecPlan = {
      val builder = ExecPlans.ExecPlan.newBuilder()
      builder.setPlanId(ep.planId)
      builder.setQueryContext(ep.queryContext.toProto)
      builder.setEnforceSampleLimit(ep.enforceSampleLimit)
      builder.setQueryCommand(ep.asInstanceOf[QueryCommand].toProto)
      builder.clearDispatcher()
      builder.setDispatcher(ep.dispatcher.toPlanDispatcherContainer)

      ep.rangeVectorTransformers.foreach(t => {
        val rvtcBuilder = RangeVectorTransformerContainer.newBuilder()
        t match {
          case srm: StitchRvsMapper => {
            builder.addRangeVectorTransformers(rvtcBuilder.setStitchRvsMapper(srm.toProto).build())
          }
          case _ => throw new IllegalArgumentException("Unexpected Range Vector Transformer")
        }
      })
      builder.build()
    }

    // scalastyle:off cyclomatic.complexity
    def toExecPlanContainerProto() : ExecPlans.ExecPlanContainer = {
      val b = ExecPlans.ExecPlanContainer.newBuilder()
      ep match {
        // non leaf plans
        case lcre: LabelCardinalityReduceExec => b.setLabelCardinalityReduceExec(lcre.toProto)
        case mpdce: MultiPartitionDistConcatExec => b.setMultiPartitionDistConcatExec(mpdce.toProto)
        case lpdce: LocalPartitionDistConcatExec => b.setLocalPartitionDistConcatExec(lpdce.toProto)
        case slpdce: SplitLocalPartitionDistConcatExec => b.setSplitLocalPartitionDistConcatExec(slpdce.toProto)
        case lprae: LocalPartitionReduceAggregateExec => b.setLocalPartitionReduceAggregateExec(lprae.toProto())
        case mprae: MultiPartitionReduceAggregateExec => b.setMultiPartitionReduceAggregateExec(mprae.toProto())
        case bje: BinaryJoinExec => b.setBinaryJoinExec(bje.toProto)
        case tcre: TsCardReduceExec => b.setTsCardReduceExec(tcre.toProto)
        case sre: StitchRvsExec => b.setStitchRvsExec(sre.toProto)
        case soe: SetOperatorExec => b.setSetOperatorExec(soe.toProto)
        case lvd: LabelValuesDistConcatExec => b.setLabelValuesDistConcatExec(lvd.toProto)
        case pkdce: PartKeysDistConcatExec => b.setPartKeysDistConcatExec(pkdce.toProto)
        case lndce: LabelNamesDistConcatExec => b.setLabelNamesDistConcatExec(lndce.toProto)
        // leaf plans
        case lne: LabelNamesExec => b.setLabelNamesExec(lne.toProto)
        case ers: EmptyResultExec => b.setEmptyResultExec(ers.toProto)
        case pke: PartKeysExec => b.setPartKeysExec(pke.toProto)
        case lve: LabelValuesExec => b.setLabelValuesExec(lve.toProto)
        case lce: LabelCardinalityExec => b.setLabelCardinalityExec(lce.toProto)
        case scie: SelectChunkInfosExec => b.setSelectChunkInfosExec(scie.toProto)
        case mspe: MultiSchemaPartitionsExec => b.setMultiSchemaPartitionsExec(mspe.toProto())
        case sboe: ScalarBinaryOperationExec => b.setScalarBinaryOperatinExec(sboe.toProto)
        case sfde: ScalarFixedDoubleExec => b.setScalarFixedDoubleExec(sfde.toProto)
        case tce: TsCardExec => b.setTsCardExec(tce.toProto)
        case tsge: TimeScalarGeneratorExec => b.setTimeScalarGeneratorExec(tsge.toProto)
        case srpe: SelectRawPartitionsExec => b.setSelectRawPartitionsExec(srpe.toProto)
        //case _ => throw new IllegalArgumentException(s"Unknown execution plan ${ep.getClass.getName}")
      }
      b.build()
    }
    // scalastyle:on cyclomatic.complexity
  }

  implicit class PlanDispatcherContainerFromProto(pdc: ExecPlans.PlanDispatcherContainer) {
    def fromProto: filodb.query.exec.PlanDispatcher = {
      val dispatcherCase = pdc.getDispatcherCase
      val dispatcher = dispatcherCase match {
        case ExecPlans.PlanDispatcherContainer.DispatcherCase.ACTORPLANDISPATCHER =>
          pdc.getActorPlanDispatcher.fromProto
        case ExecPlans.PlanDispatcherContainer.DispatcherCase.INPROCESSPLANDISPATCHER =>
          pdc.getInProcessPlanDispatcher.fromProto
        case ExecPlans.PlanDispatcherContainer.DispatcherCase.DISPATCHER_NOT_SET =>
          throw new IllegalArgumentException("Invalid PlanDispatcherContainer")
      }
      dispatcher
    }
  }

  implicit class ExecPlanContainerFromProtoConverter(epc: ExecPlans.ExecPlanContainer) {
    // scalastyle:off cyclomatic.complexity
    def fromProto(): filodb.query.exec.ExecPlan = {
      val plan: filodb.query.exec.ExecPlan = epc.getExecPlanCase match {
        // non leaf plans
        case ExecPlanCase.LABELCARDINALITYREDUCEEXEC => epc.getLabelCardinalityReduceExec.fromProto
        case ExecPlanCase.MULTIPARTITIONDISTCONCATEXEC => epc.getMultiPartitionDistConcatExec.fromProto
        case ExecPlanCase.LOCALPARTITIONDISTCONCATEXEC => epc.getLocalPartitionDistConcatExec.fromProto
        case ExecPlanCase.SPLITLOCALPARTITIONDISTCONCATEXEC => epc.getSplitLocalPartitionDistConcatExec.fromProto
        case ExecPlanCase.LOCALPARTITIONREDUCEAGGREGATEEXEC => epc.getLocalPartitionReduceAggregateExec.fromProto
        case ExecPlanCase.MULTIPARTITIONREDUCEAGGREGATEEXEC => epc.getMultiPartitionReduceAggregateExec.fromProto
        case ExecPlanCase.BINARYJOINEXEC => epc.getBinaryJoinExec.fromProto
        case ExecPlanCase.TSCARDREDUCEEXEC => epc.getTsCardReduceExec.fromProto
        case ExecPlanCase.STITCHRVSEXEC => epc.getStitchRvsExec.fromProto
        case ExecPlanCase.SETOPERATOREXEC => epc.getSetOperatorExec.fromProto
        case ExecPlanCase.LABELVALUESDISTCONCATEXEC => epc.getLabelValuesDistConcatExec.fromProto
        case ExecPlanCase.PARTKEYSDISTCONCATEXEC => epc.getPartKeysDistConcatExec.fromProto
        case ExecPlanCase.LABELNAMESDISTCONCATEXEC => epc.getLabelNamesDistConcatExec.fromProto
        // leaf plans
        case ExecPlanCase.LABELNAMESEXEC => epc.getLabelNamesExec.fromProto
        case ExecPlanCase.EMPTYRESULTEXEC => epc.getEmptyResultExec.fromProto
        case ExecPlanCase.PARTKEYSEXEC => epc.getPartKeysExec.fromProto
        case ExecPlanCase.LABELVALUESEXEC => epc.getLabelValuesExec.fromProto
        case ExecPlanCase.LABELCARDINALITYEXEC => epc.getLabelCardinalityExec.fromProto
        case ExecPlanCase.SELECTCHUNKINFOSEXEC => epc.getSelectChunkInfosExec.fromProto
        case ExecPlanCase.MULTISCHEMAPARTITIONSEXEC => epc.getMultiSchemaPartitionsExec.fromProto
        case ExecPlanCase.SCALARBINARYOPERATINEXEC => epc.getScalarBinaryOperatinExec.fromProto
        case ExecPlanCase.SCALARFIXEDDOUBLEEXEC => epc.getScalarFixedDoubleExec.fromProto
        case ExecPlanCase.TSCARDEXEC => epc.getTsCardExec.fromProto
        case ExecPlanCase.TIMESCALARGENERATOREXEC => epc.getTimeScalarGeneratorExec.fromProto
        case ExecPlanCase.SELECTRAWPARTITIONSEXEC => epc.getSelectRawPartitionsExec.fromProto
        case ExecPlanCase.EXECPLAN_NOT_SET =>
          throw new RuntimeException("Received Proto Execution Plan with null value")
      }
      plan
    }
    // scalastyle:on cyclomatic.complexity
  }



}





// scalastyle:on number.of.methods
// scalastyle:on number.of.types
// scalastyle:on file.size.limit
