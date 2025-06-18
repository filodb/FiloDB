package filodb.coordinator

import akka.serialization.SerializationExtension
import com.google.protobuf.ByteString
import com.typesafe.config.ConfigFactory
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

import filodb.core.downsample.{CounterDownsamplePeriodMarker, TimeDownsamplePeriodMarker}
import filodb.core.memstore.PartLookupResult
import filodb.core.metadata.{ComputedColumn, DataColumn}
import filodb.core.query._
import filodb.core.store.{AllChunkScan, InMemoryChunkScan, TimeRangeChunkScan, WriteBufferChunkScan}
import filodb.grpc.GrpcMultiPartitionQueryService.{RangeVectorTransformerContainer, RemoteExecPlan}
import filodb.grpc.GrpcMultiPartitionQueryService
import filodb.grpc.GrpcMultiPartitionQueryService.ExecPlanContainer.ExecPlanCase
import filodb.grpc.GrpcMultiPartitionQueryService.FuncArgs.FuncArgTypeCase
import filodb.query.{AggregationOperator, QueryCommand}
import filodb.query.exec._

// scalastyle:off number.of.methods
// scalastyle:off number.of.types
// scalastyle:off file.size.limit
// scalastyle:off method.length
// scalastyle:off line.size.limit
object ProtoConverters {

  import filodb.query.ProtoConverters._

  def execPlanToProto(execPlan: ExecPlan): RemoteExecPlan = {
    val plan = execPlan.toExecPlanContainerProto()
    val context = execPlan.queryContext.toProto
    val remoteExecPlan = RemoteExecPlan.newBuilder()
      .setExecPlan(plan)
      .setQueryContext(context)
      .build()
    remoteExecPlan
  }

  implicit class QueryConfigToProtoConverter(qc: QueryConfig) {

    def toProto: GrpcMultiPartitionQueryService.QueryConfig = {
      val builder = GrpcMultiPartitionQueryService.QueryConfig.newBuilder()
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

  implicit class QueryConfigFromProtoConverter(qc: GrpcMultiPartitionQueryService.QueryConfig) {
    def fromProto: QueryConfig = {
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
    def toProto: GrpcMultiPartitionQueryService.ChunkScanMethod = {
      val builder = GrpcMultiPartitionQueryService.ChunkScanMethod.newBuilder()
      builder.setStartTime(csm.startTime)
      builder.setEndTime(csm.endTime)
      csm match {
        case trcs: TimeRangeChunkScan => builder.setMethod(
          filodb.grpc.GrpcMultiPartitionQueryService.ChunkScanType.TIME_RANGE_CHUNK_SCAN
        )
        case acs: filodb.core.store.AllChunkScan.type => builder.setMethod(
          filodb.grpc.GrpcMultiPartitionQueryService.ChunkScanType.ALL_CHUNKS_SCAN
        )
        case imcs: filodb.core.store.InMemoryChunkScan.type => builder.setMethod(
          filodb.grpc.GrpcMultiPartitionQueryService.ChunkScanType.IN_MEMORY_CHUNK_SCAN
        )
        case wbcs: filodb.core.store.WriteBufferChunkScan.type => builder.setMethod(
          filodb.grpc.GrpcMultiPartitionQueryService.ChunkScanType.WRITE_BUFFER_CHUNK_SCAN
        )
      }
      builder.build()
    }
  }

  implicit class ChunkScanMethodFromProtoConverter(cs: GrpcMultiPartitionQueryService.ChunkScanMethod) {
    def fromProto: filodb.core.store.ChunkScanMethod = {
     cs.getMethod match {
       case GrpcMultiPartitionQueryService.ChunkScanType.IN_MEMORY_CHUNK_SCAN =>
         InMemoryChunkScan
       case GrpcMultiPartitionQueryService.ChunkScanType.ALL_CHUNKS_SCAN =>
         AllChunkScan
       case GrpcMultiPartitionQueryService.ChunkScanType.WRITE_BUFFER_CHUNK_SCAN =>
         WriteBufferChunkScan
       case GrpcMultiPartitionQueryService.ChunkScanType.TIME_RANGE_CHUNK_SCAN =>
         TimeRangeChunkScan(cs.getStartTime, cs.getEndTime)
       case _ => throw new IllegalArgumentException(s"Unexpected ChunkScanMethod ${cs.getMethod}")
     }
    }
  }

  // TypedFieldExtractor
  implicit class TypedFieldExtractorToProtoConverter(
    tfe: filodb.memory.format.RowReader.TypedFieldExtractor[_]
  ) {
    def toProto: GrpcMultiPartitionQueryService.TypedFieldExtractor = {
      tfe match {
        case filodb.memory.format.RowReader.IntFieldExtractor => GrpcMultiPartitionQueryService.TypedFieldExtractor.INT
        case filodb.memory.format.RowReader.FloatFieldExtractor => GrpcMultiPartitionQueryService.TypedFieldExtractor.FLOAT
        case filodb.memory.format.RowReader.HistogramExtractor => GrpcMultiPartitionQueryService.TypedFieldExtractor.HISTOGRAM
        case filodb.memory.format.RowReader.DateTimeFieldExtractor => GrpcMultiPartitionQueryService.TypedFieldExtractor.DATE_TIME
        case filodb.memory.format.RowReader.DoubleFieldExtractor => GrpcMultiPartitionQueryService.TypedFieldExtractor.DOUBLE
        case filodb.memory.format.RowReader.TimestampFieldExtractor => GrpcMultiPartitionQueryService.TypedFieldExtractor.TIMESTAMP
        case filodb.memory.format.RowReader.UTF8StringFieldExtractor => GrpcMultiPartitionQueryService.TypedFieldExtractor.UTF8_STRING
        case filodb.memory.format.RowReader.LongFieldExtractor => GrpcMultiPartitionQueryService.TypedFieldExtractor.LONG
        case filodb.memory.format.RowReader.BooleanFieldExtractor => GrpcMultiPartitionQueryService.TypedFieldExtractor.BOOLEAN
        case filodb.memory.format.RowReader.StringFieldExtractor => GrpcMultiPartitionQueryService.TypedFieldExtractor.STRING
        case filodb.memory.format.RowReader.ObjectFieldExtractor(_) =>
          throw new IllegalArgumentException("Cannot serialize object extractors")
        case we: filodb.memory.format.RowReader.WrappedExtractor[_, _] =>
          throw new IllegalArgumentException("Cannot serialize wrapped extractors")
      }
    }
  }

  implicit class TypedFieldExtractorFromProtoConverter(dc: GrpcMultiPartitionQueryService.TypedFieldExtractor) {
    def fromProto: filodb.memory.format.RowReader.TypedFieldExtractor[_]= {
      dc match {
        case GrpcMultiPartitionQueryService.TypedFieldExtractor.INT => filodb.memory.format.RowReader.IntFieldExtractor
        case GrpcMultiPartitionQueryService.TypedFieldExtractor.FLOAT => filodb.memory.format.RowReader.FloatFieldExtractor
        case GrpcMultiPartitionQueryService.TypedFieldExtractor.HISTOGRAM => filodb.memory.format.RowReader.HistogramExtractor
        case GrpcMultiPartitionQueryService.TypedFieldExtractor.DATE_TIME => filodb.memory.format.RowReader.DateTimeFieldExtractor
        case GrpcMultiPartitionQueryService.TypedFieldExtractor.DOUBLE => filodb.memory.format.RowReader.DoubleFieldExtractor
        case GrpcMultiPartitionQueryService.TypedFieldExtractor.TIMESTAMP => filodb.memory.format.RowReader.TimestampFieldExtractor
        case GrpcMultiPartitionQueryService.TypedFieldExtractor.UTF8_STRING => filodb.memory.format.RowReader.UTF8StringFieldExtractor
        case GrpcMultiPartitionQueryService.TypedFieldExtractor.LONG => filodb.memory.format.RowReader.LongFieldExtractor
        case GrpcMultiPartitionQueryService.TypedFieldExtractor.BOOLEAN => filodb.memory.format.RowReader.BooleanFieldExtractor
        case GrpcMultiPartitionQueryService.TypedFieldExtractor.STRING => filodb.memory.format.RowReader.StringFieldExtractor
        case GrpcMultiPartitionQueryService.TypedFieldExtractor.OBJECT =>
          throw new IllegalArgumentException("Cannot deserialize ObjectExtractor")
        case GrpcMultiPartitionQueryService.TypedFieldExtractor.WRAPPED =>
          throw new IllegalArgumentException("Cannot deserialize WrappedExtractor")
        case GrpcMultiPartitionQueryService.TypedFieldExtractor.UNRECOGNIZED =>
          throw new IllegalArgumentException("Unrecognized Extractor")
      }
    }
  }

  // ComputedColumn
  implicit class ComputedColumnToProtoConverter(cc: filodb.core.metadata.ComputedColumn) {
    def toProto: GrpcMultiPartitionQueryService.ComputedColumn = {

      val builder = GrpcMultiPartitionQueryService.ComputedColumn.newBuilder()
      builder.setId(cc.id)
      builder.setExpr(cc.expr)
      builder.setDataset(cc.dataset)
      builder.setColumnType(cc.columnType.toProto)
      cc.sourceIndices.foreach( i => builder.addSourceIndices(i))
      builder.setExtractor(cc.extractor.toProto)
      builder.build()
    }
  }

  implicit class ComputedColumnFromProtoConverter(cc: GrpcMultiPartitionQueryService.ComputedColumn) {
    def fromProto: filodb.core.metadata.Column = {
      val indicesJava : java.util.List[Integer]= cc.getSourceIndicesList()
      val sourceIndices : Seq[Int]= indicesJava.asScala.toSeq.map(i => i.intValue())
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
    def toProto: GrpcMultiPartitionQueryService.DataColumn = {
      val builder = GrpcMultiPartitionQueryService.DataColumn.newBuilder()
      builder.setId(dc.id)
      builder.setName(dc.name)
      builder.setColumnType(dc.columnType.toProto)
      val params = dc.params.entrySet().stream().forEach( e => {
        builder.putParams(e.getKey, e.getValue.render())
      })
      builder.build()
    }
  }

  implicit class DataColumnFromProtoConverter(dc: GrpcMultiPartitionQueryService.DataColumn) {
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
    def toProto: GrpcMultiPartitionQueryService.ColumnContainer = {
      val builder = GrpcMultiPartitionQueryService.ColumnContainer.newBuilder()
      c match {
        case dc: DataColumn => builder.setDataColumn(dc.toProto)
        case cc: ComputedColumn => builder.setComputedColumn(cc.toProto)
      }
      builder.build()
    }
  }

  implicit class ColumnFromProtoConverter(c: GrpcMultiPartitionQueryService.ColumnContainer) {
    def fromProto: filodb.core.metadata.Column = {
      c.getColumnCase match {
        case GrpcMultiPartitionQueryService.ColumnContainer.ColumnCase.DATACOLUMN =>
            c.getDataColumn.fromProto
        case GrpcMultiPartitionQueryService.ColumnContainer.ColumnCase.COMPUTEDCOLUMN =>
            c.getComputedColumn.fromProto
        case _ => throw new IllegalArgumentException(s"Unexpected Column ${c.getColumnCase}")
      }
    }
  }

  // RepeatedString
  implicit class RepeatedStringToProtoConverter(c: Seq[String]) {
    def toProto: GrpcMultiPartitionQueryService.RepeatedString = {
      val builder = GrpcMultiPartitionQueryService.RepeatedString.newBuilder()
      c.foreach(s => builder.addStrings(s))
      builder.build()
    }
  }

  implicit class RepeatedStringFromProtoConverter(c: GrpcMultiPartitionQueryService.RepeatedString) {
    def fromProto: Seq[String] = {
      c.getStringsList.asScala.toSeq
    }
  }

  // StringTuple
  implicit class StringTupleToProtoConverter(st: (String, String)) {
    def toProto: GrpcMultiPartitionQueryService.StringTuple = {
      val builder = GrpcMultiPartitionQueryService.StringTuple.newBuilder()
      builder.setFieldOne(st._1)
      builder.setFieldTwo(st._2)
      builder.build()
    }
  }

  implicit class StringTupleFromProtoConverter(st: GrpcMultiPartitionQueryService.StringTuple) {
    def fromProto: (String, String) = {
      (st.getFieldOne, st.getFieldTwo)
    }
  }

  // DatasetOptions
  implicit class DatasetOptionsToProtoConverter(dso: filodb.core.metadata.DatasetOptions) {
    def toProto: GrpcMultiPartitionQueryService.DatasetOptions = {
      val builder = GrpcMultiPartitionQueryService.DatasetOptions.newBuilder()
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

  implicit class DatasetOptionsFromProtoConverter(dso: GrpcMultiPartitionQueryService.DatasetOptions) {
    def fromProto: filodb.core.metadata.DatasetOptions = {
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
    def toProto: GrpcMultiPartitionQueryService.PartitionSchema = {
      val builder = GrpcMultiPartitionQueryService.PartitionSchema.newBuilder()
      ps.columns.foreach(c => builder.addColumns(c.toProto))
      ps.predefinedKeys.foreach(k => builder.addPredefinedKeys(k))
      builder.setOptions(ps.options.toProto)
      builder.build()
    }
  }

  implicit class PartitionSchemaFromProtoConverter(ps: GrpcMultiPartitionQueryService.PartitionSchema) {
    def fromProto: filodb.core.metadata.PartitionSchema = {
      filodb.core.metadata.PartitionSchema(
        ps.getColumnsList().asScala.toSeq.map(c => c.fromProto),
        ps.getPredefinedKeysList().asScala.toSeq,
        ps.getOptions.fromProto
      )
    }
  }

  def getChunkDownsampler(d : filodb.core.downsample.ChunkDownsampler) : GrpcMultiPartitionQueryService.ChunkDownsampler = {
    val builder = GrpcMultiPartitionQueryService.ChunkDownsampler.newBuilder()
    d.inputColIds.foreach(id => builder.addInputColIds(id))
    builder.build()
  }

  def getInputColIds(cd : GrpcMultiPartitionQueryService.ChunkDownsampler) : Seq[Int] = {
    cd.getInputColIdsList.asScala.toSeq.map(i => i.intValue())
  }

  // TimeDownsampler
  implicit class TimeDownsamplerToProtoConverter(td: filodb.core.downsample.TimeDownsampler) {
    def toProto: GrpcMultiPartitionQueryService.TimeDownsampler = {
      val builder = GrpcMultiPartitionQueryService.TimeDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class TimeDownsamplerFromProtoConverter(td: GrpcMultiPartitionQueryService.TimeDownsampler) {
    def fromProto: filodb.core.downsample.TimeDownsampler = {
      filodb.core.downsample.TimeDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // LastValueHDownsampler
  implicit class LastValueHDownsamplerToProtoConverter(td: filodb.core.downsample.LastValueHDownsampler) {
    def toProto: GrpcMultiPartitionQueryService.LastValueHDownsampler = {
      val builder = GrpcMultiPartitionQueryService.LastValueHDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class LastValueHDownsamplerFromProtoConverter(td: GrpcMultiPartitionQueryService.LastValueHDownsampler) {
    def fromProto: filodb.core.downsample.LastValueHDownsampler = {
      filodb.core.downsample.LastValueHDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // HistSumDownsampler
  implicit class HistSumDownsamplerToProtoConverter(td: filodb.core.downsample.HistSumDownsampler) {
    def toProto: GrpcMultiPartitionQueryService.HistSumDownsampler = {
      val builder = GrpcMultiPartitionQueryService.HistSumDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class HistSumDownsamplerFromProtoConverter(td: GrpcMultiPartitionQueryService.HistSumDownsampler) {
    def fromProto: filodb.core.downsample.HistSumDownsampler = {
      filodb.core.downsample.HistSumDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // AvgDownsampler
  implicit class AvgDownsampleroProtoConverter(td: filodb.core.downsample.AvgDownsampler) {
    def toProto: GrpcMultiPartitionQueryService.AvgDownsampler = {
      val builder = GrpcMultiPartitionQueryService.AvgDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class AvgDownsamplerFromProtoConverter(td: GrpcMultiPartitionQueryService.AvgDownsampler) {
    def fromProto: filodb.core.downsample.AvgDownsampler = {
      filodb.core.downsample.AvgDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // AvgScDownsampler
  implicit class AvgScDownsamplerToProtoConverter(td: filodb.core.downsample.AvgScDownsampler) {
    def toProto: GrpcMultiPartitionQueryService.AvgScDownsampler = {
      val builder = GrpcMultiPartitionQueryService.AvgScDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class AvgScDownsamplerFromProtoConverter(td: GrpcMultiPartitionQueryService.AvgScDownsampler) {
    def fromProto: filodb.core.downsample.AvgScDownsampler = {
      filodb.core.downsample.AvgScDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // LastValueDDownsampler
  implicit class LastValueDDownsamplerToProtoConverter(td: filodb.core.downsample.LastValueDDownsampler) {
    def toProto: GrpcMultiPartitionQueryService.LastValueDDownsampler = {
      val builder = GrpcMultiPartitionQueryService.LastValueDDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class LastValueDDownsamplerFromProtoConverter(td: GrpcMultiPartitionQueryService.LastValueDDownsampler) {
    def fromProto: filodb.core.downsample.LastValueDDownsampler = {
      filodb.core.downsample.LastValueDDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // MinDownsampler
  implicit class MinDownsamplerToProtoConverter(td: filodb.core.downsample.MinDownsampler) {
    def toProto: GrpcMultiPartitionQueryService.MinDownsampler= {
      val builder = GrpcMultiPartitionQueryService.MinDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class MinDownsamplerFromProtoConverter(td: GrpcMultiPartitionQueryService.MinDownsampler) {
    def fromProto: filodb.core.downsample.MinDownsampler = {
      filodb.core.downsample.MinDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // SumDownsampler
  implicit class SumDownsamplerToProtoConverter(td: filodb.core.downsample.SumDownsampler) {
    def toProto: GrpcMultiPartitionQueryService.SumDownsampler = {
      val builder = GrpcMultiPartitionQueryService.SumDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class SumDownsamplerFromProtoConverter(td: GrpcMultiPartitionQueryService.SumDownsampler) {
    def fromProto: filodb.core.downsample.SumDownsampler = {
      filodb.core.downsample.SumDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // AvgAcDownsampler
  implicit class AvgAcDownsamplerToProtoConverter(td: filodb.core.downsample.AvgAcDownsampler) {
    def toProto: GrpcMultiPartitionQueryService.AvgAcDownsampler = {
      val builder = GrpcMultiPartitionQueryService.AvgAcDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class AvgAcDownsamplerFromProtoConverter(td: GrpcMultiPartitionQueryService.AvgAcDownsampler) {
    def fromProto: filodb.core.downsample.AvgAcDownsampler = {
      filodb.core.downsample.AvgAcDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // MaxDownsampler
  implicit class MaxDownsampleroProtoConverter(td: filodb.core.downsample.MaxDownsampler) {
    def toProto: GrpcMultiPartitionQueryService.MaxDownsampler = {
      val builder = GrpcMultiPartitionQueryService.MaxDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class MaxDownsamplerFromProtoConverter(td: GrpcMultiPartitionQueryService.MaxDownsampler) {
    def fromProto: filodb.core.downsample.MaxDownsampler = {
      filodb.core.downsample.MaxDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // CountDownsampler
  implicit class CountDownsamplerToProtoConverter(td: filodb.core.downsample.CountDownsampler) {
    def toProto: GrpcMultiPartitionQueryService.CountDownsampler = {
      val builder = GrpcMultiPartitionQueryService.CountDownsampler.newBuilder()
      builder.setChunkDownsampler(getChunkDownsampler(td))
      builder.build()
    }
  }

  implicit class CountDownsamplerFromProtoConverter(td: GrpcMultiPartitionQueryService.CountDownsampler) {
    def fromProto: filodb.core.downsample.CountDownsampler = {
      filodb.core.downsample.CountDownsampler(
        td.getChunkDownsampler.getInputColIdsList().asScala.toSeq.map(i => i.intValue())
      )
    }
  }

  // ChunkDownsamplerContainer
  implicit class ChunkDownsamplerToProtoConverter(cd: filodb.core.downsample.ChunkDownsampler) {
    def toProto: GrpcMultiPartitionQueryService.ChunkDownsamplerContainer = {
      val builder = GrpcMultiPartitionQueryService.ChunkDownsamplerContainer.newBuilder()
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

  implicit class ChunkDownsamplerFromProtoConverter(cd: GrpcMultiPartitionQueryService.ChunkDownsamplerContainer) {
    def fromProto: filodb.core.downsample.ChunkDownsampler = {
      import GrpcMultiPartitionQueryService.ChunkDownsamplerContainer.ChunkDownsamplerCase
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
  ): GrpcMultiPartitionQueryService.DownsamplePeriodMarker = {
    val builder = GrpcMultiPartitionQueryService.DownsamplePeriodMarker.newBuilder()
    builder.setInputColId(dpm.inputColId)
    builder.build()
  }

  // CounterDownsamplePeriodMarkerContainer
  implicit class CounterDownsamplePeriodMarkerContainerToProtoConverter(
    cdpmc: filodb.core.downsample.CounterDownsamplePeriodMarker
  ) {
    def toProto: GrpcMultiPartitionQueryService.CounterDownsamplePeriodMarker = {
      val builder = GrpcMultiPartitionQueryService.CounterDownsamplePeriodMarker.newBuilder()
      builder.setDownsamplePeriodMarker(getDownsamplePeriodMarkerProto(cdpmc))
      builder.build()
    }
  }

  implicit class CounterDownsamplePeriodMarkerFromProtoConverter(cdspm: GrpcMultiPartitionQueryService.CounterDownsamplePeriodMarker) {
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
    def toProto: GrpcMultiPartitionQueryService.TimeDownsamplePeriodMarker = {
      val builder = GrpcMultiPartitionQueryService.TimeDownsamplePeriodMarker.newBuilder()
      builder.setDownsamplePeriodMarker(getDownsamplePeriodMarkerProto(tdpmc))
      builder.build()
    }
  }

  implicit class TimeDownsamplePeriodMarkerFromProtoConverter(tdspm: GrpcMultiPartitionQueryService.TimeDownsamplePeriodMarker) {
    def fromProto: filodb.core.downsample.TimeDownsamplePeriodMarker = {
      new TimeDownsamplePeriodMarker(
        tdspm.getDownsamplePeriodMarker.getInputColId
      )
    }
  }

  // DownsamplePeriodMarkerContainer
  implicit class DownsamplePeriodMarkerContainerToProtoConverter(dpm: filodb.core.downsample.DownsamplePeriodMarker) {
    def toProto: GrpcMultiPartitionQueryService.DownsamplePeriodMarkerContainer = {
      val builder = GrpcMultiPartitionQueryService.DownsamplePeriodMarkerContainer.newBuilder()
      dpm match {
        case cdpm : CounterDownsamplePeriodMarker => builder.setCounterDownsamplePeriodMarker(cdpm.toProto)
        case tdpm : TimeDownsamplePeriodMarker => builder.setTimeDownsamplePeriodMarker(tdpm.toProto)
      }
      builder.build()
    }
  }

  implicit class DownsamplePeriodMarkerContainerFromProtoConverter(dpmc: GrpcMultiPartitionQueryService.DownsamplePeriodMarkerContainer) {
    def fromProto: filodb.core.downsample.DownsamplePeriodMarker = {
      import GrpcMultiPartitionQueryService.DownsamplePeriodMarkerContainer.DownsamplePeriodMarkerCase
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
    def toProto: GrpcMultiPartitionQueryService.DataSchema = {
      val builder = GrpcMultiPartitionQueryService.DataSchema.newBuilder()
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

  implicit class DataSchemaFromProtoConverter(ds: GrpcMultiPartitionQueryService.DataSchema) {
    def fromProto: filodb.core.metadata.DataSchema = {
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
    def toProto: GrpcMultiPartitionQueryService.Schema = {
      val builder = GrpcMultiPartitionQueryService.Schema.newBuilder()
      builder.setPartition(s.partition.toProto)
      builder.setData(s.data.toProto)
      s.downsample.foreach(s => builder.setDownsample(s.toProto))
      builder.build()
    }
  }

  implicit class SchemaFromProtoConverter(s: GrpcMultiPartitionQueryService.Schema) {
    def fromProto: filodb.core.metadata.Schema = {
      val downsample = if (s.hasDownsample) {Option(s.getDownsample.fromProto)} else {None}
      filodb.core.metadata.Schema(s.getPartition.fromProto, s.getData.fromProto, downsample)
    }
  }

  // PartKeyLuceneIndexRecord
  implicit class PartKeyLuceneIndexRecordToProtoConverter(pklir: filodb.core.memstore.PartKeyLuceneIndexRecord) {
    def toProto: GrpcMultiPartitionQueryService.PartKeyLuceneIndexRecord = {
      val builder = GrpcMultiPartitionQueryService.PartKeyLuceneIndexRecord.newBuilder()
      builder.setPartKey(ByteString.copyFrom(pklir.partKey))
      builder.setStartTime(pklir.startTime)
      builder.setEndTime(pklir.endTime)
      builder.build()
    }
  }

  implicit class PartKeyLuceneIndexRecordFromProtoConverter(pklir: GrpcMultiPartitionQueryService.PartKeyLuceneIndexRecord) {
    def fromProto: filodb.core.memstore.PartKeyLuceneIndexRecord = {
      filodb.core.memstore.PartKeyLuceneIndexRecord(pklir.getPartKey.toByteArray, pklir.getStartTime, pklir.getEndTime)
    }
  }

  // PartLookupResult
  implicit class PartLookupResultToProtoConverter(plr: PartLookupResult) {
    def toProto: GrpcMultiPartitionQueryService.PartLookupResult = {
      val builder = GrpcMultiPartitionQueryService.PartLookupResult.newBuilder()
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

  implicit class PartLookupResultFromProtoConverter(plr: GrpcMultiPartitionQueryService.PartLookupResult) {
    def fromProto: PartLookupResult = {
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
    def toProto: GrpcMultiPartitionQueryService.BinaryOperator = {
      val operator = bo match {
        case filodb.query.BinaryOperator.SUB      => GrpcMultiPartitionQueryService.BinaryOperator.SUB
        case filodb.query.BinaryOperator.ADD      => GrpcMultiPartitionQueryService.BinaryOperator.ADD
        case filodb.query.BinaryOperator.MUL      => GrpcMultiPartitionQueryService.BinaryOperator.MUL
        case filodb.query.BinaryOperator.MOD      => GrpcMultiPartitionQueryService.BinaryOperator.MOD
        case filodb.query.BinaryOperator.DIV      => GrpcMultiPartitionQueryService.BinaryOperator.DIV
        case filodb.query.BinaryOperator.POW      => GrpcMultiPartitionQueryService.BinaryOperator.POW
        case filodb.query.BinaryOperator.LAND     => GrpcMultiPartitionQueryService.BinaryOperator.LAND
        case filodb.query.BinaryOperator.LOR      => GrpcMultiPartitionQueryService.BinaryOperator.LOR
        case filodb.query.BinaryOperator.LUnless  => GrpcMultiPartitionQueryService.BinaryOperator.LUNLESS
        case filodb.query.BinaryOperator.EQL      => GrpcMultiPartitionQueryService.BinaryOperator.EQL
        case filodb.query.BinaryOperator.NEQ      => GrpcMultiPartitionQueryService.BinaryOperator.NEQ
        case filodb.query.BinaryOperator.LTE      => GrpcMultiPartitionQueryService.BinaryOperator.LTE
        case filodb.query.BinaryOperator.LSS      => GrpcMultiPartitionQueryService.BinaryOperator.LSS
        case filodb.query.BinaryOperator.GTE      => GrpcMultiPartitionQueryService.BinaryOperator.GTE
        case filodb.query.BinaryOperator.GTR      => GrpcMultiPartitionQueryService.BinaryOperator.GTR
        case filodb.query.BinaryOperator.EQL_BOOL => GrpcMultiPartitionQueryService.BinaryOperator.EQL_BOOL
        case filodb.query.BinaryOperator.NEQ_BOOL => GrpcMultiPartitionQueryService.BinaryOperator.NEQ_BOOL
        case filodb.query.BinaryOperator.LTE_BOOL => GrpcMultiPartitionQueryService.BinaryOperator.LTE_BOOL
        case filodb.query.BinaryOperator.LSS_BOOL => GrpcMultiPartitionQueryService.BinaryOperator.LSS_BOOL
        case filodb.query.BinaryOperator.GTE_BOOL => GrpcMultiPartitionQueryService.BinaryOperator.GTE_BOOL
        case filodb.query.BinaryOperator.GTR_BOOL => GrpcMultiPartitionQueryService.BinaryOperator.GTR_BOOL
        case filodb.query.BinaryOperator.EQLRegex => GrpcMultiPartitionQueryService.BinaryOperator.EQL_REGEX
        case filodb.query.BinaryOperator.NEQRegex => GrpcMultiPartitionQueryService.BinaryOperator.NEQ_REGEX
      }
      operator
    }
  }

  implicit class BinaryOperatorFromProtoConverter(bo: GrpcMultiPartitionQueryService.BinaryOperator) {
    def fromProto: filodb.query.BinaryOperator = {
      val operator : filodb.query.BinaryOperator = bo match {
        case GrpcMultiPartitionQueryService.BinaryOperator.SUB => filodb.query.BinaryOperator.SUB
        case GrpcMultiPartitionQueryService.BinaryOperator.ADD => filodb.query.BinaryOperator.ADD
        case GrpcMultiPartitionQueryService.BinaryOperator.MUL => filodb.query.BinaryOperator.MUL
        case GrpcMultiPartitionQueryService.BinaryOperator.MOD => filodb.query.BinaryOperator.MOD
        case GrpcMultiPartitionQueryService.BinaryOperator.DIV => filodb.query.BinaryOperator.DIV
        case GrpcMultiPartitionQueryService.BinaryOperator.POW => filodb.query.BinaryOperator.POW
        case GrpcMultiPartitionQueryService.BinaryOperator.LAND => filodb.query.BinaryOperator.LAND
        case GrpcMultiPartitionQueryService.BinaryOperator.LOR => filodb.query.BinaryOperator.LOR
        case GrpcMultiPartitionQueryService.BinaryOperator.LUNLESS => filodb.query.BinaryOperator.LUnless
        case GrpcMultiPartitionQueryService.BinaryOperator.EQL => filodb.query.BinaryOperator.EQL
        case GrpcMultiPartitionQueryService.BinaryOperator.NEQ => filodb.query.BinaryOperator.NEQ
        case GrpcMultiPartitionQueryService.BinaryOperator.LTE => filodb.query.BinaryOperator.LTE
        case GrpcMultiPartitionQueryService.BinaryOperator.LSS => filodb.query.BinaryOperator.LSS
        case GrpcMultiPartitionQueryService.BinaryOperator.GTE => filodb.query.BinaryOperator.GTE
        case GrpcMultiPartitionQueryService.BinaryOperator.GTR => filodb.query.BinaryOperator.GTR
        case GrpcMultiPartitionQueryService.BinaryOperator.EQL_BOOL => filodb.query.BinaryOperator.EQL_BOOL
        case GrpcMultiPartitionQueryService.BinaryOperator.NEQ_BOOL => filodb.query.BinaryOperator.NEQ_BOOL
        case GrpcMultiPartitionQueryService.BinaryOperator.LTE_BOOL => filodb.query.BinaryOperator.LTE_BOOL
        case GrpcMultiPartitionQueryService.BinaryOperator.LSS_BOOL => filodb.query.BinaryOperator.LSS_BOOL
        case GrpcMultiPartitionQueryService.BinaryOperator.GTE_BOOL => filodb.query.BinaryOperator.GTE_BOOL
        case GrpcMultiPartitionQueryService.BinaryOperator.GTR_BOOL => filodb.query.BinaryOperator.GTR_BOOL
        case GrpcMultiPartitionQueryService.BinaryOperator.EQL_REGEX => filodb.query.BinaryOperator.EQLRegex
        case GrpcMultiPartitionQueryService.BinaryOperator.NEQ_REGEX => filodb.query.BinaryOperator.NEQRegex
        case GrpcMultiPartitionQueryService.BinaryOperator.UNRECOGNIZED => throw new IllegalArgumentException("Unrecognized binary operator")
      }
      operator
    }
    // scalastyle:on cyclomatic.complexity
  }

  // Cardinality
  implicit class CardinalityToProtoConverter(c: filodb.query.Cardinality) {
    def toProto: GrpcMultiPartitionQueryService.Cardinality = {
      val operator = c match {
        case filodb.query.Cardinality.OneToOne => GrpcMultiPartitionQueryService.Cardinality.ONE_TO_ONE
        case filodb.query.Cardinality.OneToMany => GrpcMultiPartitionQueryService.Cardinality.ONE_TO_MANY
        case filodb.query.Cardinality.ManyToOne => GrpcMultiPartitionQueryService.Cardinality.MANY_TO_ONE
        case filodb.query.Cardinality.ManyToMany => GrpcMultiPartitionQueryService.Cardinality.MANY_TO_MANY
      }
      operator
    }
  }

  implicit class CardinalityFromProtoConverter(c: GrpcMultiPartitionQueryService.Cardinality) {
    def fromProto: filodb.query.Cardinality = {
      val cardinality: filodb.query.Cardinality = c match {
        case GrpcMultiPartitionQueryService.Cardinality.ONE_TO_ONE => filodb.query.Cardinality.OneToOne
        case GrpcMultiPartitionQueryService.Cardinality.ONE_TO_MANY => filodb.query.Cardinality.OneToMany
        case GrpcMultiPartitionQueryService.Cardinality.MANY_TO_ONE => filodb.query.Cardinality.ManyToOne
        case GrpcMultiPartitionQueryService.Cardinality.MANY_TO_MANY => filodb.query.Cardinality.ManyToMany
        case GrpcMultiPartitionQueryService.Cardinality.UNRECOGNIZED => throw new IllegalArgumentException("Unrecognized cardinality")
      }
      cardinality
    }
  }


  // ScalarFunctionId
  implicit class ScalarFunctionIdToProtoConverter(f: filodb.query.ScalarFunctionId) {
    //noinspection ScalaStyle
    def toProto: GrpcMultiPartitionQueryService.ScalarFunctionId = {
      val function = f match {
        case filodb.query.ScalarFunctionId.Scalar => GrpcMultiPartitionQueryService.ScalarFunctionId.SCALAR_FI
        case filodb.query.ScalarFunctionId.Time => GrpcMultiPartitionQueryService.ScalarFunctionId.TIME_FI
        case filodb.query.ScalarFunctionId.DaysInMonth => GrpcMultiPartitionQueryService.ScalarFunctionId.DAYS_IN_MONTH_FI
        case filodb.query.ScalarFunctionId.DayOfMonth => GrpcMultiPartitionQueryService.ScalarFunctionId.DAY_OF_MONTH_FI
        case filodb.query.ScalarFunctionId.DayOfWeek => GrpcMultiPartitionQueryService.ScalarFunctionId.DAY_OF_WEEK_FI
        case filodb.query.ScalarFunctionId.Hour => GrpcMultiPartitionQueryService.ScalarFunctionId.HOUR_FI
        case filodb.query.ScalarFunctionId.Minute => GrpcMultiPartitionQueryService.ScalarFunctionId.MINUTE_FI
        case filodb.query.ScalarFunctionId.Month => GrpcMultiPartitionQueryService.ScalarFunctionId.MONTH_FI
        case filodb.query.ScalarFunctionId.Year => GrpcMultiPartitionQueryService.ScalarFunctionId.YEAR_FI
      }
      function
    }
  }

  implicit class ScalarFunctionIdFromProtoConverter(f: GrpcMultiPartitionQueryService.ScalarFunctionId) {
    //noinspection ScalaStyle
    def fromProto: filodb.query.ScalarFunctionId = {
      val function: filodb.query.ScalarFunctionId = f match {
        case GrpcMultiPartitionQueryService.ScalarFunctionId.SCALAR_FI => filodb.query.ScalarFunctionId.Scalar
        case GrpcMultiPartitionQueryService.ScalarFunctionId.TIME_FI => filodb.query.ScalarFunctionId.Time
        case GrpcMultiPartitionQueryService.ScalarFunctionId.DAYS_IN_MONTH_FI => filodb.query.ScalarFunctionId.DaysInMonth
        case GrpcMultiPartitionQueryService.ScalarFunctionId.DAY_OF_MONTH_FI => filodb.query.ScalarFunctionId.DayOfMonth
        case GrpcMultiPartitionQueryService.ScalarFunctionId.DAY_OF_WEEK_FI => filodb.query.ScalarFunctionId.DayOfWeek
        case GrpcMultiPartitionQueryService.ScalarFunctionId.HOUR_FI => filodb.query.ScalarFunctionId.Hour
        case GrpcMultiPartitionQueryService.ScalarFunctionId.MINUTE_FI => filodb.query.ScalarFunctionId.Minute
        case GrpcMultiPartitionQueryService.ScalarFunctionId.MONTH_FI => filodb.query.ScalarFunctionId.Month
        case GrpcMultiPartitionQueryService.ScalarFunctionId.YEAR_FI => filodb.query.ScalarFunctionId.Year
        case GrpcMultiPartitionQueryService.ScalarFunctionId.UNRECOGNIZED =>
          throw new IllegalArgumentException("Unrecognized scala function")
      }
      function
    }
  }

  // InstantFunctionId
  implicit class InstantFunctionIdToProtoConverter(f: filodb.query.InstantFunctionId) {
    // scalastyle:off cyclomatic.complexity
    def toProto: GrpcMultiPartitionQueryService.InstantFunctionId = {
      val function = f match {
        case filodb.query.InstantFunctionId.Abs => GrpcMultiPartitionQueryService.InstantFunctionId.ABS
        case filodb.query.InstantFunctionId.Ceil => GrpcMultiPartitionQueryService.InstantFunctionId.CEIL
        case filodb.query.InstantFunctionId.ClampMax => GrpcMultiPartitionQueryService.InstantFunctionId.CLAMP_MAX
        case filodb.query.InstantFunctionId.ClampMin => GrpcMultiPartitionQueryService.InstantFunctionId.CLAMP_MIN
        case filodb.query.InstantFunctionId.Exp => GrpcMultiPartitionQueryService.InstantFunctionId.EXP
        case filodb.query.InstantFunctionId.Floor => GrpcMultiPartitionQueryService.InstantFunctionId.FLOOR
        case filodb.query.InstantFunctionId.HistogramQuantile => GrpcMultiPartitionQueryService.InstantFunctionId.HISTOGRAM_QUANTILE
        case filodb.query.InstantFunctionId.HistogramMaxQuantile => GrpcMultiPartitionQueryService.InstantFunctionId.HISTOGRAM_MAX_QUANTILE
        case filodb.query.InstantFunctionId.HistogramBucket => GrpcMultiPartitionQueryService.InstantFunctionId.HISTOGRAM_BUCKET
        case filodb.query.InstantFunctionId.HistogramFraction => GrpcMultiPartitionQueryService.InstantFunctionId.HISTOGRAM_FRACTION
        case filodb.query.InstantFunctionId.Ln => GrpcMultiPartitionQueryService.InstantFunctionId.LN
        case filodb.query.InstantFunctionId.Log10 => GrpcMultiPartitionQueryService.InstantFunctionId.LOG10
        case filodb.query.InstantFunctionId.Log2 => GrpcMultiPartitionQueryService.InstantFunctionId.LOG2
        case filodb.query.InstantFunctionId.Round => GrpcMultiPartitionQueryService.InstantFunctionId.ROUND
        case filodb.query.InstantFunctionId.Sgn => GrpcMultiPartitionQueryService.InstantFunctionId.SGN
        case filodb.query.InstantFunctionId.Sqrt => GrpcMultiPartitionQueryService.InstantFunctionId.SQRT
        case filodb.query.InstantFunctionId.DaysInMonth => GrpcMultiPartitionQueryService.InstantFunctionId.DAYS_IN_MONTH
        case filodb.query.InstantFunctionId.DayOfMonth => GrpcMultiPartitionQueryService.InstantFunctionId.DAY_OF_MONTH
        case filodb.query.InstantFunctionId.DayOfWeek => GrpcMultiPartitionQueryService.InstantFunctionId.DAY_OF_WEEK
        case filodb.query.InstantFunctionId.Hour => GrpcMultiPartitionQueryService.InstantFunctionId.HOUR
        case filodb.query.InstantFunctionId.Minute => GrpcMultiPartitionQueryService.InstantFunctionId.MINUTE
        case filodb.query.InstantFunctionId.Month => GrpcMultiPartitionQueryService.InstantFunctionId.MONTH
        case filodb.query.InstantFunctionId.Year => GrpcMultiPartitionQueryService.InstantFunctionId.YEAR
        case filodb.query.InstantFunctionId.OrVectorDouble => GrpcMultiPartitionQueryService.InstantFunctionId.OR_VECTOR_DOUBLE
      }
      function
    }
  }

  implicit class InstantFunctionIdFromProtoConverter(f: GrpcMultiPartitionQueryService.InstantFunctionId) {
    def fromProto: filodb.query.InstantFunctionId = {
      val function: filodb.query.InstantFunctionId  = f match {
        case GrpcMultiPartitionQueryService.InstantFunctionId.ABS => filodb.query.InstantFunctionId.Abs
        case GrpcMultiPartitionQueryService.InstantFunctionId.CEIL => filodb.query.InstantFunctionId.Ceil
        case GrpcMultiPartitionQueryService.InstantFunctionId.CLAMP_MAX => filodb.query.InstantFunctionId.ClampMax
        case GrpcMultiPartitionQueryService.InstantFunctionId.CLAMP_MIN => filodb.query.InstantFunctionId.ClampMin
        case GrpcMultiPartitionQueryService.InstantFunctionId.EXP => filodb.query.InstantFunctionId.Exp
        case GrpcMultiPartitionQueryService.InstantFunctionId.FLOOR => filodb.query.InstantFunctionId.Floor
        case GrpcMultiPartitionQueryService.InstantFunctionId.HISTOGRAM_QUANTILE => filodb.query.InstantFunctionId.HistogramQuantile
        case GrpcMultiPartitionQueryService.InstantFunctionId.HISTOGRAM_MAX_QUANTILE => filodb.query.InstantFunctionId.HistogramMaxQuantile
        case GrpcMultiPartitionQueryService.InstantFunctionId.HISTOGRAM_BUCKET => filodb.query.InstantFunctionId.HistogramBucket
        case GrpcMultiPartitionQueryService.InstantFunctionId.HISTOGRAM_FRACTION => filodb.query.InstantFunctionId.HistogramFraction
        case GrpcMultiPartitionQueryService.InstantFunctionId.LN => filodb.query.InstantFunctionId.Ln
        case GrpcMultiPartitionQueryService.InstantFunctionId.LOG10 => filodb.query.InstantFunctionId.Log10
        case GrpcMultiPartitionQueryService.InstantFunctionId.LOG2 => filodb.query.InstantFunctionId.Log2
        case GrpcMultiPartitionQueryService.InstantFunctionId.ROUND => filodb.query.InstantFunctionId.Round
        case GrpcMultiPartitionQueryService.InstantFunctionId.SGN => filodb.query.InstantFunctionId.Sgn
        case GrpcMultiPartitionQueryService.InstantFunctionId.SQRT => filodb.query.InstantFunctionId.Sqrt
        case GrpcMultiPartitionQueryService.InstantFunctionId.DAYS_IN_MONTH => filodb.query.InstantFunctionId.DaysInMonth
        case GrpcMultiPartitionQueryService.InstantFunctionId.DAY_OF_MONTH => filodb.query.InstantFunctionId.DayOfMonth
        case GrpcMultiPartitionQueryService.InstantFunctionId.DAY_OF_WEEK => filodb.query.InstantFunctionId.DayOfWeek
        case GrpcMultiPartitionQueryService.InstantFunctionId.HOUR => filodb.query.InstantFunctionId.Hour
        case GrpcMultiPartitionQueryService.InstantFunctionId.MINUTE => filodb.query.InstantFunctionId.Minute
        case GrpcMultiPartitionQueryService.InstantFunctionId.MONTH => filodb.query.InstantFunctionId.Month
        case GrpcMultiPartitionQueryService.InstantFunctionId.YEAR => filodb.query.InstantFunctionId.Year
        case GrpcMultiPartitionQueryService.InstantFunctionId.OR_VECTOR_DOUBLE => filodb.query.InstantFunctionId.OrVectorDouble
        case GrpcMultiPartitionQueryService.InstantFunctionId.UNRECOGNIZED =>
          throw new IllegalArgumentException("Unrecognized scala function")
      }
      function
    }
  }

  // InternalRangeFunction
  implicit class InternalRangeFunctionToProtoConverter(f: InternalRangeFunction) {
    def toProto: GrpcMultiPartitionQueryService.InternalRangeFunction = {
      val function = f match {
        case InternalRangeFunction.AvgOverTime => GrpcMultiPartitionQueryService.InternalRangeFunction.AVG_OVER_TIME
        case InternalRangeFunction.Changes => GrpcMultiPartitionQueryService.InternalRangeFunction.CHANGES
        case InternalRangeFunction.CountOverTime => GrpcMultiPartitionQueryService.InternalRangeFunction.COUNT_OVER_TIME
        case InternalRangeFunction.Delta => GrpcMultiPartitionQueryService.InternalRangeFunction.DELTA
        case InternalRangeFunction.Deriv => GrpcMultiPartitionQueryService.InternalRangeFunction.DERIV
        case InternalRangeFunction.HoltWinters => GrpcMultiPartitionQueryService.InternalRangeFunction.HOLT_WINTERS
        case InternalRangeFunction.ZScore => GrpcMultiPartitionQueryService.InternalRangeFunction.ZSCORE
        case InternalRangeFunction.Idelta => GrpcMultiPartitionQueryService.InternalRangeFunction.IDELTA
        case InternalRangeFunction.Increase => GrpcMultiPartitionQueryService.InternalRangeFunction.INCREASE
        case InternalRangeFunction.Irate => GrpcMultiPartitionQueryService.InternalRangeFunction.IRATE
        case InternalRangeFunction.MaxOverTime => GrpcMultiPartitionQueryService.InternalRangeFunction.MAX_OVER_TIME
        case InternalRangeFunction.MinOverTime => GrpcMultiPartitionQueryService.InternalRangeFunction.MIN_OVER_TIME
        case InternalRangeFunction.PredictLinear => GrpcMultiPartitionQueryService.InternalRangeFunction.PREDICT_LINEAR
        case InternalRangeFunction.QuantileOverTime => GrpcMultiPartitionQueryService.InternalRangeFunction.QUANTILE_OVER_TIME
        case InternalRangeFunction.Rate => GrpcMultiPartitionQueryService.InternalRangeFunction.RATE
        case InternalRangeFunction.Resets => GrpcMultiPartitionQueryService.InternalRangeFunction.RESETS
        case InternalRangeFunction.StdDevOverTime => GrpcMultiPartitionQueryService.InternalRangeFunction.STD_DEV_OVER_TIME
        case InternalRangeFunction.StdVarOverTime => GrpcMultiPartitionQueryService.InternalRangeFunction.STD_VAR_OVER_TIME
        case InternalRangeFunction.SumOverTime => GrpcMultiPartitionQueryService.InternalRangeFunction.SUM_OVER_TIME
        case InternalRangeFunction.Last => GrpcMultiPartitionQueryService.InternalRangeFunction.LAST
        case InternalRangeFunction.LastOverTime => GrpcMultiPartitionQueryService.InternalRangeFunction.LAST_OVER_TIME
        case InternalRangeFunction.AvgWithSumAndCountOverTime =>
          GrpcMultiPartitionQueryService.InternalRangeFunction.AVG_WITH_SUM_AND_COUNT_OVER_TIME
        case InternalRangeFunction.SumAndMaxOverTime => GrpcMultiPartitionQueryService.InternalRangeFunction.SUM_AND_MAX_OVER_TIME
        case InternalRangeFunction.RateAndMinMaxOverTime => GrpcMultiPartitionQueryService.InternalRangeFunction.RATE_AND_MIN_MAX_OVER_TIME
        case InternalRangeFunction.LastSampleHistMaxMin => GrpcMultiPartitionQueryService.InternalRangeFunction.LAST_SAMPLE_HIST_MAX_MIN
        case InternalRangeFunction.Timestamp => GrpcMultiPartitionQueryService.InternalRangeFunction.TIME_STAMP
        case InternalRangeFunction.AbsentOverTime => GrpcMultiPartitionQueryService.InternalRangeFunction.ABSENT_OVER_TIME
        case InternalRangeFunction.PresentOverTime => GrpcMultiPartitionQueryService.InternalRangeFunction.PRESENT_OVER_TIME
        case InternalRangeFunction.MedianAbsoluteDeviationOverTime => GrpcMultiPartitionQueryService.InternalRangeFunction.MEDIAN_ABSOLUTE_DEVIATION_OVER_TIME
        case InternalRangeFunction.LastOverTimeIsMadOutlier => GrpcMultiPartitionQueryService.InternalRangeFunction.LAST_OVER_TIME_IS_MAD_OUTLIER
      }
      function
    }
  }

  implicit class InternalRangeFunctionFromProtoConverter(f: GrpcMultiPartitionQueryService.InternalRangeFunction) {
    def fromProto: InternalRangeFunction = {
      val function: InternalRangeFunction = f match {
        case GrpcMultiPartitionQueryService.InternalRangeFunction.AVG_OVER_TIME => InternalRangeFunction.AvgOverTime
        case GrpcMultiPartitionQueryService.InternalRangeFunction.CHANGES => InternalRangeFunction.Changes
        case GrpcMultiPartitionQueryService.InternalRangeFunction.COUNT_OVER_TIME => InternalRangeFunction.CountOverTime
        case GrpcMultiPartitionQueryService.InternalRangeFunction.DELTA => InternalRangeFunction.Delta
        case GrpcMultiPartitionQueryService.InternalRangeFunction.DERIV => InternalRangeFunction.Deriv
        case GrpcMultiPartitionQueryService.InternalRangeFunction.HOLT_WINTERS => InternalRangeFunction.HoltWinters
        case GrpcMultiPartitionQueryService.InternalRangeFunction.ZSCORE => InternalRangeFunction.ZScore
        case GrpcMultiPartitionQueryService.InternalRangeFunction.IDELTA => InternalRangeFunction.Idelta
        case GrpcMultiPartitionQueryService.InternalRangeFunction.INCREASE => InternalRangeFunction.Increase
        case GrpcMultiPartitionQueryService.InternalRangeFunction.IRATE => InternalRangeFunction.Irate
        case GrpcMultiPartitionQueryService.InternalRangeFunction.MAX_OVER_TIME => InternalRangeFunction.MaxOverTime
        case GrpcMultiPartitionQueryService.InternalRangeFunction.MIN_OVER_TIME => InternalRangeFunction.MinOverTime
        case GrpcMultiPartitionQueryService.InternalRangeFunction.PREDICT_LINEAR => InternalRangeFunction.PredictLinear
        case GrpcMultiPartitionQueryService.InternalRangeFunction.QUANTILE_OVER_TIME => InternalRangeFunction.QuantileOverTime
        case GrpcMultiPartitionQueryService.InternalRangeFunction.RATE => InternalRangeFunction.Rate
        case GrpcMultiPartitionQueryService.InternalRangeFunction.RESETS => InternalRangeFunction.Resets
        case GrpcMultiPartitionQueryService.InternalRangeFunction.STD_DEV_OVER_TIME => InternalRangeFunction.StdDevOverTime
        case GrpcMultiPartitionQueryService.InternalRangeFunction.STD_VAR_OVER_TIME => InternalRangeFunction.StdVarOverTime
        case GrpcMultiPartitionQueryService.InternalRangeFunction.SUM_OVER_TIME => InternalRangeFunction.SumOverTime
        case GrpcMultiPartitionQueryService.InternalRangeFunction.LAST => InternalRangeFunction.Last
        case GrpcMultiPartitionQueryService.InternalRangeFunction.LAST_OVER_TIME => InternalRangeFunction.LastOverTime
        case GrpcMultiPartitionQueryService.InternalRangeFunction.AVG_WITH_SUM_AND_COUNT_OVER_TIME =>
          InternalRangeFunction.AvgWithSumAndCountOverTime
        case GrpcMultiPartitionQueryService.InternalRangeFunction.SUM_AND_MAX_OVER_TIME => InternalRangeFunction.SumAndMaxOverTime
        case GrpcMultiPartitionQueryService.InternalRangeFunction.RATE_AND_MIN_MAX_OVER_TIME => InternalRangeFunction.RateAndMinMaxOverTime
        case GrpcMultiPartitionQueryService.InternalRangeFunction.LAST_SAMPLE_HIST_MAX_MIN => InternalRangeFunction.LastSampleHistMaxMin
        case GrpcMultiPartitionQueryService.InternalRangeFunction.TIME_STAMP => InternalRangeFunction.Timestamp
        case GrpcMultiPartitionQueryService.InternalRangeFunction.ABSENT_OVER_TIME => InternalRangeFunction.AbsentOverTime
        case GrpcMultiPartitionQueryService.InternalRangeFunction.PRESENT_OVER_TIME => InternalRangeFunction.PresentOverTime
        case GrpcMultiPartitionQueryService.InternalRangeFunction.MEDIAN_ABSOLUTE_DEVIATION_OVER_TIME => InternalRangeFunction.MedianAbsoluteDeviationOverTime
        case GrpcMultiPartitionQueryService.InternalRangeFunction.LAST_OVER_TIME_IS_MAD_OUTLIER => InternalRangeFunction.LastOverTimeIsMadOutlier
        case GrpcMultiPartitionQueryService.InternalRangeFunction.UNRECOGNIZED =>
          throw new IllegalArgumentException(s"Unrecognized InternalRangeFunction ${f}")
      }
      function
    }
    // scalastyle:on cyclomatic.complexity
  }

  // SortFunctionId
  implicit class SortFunctionIdToProtoConverter(f: filodb.query.SortFunctionId) {
    //noinspection ScalaStyle
    def toProto: GrpcMultiPartitionQueryService.SortFunctionId = {
      val function = f match {
        case filodb.query.SortFunctionId.Sort => GrpcMultiPartitionQueryService.SortFunctionId.SORT
        case filodb.query.SortFunctionId.SortDesc => GrpcMultiPartitionQueryService.SortFunctionId.SORT_DESC
      }
      function
    }
  }

  implicit class SortFunctionIdFromProtoConverter(f: GrpcMultiPartitionQueryService.SortFunctionId) {
    //noinspection ScalaStyle
    def fromProto: filodb.query.SortFunctionId = {
      val function: filodb.query.SortFunctionId = f match {
        case GrpcMultiPartitionQueryService.SortFunctionId.SORT => filodb.query.SortFunctionId.Sort
        case GrpcMultiPartitionQueryService.SortFunctionId.SORT_DESC => filodb.query.SortFunctionId.SortDesc
        case GrpcMultiPartitionQueryService.SortFunctionId.UNRECOGNIZED =>
          throw new IllegalArgumentException(s"Unrecognized SortFunctionId ${f}")
      }
      function
    }
  }

  // MiscellaneousFunctionId
  implicit class MiscellaneousFunctionIdToProtoConverter(f: filodb.query.MiscellaneousFunctionId) {
    def toProto: GrpcMultiPartitionQueryService.MiscellaneousFunctionId = {
      val function = f match {
        case filodb.query.MiscellaneousFunctionId.LabelReplace => GrpcMultiPartitionQueryService.MiscellaneousFunctionId.LABEL_REPLACE
        case filodb.query.MiscellaneousFunctionId.LabelJoin => GrpcMultiPartitionQueryService.MiscellaneousFunctionId.LABEL_JOIN
        case filodb.query.MiscellaneousFunctionId.HistToPromVectors =>
          GrpcMultiPartitionQueryService.MiscellaneousFunctionId.HIST_TO_PROM_VECTORS
        case filodb.query.MiscellaneousFunctionId.OptimizeWithAgg =>
          GrpcMultiPartitionQueryService.MiscellaneousFunctionId.OPTIMIZE_WITH_AGG
      }
      function
    }
  }

  implicit class MiscellaneousFunctionIdFromProtoConverter(f: GrpcMultiPartitionQueryService.MiscellaneousFunctionId) {
    def fromProto: filodb.query.MiscellaneousFunctionId = {
      val function: filodb.query.MiscellaneousFunctionId = f match {
        case GrpcMultiPartitionQueryService.MiscellaneousFunctionId.LABEL_REPLACE => filodb.query.MiscellaneousFunctionId.LabelReplace
        case GrpcMultiPartitionQueryService.MiscellaneousFunctionId.LABEL_JOIN => filodb.query.MiscellaneousFunctionId.LabelJoin
        case GrpcMultiPartitionQueryService.MiscellaneousFunctionId.HIST_TO_PROM_VECTORS =>
          filodb.query.MiscellaneousFunctionId.HistToPromVectors
        case GrpcMultiPartitionQueryService.MiscellaneousFunctionId.OPTIMIZE_WITH_AGG =>
          filodb.query.MiscellaneousFunctionId.OptimizeWithAgg
        case GrpcMultiPartitionQueryService.MiscellaneousFunctionId.UNRECOGNIZED =>
          throw new IllegalArgumentException(s"Unrecognized MiscellaneousFunctionId ${f}")
      }
      function
    }
  }

  // Filters section
  def getProtoFilter(f: Filter): GrpcMultiPartitionQueryService.Filter = {
    val builder = GrpcMultiPartitionQueryService.Filter.newBuilder()
    builder.setOperatorString(f.operatorString)
    //TODO why is value ANY???
    //need to enumerate what can be in ANY, most likely
    // number integer/float/string?
    f.valuesStrings.foreach(vs => builder.addValueStrings(vs.asInstanceOf[String]))
    builder.build()
  }

  // FilterEquals
  implicit class FilterEqualsToProtoConverter(fe: Filter.Equals) {
    def toProto: GrpcMultiPartitionQueryService.FilterEquals = {
      val builder = GrpcMultiPartitionQueryService.FilterEquals.newBuilder()
      builder.setFilter(getProtoFilter(fe))
      builder.build()
    }
  }

  implicit class FilterEqualsFromProtoConverter(fe: GrpcMultiPartitionQueryService.FilterEquals) {
    def fromProto: Filter.Equals = {
      Filter.Equals(
        fe.getFilter.getValueStringsList.get(0)
      )
    }
  }

  // FilterNotEquals
  implicit class FilterNotEqualsToProtoConverter(fne: Filter.NotEquals) {
    def toProto: GrpcMultiPartitionQueryService.FilterNotEquals = {
      val builder = GrpcMultiPartitionQueryService.FilterNotEquals.newBuilder()
      builder.setFilter(getProtoFilter(fne))
      builder.build()
    }
  }

  implicit class FilterNotEqualsFromProtoConverter(fne: GrpcMultiPartitionQueryService.FilterNotEquals) {
    def fromProto: Filter.NotEquals = {
      Filter.NotEquals(
        fne.getFilter.getValueStringsList.get(0)
      )
    }
  }

  // FilterEqualsRegex
  implicit class FilterEqualsRegexToProtoConverter(fe: Filter.EqualsRegex) {
    def toProto: GrpcMultiPartitionQueryService.FilterEqualsRegex = {
      val builder = GrpcMultiPartitionQueryService.FilterEqualsRegex.newBuilder()
      builder.setFilter(getProtoFilter(fe))
      builder.build()
    }
  }

  implicit class FilterEqualsRegexFromProtoConverter(fe: GrpcMultiPartitionQueryService.FilterEqualsRegex) {
    def fromProto: Filter.EqualsRegex = {
      Filter.EqualsRegex(
        fe.getFilter.getValueStringsList.get(0)
      )
    }
  }

  // FilterNotEqualsRegex
  implicit class FilterNotEqualsRegexToProtoConverter(fner: Filter.NotEqualsRegex) {
    def toProto: GrpcMultiPartitionQueryService.FilterNotEqualsRegex = {
      val builder = GrpcMultiPartitionQueryService.FilterNotEqualsRegex.newBuilder()
      builder.setFilter(getProtoFilter(fner))
      builder.build()
    }
  }

  implicit class FilterNotEqualsRegexFromProtoConverter(fner: GrpcMultiPartitionQueryService.FilterNotEqualsRegex) {
    def fromProto: Filter.NotEqualsRegex = {
      Filter.NotEqualsRegex(
        fner.getFilter.getValueStringsList.get(0)
      )
    }
  }

  // FilterIn
  implicit class FilterInToProtoConverter(fi: Filter.In) {
    def toProto: GrpcMultiPartitionQueryService.FilterIn = {
      val builder = GrpcMultiPartitionQueryService.FilterIn.newBuilder()
      builder.setFilter(getProtoFilter(fi))
      builder.build()
    }
  }

  implicit class FilterInFromProtoConverter(fi: GrpcMultiPartitionQueryService.FilterIn) {
    def fromProto: Filter.In = {
      Filter.In(
        fi.getFilter.getValueStringsList.asScala.toSet
      )
    }
  }

  // FilterAnd
  implicit class FilterAndToProtoConverter(fa: Filter.And) {
    def toProto: GrpcMultiPartitionQueryService.FilterAnd = {
      val builder = GrpcMultiPartitionQueryService.FilterAnd.newBuilder()
      builder.setFilter(getProtoFilter(fa))
      builder.setLeft(fa.left.toProto)
      builder.setRight(fa.right.toProto)
      builder.build()
    }
  }

  implicit class FilterAndFromProtoConverter(fi: GrpcMultiPartitionQueryService.FilterAnd) {
    def fromProto: Filter.And = {
      val left = fi.getLeft.fromProto
      val right = fi.getRight.fromProto
      Filter.And(left, right)
    }
  }

  implicit class FilterToProtoConverter(f: Filter) {
    def toProto(): GrpcMultiPartitionQueryService.FilterContainer = {
      val builder = GrpcMultiPartitionQueryService.FilterContainer.newBuilder()
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

  implicit class FilterContainerFromProtoConverter(fc: GrpcMultiPartitionQueryService.FilterContainer) {
    def fromProto: Filter = {
      import GrpcMultiPartitionQueryService.FilterContainer.FilterCase
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
    def toProto: GrpcMultiPartitionQueryService.ColumnFilter = {
      val builder = GrpcMultiPartitionQueryService.ColumnFilter.newBuilder()
      builder.setColumn(cf.column)
      builder.setFilter(cf.filter.toProto())
      builder.build()
    }
  }

  implicit class ColumnFilterFromProtoConverter(cf: GrpcMultiPartitionQueryService.ColumnFilter) {
    def fromProto: ColumnFilter = {
      ColumnFilter(cf.getColumn, cf.getFilter.fromProto)
    }
  }

  // QueryContext
  implicit class QueryContextToProtoConverter(qc: QueryContext) {
    def toProto: GrpcMultiPartitionQueryService.QueryContext = {
      val builder = GrpcMultiPartitionQueryService.QueryContext.newBuilder()
      builder.setOrigQueryParams(qc.origQueryParams.toProto)
      builder.setQueryId(qc.queryId)
      builder.setSubmitTime(qc.submitTime)
      builder.setPlannerParams(qc.plannerParams.toProto)
      val javaTraceInfoMap = mapAsJavaMap(qc.traceInfo)
      builder.putAllTraceInfo(javaTraceInfoMap)
      builder.build()
    }
  }

  implicit class QueryContextFromProtoConverter(qcProto: GrpcMultiPartitionQueryService.QueryContext) {
    def fromProto: QueryContext = {
      val originalQueryParams = qcProto.getOrigQueryParams().fromProto
      val plannerParams = qcProto.getPlannerParams.fromProto
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

    def toProto: GrpcMultiPartitionQueryService.PlanDispatcher = {
      val builder = GrpcMultiPartitionQueryService.PlanDispatcher.newBuilder()
      builder.setClusterName(pd.clusterName)
      builder.setIsLocalCall(pd.isLocalCall)
      builder.build()
    }

    def toPlanDispatcherContainer : GrpcMultiPartitionQueryService.PlanDispatcherContainer = {
      val builder = GrpcMultiPartitionQueryService.PlanDispatcherContainer.newBuilder()
      pd match {
        case apd: ActorPlanDispatcher => builder.setActorPlanDispatcher(apd.toProto)
        case ippd: InProcessPlanDispatcher => builder.setInProcessPlanDispatcher(ippd.toProto)
        case rapd: RemoteActorPlanDispatcher => builder.setRemoteActorPlanDispatcher(rapd.toProto)
        case gpd: GrpcPlanDispatcher => builder.setGrpcPlanDispatcher(gpd.toProto)
        case _ => throw new IllegalArgumentException(s"Unexpected PlanDispatcher subclass ${pd.getClass.getName}")
      }
      builder.build()

    }
  }

  implicit class ActorPlanDispatcherToProtoConverter(apd: filodb.coordinator.ActorPlanDispatcher) {
    def toProto(): GrpcMultiPartitionQueryService.ActorPlanDispatcher = {
      val builder = GrpcMultiPartitionQueryService.ActorPlanDispatcher.newBuilder()
      builder.setPlanDispatcher(apd.asInstanceOf[filodb.query.exec.PlanDispatcher].toProto)
      builder.setActorPath(akka.serialization.Serialization.serializedActorPath(apd.target))
      builder.build()
    }
  }

  implicit class ActorPlanDispatcherFromProtoConverter(apd: GrpcMultiPartitionQueryService.ActorPlanDispatcher) {
    def fromProto: ActorPlanDispatcher = {
      val serialization = SerializationExtension(ActorSystemHolder.system)
      val deserializedActorRef = serialization.system.provider.resolveActorRef(apd.getActorPath)
      val dispatcher = ActorPlanDispatcher(
        deserializedActorRef, apd.getPlanDispatcher.getClusterName
      )
      dispatcher
    }
  }

  implicit class RemoteActorPlanDispatcherToProtoConverter(apd: filodb.coordinator.RemoteActorPlanDispatcher) {
    def toProto(): GrpcMultiPartitionQueryService.RemoteActorPlanDispatcher = {
      val builder = GrpcMultiPartitionQueryService.RemoteActorPlanDispatcher.newBuilder()
      builder.setPlanDispatcher(apd.asInstanceOf[filodb.query.exec.PlanDispatcher].toProto)
      builder.setActorPath(apd.path)
      builder.build()
    }
  }

  implicit class RemoteActorPlanDispatcherFromProtoConverter(
    rapd: GrpcMultiPartitionQueryService.RemoteActorPlanDispatcher
  ) {
    def fromProto: RemoteActorPlanDispatcher = {
      val dispatcher = RemoteActorPlanDispatcher(
        rapd.getActorPath, rapd.getPlanDispatcher.getClusterName
      )
      dispatcher
    }
  }

  implicit class InProcessPlanDispatcherToProtoConverter(ippd: filodb.query.exec.InProcessPlanDispatcher) {
    def toProto(): GrpcMultiPartitionQueryService.InProcessPlanDispatcher = {
      val builder = GrpcMultiPartitionQueryService.InProcessPlanDispatcher.newBuilder()
      //builder.setPlanDispatcher(ippd.asInstanceOf[filodb.query.exec.PlanDispatcher].toProto)
      builder.setQueryConfig(ippd.queryConfig.toProto)
      builder.build()
    }
  }

  implicit class InProcessPlanDispatcherFromProtoConverter(apd: GrpcMultiPartitionQueryService.InProcessPlanDispatcher) {
    def fromProto: InProcessPlanDispatcher = {
      InProcessPlanDispatcher(apd.getQueryConfig.fromProto)
    }
  }

  implicit class GrpcPlanDispatcherToProtoConverter(gpd: filodb.coordinator.GrpcPlanDispatcher) {
    def toProto(): GrpcMultiPartitionQueryService.GrpcPlanDispatcher = {
      val builder = GrpcMultiPartitionQueryService.GrpcPlanDispatcher.newBuilder()
      builder.setEndpoint(gpd.endpoint)
      builder.setRequestTimeoutMs(gpd.requestTimeoutMs)
      builder.build()
    }
  }

  implicit class GrpcPlanDispatcherFromProtoConverter(gpd: GrpcMultiPartitionQueryService.GrpcPlanDispatcher) {
    def fromProto: GrpcPlanDispatcher = {
      val dispatcher = GrpcPlanDispatcher(gpd.getEndpoint, gpd.getRequestTimeoutMs)
      dispatcher
    }
  }

  implicit class DatasetRefToProtoConverter(dr: filodb.core.DatasetRef) {
    def toProto(): GrpcMultiPartitionQueryService.DatasetRef = {
      val builder = GrpcMultiPartitionQueryService.DatasetRef.newBuilder()
      builder.setDataset(dr.dataset)
      builder.clearDatabase()
      dr.database.foreach(db =>builder.setDatabase(db))
      builder.build()
    }
  }

  implicit class DatasetRefFromProtoConverter(dr: GrpcMultiPartitionQueryService.DatasetRef) {
    def fromProto(): filodb.core.DatasetRef = {
      val database = if (dr.hasDatabase) Some(dr.getDatabase) else None
      val datasetRef = filodb.core.DatasetRef(dr.getDataset, database)
      datasetRef
    }
  }

  implicit class QueryCommandToProtoConverter(qc: QueryCommand) {
    def toProto : GrpcMultiPartitionQueryService.QueryCommand = {
      val builder = GrpcMultiPartitionQueryService.QueryCommand.newBuilder()
      builder.setSubmitTime(qc.submitTime)
      builder.setDatasetRef(qc.dataset.toProto)
      builder.build()
    }
  }

  implicit class AggregationOperatorToProtoConverter(ao : filodb.query.AggregationOperator) {
    def toProto : GrpcMultiPartitionQueryService.AggregationOperator = {
      ao match {
        case AggregationOperator.TopK => GrpcMultiPartitionQueryService.AggregationOperator.TOP_K
        case AggregationOperator.CountValues => GrpcMultiPartitionQueryService.AggregationOperator.COUNT_VALUES
        case AggregationOperator.Count => GrpcMultiPartitionQueryService.AggregationOperator.COUNT
        case AggregationOperator.Group => GrpcMultiPartitionQueryService.AggregationOperator.GROUP
        case AggregationOperator.BottomK => GrpcMultiPartitionQueryService.AggregationOperator.BOTTOM_K
        case AggregationOperator.Min => GrpcMultiPartitionQueryService.AggregationOperator.MIN
        case AggregationOperator.Avg => GrpcMultiPartitionQueryService.AggregationOperator.AVG
        case AggregationOperator.Sum => GrpcMultiPartitionQueryService.AggregationOperator.SUM
        case AggregationOperator.Stddev => GrpcMultiPartitionQueryService.AggregationOperator.STDDEV
        case AggregationOperator.Stdvar => GrpcMultiPartitionQueryService.AggregationOperator.STDVAR
        case AggregationOperator.Quantile => GrpcMultiPartitionQueryService.AggregationOperator.QUANTILE
        case AggregationOperator.Max => GrpcMultiPartitionQueryService.AggregationOperator.MAX
        case AggregationOperator.Absent => throw new UnsupportedOperationException("Absent is not supported")
      }
    }
  }

  implicit class AggregationOperatorFromProtoConverter(ao: GrpcMultiPartitionQueryService.AggregationOperator) {
    def fromProto: AggregationOperator = {
      ao match {
        case GrpcMultiPartitionQueryService.AggregationOperator.TOP_K => AggregationOperator.TopK
        case GrpcMultiPartitionQueryService.AggregationOperator.COUNT_VALUES => AggregationOperator.CountValues
        case GrpcMultiPartitionQueryService.AggregationOperator.COUNT => AggregationOperator.Count
        case GrpcMultiPartitionQueryService.AggregationOperator.GROUP => AggregationOperator.Group
        case GrpcMultiPartitionQueryService.AggregationOperator.BOTTOM_K => AggregationOperator.BottomK
        case GrpcMultiPartitionQueryService.AggregationOperator.MIN => AggregationOperator.Min
        case GrpcMultiPartitionQueryService.AggregationOperator.AVG => AggregationOperator.Avg
        case GrpcMultiPartitionQueryService.AggregationOperator.SUM => AggregationOperator.Sum
        case GrpcMultiPartitionQueryService.AggregationOperator.STDDEV => AggregationOperator.Stddev
        case GrpcMultiPartitionQueryService.AggregationOperator.STDVAR => AggregationOperator.Stdvar
        case GrpcMultiPartitionQueryService.AggregationOperator.QUANTILE => AggregationOperator.Quantile
        case GrpcMultiPartitionQueryService.AggregationOperator.MAX => AggregationOperator.Max
        case _ => throw new IllegalArgumentException("Unknown aggregation operator")
      }
    }
  }

  def getAggregateParameter(ap: Any): GrpcMultiPartitionQueryService.AggregateParameter = {
    val builder = GrpcMultiPartitionQueryService.AggregateParameter.newBuilder()
    ap match {
      case l: Long => builder.setLongParameter(l)
      case i: Int => builder.setIntParameter(i)
      case d: Double => builder.setDoubleParameter(d)
      case s: String => builder.setStringParameter(s)
      case _ => throw new IllegalArgumentException(s"Unexpected aggregate parameter ${ap}")
    }
    builder.build()
  }

  implicit class AggregateParameterFromProto(ap: GrpcMultiPartitionQueryService.AggregateParameter) {
    def fromProto() : Any = {
      ap.getAggregateParameterCase match {
        case GrpcMultiPartitionQueryService.AggregateParameter.AggregateParameterCase.LONGPARAMETER => ap.getLongParameter
        case GrpcMultiPartitionQueryService.AggregateParameter.AggregateParameterCase.INTPARAMETER => ap.getIntParameter
        case GrpcMultiPartitionQueryService.AggregateParameter.AggregateParameterCase.DOUBLEPARAMETER => ap.getDoubleParameter
        case GrpcMultiPartitionQueryService.AggregateParameter.AggregateParameterCase.STRINGPARAMETER => ap.getStringParameter
        case GrpcMultiPartitionQueryService.AggregateParameter.AggregateParameterCase.AGGREGATEPARAMETER_NOT_SET =>
          throw new IllegalArgumentException("aggregate parameter is not set")
      }
    }
  }

  // AggregateClauseType
  implicit class AggregateClauseTypeToProto(act: filodb.query.AggregateClause.ClauseType.Value) {
    def toProto : GrpcMultiPartitionQueryService.AggregateClauseType = {
      act match {
        case filodb.query.AggregateClause.ClauseType.By => GrpcMultiPartitionQueryService.AggregateClauseType.BY
        case filodb.query.AggregateClause.ClauseType.Without => GrpcMultiPartitionQueryService.AggregateClauseType.WITHOUT
      }
    }
  }

  implicit class AggregateClauseTypeFromProto(act: GrpcMultiPartitionQueryService.AggregateClauseType) {
    def fromProto(): filodb.query.AggregateClause.ClauseType.Value = {
      act match  {
        case GrpcMultiPartitionQueryService.AggregateClauseType.BY => filodb.query.AggregateClause.ClauseType.By
        case GrpcMultiPartitionQueryService.AggregateClauseType.WITHOUT => filodb.query.AggregateClause.ClauseType.Without
        case GrpcMultiPartitionQueryService.AggregateClauseType.UNRECOGNIZED =>
          throw new IllegalArgumentException("Unrecognized aggregate clause type")
      }
    }
  }

  implicit class AggregateClauseToProto(ac : filodb.query.AggregateClause) {
    def toProto : GrpcMultiPartitionQueryService.AggregateClause = {
      val builder = GrpcMultiPartitionQueryService.AggregateClause.newBuilder()
      builder.setClauseType(ac.clauseType.toProto)
      ac.labels.foreach(l => builder.addLabels(l))
      builder.build()
    }
  }

  implicit class AggregateClauseFromProto(ac: GrpcMultiPartitionQueryService.AggregateClause) {
    def fromProto(): filodb.query.AggregateClause = {
      filodb.query.AggregateClause(
        ac.getClauseType.fromProto,
        ac.getLabelsList.asScala
      )
    }
  }

  implicit class ExecPlanFuncArgsToProtoConverter(epfs : ExecPlanFuncArgs) {
    def toProto() : GrpcMultiPartitionQueryService.ExecPlanFuncArgs = {
      val builder = GrpcMultiPartitionQueryService.ExecPlanFuncArgs.newBuilder()
      builder.setExecPlan(epfs.execPlan.toExecPlanContainerProto)
      builder.setTimeStepParams(epfs.timeStepParams.toProto)
      builder.build()
    }
  }

  implicit class ExecPlanFuncArgsFromProtoConverter(epfa: GrpcMultiPartitionQueryService.ExecPlanFuncArgs) {
    def fromProto(queryContext: QueryContext) : ExecPlanFuncArgs = {
      ExecPlanFuncArgs(
        epfa.getExecPlan.fromProto(queryContext),
        epfa.getTimeStepParams.fromProto
      )
    }
  }

  implicit class TimeFuncArgsToProtoConverter(tfa : TimeFuncArgs) {
    def toProto() : GrpcMultiPartitionQueryService.TimeFuncArgs = {
      val builder = GrpcMultiPartitionQueryService.TimeFuncArgs.newBuilder()
      builder.setTimeStepParms(tfa.timeStepParams.toProto)
      builder.build()
    }
  }

  implicit class TimeFuncArgsFromProtoConverter(tfa: GrpcMultiPartitionQueryService.TimeFuncArgs) {
    def fromProto(): TimeFuncArgs = {
      TimeFuncArgs(
        tfa.getTimeStepParms.fromProto
      )
    }
  }

  implicit class StaticFuncArgsToProtoConverter(sfa: StaticFuncArgs) {
    def toProto(): GrpcMultiPartitionQueryService.StaticFuncArgs = {
      val builder = GrpcMultiPartitionQueryService.StaticFuncArgs.newBuilder()
      builder.setScalar(sfa.scalar)
      builder.setTimeStepParams(sfa.timeStepParams.toProto)
      builder.build()
    }
  }

  implicit class StaticFuncArgsFromProtoConverter(sfa: GrpcMultiPartitionQueryService.StaticFuncArgs) {
    def fromProto(): StaticFuncArgs = {
      StaticFuncArgs(
        sfa.getScalar,
        sfa.getTimeStepParams.fromProto
      )
    }
  }

  implicit class FuncArgToProto(fa : FuncArgs) {
    def toProto : GrpcMultiPartitionQueryService.FuncArgs = {
      val builder = GrpcMultiPartitionQueryService.FuncArgs.newBuilder()
      fa match {
        case epfa : ExecPlanFuncArgs => {
          val epfaProto : GrpcMultiPartitionQueryService.ExecPlanFuncArgs  = epfa.toProto
          builder.setExecPlanFuncArgs(epfaProto)
        }
        case tfa : TimeFuncArgs => {
          val tfaProto : GrpcMultiPartitionQueryService.TimeFuncArgs = tfa.toProto
          builder.setTimeFuncArgs(tfa.toProto)
        }
        case sfa : StaticFuncArgs => {
          val sfaProto : GrpcMultiPartitionQueryService.StaticFuncArgs = sfa.toProto
          builder.setStaticFuncArgs(sfaProto)
        }
      }
      builder.build()
    }
  }

  implicit class FuncArgsFromProtoConverter(fa: GrpcMultiPartitionQueryService.FuncArgs) {
    def fromProto(queryContext: QueryContext) : FuncArgs = {
      fa.getFuncArgTypeCase match {
        case FuncArgTypeCase.EXECPLANFUNCARGS => fa.getExecPlanFuncArgs.fromProto(queryContext)
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
    def toProto : GrpcMultiPartitionQueryService.StitchRvsMapper = {
      val builder = GrpcMultiPartitionQueryService.StitchRvsMapper.newBuilder()
      builder.build()
    }
  }

  implicit class StitchRvsMapperFromProtoConverter(srm: GrpcMultiPartitionQueryService.StitchRvsMapper) {
    def fromProto : StitchRvsMapper = {
      val outputRvRange = if (srm.hasOutputRvRange) {
        Option(srm.getOutputRvRange().fromProto)
      } else {
        None
      }
      StitchRvsMapper(outputRvRange)
    }
  }

//  def getAggregateParameter(parameter : Any) : GrpcMultiPartitionQueryService.AggregateParameter = {
//    val builder = GrpcMultiPartitionQueryService.AggregateParameter.newBuilder()
//    parameter match {
//      case i: Int => builder.setIntParameter(i)
//      case l: Long => builder.setLongParameter(l)
//      case d: Double => builder.setDoubleParameter(d)
//      case s: String => builder.setStringParameter(s)
//    }
//    builder.build()
//  }
//
//  def getAggregateParametersFromProto(params: java.util.List[filodb.grpc.GrpcMultiPartitionQueryService.AggregateParameter]): Seq[Any] = {
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
    def toProto: GrpcMultiPartitionQueryService.AggregateMapReduce = {
      val builder = GrpcMultiPartitionQueryService.AggregateMapReduce.newBuilder()
      builder.setAggrOp(amr.aggrOp.toProto)
      amr.aggrParams.foreach(p => builder.addAggrParams(getAggregateParameter(p)))
      amr.clauseOpt.foreach(cp => builder.setClauseOpt(cp.toProto))
      amr.funcParams.foreach(fp => builder.addFuncParams(fp.toProto))
      builder.build()
    }
  }

  implicit class AggregateMapReduceFromProtoConverter(amr: GrpcMultiPartitionQueryService.AggregateMapReduce) {
    def fromProto(queryContext: QueryContext): AggregateMapReduce = {
      AggregateMapReduce(
        amr.getAggrOp.fromProto,
        amr.getAggrParamsList().asScala.toSeq.map(ap => ap.fromProto()),
        if (amr.hasClauseOpt) Option(amr.getClauseOpt.fromProto()) else None,
        amr.getFuncParamsList.asScala.map(fa => fa.fromProto(queryContext))
      )
    }
  }

  //HistToPromSeriesMapper
  implicit class HistToPromSeriesMapperToProtoConverter(htpsm : HistToPromSeriesMapper) {
    def toProto: GrpcMultiPartitionQueryService.HistToPromSeriesMapper = {
      val builder = GrpcMultiPartitionQueryService.HistToPromSeriesMapper.newBuilder()
      builder.setSch(htpsm.sch.toProto)
      builder.build()
    }
  }

  implicit class HistToPromSeriesMapperFromProtoConverter(htpsm: GrpcMultiPartitionQueryService.HistToPromSeriesMapper) {
    def fromProto: HistToPromSeriesMapper = {
      HistToPromSeriesMapper(htpsm.getSch.fromProto)
    }
  }

  //LabelCardinalityPresenter
  implicit class LabelCardinalityPresenterToProtoConverter(lcp : LabelCardinalityPresenter) {
    def toProto: GrpcMultiPartitionQueryService.LabelCardinalityPresenter = {
      val builder = GrpcMultiPartitionQueryService.LabelCardinalityPresenter.newBuilder()
      lcp.funcParams.foreach(fp => builder.addFuncParams(fp.toProto))
      builder.build()
    }
  }

  implicit class LabelCardinalityPresenterFromProtoConverter(lcp: GrpcMultiPartitionQueryService.LabelCardinalityPresenter) {
    def fromProto(queryContext: QueryContext): LabelCardinalityPresenter = {
      val funcParams = lcp.getFuncParamsList.asScala.map(fa => fa.fromProto(queryContext))
      new LabelCardinalityPresenter(funcParams)
    }
  }

  //HistogramQuantileMapper
  implicit class HistogramQuantileMapperToProtoConverter(hqm: HistogramQuantileMapper) {
    def toProto: GrpcMultiPartitionQueryService.HistogramQuantileMapper = {
      val builder = GrpcMultiPartitionQueryService.HistogramQuantileMapper.newBuilder()
      hqm.funcParams.foreach(fp => builder.addFuncParams(fp.toProto))
      builder.build()
    }
  }

  implicit class HistogramQuantileMapperFromProtoConverter(hqm: GrpcMultiPartitionQueryService.HistogramQuantileMapper) {
    def fromProto(queryContext: QueryContext): HistogramQuantileMapper = {
      val funcParams = hqm.getFuncParamsList.asScala.map(fa => fa.fromProto(queryContext))
      HistogramQuantileMapper(funcParams)
    }
  }

  //InstantVectorFunctionMapper
  implicit class InstantVectorFunctionMapperToProtoConverter(ivfm : InstantVectorFunctionMapper) {
    def toProto: GrpcMultiPartitionQueryService.InstantVectorFunctionMapper = {
      val builder = GrpcMultiPartitionQueryService.InstantVectorFunctionMapper.newBuilder()
      builder.setFunction(ivfm.function.toProto)
      ivfm.funcParams.foreach(fa => builder.addFuncParams(fa.toProto))
      builder.build()
    }
  }

  implicit class InstantVectorFunctionMapperFromProtoConverter(ivfm: GrpcMultiPartitionQueryService.InstantVectorFunctionMapper) {
    def fromProto(queryContext: QueryContext): InstantVectorFunctionMapper = {
      val funcParams = ivfm.getFuncParamsList.asScala.toSeq.map(fp => fp.fromProto(queryContext))
      InstantVectorFunctionMapper(
        ivfm.getFunction.fromProto,
        funcParams
      )
    }
  }

  // PeriodicSamplesMapper
  implicit class PeriodicSamplesMapperToProtoConverter(psm : PeriodicSamplesMapper) {
    def toProto: GrpcMultiPartitionQueryService.PeriodicSamplesMapper = {
      val builder = GrpcMultiPartitionQueryService.PeriodicSamplesMapper.newBuilder()
      builder.setStartMs(psm.startMs)
      builder.setStepMs(psm.stepMs)
      builder.setEndMs(psm.endMs)
      psm.window.foreach(w => builder.setWindow(w))
      psm.functionId.foreach(fi => builder.setFunctionId(fi.toProto))
      builder.setStepMultipleNotationUsed(psm.stepMultipleNotationUsed)
      psm.funcParams.foreach(fp => builder.addFuncParams(fp.toProto))
      psm.offsetMs.foreach(o => builder.setOffsetMs(o))
      builder.setRawSource(psm.rawSource)
      builder.setLeftInclusiveWindow(psm.leftInclusiveWindow)
      builder.build()
    }
  }

  implicit class PeriodicSamplesMapperFromProtoConverter(psm: GrpcMultiPartitionQueryService.PeriodicSamplesMapper) {
    def fromProto(queryContext: QueryContext): PeriodicSamplesMapper = {
      val window = if (psm.hasWindow) Option(psm.getWindow) else None
      val functionId = if (psm.hasFunctionId) Option(psm.getFunctionId.fromProto) else None
      val funcParams = psm.getFuncParamsList.asScala.toSeq.map(fa => fa.fromProto(queryContext))
      val offsetMs = if (psm.hasOffsetMs) Option(psm.getOffsetMs) else None
      PeriodicSamplesMapper(
        psm.getStartMs,
        psm.getStepMs,
        psm.getEndMs,
        window,
        functionId,
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
    def toProto: GrpcMultiPartitionQueryService.SortFunctionMapper = {
      val builder = GrpcMultiPartitionQueryService.SortFunctionMapper.newBuilder()
      builder.setFunction(sfm.function.toProto)
      builder.build()
    }
  }

  implicit class SortFunctionMapperFromProtoConverter(srm: GrpcMultiPartitionQueryService.SortFunctionMapper) {
    def fromProto: SortFunctionMapper = {
      SortFunctionMapper(srm.getFunction.fromProto)
    }
  }

  // MiscellaneousFunctionMapper
  implicit class MiscellaneousFunctionMapperToProtoConverter(mfm : MiscellaneousFunctionMapper) {
    def toProto: GrpcMultiPartitionQueryService.MiscellaneousFunctionMapper = {
      val builder = GrpcMultiPartitionQueryService.MiscellaneousFunctionMapper.newBuilder()
      builder.setFunction(mfm.function.toProto)
      mfm.funcStringParam.foreach(fsp => builder.addFuncStringParam(fsp))
      mfm.funcParams.foreach(fa => builder.addFuncParams(fa.toProto))
      builder.build()
    }
  }

  implicit class MiscellaneousFunctionMapperFromProtoConverter(mfm: GrpcMultiPartitionQueryService.MiscellaneousFunctionMapper) {
    def fromProto(queryContext: QueryContext): MiscellaneousFunctionMapper = {
      MiscellaneousFunctionMapper(
        mfm.getFunction.fromProto,
        mfm.getFuncStringParamList.asScala.toSeq,
        mfm.getFuncParamsList.asScala.toSeq.map(fa => fa.fromProto(queryContext))
      )
    }
  }

  // LimitFunctionMapper
  implicit class LimitFunctionMapperToProtoConverter(lfm : LimitFunctionMapper) {
    def toProto: GrpcMultiPartitionQueryService.LimitFunctionMapper = {
      val builder = GrpcMultiPartitionQueryService.LimitFunctionMapper.newBuilder()
      builder.setLimitToApply(lfm.limitToApply)
      builder.build()
    }
  }

  implicit class LimitFunctionMapperFromProtoConverter(lfm: GrpcMultiPartitionQueryService.LimitFunctionMapper) {
    def fromProto: LimitFunctionMapper = {
      LimitFunctionMapper(lfm.getLimitToApply)
    }
  }

  // ScalarOperationMapper
  implicit class ScalarOperationMapperToProtoConverter(som : ScalarOperationMapper) {
    def toProto: GrpcMultiPartitionQueryService.ScalarOperationMapper = {
      val builder = GrpcMultiPartitionQueryService.ScalarOperationMapper.newBuilder()
      builder.setOperator(som.operator.toProto)
      builder.setScalarOnLhs(som.scalarOnLhs)
      som.funcParams.foreach(fa => builder.addFuncParams(fa.toProto))
      builder.build()
    }
  }

  implicit class ScalarOperationMapperFromProtoConverter(som: GrpcMultiPartitionQueryService.ScalarOperationMapper) {
    def fromProto(queryContext: QueryContext): ScalarOperationMapper = {
      ScalarOperationMapper(
        som.getOperator.fromProto,
        som.getScalarOnLhs,
        som.getFuncParamsList.asScala.toSeq.map(fa => fa.fromProto(queryContext))
      )
    }
  }

  // ScalarFunctionMapper
  implicit class ScalarFunctionMapperToProtoConverter(sfm: ScalarFunctionMapper) {
    def toProto: GrpcMultiPartitionQueryService.ScalarFunctionMapper = {
      val builder = GrpcMultiPartitionQueryService.ScalarFunctionMapper.newBuilder()
      builder.setFunction(sfm.function.toProto)
      builder.setTimeStepParams(sfm.timeStepParams.toProto)
      builder.build()
    }
  }

  implicit class ScalarFunctionMapperFromProtoConverter(sfm: GrpcMultiPartitionQueryService.ScalarFunctionMapper) {
    def fromProto: ScalarFunctionMapper = {
      ScalarFunctionMapper(
        sfm.getFunction.fromProto,
        sfm.getTimeStepParams.fromProto
      )
    }
  }

  // VectorFunctionMapper
  implicit class VectorFunctionMapperToProtoConverter(vfm: VectorFunctionMapper) {
    def toProto: GrpcMultiPartitionQueryService.VectorFunctionMapper = {
      val builder = GrpcMultiPartitionQueryService.VectorFunctionMapper.newBuilder()
      builder.build()
    }
  }

  implicit class VectorFunctionMapperFromProtoConverter(srm: GrpcMultiPartitionQueryService.VectorFunctionMapper) {
    def fromProto: VectorFunctionMapper = {
      VectorFunctionMapper()
    }
  }

  // AggregatePresenter
  implicit class AggregatePresenterToProtoConverter(ap : AggregatePresenter) {
    def toProto: GrpcMultiPartitionQueryService.AggregatePresenter = {
      val builder = GrpcMultiPartitionQueryService.AggregatePresenter.newBuilder()
      builder.setAggrOp(ap.aggrOp.toProto)
      ap.aggrParams.foreach(aggrParam => builder.addAggrParams(getAggregateParameter(aggrParam)))
      builder.setRangeParams(ap.rangeParams.toProto)
      ap.funcParams.foreach(fa => builder.addFuncParams(fa.toProto))
      builder.build()
    }
  }

  implicit class AggregatePresenterFromProtoConverter(ap: GrpcMultiPartitionQueryService.AggregatePresenter) {
    def fromProto(queryContext: QueryContext): AggregatePresenter = {
      AggregatePresenter(
        ap.getAggrOp.fromProto,
        ap.getAggrParamsList.asScala.toSeq.map(aggrParam => aggrParam.fromProto()),
        ap.getRangeParams.fromProto,
        ap.getFuncParamsList.asScala.toSeq.map(fp => fp.fromProto(queryContext))
      )
    }
  }

  // AbsentFunctionMapper
  implicit class AbsentFunctionMapperToProtoConverter(afm: AbsentFunctionMapper) {
    def toProto: GrpcMultiPartitionQueryService.AbsentFunctionMapper = {
      val builder = GrpcMultiPartitionQueryService.AbsentFunctionMapper.newBuilder()
      afm.columnFilter.foreach(cf => builder.addColumnFilter(cf.toProto))
      builder.setRangeParams(afm.rangeParams.toProto)
      builder.setMetricColumn(afm.metricColumn)
      builder.build()
    }
  }

  implicit class AbsentFunctionMapperFromProtoConverter(afm: GrpcMultiPartitionQueryService.AbsentFunctionMapper) {
    def fromProto: AbsentFunctionMapper = {
      AbsentFunctionMapper(
        afm.getColumnFilterList.asScala.toSeq.map(cf => cf.fromProto),
        afm.getRangeParams.fromProto,
        afm.getMetricColumn
      )
    }
  }

  // RepeatTransformer
  implicit class RepeatTransformerToProtoConverter(rpt: RepeatTransformer) {
    def toProto: GrpcMultiPartitionQueryService.RepeatTransformer = {
      val builder = GrpcMultiPartitionQueryService.RepeatTransformer.newBuilder()
      builder.setStartMs(rpt.startMs)
      builder.setStepMs(rpt.stepMs)
      builder.setEndMs(rpt.endMs)
      builder.setExecPlan(rpt.execPlan)
      builder.build()
    }
  }

  implicit class RepeatTransformerFromProtoConverter(rpt: GrpcMultiPartitionQueryService.RepeatTransformer) {
    def fromProto(): RepeatTransformer = {
      //      RepeatTransformer(0, 0, 0, "")
      RepeatTransformer(rpt.getStartMs, rpt.getStepMs, rpt.getEndMs, rpt.getExecPlan)
    }
  }


  implicit class RangeVectorTransformerToProtoConverter(rangeVectorTransformer: RangeVectorTransformer) {
    def toProto(): GrpcMultiPartitionQueryService.RangeVectorTransformerContainer = {
      val b = RangeVectorTransformerContainer.newBuilder()
      rangeVectorTransformer match {
        case srm: StitchRvsMapper => b.setStitchRvsMapper(srm.toProto).build()
        case amr: AggregateMapReduce => b.setAggregateMapReduce(amr.toProto).build()
        case htpsm: HistToPromSeriesMapper => b.setHistToPromSeriesMapper(htpsm.toProto).build()
        case lcp: LabelCardinalityPresenter => b.setLabelCardinalityPresenter(lcp.toProto).build()
        case hqm: HistogramQuantileMapper => b.setHistogramQuantileMapper(hqm.toProto).build()
        case ivfm: InstantVectorFunctionMapper => b.setInstantVectorFunctionMapper(ivfm.toProto).build()
        case psm: PeriodicSamplesMapper => b.setPeriodicSamplesMapper(psm.toProto).build()
        case sfm: SortFunctionMapper => b.setSortFunctionMapper(sfm.toProto).build()
        case mfm: MiscellaneousFunctionMapper => b.setMiscellaneousFunctionMapper(mfm.toProto).build()
        case lfm: LimitFunctionMapper => b.setLimitFunctionMapper(lfm.toProto).build()
        case som: ScalarOperationMapper => b.setScalarOperationMapper(som.toProto).build()
        case sfum: ScalarFunctionMapper => b.setScalarFunctionMapper(sfum.toProto).build()
        case vfm: VectorFunctionMapper => b.setVectorFunctionMapper(vfm.toProto).build()
        case ap: AggregatePresenter => b.setAggregatePresenter(ap.toProto).build()
        case afm: AbsentFunctionMapper => b.setAbsentFunctionMapper(afm.toProto).build()
        case rpt: RepeatTransformer => b.setRepeatTransformer(rpt.toProto).build()
        case _ => throw new IllegalArgumentException("Unexpected Range Vector Transformer")
      }
    }
  }

  implicit class RangeVectorTransformerFromProtoConverter(rvtc: RangeVectorTransformerContainer) {
    def fromProto(queryContext: QueryContext): RangeVectorTransformer = {
      import filodb.grpc.GrpcMultiPartitionQueryService.RangeVectorTransformerContainer.RangeVectorTransfomerCase
      rvtc.getRangeVectorTransfomerCase match {
        case RangeVectorTransfomerCase.STITCHRVSMAPPER => rvtc.getStitchRvsMapper().fromProto
        case RangeVectorTransfomerCase.AGGREGATEMAPREDUCE => rvtc.getAggregateMapReduce().fromProto(queryContext)
        case RangeVectorTransfomerCase.HISTTOPROMSERIESMAPPER => rvtc.getHistToPromSeriesMapper().fromProto
        case RangeVectorTransfomerCase.LABELCARDINALITYPRESENTER => rvtc.getLabelCardinalityPresenter().fromProto(queryContext)
        case RangeVectorTransfomerCase.HISTOGRAMQUANTILEMAPPER => rvtc.getHistogramQuantileMapper().fromProto(queryContext)
        case RangeVectorTransfomerCase.INSTANTVECTORFUNCTIONMAPPER => rvtc.getInstantVectorFunctionMapper().fromProto(queryContext)
        case RangeVectorTransfomerCase.PERIODICSAMPLESMAPPER => rvtc.getPeriodicSamplesMapper().fromProto(queryContext)
        case RangeVectorTransfomerCase.SORTFUNCTIONMAPPER => rvtc.getSortFunctionMapper().fromProto
        case RangeVectorTransfomerCase.MISCELLANEOUSFUNCTIONMAPPER => rvtc.getMiscellaneousFunctionMapper().fromProto(queryContext)
        case RangeVectorTransfomerCase.LIMITFUNCTIONMAPPER => rvtc.getLimitFunctionMapper().fromProto
        case RangeVectorTransfomerCase.SCALAROPERATIONMAPPER => rvtc.getScalarOperationMapper().fromProto(queryContext)
        case RangeVectorTransfomerCase.SCALARFUNCTIONMAPPER => rvtc.getScalarFunctionMapper().fromProto
        case RangeVectorTransfomerCase.VECTORFUNCTIONMAPPER => rvtc.getVectorFunctionMapper().fromProto
        case RangeVectorTransfomerCase.AGGREGATEPRESENTER => rvtc.getAggregatePresenter().fromProto(queryContext)
        case RangeVectorTransfomerCase.ABSENTFUNCTIONMAPPER => rvtc.getAbsentFunctionMapper().fromProto
        case RangeVectorTransfomerCase.REPEATTRANSFORMER => rvtc.getRepeatTransformer().fromProto
        case RangeVectorTransfomerCase.RANGEVECTORTRANSFOMER_NOT_SET =>
          throw new IllegalArgumentException("Unexpected Range Vector Transformer")
      }
    }
  }

  implicit class LeafExecPlanToProtoConverter(lep: LeafExecPlan) {

    def toProto(): GrpcMultiPartitionQueryService.LeafExecPlan = {
      val builder = GrpcMultiPartitionQueryService.LeafExecPlan.newBuilder()
      builder.setExecPlan(lep.asInstanceOf[filodb.query.exec.ExecPlan].toProto)
      builder.setSubmitTime(lep.submitTime)
      builder.build()
    }
  }


  implicit class NonLeafExecPlanToProtoConverter(mspe: filodb.query.exec.NonLeafExecPlan) {
    def toProto(): GrpcMultiPartitionQueryService.NonLeafExecPlan = {
      val builder = GrpcMultiPartitionQueryService.NonLeafExecPlan.newBuilder()
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
    def toProto(): GrpcMultiPartitionQueryService.LabelCardinalityReduceExec = {
      val builder = GrpcMultiPartitionQueryService.LabelCardinalityReduceExec.newBuilder()
      builder.setNonLeafExecPlan(lcre.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class LabelCardinalityReduceExecFromProtoConverter(lcre: GrpcMultiPartitionQueryService.LabelCardinalityReduceExec) {
    def fromProto(queryContext: QueryContext): LabelCardinalityReduceExec = {
      val execPlan = lcre.getNonLeafExecPlan().getExecPlan()
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[GrpcMultiPartitionQueryService.ExecPlanContainer] = lcre.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto(queryContext))
      val p = LabelCardinalityReduceExec(
        queryContext,
        planDispatcher,
        children
      )
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // MultiPartitionDistConcatExec
  implicit class MultiPartitionDistConcatExecToProtoConverter(mpdce: filodb.query.exec.MultiPartitionDistConcatExec) {
    def toProto(): GrpcMultiPartitionQueryService.MultiPartitionDistConcatExec = {
      val builder = GrpcMultiPartitionQueryService.MultiPartitionDistConcatExec.newBuilder()
      builder.setNonLeafExecPlan(mpdce.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class MultiPartitionDistConcatExecFromProtoConverter(mpdce: GrpcMultiPartitionQueryService.MultiPartitionDistConcatExec) {
    def fromProto(queryContext: QueryContext): filodb.query.exec.MultiPartitionDistConcatExec = {
      val execPlan = mpdce.getNonLeafExecPlan().getExecPlan()
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[GrpcMultiPartitionQueryService.ExecPlanContainer] = mpdce.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto(queryContext))
      val p = filodb.query.exec.MultiPartitionDistConcatExec(queryContext, planDispatcher, children)
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // LocalPartitionDistConcatExec
  implicit class LocalPartitionDistConcatExecToProtoConverter(mspe: filodb.query.exec.LocalPartitionDistConcatExec) {
    def toProto(): GrpcMultiPartitionQueryService.LocalPartitionDistConcatExec = {
      val builder = GrpcMultiPartitionQueryService.LocalPartitionDistConcatExec.newBuilder()
      builder.setNonLeafExecPlan(mspe.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class LocalPartitionDistConcatExecFromProtoConverter(lpdce: GrpcMultiPartitionQueryService.LocalPartitionDistConcatExec) {
    def fromProto(queryContext: QueryContext): LocalPartitionDistConcatExec = {
      val execPlan = lpdce.getNonLeafExecPlan().getExecPlan()
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[GrpcMultiPartitionQueryService.ExecPlanContainer] = lpdce.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto(queryContext))
      val p = LocalPartitionDistConcatExec(queryContext, planDispatcher, children)
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // SplitLocalPartitionDistConcatExec
  implicit class SplitLocalPartitionDistConcatExecToProtoConverter(
    mspe: filodb.query.exec.SplitLocalPartitionDistConcatExec
  ) {
    def toProto(): GrpcMultiPartitionQueryService.SplitLocalPartitionDistConcatExec = {
      val builder = GrpcMultiPartitionQueryService.SplitLocalPartitionDistConcatExec.newBuilder()
      builder.setNonLeafExecPlan(mspe.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class SplitLocalPartitionDistConcatExecFromProtoConverter(
    slpdce: GrpcMultiPartitionQueryService.SplitLocalPartitionDistConcatExec
  ) {
    def fromProto(queryContext: QueryContext): filodb.query.exec.SplitLocalPartitionDistConcatExec = {
      val execPlan = slpdce.getNonLeafExecPlan().getExecPlan()
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[GrpcMultiPartitionQueryService.ExecPlanContainer] = slpdce.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto(queryContext))
      val p = SplitLocalPartitionDistConcatExec(queryContext, planDispatcher, children, None)
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // LocalPartitionReduceAggregateExec
  implicit class LocalPartitionReduceAggregateExecToProtoConverter(lpra: LocalPartitionReduceAggregateExec) {
    def toProto(): GrpcMultiPartitionQueryService.LocalPartitionReduceAggregateExec = {
      val builder = GrpcMultiPartitionQueryService.LocalPartitionReduceAggregateExec.newBuilder()
      builder.setNonLeafExecPlan(lpra.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.setAggrOp(lpra.aggrOp.toProto)
      lpra.aggrParams.foreach(ap => builder.addAggrParams(getAggregateParameter(ap)))
      builder.build()
    }
  }

  implicit class LocalPartitionReduceAggregateExecFromProtoConverter(
    lcre: GrpcMultiPartitionQueryService.LocalPartitionReduceAggregateExec
  ) {
    def fromProto(queryContext: QueryContext): LocalPartitionReduceAggregateExec = {
      val execPlan = lcre.getNonLeafExecPlan().getExecPlan()
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[GrpcMultiPartitionQueryService.ExecPlanContainer] = lcre.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto(queryContext))

      val p = LocalPartitionReduceAggregateExec(
        queryContext: QueryContext,
        dispatcher: PlanDispatcher,
        children: Seq[ExecPlan],
        lcre.getAggrOp.fromProto,
        lcre.getAggrParamsList.asScala.toSeq.map(ap => ap.fromProto)
      )
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // MultiPartitionReduceAggregateExec
  implicit class MultiPartitionReduceAggregateExecToProtoConverter(mprae: MultiPartitionReduceAggregateExec) {
    def toProto(): GrpcMultiPartitionQueryService.MultiPartitionReduceAggregateExec = {
      val builder = GrpcMultiPartitionQueryService.MultiPartitionReduceAggregateExec.newBuilder()
      builder.setNonLeafExecPlan(mprae.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.setAggrOp(mprae.aggrOp.toProto)
      mprae.aggrParams.foreach(ap => builder.addAggrParams(getAggregateParameter(ap)))
      builder.build()
    }
  }

  implicit class MultiPartitionReduceAggregateExecFromProtoConverter(
    mprae: GrpcMultiPartitionQueryService.MultiPartitionReduceAggregateExec
  ) {
    def fromProto(queryContext: QueryContext): MultiPartitionReduceAggregateExec = {

      val execPlan = mprae.getNonLeafExecPlan().getExecPlan()
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[GrpcMultiPartitionQueryService.ExecPlanContainer] = mprae.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto(queryContext))

      val p = MultiPartitionReduceAggregateExec(
        queryContext: QueryContext,
        dispatcher: PlanDispatcher,
        children: Seq[ExecPlan],
        mprae.getAggrOp.fromProto,
        mprae.getAggrParamsList.asScala.toSeq.map(ap => ap.fromProto)
      )
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // BinaryJoinExec
  implicit class BinaryJoinExecToProtoConverter(bje: BinaryJoinExec) {
    def toProto(): GrpcMultiPartitionQueryService.BinaryJoinExec = {
      val builder = GrpcMultiPartitionQueryService.BinaryJoinExec.newBuilder()
      builder.setNonLeafExecPlan(bje.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      bje.lhs.foreach(ep => builder.addLhs(ep.toExecPlanContainerProto))
      bje.rhs.foreach(ep => builder.addRhs(ep.toExecPlanContainerProto))
      builder.setBinaryOp(bje.binaryOp.toProto)
      builder.setCardinality(bje.cardinality.toProto)
      builder.build()
      bje.on.foreach(onSeq => onSeq.foreach(on => builder.addOn(on)))
      bje.ignoring.foreach(ignoring => builder.addIgnoring(ignoring))
      bje.include.foreach(include => builder.addInclude(include))
      builder.setMetricColumn(bje.metricColumn)
      bje.outputRvRange.foreach(orr => builder.setOutputRvRange(orr.toProto))
      builder.build()
    }
  }

  implicit class BinaryJoinExecFromProtoConverter(bje: GrpcMultiPartitionQueryService.BinaryJoinExec) {
    def fromProto(queryContext: QueryContext): BinaryJoinExec = {
      val execPlan = bje.getNonLeafExecPlan().getExecPlan()
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val lhs: Seq[filodb.query.exec.ExecPlan] =
        bje.getLhsList.asScala.toSeq.map(c => c.fromProto(queryContext))
      val rhs: Seq[filodb.query.exec.ExecPlan] =
        bje.getRhsList.asScala.toSeq.map(c => c.fromProto(queryContext))
      val on = bje.getOnList.asScala.toSeq
      val onOption = if (on.isEmpty) None else Option(on)
      val ignoring = bje.getIgnoringList.asScala.toList
      val include = bje.getIncludeList.asScala.toList
      val outputRvRange =
        if (bje.hasOutputRvRange) Option(bje.getOutputRvRange.fromProto) else None

      val p = BinaryJoinExec(
        queryContext: QueryContext,
        dispatcher: PlanDispatcher,
        lhs: Seq[ExecPlan],
        rhs: Seq[ExecPlan],
        bje.getBinaryOp.fromProto,
        bje.getCardinality.fromProto,
        onOption,
        ignoring,
        include,
        bje.getMetricColumn,
        outputRvRange: Option[RvRange]
      )
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // TsCardReduceExec
  implicit class TsCardReduceExecToProtoConverter(tcre: TsCardReduceExec) {
    def toProto(): GrpcMultiPartitionQueryService.TsCardReduceExec = {
      val builder = GrpcMultiPartitionQueryService.TsCardReduceExec.newBuilder()
      builder.setNonLeafExecPlan(tcre.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class TsCardReduceExecFromProtoConverter(tcre: GrpcMultiPartitionQueryService.TsCardReduceExec) {
    def fromProto(queryContext: QueryContext): TsCardReduceExec = {
      val execPlan = tcre.getNonLeafExecPlan().getExecPlan()
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[GrpcMultiPartitionQueryService.ExecPlanContainer] = tcre.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto(queryContext))
      val p = TsCardReduceExec(
        queryContext,
        dispatcher,
        children
      )
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // StitchRvsExec
  implicit class StitchRvsExecToProtoConverter(sre: StitchRvsExec) {
    def toProto(): GrpcMultiPartitionQueryService.StitchRvsExec = {
      val builder = GrpcMultiPartitionQueryService.StitchRvsExec.newBuilder()
      builder.setNonLeafExecPlan(sre.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      sre.outputRvRange.foreach(orr => builder.setOutputRvRange(orr.toProto))
      builder.build()
    }
  }

  implicit class StitchRvsExecFromProtoConverter(sre: GrpcMultiPartitionQueryService.StitchRvsExec) {
    def fromProto(queryContext: QueryContext): StitchRvsExec = {
      val execPlan = sre.getNonLeafExecPlan().getExecPlan()
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[GrpcMultiPartitionQueryService.ExecPlanContainer] = sre.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto(queryContext))
      val outputRvRange =
        if (sre.hasOutputRvRange) Option(sre.getOutputRvRange.fromProto) else None
      val p = StitchRvsExec(
        queryContext,
        dispatcher,
        outputRvRange,
        children
      )
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // SetOperatorExec
  implicit class SetOperatorExecToProtoConverter(soe: SetOperatorExec) {
    def toProto(): GrpcMultiPartitionQueryService.SetOperatorExec = {
      val builder = GrpcMultiPartitionQueryService.SetOperatorExec.newBuilder()
      builder.setNonLeafExecPlan(soe.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      soe.lhs.foreach(ep => builder.addLhs(ep.toExecPlanContainerProto))
      soe.rhs.foreach(ep => builder.addRhs(ep.toExecPlanContainerProto))
      builder.setBinaryOp(soe.binaryOp.toProto)
      builder.build()
      soe.on.foreach(onOption =>(onOption.foreach(on => builder.addOn(on))))
      soe.ignoring.foreach(ignoring => builder.addIgnoring(ignoring))
      builder.setMetricColumn(soe.metricColumn)
      soe.outputRvRange.foreach(orr => builder.setOutputRvRange(orr.toProto))

      builder.build()
    }
  }

  implicit class SetOperatorExecFromProtoConverter(soe: GrpcMultiPartitionQueryService.SetOperatorExec) {
    def fromProto(queryContext: QueryContext): SetOperatorExec = {
      val execPlan = soe.getNonLeafExecPlan().getExecPlan()
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[GrpcMultiPartitionQueryService.ExecPlanContainer] = soe.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto(queryContext))
      val lhs: Seq[filodb.query.exec.ExecPlan] =
        soe.getLhsList.asScala.toSeq.map(c => c.fromProto(queryContext))
      val rhs: Seq[filodb.query.exec.ExecPlan] =
        soe.getRhsList.asScala.toSeq.map(c => c.fromProto(queryContext))
      val on = soe.getOnList.asScala.toSeq
      val onOption = if (on.isEmpty) None else Option(on)
      val ignoring = soe.getIgnoringList.asScala.toSeq
      val outputRvRange =
        if (soe.hasOutputRvRange) Option(soe.getOutputRvRange.fromProto) else None

      val p = SetOperatorExec(
        queryContext: QueryContext,
        dispatcher: PlanDispatcher,
        lhs,
        rhs,
        soe.getBinaryOp.fromProto,
        onOption,
        ignoring,
        soe.getMetricColumn,
        outputRvRange
      )
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // LabelValuesDistConcatExec
  implicit class LabelValuesDistConcatExecToProtoConverter(lvdce: LabelValuesDistConcatExec) {
    def toProto(): GrpcMultiPartitionQueryService.LabelValuesDistConcatExec = {
      val builder = GrpcMultiPartitionQueryService.LabelValuesDistConcatExec.newBuilder()
      builder.setNonLeafExecPlan(lvdce.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class LabelValuesDistConcatExecFromProtoConverter(lvdce: GrpcMultiPartitionQueryService.LabelValuesDistConcatExec) {
    def fromProto(queryContext: QueryContext): LabelValuesDistConcatExec = {
      val execPlan = lvdce.getNonLeafExecPlan().getExecPlan()
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[GrpcMultiPartitionQueryService.ExecPlanContainer] = lvdce.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto(queryContext))
      val p = LabelValuesDistConcatExec(
        queryContext,
        dispatcher,
        children
      )
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // PartKeysDistConcatExec
  implicit class PartKeysDistConcatExecToProtoConverter(pkdce: PartKeysDistConcatExec) {
    def toProto(): GrpcMultiPartitionQueryService.PartKeysDistConcatExec = {
      val builder = GrpcMultiPartitionQueryService.PartKeysDistConcatExec.newBuilder()
      builder.setNonLeafExecPlan(pkdce.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class PartKeysDistConcatExecFromProtoConverter(pkdce: GrpcMultiPartitionQueryService.PartKeysDistConcatExec) {
    def fromProto(queryContext: QueryContext): PartKeysDistConcatExec = {
      val execPlan = pkdce.getNonLeafExecPlan().getExecPlan()
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[GrpcMultiPartitionQueryService.ExecPlanContainer] = pkdce.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto(queryContext))
      val p = PartKeysDistConcatExec(
        queryContext,
        dispatcher,
        children
      )
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // LabelNamesDistConcatExec
  implicit class LabelNamesDistConcatExecToProtoConverter(lndce: LabelNamesDistConcatExec) {
    def toProto(): GrpcMultiPartitionQueryService.LabelNamesDistConcatExec = {
      val builder = GrpcMultiPartitionQueryService.LabelNamesDistConcatExec.newBuilder()
      builder.setNonLeafExecPlan(lndce.asInstanceOf[filodb.query.exec.NonLeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class LabelNamesDistConcatExecFromProtoConverter(lndce: GrpcMultiPartitionQueryService.LabelNamesDistConcatExec) {
    def fromProto(queryContext: QueryContext): LabelNamesDistConcatExec = {
      val execPlan = lndce.getNonLeafExecPlan().getExecPlan()
      val dispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val protoChildren: Seq[GrpcMultiPartitionQueryService.ExecPlanContainer] = lndce.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto(queryContext))
      val p = LabelNamesDistConcatExec(
        queryContext: QueryContext,
        dispatcher: PlanDispatcher,
        children: Seq[ExecPlan])
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // GenericRemoteExec
  implicit class GenericRemoteExecToProtoConverter(gre: GenericRemoteExec) {
    def toProto(): GrpcMultiPartitionQueryService.GenericRemoteExec = {
      val builder = GrpcMultiPartitionQueryService.GenericRemoteExec.newBuilder()
      builder.setExecPlan(gre.execPlan.toExecPlanContainerProto())
      builder.setDispatcher(gre.dispatcher.toPlanDispatcherContainer)
      builder.build()
    }
  }

  implicit class GenericRemoteExecFromProtoConverter(gre: GrpcMultiPartitionQueryService.GenericRemoteExec) {
    def fromProto(queryContext: QueryContext): GenericRemoteExec = {
      val execPlan = gre.getExecPlan.fromProto(queryContext)
      val dispatcher = gre.getDispatcher.fromProto
      val p = GenericRemoteExec(dispatcher, execPlan)
      p
    }
  }

  //
  //
  // Leaf Plans
  //
  //

  // LabelNamesExec
  implicit class LabelNamesExecToProtoConverter(lne: filodb.query.exec.LabelNamesExec) {
    def toProto(): GrpcMultiPartitionQueryService.LabelNamesExec = {
      val builder = GrpcMultiPartitionQueryService.LabelNamesExec.newBuilder()
      builder.setLeafExecPlan(lne.asInstanceOf[filodb.query.exec.LeafExecPlan].toProto)
      builder.setShard(lne.shard)
      lne.filters.foreach(f => builder.addFilters(f.toProto))
      builder.setStartMs(lne.startMs)
      builder.setEndMs(lne.endMs)
      builder.build()
    }
  }

  implicit class LabelNamesExecFromProtoConverter(lne: GrpcMultiPartitionQueryService.LabelNamesExec) {
    def fromProto(queryContext: QueryContext): filodb.query.exec.LabelNamesExec = {
      val execPlan = lne.getLeafExecPlan().getExecPlan()
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val filters = lne.getFiltersList.asScala.toSeq.map(
        f => f.fromProto
      )
      val datasetRef = execPlan.getQueryCommand.getDatasetRef.fromProto()
      val p = filodb.query.exec.LabelNamesExec(
        queryContext,
        planDispatcher,
        datasetRef,
        lne.getShard,
        filters,
        lne.getStartMs,
        lne.getEndMs
      )
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // EmptyResultExec
  implicit class EmptyResultExecToProtoConverter(lne: filodb.query.exec.EmptyResultExec) {
    def toProto(): GrpcMultiPartitionQueryService.EmptyResultExec = {
      val builder = GrpcMultiPartitionQueryService.EmptyResultExec.newBuilder()
      builder.setLeafExecPlan(lne.asInstanceOf[filodb.query.exec.LeafExecPlan].toProto)
      builder.build()
    }
  }

  implicit class EmptyResultExecFromProtoConverter(lne: GrpcMultiPartitionQueryService.EmptyResultExec) {
    def fromProto(queryContext: QueryContext): filodb.query.exec.EmptyResultExec = {
      val execPlan = lne.getLeafExecPlan().getExecPlan()
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val datasetRef = execPlan.getQueryCommand.getDatasetRef.fromProto()
      val inProcessPlanDispatcher = planDispatcher match {
        case ippd : InProcessPlanDispatcher => ippd
      }
      val p = filodb.query.exec.EmptyResultExec(
        queryContext,
        datasetRef,
        inProcessPlanDispatcher
      )
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // PartKeysExec
  implicit class PartKeysExecToProtoConverter(pke: filodb.query.exec.PartKeysExec) {
    def toProto(): GrpcMultiPartitionQueryService.PartKeysExec = {
      val builder = GrpcMultiPartitionQueryService.PartKeysExec.newBuilder()
      builder.setLeafExecPlan(pke.asInstanceOf[filodb.query.exec.LeafExecPlan].toProto)
      builder.setShard(pke.shard)
      pke.filters.foreach(f => builder.addFilters(f.toProto))
      builder.setFetchFirstLastSampleTimes(pke.fetchFirstLastSampleTimes)
      builder.setStart(pke.start)
      builder.setEnd(pke.end)
      builder.build()
    }
  }

  implicit class PartKeysExecFromProtoConverter(pke: GrpcMultiPartitionQueryService.PartKeysExec) {
    def fromProto(queryContext: QueryContext): filodb.query.exec.PartKeysExec = {
      val execPlan = pke.getLeafExecPlan().getExecPlan()
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val filters = pke.getFiltersList.asScala.toSeq.map(
        f => f.fromProto
      )
      val datasetRef = execPlan.getQueryCommand.getDatasetRef.fromProto()
      val p = filodb.query.exec.PartKeysExec(
        queryContext,
        planDispatcher,
        datasetRef,
        pke.getShard,
        filters,
        pke.getFetchFirstLastSampleTimes,
        pke.getStart,
        pke.getEnd
      )
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // LabelValuesExec
  implicit class LabelValuesExecToProtoConverter(lve: filodb.query.exec.LabelValuesExec) {
    def toProto(): GrpcMultiPartitionQueryService.LabelValuesExec = {
      val builder = GrpcMultiPartitionQueryService.LabelValuesExec.newBuilder()
      builder.setLeafExecPlan(lve.asInstanceOf[filodb.query.exec.LeafExecPlan].toProto)
      builder.setShard(lve.shard)
      lve.filters.foreach(f => builder.addFilters(f.toProto))
      lve.columns.foreach(c => builder.addColumns(c))
      builder.setStartMs(lve.startMs)
      builder.setEndMs(lve.endMs)
      builder.build()
    }
  }

  implicit class LabelValuesExecFromProtoConverter(lve: GrpcMultiPartitionQueryService.LabelValuesExec) {
    def fromProto(queryContext: QueryContext): filodb.query.exec.LabelValuesExec = {
      val execPlan = lve.getLeafExecPlan().getExecPlan()
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val filters = lve.getFiltersList.asScala.toSeq.map(
        f => f.fromProto
      )
      val columns = lve.getColumnsList.asScala.toSeq
      val datasetRef = execPlan.getQueryCommand.getDatasetRef.fromProto()
      val p = filodb.query.exec.LabelValuesExec(
        queryContext,
        planDispatcher,
        datasetRef,
        lve.getShard,
        filters,
        columns,
        lve.getStartMs,
        lve.getEndMs
      )
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // LabelCardinalityExec
  implicit class LabelCardinalityExecToProtoConverter(lce: filodb.query.exec.LabelCardinalityExec) {
    def toProto(): GrpcMultiPartitionQueryService.LabelCardinalityExec = {
      val builder = GrpcMultiPartitionQueryService.LabelCardinalityExec.newBuilder()
      builder.setLeafExecPlan(lce.asInstanceOf[filodb.query.exec.LeafExecPlan].toProto)
      builder.setShard(lce.shard)
      lce.filters.foreach(f => builder.addFilters(f.toProto))
      builder.setStartMs(lce.startMs)
      builder.setEndMs(lce.endMs)
      builder.build()
    }
  }

  implicit class LabelCardinalityExecFromProtoConverter(lce: GrpcMultiPartitionQueryService.LabelCardinalityExec) {
    def fromProto(queryContext: QueryContext): filodb.query.exec.LabelCardinalityExec = {
      val execPlan = lce.getLeafExecPlan().getExecPlan()
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.getDispatcher.fromProto
      val filters = lce.getFiltersList.asScala.toSeq.map(
        f => f.fromProto
      )
      val datasetRef = execPlan.getQueryCommand.getDatasetRef.fromProto()
      val p = filodb.query.exec.LabelCardinalityExec(
        queryContext,
        planDispatcher,
        datasetRef,
        lce.getShard,
        filters,
        lce.getStartMs,
        lce.getEndMs
      )
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // SelectChunkInfosExec
  implicit class SelectChunkInfosExecToProtoConverter(scie: SelectChunkInfosExec) {

    def toProto(): GrpcMultiPartitionQueryService.SelectChunkInfosExec = {
      val builder = GrpcMultiPartitionQueryService.SelectChunkInfosExec.newBuilder()
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

  implicit class SelectChunkInfosExecFromProtoConverter(scie: GrpcMultiPartitionQueryService.SelectChunkInfosExec) {
    def fromProto(queryContext: QueryContext): SelectChunkInfosExec = {
      val ep = scie.getLeafExecPlan.getExecPlan
      val dispatcher = ep.getDispatcher.fromProto
      val datasetRef = scie.getLeafExecPlan.getExecPlan.getQueryCommand.getDatasetRef.fromProto
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
      val p = SelectChunkInfosExec(
        queryContext, dispatcher, datasetRef,
        scie.getShard,
        filters,
        chunkMethod, schema, colName
      )
      ep.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // MultiSchemaPartitionsExec
  implicit class MultiSchemaPartitionsExecToProtoConverter(mspe: MultiSchemaPartitionsExec) {

    def toProto(): GrpcMultiPartitionQueryService.MultiSchemaPartitionsExec = {
      val builder = GrpcMultiPartitionQueryService.MultiSchemaPartitionsExec.newBuilder()
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

  implicit class MultiSchemaPartitionsExecFromProtoConverter(mspe: GrpcMultiPartitionQueryService.MultiSchemaPartitionsExec) {
    def fromProto(queryContext: QueryContext): MultiSchemaPartitionsExec = {
      val ep = mspe.getLeafExecPlan.getExecPlan
      val dispatcher = ep.getDispatcher.fromProto
      val datasetRef = mspe.getLeafExecPlan.getExecPlan.getQueryCommand.getDatasetRef.fromProto
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
      val p = MultiSchemaPartitionsExec(
        queryContext, dispatcher, datasetRef,
        mspe.getShard,
        filters,
        chunkMethod, mspe.getMetricColumn, schema, colName
      )
      ep.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // ScalarBinaryOperationExec
  implicit class ScalarBinaryOperationExecToProtoConverter(sboe: ScalarBinaryOperationExec) {

    def toProto(): GrpcMultiPartitionQueryService.ScalarBinaryOperationExec = {
      val builder = GrpcMultiPartitionQueryService.ScalarBinaryOperationExec.newBuilder()
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

  implicit class ScalarBinaryOperationExecFromProtoConverter(sboe: GrpcMultiPartitionQueryService.ScalarBinaryOperationExec) {
    def fromProto(queryContext: QueryContext): ScalarBinaryOperationExec = {
      val ep = sboe.getLeafExecPlan.getExecPlan
      val dispatcher = ep.getDispatcher.fromProto
      val datasetRef = sboe.getLeafExecPlan.getExecPlan.getQueryCommand.getDatasetRef.fromProto
      val lhs = sboe.getLhsCase match {
        case GrpcMultiPartitionQueryService.ScalarBinaryOperationExec.LhsCase.DOUBLEVALUELHS => Left(sboe.getDoubleValueLhs)
        case GrpcMultiPartitionQueryService.ScalarBinaryOperationExec.LhsCase.SCALARBINARYOPERATIONEXECLHS=>
          Right(sboe.getScalarBinaryOperationExecLhs.fromProto(queryContext))
        case GrpcMultiPartitionQueryService.ScalarBinaryOperationExec.LhsCase.LHS_NOT_SET =>
          throw new IllegalArgumentException("invalid lhs")
      }
      val rhs = sboe.getRhsCase match {
        case GrpcMultiPartitionQueryService.ScalarBinaryOperationExec.RhsCase.DOUBLEVALUERHS => Left(sboe.getDoubleValueRhs)
        case GrpcMultiPartitionQueryService.ScalarBinaryOperationExec.RhsCase.SCALARBINARYOPERATIONEXECRHS =>
          Right(sboe.getScalarBinaryOperationExecRhs.fromProto(queryContext))
        case GrpcMultiPartitionQueryService.ScalarBinaryOperationExec.RhsCase.RHS_NOT_SET =>
          throw new IllegalArgumentException("invalid rhs")
      }
      val inProcessPlanDispatcher = dispatcher match {
        case ippd: InProcessPlanDispatcher => ippd
      }
      val p = ScalarBinaryOperationExec(
        queryContext,
        datasetRef,
        sboe.getParams.fromProto,
        lhs: Either[Double, ScalarBinaryOperationExec],
        rhs: Either[Double, ScalarBinaryOperationExec],
        sboe.getOperator.fromProto,
        inProcessPlanDispatcher
      )
      ep.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // ScalarFixedDoubleExec
  implicit class ScalarFixedDoubleExecToProtoConverter(sfde: ScalarFixedDoubleExec) {
    def toProto(): GrpcMultiPartitionQueryService.ScalarFixedDoubleExec = {
      val builder = GrpcMultiPartitionQueryService.ScalarFixedDoubleExec.newBuilder()
      builder.setLeafExecPlan(sfde.asInstanceOf[LeafExecPlan].toProto)
      builder.setParams(sfde.params.toProto)
      builder.setValue(sfde.value)
      builder.build()
    }
  }

  implicit class ScalarFixedDoubleExecFromProtoConverter(sfde: GrpcMultiPartitionQueryService.ScalarFixedDoubleExec) {
    def fromProto(queryContext: QueryContext): ScalarFixedDoubleExec = {
      val ep = sfde.getLeafExecPlan.getExecPlan
      val dispatcher = ep.getDispatcher.fromProto
      val datasetRef = sfde.getLeafExecPlan.getExecPlan.getQueryCommand.getDatasetRef.fromProto
      val inProcessPlanDispatcher = dispatcher match {
        case ippd: InProcessPlanDispatcher => ippd
      }
      val p = ScalarFixedDoubleExec(
        queryContext,
        datasetRef,
        sfde.getParams.fromProto,
        sfde.getValue,
        inProcessPlanDispatcher
      )
      ep.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // TsCardExec
  implicit class TsCardExecToProtoConverter(tce: TsCardExec) {
    def toProto(): GrpcMultiPartitionQueryService.TsCardExec = {
      val builder = GrpcMultiPartitionQueryService.TsCardExec.newBuilder()
      builder.setLeafExecPlan(tce.asInstanceOf[LeafExecPlan].toProto)
      builder.setShard(tce.shard)
      tce.shardKeyPrefix.foreach(f => builder.addShardKeyPrefix(f))
      builder.setNumGroupByFields(tce.numGroupByFields)
      builder.setClusterName(tce.clusterName)
      builder.build()
    }
  }

  implicit class TsCardExecFromProtoConverter(tce: GrpcMultiPartitionQueryService.TsCardExec) {
    def fromProto(queryContext: QueryContext): TsCardExec = {
      val ep = tce.getLeafExecPlan.getExecPlan
      val dispatcher = ep.getDispatcher.fromProto
      val datasetRef = tce.getLeafExecPlan.getExecPlan.getQueryCommand.getDatasetRef.fromProto
      val shardKeyPrefix = tce.getShardKeyPrefixList.asScala.toSeq
      val numGroupByFields = tce.getNumGroupByFields
      val p = TsCardExec(
        queryContext, dispatcher, datasetRef,
        tce.getShard,
        shardKeyPrefix,
        tce.getNumGroupByFields,
        tce.getClusterName
      )
      ep.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // TimeScalarGeneratorExec
  implicit class TimeScalarGeneratorExecProtoConverter(tsge: TimeScalarGeneratorExec) {
    def toProto(): GrpcMultiPartitionQueryService.TimeScalarGeneratorExec = {
      val builder = GrpcMultiPartitionQueryService.TimeScalarGeneratorExec.newBuilder()
      builder.setLeafExecPlan(tsge.asInstanceOf[LeafExecPlan].toProto)
      builder.setParams(tsge.params.toProto)
      builder.setFunction(tsge.function.toProto)
      builder.build()
    }
  }

  implicit class TimeScalarGeneratorExecFromProtoConverter(tsge: GrpcMultiPartitionQueryService.TimeScalarGeneratorExec) {
    def fromProto(queryContext: QueryContext): TimeScalarGeneratorExec = {
      val ep = tsge.getLeafExecPlan.getExecPlan
      val dispatcher = ep.getDispatcher.fromProto
      val datasetRef = tsge.getLeafExecPlan.getExecPlan.getQueryCommand.getDatasetRef.fromProto
      val inProcessPlanDispatcher = dispatcher match {
        case ippd: InProcessPlanDispatcher => ippd
      }
      val p = TimeScalarGeneratorExec(
        queryContext,
        datasetRef,
        tsge.getParams.fromProto,
        tsge.getFunction.fromProto,
        inProcessPlanDispatcher
      )
      ep.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  // SelectRawPartitionsExec
  implicit class SelectRawPartitionsExecToProtoConverter(srp: SelectRawPartitionsExec) {

    def toProto(): GrpcMultiPartitionQueryService.SelectRawPartitionsExec = {
      val builder = GrpcMultiPartitionQueryService.SelectRawPartitionsExec.newBuilder()
      builder.setLeafExecPlan(srp.asInstanceOf[LeafExecPlan].toProto)
      srp.dataSchema.map(s => builder.setDataSchema(s.toProto))
      srp.lookupRes.foreach(lr => builder.setLookupRes(lr.toProto))
      builder.setFilterSchemas(srp.filterSchemas)
      srp.colIds.foreach(colId => builder.addColIds(Integer.valueOf(colId)))
      builder.setPlanId(srp.planId)
      builder.build()
    }
  }

  implicit class SelectRawPartitionsExecFromProtoConverter(srpe: GrpcMultiPartitionQueryService.SelectRawPartitionsExec) {
    def fromProto(queryContext: QueryContext): SelectRawPartitionsExec = {
      val ep = srpe.getLeafExecPlan.getExecPlan
      val dataSchema = if (srpe.hasDataSchema) Option(srpe.getDataSchema.fromProto) else None
      val lookupRes = if (srpe.hasLookupRes) Option(srpe.getLookupRes.fromProto) else None
      val colIds = srpe.getColIdsList.asScala.map(intgr => intgr.intValue())
      val leafExecPlan = srpe.getLeafExecPlan
      val execPlan = leafExecPlan.getExecPlan
      val dispatcher = execPlan.getDispatcher.fromProto
      val datasetRef = leafExecPlan.getExecPlan.getQueryCommand.getDatasetRef.fromProto
      val p = SelectRawPartitionsExec(
        queryContext, dispatcher, datasetRef,
        dataSchema,
        lookupRes,
        srpe.getFilterSchemas,
        colIds,
        srpe.getPlanId
      )
      execPlan.getRangeVectorTransformersList().asScala.foreach(t => p.addRangeVectorTransformer(t.fromProto(queryContext)))
      p
    }
  }

  implicit class ExecPlanToProtoConverter(ep: filodb.query.exec.ExecPlan) {
    def toProto(): GrpcMultiPartitionQueryService.ExecPlan = {
      val builder = GrpcMultiPartitionQueryService.ExecPlan.newBuilder()
      builder.setPlanId(ep.planId)
      builder.setEnforceSampleLimit(ep.enforceSampleLimit)
      builder.setQueryCommand(ep.asInstanceOf[QueryCommand].toProto)
      builder.clearDispatcher()
      builder.setDispatcher(ep.dispatcher.toPlanDispatcherContainer)
      ep.rangeVectorTransformers.foreach(t => {
        val protoRangeVectorTransfomer = t.toProto()
        builder.addRangeVectorTransformers(protoRangeVectorTransfomer)
      })
      builder.build()
    }

    // scalastyle:off cyclomatic.complexity
    def toExecPlanContainerProto() : GrpcMultiPartitionQueryService.ExecPlanContainer = {
      val b = GrpcMultiPartitionQueryService.ExecPlanContainer.newBuilder()
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
        case gre: GenericRemoteExec => b.setGenericRemoteExec(gre.toProto)
        //case _ => throw new IllegalArgumentException(s"Unknown execution plan ${ep.getClass.getName}")
      }
      b.build()
    }
    // scalastyle:on cyclomatic.complexity
  }

  implicit class PlanDispatcherContainerFromProto(pdc: GrpcMultiPartitionQueryService.PlanDispatcherContainer) {
    def fromProto: filodb.query.exec.PlanDispatcher = {
      val dispatcherCase = pdc.getDispatcherCase
      val dispatcher = dispatcherCase match {
        case GrpcMultiPartitionQueryService.PlanDispatcherContainer.DispatcherCase.ACTORPLANDISPATCHER =>
          pdc.getActorPlanDispatcher.fromProto
        case GrpcMultiPartitionQueryService.PlanDispatcherContainer.DispatcherCase.INPROCESSPLANDISPATCHER =>
          pdc.getInProcessPlanDispatcher.fromProto
        case GrpcMultiPartitionQueryService.PlanDispatcherContainer.DispatcherCase.REMOTEACTORPLANDISPATCHER =>
          pdc.getRemoteActorPlanDispatcher.fromProto
        case GrpcMultiPartitionQueryService.PlanDispatcherContainer.DispatcherCase.GRPCPLANDISPATCHER =>
          pdc.getGrpcPlanDispatcher.fromProto
        case GrpcMultiPartitionQueryService.PlanDispatcherContainer.DispatcherCase.DISPATCHER_NOT_SET =>
          throw new IllegalArgumentException("Invalid PlanDispatcherContainer")
      }
      dispatcher
    }
  }

  implicit class ExecPlanContainerFromProtoConverter(epc: GrpcMultiPartitionQueryService.ExecPlanContainer) {
    // scalastyle:off cyclomatic.complexity
    def fromProto(queryContext: QueryContext): filodb.query.exec.ExecPlan = {
      val plan: filodb.query.exec.ExecPlan = epc.getExecPlanCase match {
        // non leaf plans
        case ExecPlanCase.LABELCARDINALITYREDUCEEXEC => epc.getLabelCardinalityReduceExec.fromProto(queryContext)
        case ExecPlanCase.MULTIPARTITIONDISTCONCATEXEC => epc.getMultiPartitionDistConcatExec.fromProto(queryContext)
        case ExecPlanCase.LOCALPARTITIONDISTCONCATEXEC => epc.getLocalPartitionDistConcatExec.fromProto(queryContext)
        case ExecPlanCase.SPLITLOCALPARTITIONDISTCONCATEXEC => epc.getSplitLocalPartitionDistConcatExec.fromProto(queryContext)
        case ExecPlanCase.LOCALPARTITIONREDUCEAGGREGATEEXEC => epc.getLocalPartitionReduceAggregateExec.fromProto(queryContext)
        case ExecPlanCase.MULTIPARTITIONREDUCEAGGREGATEEXEC => epc.getMultiPartitionReduceAggregateExec.fromProto(queryContext)
        case ExecPlanCase.BINARYJOINEXEC => epc.getBinaryJoinExec.fromProto(queryContext)
        case ExecPlanCase.TSCARDREDUCEEXEC => epc.getTsCardReduceExec.fromProto(queryContext)
        case ExecPlanCase.STITCHRVSEXEC => epc.getStitchRvsExec.fromProto(queryContext)
        case ExecPlanCase.SETOPERATOREXEC => epc.getSetOperatorExec.fromProto(queryContext)
        case ExecPlanCase.LABELVALUESDISTCONCATEXEC => epc.getLabelValuesDistConcatExec.fromProto(queryContext)
        case ExecPlanCase.PARTKEYSDISTCONCATEXEC => epc.getPartKeysDistConcatExec.fromProto(queryContext)
        case ExecPlanCase.LABELNAMESDISTCONCATEXEC => epc.getLabelNamesDistConcatExec.fromProto(queryContext)
        // leaf plans
        case ExecPlanCase.LABELNAMESEXEC => epc.getLabelNamesExec.fromProto(queryContext)
        case ExecPlanCase.EMPTYRESULTEXEC => epc.getEmptyResultExec.fromProto(queryContext)
        case ExecPlanCase.PARTKEYSEXEC => epc.getPartKeysExec.fromProto(queryContext)
        case ExecPlanCase.LABELVALUESEXEC => epc.getLabelValuesExec.fromProto(queryContext)
        case ExecPlanCase.LABELCARDINALITYEXEC => epc.getLabelCardinalityExec.fromProto(queryContext)
        case ExecPlanCase.SELECTCHUNKINFOSEXEC => epc.getSelectChunkInfosExec.fromProto(queryContext)
        case ExecPlanCase.MULTISCHEMAPARTITIONSEXEC => epc.getMultiSchemaPartitionsExec.fromProto(queryContext)
        case ExecPlanCase.SCALARBINARYOPERATINEXEC => epc.getScalarBinaryOperatinExec.fromProto(queryContext)
        case ExecPlanCase.SCALARFIXEDDOUBLEEXEC => epc.getScalarFixedDoubleExec.fromProto(queryContext)
        case ExecPlanCase.TSCARDEXEC => epc.getTsCardExec.fromProto(queryContext)
        case ExecPlanCase.TIMESCALARGENERATOREXEC => epc.getTimeScalarGeneratorExec.fromProto(queryContext)
        case ExecPlanCase.SELECTRAWPARTITIONSEXEC => epc.getSelectRawPartitionsExec.fromProto(queryContext)
        case ExecPlanCase.GENERICREMOTEEXEC => epc.getGenericRemoteExec.fromProto(queryContext)
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
