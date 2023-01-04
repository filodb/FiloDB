package filodb.query

import com.google.protobuf.ByteString
import com.typesafe.scalalogging.StrictLogging

import filodb.core.binaryrecord2.{RecordContainer, RecordSchema}
import filodb.core.metadata.Column.ColumnType._
import filodb.core.query.{ColumnInfo, CustomRangeVectorKey, PlannerParams, PromQlQueryParams, QueryStats,
  RangeVectorKey, ResultSchema, RvRange, SerializedRangeVector, Stat, TsdbQueryParams, UnavailablePromQlQueryParams}
import filodb.grpc.{GrpcMultiPartitionQueryService, ProtoRangeVector}
import filodb.grpc.GrpcMultiPartitionQueryService.QueryParams


object ProtoConverters {

  implicit class RangeVectorToProtoConversion(rv: SerializedRangeVector) {

    def toProto: ProtoRangeVector.RangeVector = {
      import collection.JavaConverters._
      val builder = ProtoRangeVector.RangeVector.newBuilder()
      builder.setKey(rv.key.toProto)
      builder.setNumRowsSerialized(rv.numRowsSerialized)
      builder.addAllRecordContainers(rv.containersIterator.map(container => ByteString.copyFrom(
        if (container.hasArray) container.array else container.trimmedArray)).toIterable.asJava)
      builder.setRecordSchema(rv.schema.toProto)
      rv.outputRange match {
        case Some(rvr: RvRange) => builder.setRvRange(rvr.toProto)
        case _ =>
      }
      builder.build()
    }
  }

  implicit class RangeVectorFromProtoConversion(rvProto: ProtoRangeVector.RangeVector) {

    def fromProto: SerializedRangeVector = {
      import collection.JavaConverters._

      new SerializedRangeVector(rvProto.getKey.fromProto,
        rvProto.getNumRowsSerialized,
        rvProto.getRecordContainersList.asScala.map(byteString => RecordContainer(byteString.toByteArray)),
        rvProto.getRecordSchema.fromProto,
        0,
        if (rvProto.hasRvRange) Some(rvProto.getRvRange.fromProto) else None)
    }
  }


  implicit class RangeVectorKeyToProtoConversion(rvk: RangeVectorKey) {
    def toProto: ProtoRangeVector.RangeVectorKey = {
      import collection.JavaConverters._
      val builder = ProtoRangeVector.RangeVectorKey.newBuilder()
      builder.putAllLabels(rvk.labelValues.map {
        case (key, value) => key.toString -> value.toString
      }.asJava)
      builder.build()
    }
  }

  implicit class RangeVectorKeyFromProtoConversion(rvkProto: ProtoRangeVector.RangeVectorKey) {
    def fromProto: RangeVectorKey = {
      import collection.JavaConverters._
      import filodb.memory.format.ZeroCopyUTF8String._

      CustomRangeVectorKey(labelValues = rvkProto.getLabelsMap.asScala.map {
        case (key, value) => key.utf8 -> value.utf8
      }.toMap)
    }
  }

  implicit class RecordSchemaToProtoConversion(rvk: RecordSchema) {
    def toProto: ProtoRangeVector.RecordSchema = {
      val builder = ProtoRangeVector.RecordSchema.newBuilder()
      rvk.columns.foreach(ci => builder.addColumns(ci.toProto))
      if (rvk.partitionFieldStart.isDefined) builder.setPartitionFieldStart(rvk.partitionFieldStart.get)
      rvk.predefinedKeys.foreach(k => builder.addPredefinedKeys(k))
      rvk.brSchema.foreach {
        case (key, schema)   => builder.putBrSchema(key, schema.toProto)
      }
      if (rvk.schemaVersion.isDefined) builder.setSchemaVersion(rvk.schemaVersion.get)
      builder.build()
    }
  }


  implicit class RecordSchemaFromProtoConversion(rvkProto: ProtoRangeVector.RecordSchema) {
    def fromProto: RecordSchema = {
      import collection.JavaConverters._
      new RecordSchema(
                   columns = rvkProto.getColumnsList.asScala.toList.map(ci => ci.fromProto),
                   partitionFieldStart = if (rvkProto.hasPartitionFieldStart)
                                            Some(rvkProto.getPartitionFieldStart) else None,
                   predefinedKeys = rvkProto.getPredefinedKeysList.asScala.toList,
                   brSchema = rvkProto.getBrSchemaMap.asScala.map
                     { case (key, value) => (key.toInt, value.fromProto)}.toMap,
                   schemaVersion = if (rvkProto.hasSchemaVersion) Some(rvkProto.getSchemaVersion) else None)
    }
  }

  implicit class RvRangeToProtoConversion(rvr: RvRange) {
    def toProto: ProtoRangeVector.RvRange = {
      val builder = ProtoRangeVector.RvRange.newBuilder()
      builder.setStartMs(rvr.startMs)
      builder.setEndMs(rvr.endMs)
      builder.setStep(rvr.stepMs)
      builder.build()
    }
  }

  implicit class RvRangeFromProtoConversion(rvrProto: ProtoRangeVector.RvRange) {
    def fromProto: RvRange = RvRange(rvrProto.getStartMs, rvrProto.getStep, rvrProto.getEndMs)
  }

  implicit class ColumnInfoToProtoConversion(ci: ColumnInfo) {
    def toProto: ProtoRangeVector.ColumnInfo = {
      val builder = ProtoRangeVector.ColumnInfo.newBuilder()
      val grpcColType = ci.colType match {
        case IntColumn => ProtoRangeVector.ColumnType.IntColumn
        case LongColumn => ProtoRangeVector.ColumnType.LongColumn
        case DoubleColumn => ProtoRangeVector.ColumnType.DoubleColumn
        case StringColumn => ProtoRangeVector.ColumnType.StringColumn
        case TimestampColumn => ProtoRangeVector.ColumnType.TimestampColumn
        case MapColumn => ProtoRangeVector.ColumnType.MapColumn
        case BinaryRecordColumn => ProtoRangeVector.ColumnType.BinaryRecordColumn
        case HistogramColumn => ProtoRangeVector.ColumnType.HistogramColumn
      }
      builder.setColumnType(grpcColType)
      builder.setName(ci.name)
      builder.build()
    }
  }

  implicit class ColumnInfoFromProtoConversion(ci: ProtoRangeVector.ColumnInfo) {
    def fromProto: ColumnInfo = {
      val colType = ci.getColumnType match {
        case ProtoRangeVector.ColumnType.IntColumn => IntColumn
        case ProtoRangeVector.ColumnType.LongColumn => LongColumn
        case ProtoRangeVector.ColumnType.DoubleColumn => DoubleColumn
        case ProtoRangeVector.ColumnType.StringColumn => StringColumn
        case ProtoRangeVector.ColumnType.TimestampColumn => TimestampColumn
        case ProtoRangeVector.ColumnType.MapColumn => MapColumn
        case ProtoRangeVector.ColumnType.BinaryRecordColumn => BinaryRecordColumn
        case ProtoRangeVector.ColumnType.HistogramColumn => HistogramColumn
        case ProtoRangeVector.ColumnType.UNRECOGNIZED =>
          throw new IllegalStateException("Unrecognized colType found")
      }
      ColumnInfo(ci.getName, colType)
    }
  }

  implicit class QueryParamsFromProtoConversion(qp: QueryParams) {
    def fromProto: TsdbQueryParams = {
      if (qp.getIsUnavailable)
        UnavailablePromQlQueryParams
      else
        PromQlQueryParams(promQl = qp.getPromQL, startSecs = qp.getStart,
          stepSecs = qp.getStep, endSecs = qp.getEnd, verbose = qp.getVerbose)
    }
  }


  implicit class QueryParamsToProtoConversion(qp: TsdbQueryParams) {
    def toProto: QueryParams = {
      val builder = QueryParams.newBuilder()
      qp match {
        case UnavailablePromQlQueryParams => builder.setIsUnavailable(true)
        case PromQlQueryParams(promQl, startSecs, stepSecs, endSecs, _, verbose) =>
          builder.setIsUnavailable(false)
          builder.setPromQL(promQl)
          builder.setStart(startSecs)
          builder.setStep(stepSecs)
          builder.setEnd(endSecs)
          builder.setVerbose(verbose)
          builder.setTime(endSecs)
      }
      builder.build()
    }
  }

  implicit class PlannerParamsToProtoConverter(pp: PlannerParams) {
    def toProto: GrpcMultiPartitionQueryService.PlannerParams = {
      val builder = GrpcMultiPartitionQueryService.PlannerParams.newBuilder()
      builder.setApplicationId(pp.applicationId)
      builder.setQueryTimeoutMillis(pp.queryTimeoutMillis)
      builder.setSampleLimit(pp.sampleLimit)
      builder.setGroupByCardLimit(pp.groupByCardLimit)
      builder.setJoinQueryCardLimit(pp.joinQueryCardLimit)
      builder.setResultByteLimit(pp.resultByteLimit)
      builder.setTimeSplitEnabled(pp.timeSplitEnabled)
      builder.setMinTimeRangeForSplitMs(pp.minTimeRangeForSplitMs)
      builder.setSplitSizeMs(pp.splitSizeMs)
      builder.setSkipAggregatePresent(pp.skipAggregatePresent)
      builder.setProcessFailure(pp.processFailure)
      builder.setProcessMultiPartition(pp.processMultiPartition)
      builder.setAllowPartialResults(pp.allowPartialResults)
      builder.build()
    }
  }

  implicit class PlannerParamsFromProtoConverter(gpp: GrpcMultiPartitionQueryService.PlannerParams) {
    def fromProto: PlannerParams = {
      val pp = PlannerParams()
      pp.copy(
        applicationId = if (gpp.hasApplicationId) gpp.getApplicationId else pp.applicationId,
        queryTimeoutMillis = if (gpp.hasQueryTimeoutMillis) gpp.getQueryTimeoutMillis else pp.queryTimeoutMillis,
        sampleLimit = if (gpp.hasSampleLimit) gpp.getSampleLimit else pp.sampleLimit,
        groupByCardLimit = if (gpp.hasGroupByCardLimit) gpp.getGroupByCardLimit else pp.groupByCardLimit,
        joinQueryCardLimit = if (gpp.hasJoinQueryCardLimit) gpp.getJoinQueryCardLimit else pp.joinQueryCardLimit,
        resultByteLimit = if (gpp.hasResultByteLimit) gpp.getResultByteLimit else pp.resultByteLimit,
        timeSplitEnabled = if (gpp.hasTimeSplitEnabled) gpp.getTimeSplitEnabled else pp.timeSplitEnabled,
        minTimeRangeForSplitMs = if (gpp.hasMinTimeRangeForSplitMs) gpp.getMinTimeRangeForSplitMs
        else pp.minTimeRangeForSplitMs,
        splitSizeMs = if (gpp.hasSplitSizeMs) gpp.getSplitSizeMs else pp.splitSizeMs,
        skipAggregatePresent = if (gpp.hasSkipAggregatePresent) gpp.getSkipAggregatePresent
        else pp.skipAggregatePresent,
        processFailure = if (gpp.hasProcessFailure) gpp.getProcessFailure else pp.processFailure,
        processMultiPartition = if (gpp.hasProcessMultiPartition) gpp.getProcessMultiPartition
        else pp.processMultiPartition,
        allowPartialResults = if (gpp.hasAllowPartialResults) gpp.getAllowPartialResults else pp.allowPartialResults
      )
    }
  }

  implicit class StatToProtoConverter(stat: Stat) {
    def toProto: GrpcMultiPartitionQueryService.Stat = {
      val builder = GrpcMultiPartitionQueryService.Stat.newBuilder()
      builder.setResultBytes(stat.resultBytes.get())
      builder.setDataBytesScanned(stat.dataBytesScanned.get())
      builder.setTimeSeriesScanned(stat.timeSeriesScanned.get())
      builder.build()
    }
  }

  implicit class StatFromProtoConverter(statGrpc: GrpcMultiPartitionQueryService.Stat) {
    def fromProto: Stat = {
      val stat = Stat()
      stat.timeSeriesScanned.addAndGet(statGrpc.getTimeSeriesScanned)
      stat.dataBytesScanned.addAndGet(statGrpc.getDataBytesScanned)
      stat.resultBytes.addAndGet(statGrpc.getResultBytes)
      stat
    }
  }
  implicit class QueryStatsToProtoConverter(stats: QueryStats) {
    def toProto: GrpcMultiPartitionQueryService.QueryResultStats = {
      val builder = GrpcMultiPartitionQueryService.QueryResultStats.newBuilder()
      stats.stat.foreach {
        case (key, stat)  =>
          builder.putStats( key.mkString("##@##"), stat.toProto)
      }
      builder.build()
    }
  }

  implicit class QueryStatsFromProtoConverter(statGrpc: GrpcMultiPartitionQueryService.QueryResultStats) {
    def fromProto: QueryStats = {
      val qs = QueryStats()
      statGrpc.getStatsMap.forEach {
        case (key, stat)  =>  qs.stat.put(key.split("##@##").toList, stat.fromProto)
      }
      qs
    }
  }

  implicit class StackTraceElementToProtoConverter(stackTraceElement: StackTraceElement) {
    def toProto: GrpcMultiPartitionQueryService.StackTraceElement = {
      val builder = GrpcMultiPartitionQueryService.StackTraceElement.newBuilder()
      builder.setDeclaringClass(stackTraceElement.getClassName)
      builder.setFileName(stackTraceElement.getFileName)
      builder.setLineNumber(stackTraceElement.getLineNumber)
      builder.setMethodName(stackTraceElement.getMethodName)
      builder.build()
    }
  }


  implicit class StackTraceElementFromProtoConverter
    (stackTraceElement: GrpcMultiPartitionQueryService.StackTraceElement) {
    def fromProto: StackTraceElement = {
      new StackTraceElement(stackTraceElement.getDeclaringClass, stackTraceElement.getMethodName,
        stackTraceElement.getFileName, stackTraceElement.getLineNumber)
    }
  }

  implicit class ThrowableToProtoConverter(t: Throwable) {
    def toProto: GrpcMultiPartitionQueryService.Throwable = {
      val builder = GrpcMultiPartitionQueryService.Throwable.newBuilder()
      builder.setMessage(t.getMessage)
      if (t.getCause != null)
        builder.setCause(t.getCause.toProto)
      t.getStackTrace.iterator.foreach(elem => builder.addStack(elem.toProto))
      builder.build()
    }
  }


  implicit class ThrowableFromProtoConverter(throwableProto: GrpcMultiPartitionQueryService.Throwable) {
    def fromProto: Throwable = {
      import scala.collection.JavaConverters._
      val t = if (throwableProto.hasCause)
                new Throwable(throwableProto.getMessage, throwableProto.getCause.fromProto)
              else
                new Throwable(throwableProto.getMessage)
      t.setStackTrace(
        throwableProto.getStackList.asScala.iterator
        .map(e => new StackTraceElement(e.getDeclaringClass, e.getMethodName, e.getFileName, e.getLineNumber)).toArray)
      t
    }
  }

  implicit class ResultSchemaToProtoConverter(resultSchema: ResultSchema) {
    def toProto: ProtoRangeVector.ResultSchema = {
      import scala.collection.JavaConverters._
      val builder = ProtoRangeVector.ResultSchema.newBuilder()
      builder.setNumRowKeys(resultSchema.numRowKeyColumns)
      if (resultSchema.fixedVectorLen.isDefined) {
        builder.setFixedVectorLen(resultSchema.fixedVectorLen.get)
      }
      builder.addAllColIds(resultSchema.colIDs.map(Integer.valueOf).asJava)
      builder.addAllColumns(resultSchema.columns.map(_.toProto).asJava)
      resultSchema.brSchemas.map {
        case (key, value) => builder.putBrSchemas(key, value.toProto)
      }
      builder.build()
    }
  }


  implicit class ResultSchemaFromProtoConverter(resultSchemaProto: ProtoRangeVector.ResultSchema) {
    def fromProto: ResultSchema = {
      import scala.collection.JavaConverters._
      ResultSchema(
        resultSchemaProto.getColumnsList.asScala.map(_.fromProto).toList,
        resultSchemaProto.getNumRowKeys,
        resultSchemaProto.getBrSchemasMap.asScala.map {
          case (key, value) => (key.toInt, value.fromProto)
        }.toMap,
        if (resultSchemaProto.hasFixedVectorLen) Some(resultSchemaProto.getFixedVectorLen) else None,
        resultSchemaProto.getColIdsList.asScala.map(_.toInt).toList)
    }
  }

  implicit class ResponseToProtoConverter(response: QueryResponse) {
    def toProto: GrpcMultiPartitionQueryService.Response = {
      import scala.collection.JavaConverters._
      val builder = GrpcMultiPartitionQueryService.Response.newBuilder()
      response match {
        case QueryError(id, stats, throwable)                                                       =>
                                          builder.setId(id)
                                          builder.setStats(stats.toProto)
                                          builder.setThrowable(throwable.toProto)
        case QueryResult(id, resultSchema, result, queryStats, mayBePartial, partialResultReason)   =>
                                          builder.setId(id)
                                          builder.setResultSchema(resultSchema.toProto)
                                          builder.setStats(queryStats.toProto)
                                          builder.setMayBePartial(mayBePartial)
                                          builder.addAllResult(result.map {
                                            case srv: SerializedRangeVector   => srv.toProto
                                            case _                            =>
                                              throw new IllegalStateException("Expected a SerializedRangeVector")
                                          }.asJava)
                                          if (partialResultReason.isDefined)
                                            builder.setPartialResultReason(partialResultReason.get)
      }
      builder.build()
    }
  }


  implicit class ResponseFromProtoConverter(responseProto: GrpcMultiPartitionQueryService.Response) {
    def fromProto: QueryResponse = {
      import scala.collection.JavaConverters._
      if (responseProto.hasThrowable) {
        QueryError(responseProto.getId, responseProto.getStats.fromProto, responseProto.getThrowable.fromProto)
      } else {
        QueryResult(responseProto.getId, responseProto.getResultSchema.fromProto,
          responseProto.getResultList.asScala.map(_.fromProto).toList, responseProto.getStats.fromProto,
          responseProto.getMayBePartial,
          if (responseProto.hasPartialResultReason) Some(responseProto.getPartialResultReason) else None)
      }
    }
  }

  implicit class StreamingResponseToProtoConverter(response: StreamQueryResponse) {
    def toProto: GrpcMultiPartitionQueryService.StreamingResponse = {
      val builder = GrpcMultiPartitionQueryService.StreamingResponse.newBuilder()
      response match {
        case StreamQueryResultHeader(id, resultSchema) =>
                                    builder.setHeader(
                                    builder
                                      .getHeaderBuilder
                                      .setId(id)
                                      .setResultSchema(resultSchema.toProto))
        case StreamQueryResult(id, result) =>
                                    builder.setBody(builder.getBodyBuilder.setId(id)
                                      .setResult(result match {
                                        case srv: SerializedRangeVector   => srv.toProto
                                        case _                            =>
                                          throw new IllegalStateException("Expected a SerializedRangeVector")
                                      }))
        case StreamQueryResultFooter(id, queryStats, mayBePartial, partialResultReason) =>
                                  val footerBuilder = builder.getFooterBuilder.setId(id)
                                    .setStats(queryStats.toProto)
                                    .setMayBePartial(mayBePartial)
                                  partialResultReason.foreach(footerBuilder.setPartialResultReason)
                                  builder.setFooter(footerBuilder)
        case StreamQueryError(id, queryStats, t) =>
                                  builder.setError(
                                    builder.getErrorBuilder.setId(id)
                                      .setStats(queryStats.toProto).setThrowable(t.toProto)
                                  )
      }
      builder.build()
    }
  }


  implicit class StreamingResponseFromProtoConverter(responseProto: GrpcMultiPartitionQueryService.StreamingResponse) {
    def fromProto: StreamQueryResponse = {
      // Not checking optional type's existence
      if (responseProto.hasBody) {
        val body = responseProto.getBody
        StreamQueryResult(body.getId, body.getResult.fromProto)
      } else if (responseProto.hasFooter) {
        val footer = responseProto.getFooter
        StreamQueryResultFooter(footer.getId, footer.getStats.fromProto,
          footer.getMayBePartial,
          if (footer.hasPartialResultReason) Some(footer.getPartialResultReason) else None)
      } else if (responseProto.hasHeader) {
        val header = responseProto.getHeader
        StreamQueryResultHeader(header.getId, header.getResultSchema.fromProto)
      } else {
        val error = responseProto.getError
        StreamQueryError(error.getId, error.getStats.fromProto, error.getThrowable.fromProto)
      }
    }
  }

  implicit class StreamingResponseIteratorToQueryResponseProtoConverter(
                    responseProto: Iterator[GrpcMultiPartitionQueryService.StreamingResponse]) extends StrictLogging {
    def toQueryResponse: QueryResponse =
      responseProto.foldLeft(
        // Reduce streaming response to a
        // Tuple of (id, result schema, list of rvs, query stats, partial result flag, partial result reason, exception)

      ("", ResultSchema.empty, List.empty[SerializedRangeVector],
                  QueryStats(), false, Option.empty[String], Option.empty[Throwable])) {
        case ((id, schema, rvs, stats, isPartial, partialReason, t), response) =>
          if (response.hasBody) {
              val body = response.getBody
              (id, schema, body.getResult.fromProto :: rvs, stats, isPartial, partialReason, t)
          } else if (response.hasFooter) {
            val footer = response.getFooter
            (id, schema, rvs, footer.getStats.fromProto, footer.getMayBePartial,
              if (footer.hasPartialResultReason) Some(footer.getPartialResultReason) else None, t)
          } else if (response.hasHeader) {
            val header = response.getHeader
            (header.getId, header.getResultSchema.fromProto, rvs, stats, isPartial, partialReason, t)
          } else {
            val error = response.getError
            (error.getId, schema, rvs, error.getStats.fromProto, isPartial,
              partialReason, Some(error.getThrowable.fromProto))
          }
      } match {
        case (id, schema, rvs, stats, isPartial, partialReason, None) =>
                                                          QueryResult(id, schema, rvs, stats, isPartial, partialReason)
        case (id, _, _, stats, _, _, Some(t))                        =>
                                                          QueryError(id, stats, t)

      }
  }

}
