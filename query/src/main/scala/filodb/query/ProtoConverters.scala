package filodb.query


import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.JavaConverters._

import akka.pattern.AskTimeoutException
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.StrictLogging

import filodb.core.binaryrecord2.{RecordContainer, RecordSchema}
import filodb.core.memstore.SchemaMismatch
import filodb.core.metadata.Column.ColumnType._
import filodb.core.query._
import filodb.grpc.{GrpcMultiPartitionQueryService, ProtoRangeVector}
import filodb.grpc.GrpcMultiPartitionQueryService.QueryParams

// scalastyle:off number.of.methods
// scalastyle:off number.of.types
// scalastyle:off file.size.limit
// scalastyle:off method.length
object ProtoConverters {

  implicit class RangeVectorToProtoConversion(rv: SerializedRangeVector) {

    def toProto: ProtoRangeVector.SerializedRangeVector = {
      import collection.JavaConverters._
      val builder = ProtoRangeVector.SerializedRangeVector.newBuilder()
      builder.setKey(rv.key.toProto)
      builder.setNumRowsSerialized(rv.numRowsSerialized)
      builder.addAllRecordContainers(rv.containersIterator.map(container => ByteString.copyFrom(
        if (container.hasArray) container.array else container.trimmedArray)).toIterable.asJava)
      builder.setRecordSchema(rv.schema.toProto)
      builder.setStartRecordNo(rv.startRecordNo)
      rv.outputRange match {
        case Some(rvr: RvRange) => builder.setRvRange(rvr.toProto)
        case _ =>
      }
      builder.build()
    }
  }

  implicit class RangeVectorFromProtoConversion(rvProto: ProtoRangeVector.SerializedRangeVector) {

    def fromProto: SerializedRangeVector = {
      import collection.JavaConverters._

      new SerializedRangeVector(rvProto.getKey.fromProto,
        rvProto.getNumRowsSerialized,
        rvProto.getRecordContainersList.asScala.map(byteString => RecordContainer(byteString.toByteArray)),
        rvProto.getRecordSchema.fromProto,
        rvProto.getStartRecordNo,
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
      builder.setSchemaVersion(rvk.schemaVersion)
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
                   schemaVersion = rvkProto.getSchemaVersion)
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

  // ColumnType
  implicit class ColumnTypeToProtoConverter(ct: filodb.core.metadata.Column.ColumnType) {
    def toProto: ProtoRangeVector.ColumnType = {
      val grpcColType = ct match {
        case IntColumn => ProtoRangeVector.ColumnType.IntColumn
        case LongColumn => ProtoRangeVector.ColumnType.LongColumn
        case DoubleColumn => ProtoRangeVector.ColumnType.DoubleColumn
        case StringColumn => ProtoRangeVector.ColumnType.StringColumn
        case TimestampColumn => ProtoRangeVector.ColumnType.TimestampColumn
        case MapColumn => ProtoRangeVector.ColumnType.MapColumn
        case BinaryRecordColumn => ProtoRangeVector.ColumnType.BinaryRecordColumn
        case HistogramColumn => ProtoRangeVector.ColumnType.HistogramColumn
      }

      grpcColType
    }
  }

  implicit class ColumnTypeFromProtoConverter(ct: ProtoRangeVector.ColumnType) {
    def fromProto: filodb.core.metadata.Column.ColumnType = {
      val colType = ct match {
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
      colType
    }
  }

  implicit class ColumnInfoToProtoConversion(ci: ColumnInfo) {
    def toProto: ProtoRangeVector.ColumnInfo = {
      val builder = ProtoRangeVector.ColumnInfo.newBuilder()
      val grpcColType = ci.colType.toProto
      builder.setColumnType(grpcColType)
      builder.setName(ci.name)
      builder.build()
    }
  }

  implicit class ColumnInfoFromProtoConversion(ci: ProtoRangeVector.ColumnInfo) {
    def fromProto: ColumnInfo = {
      val colType = ci.getColumnType.fromProto
      ColumnInfo(ci.getName, colType)
    }
  }

  implicit class QueryParamsFromProtoConversion(qp: QueryParams) {
    def fromProto: TsdbQueryParams = {
      if (qp.getIsUnavailable)
        UnavailablePromQlQueryParams
      else
        PromQlQueryParams(promQl = qp.getPromQL, startSecs = qp.getStart,
          stepSecs = qp.getStep, endSecs = qp.getEnd,
          remoteQueryPath = if (qp.hasRemoteQueryPath) Some(qp.getRemoteQueryPath) else None,
          verbose = qp.getVerbose)
    }
  }


  implicit class QueryParamsToProtoConversion(qp: TsdbQueryParams) {
    def toProto: QueryParams = {
      val builder = QueryParams.newBuilder()
      qp match {
        case UnavailablePromQlQueryParams => builder.setIsUnavailable(true)
        case PromQlQueryParams(promQl, startSecs, stepSecs, endSecs, remoteQueryPath, verbose) =>
          builder.setIsUnavailable(false)
          builder.setPromQL(promQl)
          builder.setStart(startSecs)
          builder.setStep(stepSecs)
          builder.setEnd(endSecs)
          builder.setVerbose(verbose)
          builder.setTime(endSecs)
          if (remoteQueryPath.isDefined)
            builder.setRemoteQueryPath(remoteQueryPath.get)
      }
      builder.build()
    }
  }

  implicit class DownClusterToProtoConversion(dc: DownCluster) {
    def toProto: GrpcMultiPartitionQueryService.DownCluster = {
      val builder = GrpcMultiPartitionQueryService.DownCluster.newBuilder()
      builder.setClusterType(dc.clusterType)
      dc.downShards.foreach(s => builder.addDownShards(s))
      builder.build()
    }
  }

  implicit class DownClusterFromProtoConverter(dc: GrpcMultiPartitionQueryService.DownCluster) {
    def fromProto: DownCluster = {
      val shards = scala.collection.mutable.LinkedHashSet[Int]()
      dc.getDownShardsList.asScala.foreach(s => shards.add(s))
      DownCluster(dc.getClusterType(), shards)
    }
  }

  implicit class DownWorkUnitToProtoConversion(dc: DownWorkUnit) {
    def toProto: GrpcMultiPartitionQueryService.DownWorkUnit = {
      val builder = GrpcMultiPartitionQueryService.DownWorkUnit.newBuilder()
      builder.setName(dc.name)
      dc.downClusters.foreach(dc => builder.addDownClusters(dc.toProto))
      builder.build()
    }
  }

  implicit class DownWorkUnitFromProtoConverter(dc: GrpcMultiPartitionQueryService.DownWorkUnit) {
    def fromProto: DownWorkUnit = {
      val downClusters = scala.collection.mutable.LinkedHashSet[DownCluster]()
      dc.getDownClustersList.asScala.foreach(downCluster => downClusters.add(downCluster.fromProto))
      DownWorkUnit(dc.getName(), downClusters)
    }
  }

  implicit class DownPartitionToProtoConversion(dp: DownPartition) {
    def toProto: GrpcMultiPartitionQueryService.DownPartition = {
      val builder = GrpcMultiPartitionQueryService.DownPartition.newBuilder()
      builder.setName(dp.name)
      dp.downWorkUnits.foreach(dc => builder.addDownWorkUnits(dc.toProto))
      builder.build()
    }
  }

  implicit class DownPartitionFromProtoConverter(dp: GrpcMultiPartitionQueryService.DownPartition) {
    def fromProto: DownPartition = {
      val downDCs = scala.collection.mutable.LinkedHashSet[DownWorkUnit]()
      dp.getDownWorkUnitsList.asScala.foreach(downDC => downDCs.add(downDC.fromProto))
      DownPartition(dp.getName, downDCs)
    }
  }

  implicit class FailoverModeToProtoConverter(fm: FailoverMode) {
    def toProto: GrpcMultiPartitionQueryService.FailoverMode = {
      fm match {
        case LegacyFailoverMode => GrpcMultiPartitionQueryService.FailoverMode.LEGACY_FAILOVER_MODE
        case ShardLevelFailoverMode => GrpcMultiPartitionQueryService.FailoverMode.SHARD_LEVEL_FAILOVER_MODE
      }
    }
  }

  implicit class FailoverModeFromProtoConverter(fm: GrpcMultiPartitionQueryService.FailoverMode) {
    def fromProto: FailoverMode = {
      fm match {
        case GrpcMultiPartitionQueryService.FailoverMode.LEGACY_FAILOVER_MODE => LegacyFailoverMode
        case GrpcMultiPartitionQueryService.FailoverMode.SHARD_LEVEL_FAILOVER_MODE => ShardLevelFailoverMode
        case GrpcMultiPartitionQueryService.FailoverMode.UNRECOGNIZED =>
          throw new IllegalArgumentException("Unrecognized failover mode")
      }
    }
  }

  implicit class PlannerParamsToProtoConverter(pp: PlannerParams) {
    def toProto: GrpcMultiPartitionQueryService.PlannerParams = {
      val enforcedLimits = pp.enforcedLimits.toProto
      val warnLimits = pp.warnLimits.toProto
      val builder = GrpcMultiPartitionQueryService.PlannerParams.newBuilder()
      builder.setApplicationId(pp.applicationId)
      builder.setQueryTimeoutMillis(pp.queryTimeoutMillis)
      builder.setEnforcedLimits(enforcedLimits)
      builder.setWarnLimits(warnLimits)
      pp.queryOrigin.foreach(qo => builder.setQueryOrigin(qo))
      pp.queryOriginId.foreach(qoi => builder.setQueryOriginId(qoi))
      pp.queryPrincipal.foreach(qp => builder.setQueryPrincipal(qp))
      builder.setTimeSplitEnabled(pp.timeSplitEnabled)
      builder.setMinTimeRangeForSplitMs(pp.minTimeRangeForSplitMs)
      builder.setSplitSizeMs(pp.splitSizeMs)
      builder.setSkipAggregatePresent(pp.skipAggregatePresent)
      builder.setProcessFailure(pp.processFailure)
      builder.setProcessMultiPartition(pp.processMultiPartition)
      builder.setAllowPartialResults(pp.allowPartialResults)
      builder.setUseProtoExecPlans(pp.useProtoExecPlans)
      builder.setReduceShardKeyRegexFanout(pp.reduceShardKeyRegexFanout)
      builder.setMaxShardKeyRegexFanoutBatchSize(pp.maxShardKeyRegexFanoutBatchSize)
      builder.setAllowNestedAggregatePushdown(pp.allowNestedAggregatePushdown)
      pp.downPartitions.foreach(dp => builder.addDownPartitions(dp.toProto))
      builder.setFailoverMode(pp.failoverMode.toProto)
      builder.build()
    }
  }

  implicit class PlannerParamsFromProtoConverter(gpp: GrpcMultiPartitionQueryService.PlannerParams) {
    def fromProto: PlannerParams = {
      val enforcedLimits = gpp.getEnforcedLimits.fromProto(PerQueryLimits.defaultEnforcedLimits())
      val warnLimits = gpp.getWarnLimits.fromProto(PerQueryLimits.defaultWarnLimits())
      val downPartitionsMutableSet = scala.collection.mutable.Set[DownPartition]()
      val downPartitions = gpp.getDownPartitionsList().asScala.foreach(dp => downPartitionsMutableSet.add(dp.fromProto))
      val failoverMode = if (gpp.hasFailoverMode) {
        gpp.getFailoverMode.fromProto
      } else {
        LegacyFailoverMode
      }
      val pp = PlannerParams()

      pp.copy(
        applicationId = if (gpp.hasApplicationId) gpp.getApplicationId else pp.applicationId,
        queryTimeoutMillis = if (gpp.hasQueryTimeoutMillis) gpp.getQueryTimeoutMillis else pp.queryTimeoutMillis,
        enforcedLimits = enforcedLimits,
        warnLimits = warnLimits,
        queryOrigin = if (gpp.hasQueryOrigin) Option(gpp.getQueryOrigin) else None,
        queryOriginId = if (gpp.hasQueryOriginId) Option(gpp.getQueryOriginId) else None,
        queryPrincipal = if (gpp.hasQueryPrincipal) Option(gpp.getQueryPrincipal) else None,
        timeSplitEnabled = if (gpp.hasTimeSplitEnabled) gpp.getTimeSplitEnabled else pp.timeSplitEnabled,
        minTimeRangeForSplitMs = if (gpp.hasMinTimeRangeForSplitMs) gpp.getMinTimeRangeForSplitMs
        else pp.minTimeRangeForSplitMs,
        splitSizeMs = if (gpp.hasSplitSizeMs) gpp.getSplitSizeMs else pp.splitSizeMs,
        skipAggregatePresent = if (gpp.hasSkipAggregatePresent) gpp.getSkipAggregatePresent
        else pp.skipAggregatePresent,
        processFailure = if (gpp.hasProcessFailure) gpp.getProcessFailure else pp.processFailure,
        processMultiPartition = if (gpp.hasProcessMultiPartition) gpp.getProcessMultiPartition
        else pp.processMultiPartition,
        allowPartialResults = if (gpp.hasAllowPartialResults) gpp.getAllowPartialResults else pp.allowPartialResults,
        useProtoExecPlans = if (gpp.hasUseProtoExecPlans) gpp.getUseProtoExecPlans else pp.useProtoExecPlans,
        reduceShardKeyRegexFanout = if (gpp.hasReduceShardKeyRegexFanout) gpp.getReduceShardKeyRegexFanout
        else pp.reduceShardKeyRegexFanout,
        maxShardKeyRegexFanoutBatchSize = if (gpp.hasMaxShardKeyRegexFanoutBatchSize)
          gpp.getMaxShardKeyRegexFanoutBatchSize else pp.maxShardKeyRegexFanoutBatchSize,
        allowNestedAggregatePushdown =
          if (gpp.hasAllowNestedAggregatePushdown) gpp.getAllowNestedAggregatePushdown
          else pp.allowNestedAggregatePushdown,
        downPartitions = downPartitionsMutableSet,
        failoverMode = failoverMode
      )
    }
  }

  implicit class PerQueryLimitsToProtoConverter(sq: PerQueryLimits) {
    def toProto: GrpcMultiPartitionQueryService.PerQueryLimits = {
      val quotaBuilder = GrpcMultiPartitionQueryService.PerQueryLimits.newBuilder()
      quotaBuilder.setExecPlanSamples(sq.execPlanSamples)
      quotaBuilder.setExecPlanResultBytes(sq.execPlanResultBytes)
      quotaBuilder.setGroupByCardinality(sq.groupByCardinality)
      quotaBuilder.setJoinQueryCardinality(sq.joinQueryCardinality)
      quotaBuilder.setTimeSeriesSamplesScannedBytes(sq.timeSeriesSamplesScannedBytes)
      quotaBuilder.setTimeSeriesScanned(sq.timeSeriesScanned)
      quotaBuilder.setRawScannedBytes(sq.rawScannedBytes)
      quotaBuilder.build()
    }
  }
  implicit class PerQueryLimitsFromProtoConverter(giq: GrpcMultiPartitionQueryService.PerQueryLimits) {
    def fromProto(): PerQueryLimits = {
      val q = PerQueryLimits()
      fromProto(q)
    }
    def fromProto(defaultQ: PerQueryLimits): PerQueryLimits = {
      val limits = defaultQ.copy(
        execPlanSamples =
          if (giq.hasExecPlanSamples)
            giq.getExecPlanSamples
          else
            defaultQ.execPlanSamples,
        execPlanResultBytes =
          if (giq.hasExecPlanResultBytes)
            giq.getExecPlanResultBytes
          else
            defaultQ.execPlanResultBytes,
        groupByCardinality =
          if (giq.hasGroupByCardinality)
            giq.getGroupByCardinality
          else
            defaultQ.groupByCardinality,
        joinQueryCardinality =
          if (giq.hasJoinQueryCardinality)
            giq.getJoinQueryCardinality
          else
            defaultQ.joinQueryCardinality,
        timeSeriesSamplesScannedBytes =
          if (giq.hasTimeSeriesSamplesScannedBytes)
            giq.getTimeSeriesSamplesScannedBytes
          else
            defaultQ.timeSeriesSamplesScannedBytes,
        timeSeriesScanned =
          if (giq.hasTimeSeriesScanned)
            giq.getTimeSeriesScanned
          else
            defaultQ.timeSeriesScanned,
        rawScannedBytes =
          if (giq.hasRawScannedBytes)
            giq.getRawScannedBytes
          else
            defaultQ.rawScannedBytes
      )
      limits
    }
  }

  implicit class StatToProtoConverter(stat: Stat) {
    def toProto: GrpcMultiPartitionQueryService.Stat = {
      val builder = GrpcMultiPartitionQueryService.Stat.newBuilder()
      builder.setResultBytes(stat.resultBytes.get())
      builder.setDataBytesScanned(stat.dataBytesScanned.get())
      builder.setTimeSeriesScanned(stat.timeSeriesScanned.get())
      builder.setCpuNanos(stat.cpuNanos.get())
      builder.build()
    }
  }

  implicit class StatFromProtoConverter(statGrpc: GrpcMultiPartitionQueryService.Stat) {
    def fromProto: Stat = {
      val stat = Stat()
      stat.timeSeriesScanned.addAndGet(statGrpc.getTimeSeriesScanned)
      stat.dataBytesScanned.addAndGet(statGrpc.getDataBytesScanned)
      stat.resultBytes.addAndGet(statGrpc.getResultBytes)
      stat.cpuNanos.addAndGet(statGrpc.getCpuNanos)
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

  implicit class QueryWarningsToProtoConverter(w: QueryWarnings) {
    def toProto: GrpcMultiPartitionQueryService.QueryWarnings = {
      val builder = GrpcMultiPartitionQueryService.QueryWarnings.newBuilder()
      builder.setExecPlanSamples(w.execPlanSamples.get())
      builder.setExecPlanResultBytes(w.execPlanResultBytes.get())
      builder.setGroupByCardinality(w.groupByCardinality.get())
      builder.setJoinQueryCardinality(w.joinQueryCardinality.get())
      builder.setTimeSeriesSamplesScannedBytes(w.timeSeriesSamplesScannedBytes.get())
      builder.setTimeSeriesScanned(w.timeSeriesScanned.get())
      builder.setRawScannedBytes(w.rawScannedBytes.get())
      builder.build()
    }
  }

  implicit class QueryWarningsFromProtoConverter(wGrpc: GrpcMultiPartitionQueryService.QueryWarnings) {
    def fromProto: QueryWarnings = {
      val ws = QueryWarnings(
        new AtomicInteger(wGrpc.getExecPlanSamples()),
        new AtomicLong(wGrpc.getExecPlanResultBytes()),
        new AtomicInteger(wGrpc.getGroupByCardinality()),
        new AtomicInteger(wGrpc.getJoinQueryCardinality()),
        new AtomicLong(wGrpc.getTimeSeriesSamplesScannedBytes()),
        new AtomicInteger(wGrpc.getTimeSeriesScanned()),
        new AtomicLong(wGrpc.getRawScannedBytes())
      )
      ws
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
      t match {
        case filodb.core.query.QueryLimitException(message, queryId)                          =>
                                                      builder.putMetadata("queryId", queryId)
                                                      builder.putMetadata("message", message)
        case filodb.core.QueryTimeoutException(elapsedQueryTime, timedOutAt)                  =>
                                                      builder.putMetadata("timedOutAt", timedOutAt)
                                                      builder.putMetadata("elapsedQueryTime", s"$elapsedQueryTime")
        case SchemaMismatch(expected, found, clazz)                                           =>
                                                      builder.putMetadata("expected", expected)
                                                      builder.putMetadata("found", found)
                                                      builder.putMetadata("clazz", clazz)
        case RemoteQueryFailureException(statusCode, requestStatus, errorType, errorMessage)  =>
                                                      builder.putMetadata("statusCode", s"$statusCode")
                                                      builder.putMetadata("requestStatus", requestStatus)
                                                      builder.putMetadata("errorType", errorType)
                                                      builder.putMetadata("errorMessage", errorMessage)

        case _                                                                                =>
                                                      if (t.getMessage != null) builder.setMessage(t.getMessage)

      }
      builder.setExceptionClass(t.getClass.getName)
      if (t.getCause != null) {
        builder.setCause(t.getCause.toProto)
        builder.setExceptionClass(t.getClass.getName)
      }
      t.getStackTrace.iterator.foreach(elem => builder.addStack(elem.toProto))
      builder.build()
    }
  }


  implicit class ThrowableFromProtoConverter(throwableProto: GrpcMultiPartitionQueryService.Throwable) {
    def fromProto: Throwable = {
      import scala.collection.JavaConverters._
      val cause = if (throwableProto.hasCause) Some(throwableProto.getCause.fromProto) else None
      // to avoid multiple combinations, we will treat null message as an empty string
      val message  = if (throwableProto.hasMessage) throwableProto.getMessage else ""
      val t = throwableProto.getExceptionClass match {
        case "filodb.core.query.QueryLimitException" =>
                val metaMap = throwableProto.getMetadataMap
                val queryId = metaMap.getOrDefault("queryId", "")
                val message = metaMap.getOrDefault("message", "")
                QueryLimitException(message, queryId)
        case "filodb.core.QueryTimeoutException"     =>
                  val metaMap = throwableProto.getMetadataMap
                  val eqt = metaMap.getOrDefault("elapsedQueryTime", "0").toLong
                  val timedOutAt = metaMap.getOrDefault("timedOutAt", "")
                  filodb.core.QueryTimeoutException(eqt, timedOutAt)
        case "java.lang.IllegalArgumentException"    =>
            cause.map(new IllegalArgumentException(throwableProto.getMessage, _))
              .getOrElse(new IllegalArgumentException(throwableProto.getMessage))
        case "java.lang.UnsupportedOperationException"    =>
          cause.map(new UnsupportedOperationException(throwableProto.getMessage, _))
            .getOrElse(new UnsupportedOperationException(throwableProto.getMessage))
        case "filodb.query.BadQueryException"        =>
            new BadQueryException(if (throwableProto.hasMessage) throwableProto.getMessage else "")
        case "java.util.concurrent.TimeoutException" =>
          if (throwableProto.hasMessage) new TimeoutException(throwableProto.getMessage)
          else new TimeoutException()
        case "scala.NotImplementedError" =>
          new NotImplementedError(message)
        case "filodb.core.memstore.SchemaMismatch" =>
                val metaMap = throwableProto.getMetadataMap
                val expected = metaMap.getOrDefault("expected", "")
                val found = metaMap.getOrDefault("found", "")
                val clazz = metaMap.getOrDefault("clazz", "")
                SchemaMismatch(expected, found, clazz)
        case "filodb.core.query.ServiceUnavailableException"  =>
          new ServiceUnavailableException(message)
        case "filodb.query.RemoteQueryFailureException"       =>
          val metaMap = throwableProto.getMetadataMap
          val statusCode = metaMap.getOrDefault("statusCode", "0").toInt
          val requestStatus = metaMap.getOrDefault("requestStatus", "")
          val errorType = metaMap.getOrDefault("errorType", "")
          val errorMessage = metaMap.getOrDefault("errorMessage", "")
          RemoteQueryFailureException(statusCode, requestStatus, errorType, errorMessage)
        case "akka.pattern.AskTimeoutException"               =>
          cause.map(new AskTimeoutException(message, _)).getOrElse(new AskTimeoutException(message))
        case _          =>
            cause.map(new Throwable(message, _)).getOrElse(new Throwable(message))
      }
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
        case QueryResult(id, resultSchema, result, queryStats, warnings, mayBePartial, partialResultReason)   =>
                                          builder.setId(id)
                                          builder.setResultSchema(resultSchema.toProto)
                                          builder.setStats(queryStats.toProto)
                                          builder.setWarnings(warnings.toProto)
                                          builder.setMayBePartial(mayBePartial)
                                          builder.addAllResult(
                                            result.map(_.asInstanceOf[SerializableRangeVector].toProto).asJava)
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
        QueryResult(
          responseProto.getId, responseProto.getResultSchema.fromProto,
          responseProto.getResultList.asScala.map(_.fromProto).toList,
          responseProto.getStats.fromProto,
          if (responseProto.hasWarnings) responseProto.getWarnings.fromProto else QueryWarnings(),
          responseProto.getMayBePartial,
          if (responseProto.hasPartialResultReason) Some(responseProto.getPartialResultReason) else None)
      }
    }
  }

  implicit class StreamingResponseToProtoConverter(response: StreamQueryResponse) {
    def toProto: GrpcMultiPartitionQueryService.StreamingResponse = {
      val builder = GrpcMultiPartitionQueryService.StreamingResponse.newBuilder()
      response match {
        case StreamQueryResultHeader(id, planId, resultSchema) =>
                                    builder.setHeader(
                                    builder
                                      .getHeaderBuilder
                                      .setQueryId(id)
                                      .setPlanId(planId)
                                      .setResultSchema(resultSchema.toProto))
        case StreamQueryResult(id, planId, result) =>
                                    val bodyBuilder = builder.getBodyBuilder.setQueryId(id).setPlanId(planId)
                                    result.foreach {
                                      case srv: SerializableRangeVector   => bodyBuilder.addResult(srv.toProto)
                                      case other: RangeVector             =>
                                        throw new IllegalStateException(s"Expected a SerializableRangeVector," +
                                          s"got ${other.getClass}")
                                    }
        case StreamQueryResultFooter(id, planId, queryStats, warnings, mayBePartial, partialResultReason) =>
                                  val footerBuilder = builder.getFooterBuilder.setQueryId(id).setPlanId(planId)
                                    .setStats(queryStats.toProto)
                                    .setWarnings(warnings.toProto)
                                    .setMayBePartial(mayBePartial)
                                  partialResultReason.foreach(footerBuilder.setPartialResultReason)
                                  builder.setFooter(footerBuilder)
        case StreamQueryError(id, planId, queryStats, t) =>
                                  builder.setError(
                                    builder.getErrorBuilder.setQueryId(id).setPlanId(planId)
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
        StreamQueryResult(body.getQueryId, body.getPlanId, body.getResultList.fromProto)
      } else if (responseProto.hasFooter) {
        val footer = responseProto.getFooter
        StreamQueryResultFooter(
          footer.getQueryId,
          footer.getPlanId,
          footer.getStats.fromProto,
          if (footer.hasWarnings) footer.getWarnings.fromProto else new QueryWarnings(),
          footer.getMayBePartial,
          if (footer.hasPartialResultReason) Some(footer.getPartialResultReason) else None)
      } else if (responseProto.hasHeader) {
        val header = responseProto.getHeader
        StreamQueryResultHeader(header.getQueryId, header.getPlanId, header.getResultSchema.fromProto)
      } else {
        val error = responseProto.getError
        StreamQueryError(error.getQueryId, error.getPlanId, error.getStats.fromProto, error.getThrowable.fromProto)
      }
    }
  }

  implicit class StreamingResponseIteratorToQueryResponseProtoConverter(
                    responseProto: Iterator[GrpcMultiPartitionQueryService.StreamingResponse]) extends StrictLogging {
    def toQueryResponse: QueryResponse =
      responseProto.foldLeft(
        // Reduce streaming response to a
        // Tuple of (id, result schema, list of rvs, query stats, partial result flag, partial result reason, exception)
        (
          "",
          ResultSchema.empty,
          Seq.empty[SerializableRangeVector],
          QueryStats(),
          QueryWarnings(),
          false, Option.empty[String], Option.empty[Throwable]
        )
      ) {
        case ((id, schema, rvs, stats, warnings, isPartial, partialReason, t), response) =>
          if (response.hasBody) {
              val body = response.getBody
              (id, schema,
                rvs ++ body.getResultList.asScala.map(_.fromProto).toList, stats, warnings, isPartial, partialReason, t)
          } else if (response.hasFooter) {
            val footer = response.getFooter
            (
              id, schema, rvs, footer.getStats.fromProto,
              if (footer.hasWarnings) footer.getWarnings.fromProto else QueryWarnings(),
              footer.getMayBePartial,
              if (footer.hasPartialResultReason) Some(footer.getPartialResultReason) else None,
              t
            )
          } else if (response.hasHeader) {
            val header = response.getHeader
            (header.getQueryId, header.getResultSchema.fromProto, rvs, stats, warnings, isPartial, partialReason, t)
          } else {
            val error = response.getError
            (error.getQueryId, schema, rvs, error.getStats.fromProto, QueryWarnings(), isPartial,
              partialReason, Some(error.getThrowable.fromProto))
          }
      } match {
        case (id, schema, rvs, stats, warnings, isPartial, partialReason, None) =>
          QueryResult(id, schema, rvs, stats, warnings, isPartial, partialReason)
        case (id, _, _, stats, _, _, _, Some(t))                                =>
          QueryError(id, stats, t)
      }
  }

  implicit class RangeParamFromProtoConverter(rp: RangeParams) {
    def toProto: ProtoRangeVector.RangeParams = {
      val builder = ProtoRangeVector.RangeParams.newBuilder()
      builder.setStep(rp.stepSecs)
      builder.setEndSecs(rp.endSecs)
      builder.setStartSecs(rp.startSecs)
      builder.build()
    }
  }

  implicit class RangeParamToProtoConverter(rp: ProtoRangeVector.RangeParams) {
    def fromProto: RangeParams = RangeParams(rp.getStartSecs, rp.getStep, rp.getEndSecs)
  }

  implicit class ScalarFixedDoubleToProtoConverter(sfd: ScalarFixedDouble) {
    def toProto: ProtoRangeVector.ScalarFixedDouble = {
      val builder = ProtoRangeVector.ScalarFixedDouble.newBuilder()
      builder.setValue(sfd.value)
      builder.setRangeParams(sfd.rangeParams.toProto)
      builder.build()
    }
  }

  implicit class TimeScalarFromProtoConverter(ts: ProtoRangeVector.TimeScalar) {
    def fromProto: TimeScalar = TimeScalar(ts.getRangeParams.fromProto)
  }

  implicit class TimeScalarToProtoConverter(ts: TimeScalar) {
    def toProto: ProtoRangeVector.TimeScalar = {
      val builder = ProtoRangeVector.TimeScalar.newBuilder()
      builder.setRangeParams(ts.rangeParams.toProto)
      builder.build()
    }
  }

  implicit class HourScalarFromProtoConverter(hs: ProtoRangeVector.HourScalar) {
    def fromProto: HourScalar = HourScalar(hs.getRangeParams.fromProto)
  }

  implicit class HourScalarToProtoConverter(hs: HourScalar) {
    def toProto: ProtoRangeVector.HourScalar = {
      val builder = ProtoRangeVector.HourScalar.newBuilder()
      builder.setRangeParams(hs.rangeParams.toProto)
      builder.build()
    }
  }

  implicit class MinuteScalarFromProtoConverter(ms: ProtoRangeVector.MinuteScalar) {
    def fromProto: MinuteScalar = MinuteScalar(ms.getRangeParams.fromProto)
  }

  implicit class MinuteScalarToProtoConverter(ms: MinuteScalar) {
    def toProto: ProtoRangeVector.MinuteScalar = {
      val builder = ProtoRangeVector.MinuteScalar.newBuilder()
      builder.setRangeParams(ms.rangeParams.toProto)
      builder.build()
    }
  }

  implicit class MonthScalarFromProtoConverter(ms: ProtoRangeVector.MonthScalar) {
    def fromProto: MonthScalar = MonthScalar(ms.getRangeParams.fromProto)
  }

  implicit class MonthScalarToProtoConverter(ms: MonthScalar) {
    def toProto: ProtoRangeVector.MonthScalar = {
      val builder = ProtoRangeVector.MonthScalar.newBuilder()
      builder.setRangeParams(ms.rangeParams.toProto)
      builder.build()
    }
  }

  implicit class YearScalarFromProtoConverter(ys: ProtoRangeVector.YearScalar) {
    def fromProto: YearScalar = YearScalar(ys.getRangeParams.fromProto)
  }

  implicit class YearScalarToProtoConverter(ys: YearScalar) {
    def toProto: ProtoRangeVector.YearScalar = {
      val builder = ProtoRangeVector.YearScalar.newBuilder()
      builder.setRangeParams(ys.rangeParams.toProto)
      builder.build()
    }
  }

  implicit class DayOfMonthScalarFromProtoConverter(dom: ProtoRangeVector.DayOfMonthScalar) {
    def fromProto: DayOfMonthScalar = DayOfMonthScalar(dom.getRangeParams.fromProto)
  }

  implicit class DayOfMonthScalarToProtoConverter(dom: DayOfMonthScalar) {
    def toProto: ProtoRangeVector.DayOfMonthScalar = {
      val builder = ProtoRangeVector.DayOfMonthScalar.newBuilder()
      builder.setRangeParams(dom.rangeParams.toProto)
      builder.build()
    }
  }

  implicit class DayOfWeekScalarFromProtoConverter(dow: ProtoRangeVector.DayOfWeekScalar) {
    def fromProto: DayOfWeekScalar = DayOfWeekScalar(dow.getRangeParams.fromProto)
  }

  implicit class DayOfWeekScalarToProtoConverter(dom: DayOfWeekScalar) {
    def toProto: ProtoRangeVector.DayOfWeekScalar = {
      val builder = ProtoRangeVector.DayOfWeekScalar.newBuilder()
      builder.setRangeParams(dom.rangeParams.toProto)
      builder.build()
    }
  }

  implicit class DaysInMonthScalarFromProtoConverter(dow: ProtoRangeVector.DaysInMonthScalar) {
    def fromProto: DaysInMonthScalar = DaysInMonthScalar(dow.getRangeParams.fromProto)
  }

  implicit class DaysInMonthScalarToProtoConverter(dom: DaysInMonthScalar) {
    def toProto: ProtoRangeVector.DaysInMonthScalar = {
      val builder = ProtoRangeVector.DaysInMonthScalar.newBuilder()
      builder.setRangeParams(dom.rangeParams.toProto)
      builder.build()
    }
  }

  implicit class ScalarVaryingDoubleFromProtoConverter(svd: ProtoRangeVector.ScalarVaryingDouble) {
    import collection.JavaConverters._
    def fromProto: ScalarVaryingDouble = ScalarVaryingDouble(
      svd.getTimeValueMapMap.asScala.map{ case (k, v) => (k.toLong, v.toDouble)}.toMap,
      if (svd.hasRvRange) Some(svd.getRvRange.fromProto) else None)
  }

  implicit class ScalarVaryingDoubleToProtoConverter(dom: ScalarVaryingDouble) {
    import collection.JavaConverters._
    def toProto: ProtoRangeVector.ScalarVaryingDouble = {
      val builder = ProtoRangeVector.ScalarVaryingDouble.newBuilder()
      dom.outputRange match {
        case Some(rvRange)  =>  builder.setRvRange(rvRange.toProto)
        case None           =>
      }
      builder.putAllTimeValueMap(dom.timeValueMap.map{
        case (k, v) => (java.lang.Long.valueOf(k), java.lang.Double.valueOf(v))}.asJava)
      builder.build()
    }
  }

  implicit class ScalarFixedDoubleFromProtoConverter(sfd: ProtoRangeVector.ScalarFixedDouble) {
    def fromProto: ScalarFixedDouble = ScalarFixedDouble(sfd.getRangeParams.fromProto, sfd.getValue)
  }

  implicit class RepeatValueVectorToProtoConverter(rv: RepeatValueVector) {
    def toProto: ProtoRangeVector.RepeatValueVector = {
      val builder = ProtoRangeVector.RepeatValueVector.newBuilder()
      rv.outputRange.map(_.toProto).map(builder.setRvRange)
      builder.setRecordSchema(rv.recordSchema.toProto)
      builder.setKey(rv.key.toProto)
      builder.build()
    }
  }

  implicit class RepeatValueVectorFromProtoConverter(rvProto: ProtoRangeVector.RepeatValueVector) {
    def fromProto: RepeatValueVector = {
      import collection.JavaConverters._
      val schema = rvProto.getRecordSchema.fromProto
      val containers = rvProto.getRecordContainersList
        .asScala.map(byteString => RecordContainer(byteString.toByteArray))
      val rowReader = containers.toIterator.flatMap(_.iterate(schema)).find(_ => true)
      new RepeatValueVector(rvProto.getKey.fromProto,
        rvProto.getRvRange.getStartMs, rvProto.getRvRange.getStep, rvProto.getRvRange.getEndMs,
        rowReader,
        schema)
    }
  }

  implicit class SerializableRangeVectorFromProtoConverter(rv: ProtoRangeVector.SerializableRangeVector) {
    def fromProto: SerializableRangeVector =
      if (rv.hasScalarFixedDouble) {
        rv.getScalarFixedDouble.fromProto
      } else if (rv.hasTimeScalar) {
        rv.getTimeScalar.fromProto
      } else if (rv.hasHourScalar) {
        rv.getHourScalar.fromProto
      } else if (rv.hasMinuteScalar) {
        rv.getMinuteScalar.fromProto
      } else if (rv.hasMonthScalar) {
        rv.getMonthScalar.fromProto
      } else if (rv.hasYearScalar) {
        rv.getYearScalar.fromProto
      } else if (rv.hasDayOfMonthScalar) {
        rv.getDayOfMonthScalar.fromProto
      } else if (rv.hasDayOfWeekScalar) {
        rv.getDayOfWeekScalar.fromProto
      } else if (rv.hasDaysInMonthScalar) {
        rv.getDaysInMonthScalar.fromProto
      } else if (rv.hasSerializedRangeVector) {
        rv.getSerializedRangeVector.fromProto
      } else if (rv.hasRepeatValueVector) {
        rv.getRepeatValueVector.fromProto
      } else {
        rv.getScalarVaryingDouble.fromProto
      }
  }


  implicit class SerializableRangeVectorListFromProtoConverter(
                                            rvList: java.util.List[ProtoRangeVector.SerializableRangeVector]) {
    import collection.JavaConverters._
    def fromProto: Seq[SerializableRangeVector] = rvList.asScala.map(_.fromProto)
  }

  implicit class SerializableRangeVectorToProtoConverter(rv: SerializableRangeVector) {
    def toProto: ProtoRangeVector.SerializableRangeVector = {
      val builder = ProtoRangeVector.SerializableRangeVector.newBuilder()
      rv match {
        case sfd: ScalarFixedDouble            => builder.setScalarFixedDouble(sfd.toProto).build()
        case ts: TimeScalar                    => builder.setTimeScalar(ts.toProto).build()
        case hs: HourScalar                    => builder.setHourScalar(hs.toProto).build()
        case ms: MinuteScalar                  => builder.setMinuteScalar(ms.toProto).build()
        case mos: MonthScalar                  => builder.setMonthScalar(mos.toProto).build()
        case ys: YearScalar                    => builder.setYearScalar(ys.toProto).build()
        case dm: DayOfMonthScalar              => builder.setDayOfMonthScalar(dm.toProto).build()
        case dw: DayOfWeekScalar               => builder.setDayOfWeekScalar(dw.toProto).build()
        case dims: DaysInMonthScalar           => builder.setDaysInMonthScalar(dims.toProto).build()
        case srv: SerializedRangeVector        => builder.setSerializedRangeVector(srv.toProto).build()
        case svd: ScalarVaryingDouble          => builder.setScalarVaryingDouble(svd.toProto).build()
        case rvv: RepeatValueVector            => builder.setRepeatValueVector(rvv.toProto).build()
      }
    }
  }

  implicit class SerializableRangeVectorListToProtoConverter(rvSeq: Seq[SerializableRangeVector]) {
    import collection.JavaConverters._
    def toProto: java.util.List[ProtoRangeVector.SerializableRangeVector] = rvSeq.map(_.toProto).asJava
  }


}
// scalastyle:on number.of.methods
// scalastyle:on number.of.types
// scalastyle:on file.size.limit
