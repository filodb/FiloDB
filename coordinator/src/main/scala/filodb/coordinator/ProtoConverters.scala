package filodb.coordinator

import filodb.core.query.Filter.Equals
import filodb.core.query._
import filodb.core.store.{AllChunkScan, InMemoryChunkScan, TimeRangeChunkScan, WriteBufferChunkScan}
import filodb.grpc.ExecPlans
import filodb.grpc.ExecPlans.ExecPlan.DispatcherCase
import filodb.grpc.ExecPlans.ExecPlanContainer.ExecPlanCase
import filodb.query.QueryCommand
import filodb.query.exec.{LeafExecPlan, LocalPartitionDistConcatExec, MultiSchemaPartitionsExec}

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters
import scala.concurrent.Await
import scala.concurrent.duration.Duration

// scalastyle:off number.of.methods
// scalastyle:off number.of.types
// scalastyle:off file.size.limit
// scalastyle:off method.length
object ProtoConverters {

  import filodb.query.ProtoConverters._

  implicit class ChunkScanMethodToProtoConverter(csm: filodb.core.store.ChunkScanMethod) {
    def toProto: ExecPlans.ChunkScan = {
      val builder = ExecPlans.ChunkScan.newBuilder()
      builder.setStartTime(csm.startTime)
      builder.setEndTime(csm.endTime)
      csm match {
        case trcs: TimeRangeChunkScan => builder.setMethod(
          filodb.grpc.ExecPlans.ChunkScanMethod.TIME_RANGE_CHUNK_SCAN
        )
        case acs: filodb.core.store.AllChunkScan.type => builder.setMethod(
          filodb.grpc.ExecPlans.ChunkScanMethod.ALL_CHUNKS_SCAN
        )
        case imcs: filodb.core.store.InMemoryChunkScan.type => builder.setMethod(
          filodb.grpc.ExecPlans.ChunkScanMethod.IN_MEMORY_CHUNK_SCAN
        )
        case wbcs: filodb.core.store.WriteBufferChunkScan.type => builder.setMethod(
          filodb.grpc.ExecPlans.ChunkScanMethod.WRITE_BUFFER_CHUNK_SCAN
        )
      }
      builder.build()
    }
  }

  implicit class ChunkScanMethodFromProtoConverter(cs: ExecPlans.ChunkScan) {
    def fromProto: filodb.core.store.ChunkScanMethod = {
     cs.getMethod match {
       case ExecPlans.ChunkScanMethod.IN_MEMORY_CHUNK_SCAN =>
         InMemoryChunkScan
       case ExecPlans.ChunkScanMethod.ALL_CHUNKS_SCAN =>
         AllChunkScan
       case ExecPlans.ChunkScanMethod.WRITE_BUFFER_CHUNK_SCAN =>
         WriteBufferChunkScan
       case ExecPlans.ChunkScanMethod.TIME_RANGE_CHUNK_SCAN =>
         TimeRangeChunkScan(cs.getStartTime, cs.getEndTime)
       case _ => throw new IllegalArgumentException(s"Unexpected ChunkScanMethod ${cs.getMethod}")
     }
    }
  }

  implicit class FilterToProtoConverter(f: Filter) {
    def toProto: ExecPlans.Filter = {
      val builder = ExecPlans.Filter.newBuilder()
      builder.setOperatorString(f.operatorString)
      //TODO why is value ANY???
      //need to enumerate what can be in ANY, most likely
      // number integer/float/string?
      f.valuesStrings.foreach(vs => builder.addValueStrings(vs.asInstanceOf[String]))
      builder.build()
    }
  }

  implicit class FilterEqualsToProtoConverter(fe: Filter.Equals) {
    def toProto: ExecPlans.FilterEquals = {
      val builder = ExecPlans.FilterEquals.newBuilder()
      builder.setFilter(fe.asInstanceOf[Filter].toProto)
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
  //hack need to support filters properly
  implicit class FilterFromProtoConverter(f: ExecPlans.Filter) {
    def fromProto: Filter = {
//      import scala.collection.JavaConverters._
//      val filterEquals: Filter.Equals = Filter.Equals(
//        f.getValueStringsList.asScala.toSeq
//      )
      val filterEquals: Filter.Equals = Filter.Equals(
        f.getValueStringsList.get(0)
      )
      filterEquals
    }
  }

  implicit class ColumnFilterToProtoConverter(cf: ColumnFilter) {
    def toProto: ExecPlans.ColumnFilter = {
      val builder = ExecPlans.ColumnFilter.newBuilder()
      builder.setColumn(cf.column)
      cf.filter match {
        case e: Equals => builder.setFilterEquals(e.toProto)
        case _ => throw new IllegalArgumentException(s"Unexpected filter ${cf.filter.getClass}")
      }
      builder.build()
    }
  }

  implicit class ColumnFilterFromProtoConverter(cf: ExecPlans.ColumnFilter) {
    def fromProto: ColumnFilter = {
//      import scala.collection.JavaConverters._
      val filter = cf.getFilterCase match {
        case ExecPlans.ColumnFilter.FilterCase.FILTEREQUALS => cf.getFilterEquals.fromProto
        case _ => throw new IllegalArgumentException(s"Unsupported filter ${cf.getFilterCase.toString}")
      }
      ColumnFilter(cf.getColumn, filter)
    }
  }

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
      val a: akka.actor.ActorRef = Await.result(f, Duration(10L, TimeUnit.SECONDS))
      val dispatcher = ActorPlanDispatcher(
        a, apd.getPlanDispatcher.getClusterName
      )
      dispatcher
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

  implicit class ExecPlanToProtoConverter(ep: filodb.query.exec.ExecPlan) {
    def toProto: ExecPlans.ExecPlan = {
      val builder = ExecPlans.ExecPlan.newBuilder()
      builder.setPlanId(ep.planId)
      builder.setQueryContext(ep.queryContext.toProto)
      builder.setEnforceSampleLimit(ep.enforceSampleLimit)
      builder.setQueryCommand(ep.asInstanceOf[QueryCommand].toProto)
      builder.clearDispatcher()
      ep.dispatcher match {
        case apd: ActorPlanDispatcher => builder.setActorPlanDispatcher(apd.toProto)
        case _ => throw new IllegalArgumentException(s"Unexpected PlanDispatcher subclass ${ep.dispatcher.getClass.getName}")
      }
      builder.build()
    }

    def toExecPlanContainerProto: ExecPlans.ExecPlanContainer = {
      val builder = ExecPlans.ExecPlanContainer.newBuilder()
      ep match {
        case mspe : MultiSchemaPartitionsExec => builder.setMultiSchemaPartitionsExec(mspe.toProto)
        case lpdce : LocalPartitionDistConcatExec => builder.setLocalPartitionDistConcatExec(lpdce.toProto())
        case _ => throw new IllegalArgumentException(s"Unexpected ExecPlan subclass ${ep.getClass.getName}");
      }
      builder.build()
    }
  }

  implicit class ExecPlanFromProtoConverter(ep: ExecPlans.ExecPlan) {
    def dispatcherFromProto: filodb.query.exec.PlanDispatcher = {
      val dispatcherCase: DispatcherCase = ep.getDispatcherCase
      val dispatcher = dispatcherCase match {
        case DispatcherCase.ACTORPLANDISPATCHER => ep.getActorPlanDispatcher.fromProto
        case _ => throw new IllegalArgumentException("Unknown type of Dispatcher")
      }
      dispatcher
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

  implicit class MultiSchemaPartitionsExecToProtoConverter(mspe: MultiSchemaPartitionsExec) {

    //import collection.JavaConverters._

    def toProto(): ExecPlans.MultiSchemaPartitionsExec = {
      val builder = ExecPlans.MultiSchemaPartitionsExec.newBuilder()
      builder.setLeafExecPlan(mspe.asInstanceOf[LeafExecPlan].toProto)
      builder.setShard(mspe.shard)
      mspe.filters.foreach(f => builder.addFilters(f.toProto))
      builder.setChunkScan(mspe.chunkMethod.toProto)
      builder.setMetricColumn(mspe.metricColumn)
      builder.clearSchema()
      mspe.schema.foreach(s => builder.setSchema(s))
      builder.clearColName()
      mspe.colName.foreach(cn => builder.setColName(cn))

      builder.build()
    }
  }

  implicit class MultiSchemaPartitionsExecFromProtoConverter(mspe: ExecPlans.MultiSchemaPartitionsExec) {
    def fromProto: MultiSchemaPartitionsExec = {
      val ep = mspe.getLeafExecPlan.getExecPlan
      val queryContext = ep.getQueryContext.fromProto
      val dispatcher = ep.dispatcherFromProto
      val datasetRef = mspe.getLeafExecPlan.getExecPlan.getQueryCommand.getDatasetRef.fromProto
      import scala.collection.JavaConverters._
      val filters = mspe.getFiltersList.asScala.toSeq.map(
        f  => f.fromProto
      )
      val chunkMethod = mspe.getChunkScan.fromProto
      val schema = if (mspe.hasSchema) {Some(mspe.getSchema)} else {None}
      val colName = if (mspe.hasColName) {Some(mspe.getColName)} else {None}
      MultiSchemaPartitionsExec(
        queryContext, dispatcher, datasetRef,
        mspe.getShard,
        filters,
        chunkMethod, mspe.getMetricColumn, schema, colName
      )

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
      val planDispatcher: filodb.query.exec.PlanDispatcher = execPlan.dispatcherFromProto
      val protoChildren: Seq[ExecPlans.ExecPlanContainer] = lpdce.getNonLeafExecPlan.getChildrenList().asScala.toSeq
      val children: Seq[filodb.query.exec.ExecPlan] = protoChildren.map(e => e.fromProto)
      LocalPartitionDistConcatExec(queryContext, planDispatcher, children)
    }
  }

  implicit class ExecPlanContainerFromProtoConverter(epc: ExecPlans.ExecPlanContainer) {
    def fromProto(): filodb.query.exec.ExecPlan = {
      val plan: filodb.query.exec.ExecPlan = epc.getExecPlanCase match {
        case ExecPlanCase.MULTISCHEMAPARTITIONSEXEC => epc.getMultiSchemaPartitionsExec.fromProto
        case ExecPlanCase.LOCALPARTITIONDISTCONCATEXEC => epc.getLocalPartitionDistConcatExec.fromProto
        case ExecPlanCase.EXECPLAN_NOT_SET => throw new RuntimeException("Received Proto Execution Plan with null value")
        case _ => throw new IllegalArgumentException(s"Unknown subclass of an execution plan ${epc.getExecPlanCase}")
      }
      plan
    }
  }

  def execPlanToProto(ep : filodb.query.exec.ExecPlan): ExecPlans.ExecPlanContainer = {
    val b = ExecPlans.ExecPlanContainer.newBuilder()
    ep match  {
      case lpdce : LocalPartitionDistConcatExec => {
        b.setLocalPartitionDistConcatExec(lpdce.toProto)
      }
      case mspe : MultiSchemaPartitionsExec => {
        b.setMultiSchemaPartitionsExec(mspe.toProto())
      }
    }
    b.build()
  }
}





// scalastyle:on number.of.methods
// scalastyle:on number.of.types
// scalastyle:on file.size.limit
