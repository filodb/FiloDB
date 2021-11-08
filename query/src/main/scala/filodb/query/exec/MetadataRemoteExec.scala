package filodb.query.exec

import scala.concurrent.Future

import kamon.trace.Span
import monix.execution.Scheduler

import filodb.core.DatasetRef
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.{StringArrayRowReader, UTF8MapIteratorRowReader}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._

case class MetadataRemoteExec(queryEndpoint: String,
                              requestTimeoutMs: Long,
                              urlParams: Map[String, String],
                              queryContext: QueryContext,
                              dispatcher: PlanDispatcher,
                              dataset: DatasetRef,
                              remoteExecHttpClient: RemoteExecHttpClient) extends RemoteExec {

  private val lvColumns = Seq(ColumnInfo("LabelValues", ColumnType.MapColumn))
  private val resultSchema = ResultSchema(lvColumns, 1)
  private val recordSchema = SerializedRangeVector.toSchema(lvColumns)

  private val labelColumns = Seq(ColumnInfo("Labels", ColumnType.StringColumn))
  private val labelsResultSchema = ResultSchema(labelColumns, 1)
  private val labelsRecordSchema = SerializedRangeVector.toSchema(labelColumns)

  private val builder = SerializedRangeVector.newBuilder()

  override def sendHttpRequest(execPlan2Span: Span, httpTimeoutMs: Long)
                              (implicit sched: Scheduler): Future[QueryResponse] = {
    remoteExecHttpClient.httpMetadataPost(queryEndpoint, httpTimeoutMs,
      queryContext.submitTime, getUrlParams(), queryContext.traceInfo)
      .map { response =>
        response.unsafeBody match {
          case Left(error) =>    // FIXME need to extract statistics from query error, aggregate them, send upstream
                                 QueryError(queryContext.queryId, QueryStats(), error.error)
          case Right(successResponse) => toQueryResponse(successResponse, queryContext.queryId, execPlan2Span)
        }
      }
  }

  def toQueryResponse(response: MetadataSuccessResponse, id: String, parentSpan: kamon.trace.Span): QueryResponse = {
      if (response.data.isEmpty) mapTypeQueryResponse(response, id)
      else response.data.head match {
        case _: MetadataSampl => mapTypeQueryResponse(response, id)
        case _ => labelsQueryResponse(response, id)
      }
  }

  def mapTypeQueryResponse(response: MetadataSuccessResponse, id: String): QueryResponse = {
    val data = response.data.asInstanceOf[Seq[MetadataSampl]]
    val iteratorMap = data.map { r => r.values.map { v => (v._1.utf8, v._2.utf8) }}

    import NoCloseCursor._
    val rangeVector = IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
        UTF8MapIteratorRowReader(iteratorMap.toIterator), None)

    val srvSeq = Seq(SerializedRangeVector(rangeVector, builder, recordSchema,
      queryWithPlanName(queryContext)))

    // FIXME need to send and parse query stats in remote calls
    QueryResult(id, resultSchema, srvSeq, QueryStats(),
      if (response.partial.isDefined) response.partial.get else false, response.message)
  }

  def labelsQueryResponse(response: MetadataSuccessResponse, id: String): QueryResponse = {
    val data = response.data.asInstanceOf[Seq[LabelSampl]]
    val iteratorMap = data.flatMap { r => r.values}

    import NoCloseCursor._
    val rangeVector = IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
      NoCloseCursor(StringArrayRowReader(iteratorMap)), None)

    val srvSeq = Seq(SerializedRangeVector(rangeVector, builder, labelsRecordSchema,
      queryWithPlanName(queryContext)))

    // FIXME need to send and parse query stats in remote calls
    QueryResult(id, labelsResultSchema, srvSeq, QueryStats(),
      if (response.partial.isDefined) response.partial.get else false, response.message)
  }
}
