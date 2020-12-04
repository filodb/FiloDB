package filodb.query.exec

import scala.concurrent.Future

import kamon.trace.Span
import monix.execution.Scheduler

import filodb.core.DatasetRef
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.UTF8MapIteratorRowReader
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._

case class MetadataRemoteExec(queryEndpoint: String,
                              requestTimeoutMs: Long,
                              urlParams: Map[String, Any],
                              queryContext: QueryContext,
                              dispatcher: PlanDispatcher,
                              dataset: DatasetRef,
                              remoteExecHttpClient: RemoteExecHttpClient) extends RemoteExec {

  private val columns = Seq(ColumnInfo("Labels", ColumnType.MapColumn))
  private val resultSchema = ResultSchema(columns, 1)
  private val recordSchema = SerializedRangeVector.toSchema(columns)
  private val builder = SerializedRangeVector.newBuilder()

  override def sendHttpRequest(execPlan2Span: Span, httpTimeoutMs: Long)
                              (implicit sched: Scheduler): Future[QueryResponse] = {
    remoteExecHttpClient.httpMetadataGet(queryContext.plannerParams.applicationId, queryEndpoint,
      httpTimeoutMs, queryContext.submitTime, getUrlParams())
      .map { response =>
        response.unsafeBody match {
          case Left(error) => QueryError(queryContext.queryId, error.error)
          case Right(successResponse) => toQueryResponse(successResponse.data, queryContext.queryId, execPlan2Span)
        }
      }
  }

  def toQueryResponse(data: Seq[Map[String, String]], id: String, parentSpan: kamon.trace.Span): QueryResponse = {
    val iteratorMap = data.map { r => r.map { v => (v._1.utf8, v._2.utf8) }}

    import NoCloseCursor._
    val rangeVector = IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
      UTF8MapIteratorRowReader(iteratorMap.toIterator))

    val srvSeq = Seq(SerializedRangeVector(rangeVector, builder, recordSchema, printTree(false)))

    QueryResult(id, resultSchema, srvSeq)
  }
}
