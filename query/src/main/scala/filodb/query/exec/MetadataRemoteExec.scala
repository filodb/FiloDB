package filodb.query.exec

import kamon.Kamon
import kamon.trace.Span
import monix.execution.Scheduler
import scala.concurrent.Future

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
                              params: PromQlQueryParams) extends RemoteExec {

  private val columns = Seq(ColumnInfo("Labels", ColumnType.MapColumn))
  private val resultSchema = ResultSchema(columns, 1)
  private val recordSchema = SerializedRangeVector.toSchema(columns)
  private val builder = SerializedRangeVector.newBuilder()

  override def sendHttpRequest(execPlan2Span: Span, httpTimeoutMs: Long)
                              (implicit sched: Scheduler): Future[QueryResponse] = {
    PromRemoteExec.httpMetadataGet(queryEndpoint, httpTimeoutMs, queryContext.submitTime, getUrlParams())
      .map { response =>
        response.unsafeBody match {
          case Left(error) => QueryError(queryContext.queryId, error.error)
          case Right(successResponse) => toQueryResponse(successResponse.data, queryContext.queryId, execPlan2Span)
        }
      }
  }

  def toQueryResponse(data: Seq[Map[String, String]], id: String, parentSpan: kamon.trace.Span): QueryResponse = {
    val span = Kamon.spanBuilder(s"create-queryresponse-${getClass.getSimpleName}")
      .asChildOf(parentSpan)
      .tag("query-id", id)
      .start()

    val iteratorMap = data.map { r => r.map { v => (v._1.utf8, v._2.utf8) }}

    import NoCloseCursor._
    val rangeVector = IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
      new UTF8MapIteratorRowReader(iteratorMap.toIterator))

    val srvSeq = Seq(SerializedRangeVector(rangeVector, builder, recordSchema))

    span.finish()
    QueryResult(id, resultSchema, srvSeq)
  }
}
