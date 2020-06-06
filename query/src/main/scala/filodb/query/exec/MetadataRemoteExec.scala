package filodb.query.exec

import kamon.Kamon
import filodb.core.DatasetRef
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.UTF8MapIteratorRowReader
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._
import kamon.trace.Span
import monix.execution.Scheduler

import scala.concurrent.Future

case class PromSeriesMatchMetadataExec(queryContext: QueryContext,
                                       dispatcher: PlanDispatcher,
                                       dataset: DatasetRef,
                                       params: PromQlQueryParams) extends RemoteExec {
//  val columns = Seq(ColumnInfo("TimeSeries", ColumnType.BinaryRecordColumn),
//    ColumnInfo("_firstSampleTime_", ColumnType.LongColumn),
//    ColumnInfo("_lastSampleTime_", ColumnType.LongColumn))
//  val resultSchema = ResultSchema(columns, 3, Map(0 -> Schemas.promCounter.partition.binSchema))
  val columns = Seq(ColumnInfo("value", ColumnType.MapColumn))
  val resultSchema = ResultSchema(columns, 1)

  override def getUrlParams(): Map[String, Any] = {
    var urlParams = Map("match[]" -> params.promQl,
      "start" -> params.startSecs,
      "end" -> params.endSecs,
      "processFailure" -> params.processFailure,
      "processMultiPartition" -> params.processMultiPartition)
    if (params.spread.isDefined) urlParams = urlParams + ("spread" -> params.spread.get)
    urlParams
  }

  override def sendHttpRequest(execPlan2Span: Span, httpEndpoint: String, httpTimeoutMs: Long)
                              (implicit sched: Scheduler): Future[QueryResponse] = {
    PromQlRemoteExec.httpMetadataGet(httpEndpoint, httpTimeoutMs, queryContext.submitTime, getUrlParams())
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

    // TODO: fix schema mismatch error by changing schema to map column
    val iteratorMap = data.map { r =>

      // TODO: empty partKey records should not fail
      r.map { v => (v._1.utf8, v._2.utf8) }

    }

    val rangeVector = IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
      new UTF8MapIteratorRowReader(iteratorMap.toIterator))

    val schema = SerializedRangeVector.toSchema(columns)
    val srvSeq = Seq(SerializedRangeVector(rangeVector, SerializedRangeVector.newBuilder(), schema, printTree(false)))

    span.finish()
    QueryResult(id, resultSchema, srvSeq)
  }
}

case class PromLabelValuesRemoteExec(queryContext: QueryContext,
                                     dispatcher: PlanDispatcher,
                                     dataset: DatasetRef,
                                     params: PromQlQueryParams,
                                     labelNames: Seq[String],
                                     labelConstraints: Map[String, String]) extends RemoteExec {
  val columns = Seq(ColumnInfo("Labels", ColumnType.MapColumn))
  val resultSchema = ResultSchema(columns, 1)

  override def getUrlParams(): Map[String, Any] = {
    var urlParams = Map("filter" -> labelConstraints.map{case (k, v) => k + "=" + v}.mkString(","),
      "labels" -> labelNames.mkString(","),
      "start" -> params.startSecs,
      "end" -> params.endSecs,
      "processFailure" -> params.processFailure,
      "processMultiPartition" -> params.processMultiPartition)
    if (params.spread.isDefined) urlParams = urlParams + ("spread" -> params.spread.get)
    urlParams
  }

  override def sendHttpRequest(execPlan2Span: Span, httpEndpoint: String, httpTimeoutMs: Long)
                              (implicit sched: Scheduler): Future[QueryResponse] = {
    PromQlRemoteExec.httpMetadataGet(httpEndpoint, httpTimeoutMs, queryContext.submitTime, getUrlParams())
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

    val rangeVector = IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
      new UTF8MapIteratorRowReader(iteratorMap.toIterator))

    val schema = SerializedRangeVector.toSchema(columns)
    val srvSeq = Seq(SerializedRangeVector(rangeVector, SerializedRangeVector.newBuilder(), schema, printTree(false)))

    span.finish()
    QueryResult(id, resultSchema, srvSeq)
  }
}
