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

  private val lvColumns = Seq(ColumnInfo("metadataMap", ColumnType.MapColumn))
  private val resultSchema = ResultSchema(lvColumns, 1)
  private val recordSchema = SerializedRangeVector.toSchema(lvColumns)

  private val labelColumns = Seq(ColumnInfo("Labels", ColumnType.StringColumn))
  private val labelsResultSchema = ResultSchema(labelColumns, 1)
  private val labelsRecordSchema = SerializedRangeVector.toSchema(labelColumns)

  private val lcLabelNameField  = "label"
  private val lcLabelCountField = "count"

  private val builder = SerializedRangeVector.newBuilder()

  override def sendHttpRequest(execPlan2Span: Span, httpTimeoutMs: Long)
                              (implicit sched: Scheduler): Future[QueryResponse] = {
    import PromCirceSupport._
    import io.circe.parser
    remoteExecHttpClient.httpMetadataPost(queryEndpoint, httpTimeoutMs,
      queryContext.submitTime, getUrlParams(), queryContext.traceInfo)
      .map { response =>
        // Error response from remote partition is a nested json present in response.body
        // as response status code is not 2xx
        if (response.body.isLeft) {
          parser.decode[ErrorResponse](response.body.left.get) match {
            case Right(errorResponse) =>
              QueryError(queryContext.queryId, readQueryStats(errorResponse.queryStats),
                RemoteQueryFailureException(response.code, errorResponse.status, errorResponse.errorType,
                  errorResponse.error))
            case Left(ex)             => QueryError(queryContext.queryId, QueryStats(), ex)
          }
        }
        else {
          response.unsafeBody match {
            case Left(error)            => QueryError(queryContext.queryId, QueryStats(), error.error)
            case Right(successResponse) => toQueryResponse(successResponse, queryContext.queryId, execPlan2Span)
          }
        }
      }
  }

  def toQueryResponse(response: MetadataSuccessResponse, id: String, parentSpan: kamon.trace.Span): QueryResponse = {
      if (response.data.isEmpty) mapTypeQueryResponse(response, id)
      else response.data.head match {
        case _: MetadataMapSampl         => mapTypeQueryResponse(response, id)
        case _: LabelCardinalitySampl    => mapLabelCardinalityResponse(response, id)
        case _: TsCardinalitiesSampl     => mapTsCardinalitiesResponse(response, id)
        case _                           => labelsQueryResponse(response, id)
      }
  }

  private def mapTsCardinalitiesResponse(response: MetadataSuccessResponse, id: String): QueryResponse = {
    import NoCloseCursor._
    import TsCardinalities._
    import TsCardExec._

    val RECORD_SCHEMA = SerializedRangeVector.toSchema(RESULT_SCHEMA.columns)

    val rows = response.data.asInstanceOf[Seq[TsCardinalitiesSampl]]
      .map { ts =>
        val prefix = SHARD_KEY_LABELS.take(ts.group.size).map(l => ts.group(l))
        val counts = CardCounts(ts.cardinality("active"), ts.cardinality("total"))
        CardRowReader(prefixToGroup(prefix), counts)
      }
    val rv = IteratorBackedRangeVector(CustomRangeVectorKey.empty, NoCloseCursor(rows.iterator), None)
    val srv = SerializedRangeVector(rv, builder, RECORD_SCHEMA, queryWithPlanName(queryContext))
    QueryResult(id, RESULT_SCHEMA, Seq(srv))
  }

  private def mapLabelCardinalityResponse(response: MetadataSuccessResponse, id: String): QueryResponse = {

    import NoCloseCursor._
    val data = response.data.asInstanceOf[Seq[LabelCardinalitySampl]]
      .map(lc => {
          val key = CustomRangeVectorKey(lc.metric.map{case (k, v) => (k.utf8, v.utf8)})
          val data = Seq(lc.cardinality.map(k => (k.getOrElse(lcLabelNameField, "").utf8,
                                                  k.getOrElse(lcLabelCountField, "").utf8)).toMap)
          val rv = IteratorBackedRangeVector(key, UTF8MapIteratorRowReader(data.toIterator), None)
          SerializedRangeVector(rv, builder, recordSchema, queryWithPlanName(queryContext))
        }
      )
    QueryResult(id, resultSchema, data)
  }

  def mapTypeQueryResponse(response: MetadataSuccessResponse, id: String): QueryResponse = {
    val data = response.data.asInstanceOf[Seq[MetadataMapSampl]]
    // FIXME
    // Single label value query, older version returns Map type where as newer version works with List type
    // so this explicit handling is added for backward compatibility.
    if(data.nonEmpty && urlParams.get("labels").map(_.split(",").size).getOrElse(0) == 1) {
      val iteratorMap = data.flatMap{ r => r.value.map { v => v._2 }}
      import NoCloseCursor._
      val rangeVector = IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
        NoCloseCursor(StringArrayRowReader(iteratorMap)), None)

      val srvSeq = Seq(SerializedRangeVector(rangeVector, builder, labelsRecordSchema,
        queryWithPlanName(queryContext)))

      QueryResult(id, labelsResultSchema, srvSeq, QueryStats(),
        if (response.partial.isDefined) response.partial.get else false, response.message)
    } else {
      val iteratorMap = data.map { r => r.value.map { v => (v._1.utf8, v._2.utf8) }}

      import NoCloseCursor._
      val rangeVector = IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
        UTF8MapIteratorRowReader(iteratorMap.toIterator), None)

      val srvSeq = Seq(SerializedRangeVector(rangeVector, builder, recordSchema,
        queryWithPlanName(queryContext)))

      val schema = if (data.isEmpty) ResultSchema.empty else resultSchema
      // FIXME need to send and parse query stats in remote calls
      QueryResult(id, schema, srvSeq, QueryStats(),
        if (response.partial.isDefined) response.partial.get else false, response.message)
    }

  }

  def labelsQueryResponse(response: MetadataSuccessResponse, id: String): QueryResponse = {
    val data = response.data.asInstanceOf[Seq[LabelSampl]]
    val iteratorMap = data.map { r => r.value}

    import NoCloseCursor._
    val rangeVector = IteratorBackedRangeVector(new CustomRangeVectorKey(Map.empty),
      NoCloseCursor(StringArrayRowReader(iteratorMap)), None)

    val srvSeq = Seq(SerializedRangeVector(rangeVector, builder, labelsRecordSchema,
      queryWithPlanName(queryContext)))

    val schema = if (data.isEmpty) ResultSchema.empty else labelsResultSchema
    // FIXME need to send and parse query stats in remote calls
    QueryResult(id, schema, srvSeq, QueryStats(),
      if (response.partial.isDefined) response.partial.get else false, response.message)
  }
}
