package filodb.query.exec

import java.util.concurrent.TimeUnit

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.sys.ShutdownHookThread

import com.softwaremill.sttp.asynchttpclient.future.AsyncHttpClientFutureBackend
import com.softwaremill.sttp.circe._
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.trace.Span
import monix.eval.Task
import monix.execution.Scheduler

import filodb.core.DatasetRef
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.core.store.ChunkSource
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.memory.format.vectors.{CustomBuckets, MutableHistogram}
import filodb.query._

trait RemoteExec extends LeafExecPlan with StrictLogging {

  val queryContext: QueryContext

  val queryEndpoint: String

  val requestTimeoutMs: Long

  val urlParams: Map[String, Any]
  val promQlQueryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]

  def args: String = s"${promQlQueryParams.toString}, ${queryContext.plannerParam}, queryEndpoint=$queryEndpoint, " +
    s"requestTimeoutMs=$requestTimeoutMs"

  def limit: Int = ???

  /**
   * Since execute is already overrided here, doExecute() can be empty.
   */
  def doExecute(source: ChunkSource,
                querySession: QuerySession)
               (implicit sched: Scheduler): ExecResult = ???

  override def execute(source: ChunkSource,
                       querySession: QuerySession)
                      (implicit sched: Scheduler): Task[QueryResponse] = {
    val execPlan2Span = Kamon.spanBuilder(s"execute-${getClass.getSimpleName}")
      .asChildOf(Kamon.currentSpan())
      .tag("query-id", queryContext.queryId)
      .start()

    if (queryEndpoint == null) {
      throw new BadQueryException("Remote Query endpoint can not be null in RemoteExec.")
    }

    // Please note that the following needs to be wrapped inside `runWithSpan` so that the context will be propagated
    // across threads. Note that task/observable will not run on the thread where span is present since
    // kamon uses thread-locals.
    Kamon.runWithSpan(execPlan2Span, true) {
      Task.fromFuture(sendHttpRequest(execPlan2Span, requestTimeoutMs))
    }
  }

  def sendHttpRequest(execPlan2Span: Span, httpTimeoutMs: Long)
                     (implicit sched: Scheduler): Future[QueryResponse]

  def getUrlParams(): Map[String, Any] = {
    var finalUrlParams = urlParams ++
      Map("start" -> promQlQueryParams.startSecs,
        "end" -> promQlQueryParams.endSecs,
        "time" -> promQlQueryParams.endSecs,
        "step" -> promQlQueryParams.stepSecs,
        "processFailure" -> queryContext.plannerParam.processFailure,
        "processMultiPartition" -> queryContext.plannerParam.processMultiPartition,
        "histogramMap" -> "true",
        "skipAggregatePresent" -> queryContext.plannerParam.skipAggregatePresent,
        "verbose" -> promQlQueryParams.verbose)
    if (queryContext.plannerParam.spread.isDefined) finalUrlParams = finalUrlParams + ("spread" -> queryContext.
      plannerParam.spread.get)
    logger.debug("URLParams for RemoteExec:" + finalUrlParams)
    finalUrlParams
  }

}

case class PromQlRemoteExec(queryEndpoint: String,
                            requestTimeoutMs: Long,
                            queryContext: QueryContext,
                            dispatcher: PlanDispatcher,
                            dataset: DatasetRef) extends RemoteExec {
  private val defaultColumns = Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn))
  private val defaultRecSchema = SerializedRangeVector.toSchema(defaultColumns)
  private val defaultResultSchema = ResultSchema(defaultColumns, 1)

  private val histColumns = Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("h", ColumnType.HistogramColumn))
  private val histRecSchema = SerializedRangeVector.toSchema(histColumns)
  private val histResultSchema = ResultSchema(histColumns, 1)
  private val avgColumns = defaultColumns :+ ColumnInfo("count", ColumnType.LongColumn)
  private val avgRecSchema = SerializedRangeVector.toSchema(avgColumns)
  private val avgResultSchema = ResultSchema(avgColumns, 1)
  private val builder = SerializedRangeVector.newBuilder()

  override val urlParams = Map("query" -> promQlQueryParams.promQl)

  override def sendHttpRequest(execPlan2Span: Span, httpTimeoutMs: Long)
                              (implicit sched: Scheduler): Future[QueryResponse] = {
    PromRemoteExec.httpGet(queryEndpoint, requestTimeoutMs, queryContext.submitTime, getUrlParams())
      .map { response =>
        response.unsafeBody match {
          case Left(error) => QueryError(queryContext.queryId, error.error)
          case Right(successResponse) => toQueryResponse(successResponse.data, queryContext.queryId, execPlan2Span)
        }
      }
  }

  // TODO: Set histogramMap=true and parse histogram maps.  The problem is that code below assumes normal double
  //   schema.  Would need to detect ahead of time to use TransientHistRow(), so we'd need to add schema to output,
  //   and detect it in execute() above.  Need to discuss compatibility issues with Prometheus.
  def toQueryResponse(data: Data, id: String, parentSpan: kamon.trace.Span): QueryResponse = {
    val span = Kamon.spanBuilder(s"create-queryresponse-${getClass.getSimpleName}")
      .asChildOf(parentSpan)
      .tag("query-id", id)
      .start()

    val queryResponse = if (data.result.isEmpty) {
      logger.debug("PromQlRemoteExec generating empty QueryResult as result is empty")
      QueryResult(id, ResultSchema.empty, Seq.empty)
    } else {
      if (data.result.head.aggregateResponse.isDefined) genAggregateResult(data, id)
      else {
        val samples = data.result.head.values.getOrElse(Seq(data.result.head.value.get))
        if (samples.isEmpty) {
          logger.debug("PromQlRemoteExec generating empty QueryResult as samples is empty")
          QueryResult(id, ResultSchema.empty, Seq.empty)
        } else {
          samples.head match {
            // Passing histogramMap = true so DataSampl will be HistSampl for histograms
            case HistSampl(timestamp, buckets) => genHistQueryResult(data, id)
            case _ => genDefaultQueryResult(data, id)
          }
        }
      }
    }
    span.finish()
    queryResponse
  }

  def genAggregateResult(data: Data, id: String): QueryResult = {

    val aggregateResponse = data.result.head.aggregateResponse.get
    if (aggregateResponse.aggregateSampl.isEmpty) QueryResult(id, ResultSchema.empty, Seq.empty)
    else {
      aggregateResponse.aggregateSampl.head match {
        case AvgSampl(timestamp, value, count) => genAvgQueryResult(data, id)
      }
    }
  }
  def genDefaultQueryResult(data: Data, id: String): QueryResult = {
    val rangeVectors = data.result.map { r =>
      val samples = r.values.getOrElse(Seq(r.value.get))

      val rv = new RangeVector {
        val row = new TransientRow()

        override def key: RangeVectorKey = CustomRangeVectorKey(r.metric.map(m => m._1.utf8 -> m._2.utf8))

        override def rows(): RangeVectorCursor = {
          import NoCloseCursor._
          samples.iterator.collect { case v: Sampl =>
            row.setLong(0, v.timestamp * 1000)
            row.setDouble(1, v.value)
            row
          }
        }
        override def numRows: Option[Int] = Option(samples.size)

      }
      SerializedRangeVector(rv, builder, defaultRecSchema, "PromQlRemoteExec-default")
      // TODO: Handle stitching with verbose flag
    }
    QueryResult(id, defaultResultSchema, rangeVectors)
  }

  def genHistQueryResult(data: Data, id: String): QueryResult = {

    val rangeVectors = data.result.map { r =>
      val samples = r.values.getOrElse(Seq(r.value.get))

      val rv = new RangeVector {
        val row = new TransientHistRow()

        override def key: RangeVectorKey = CustomRangeVectorKey(r.metric.map(m => m._1.utf8 -> m._2.utf8))

        override def rows(): RangeVectorCursor = {
          import NoCloseCursor._

          samples.iterator.collect { case v: HistSampl =>
            row.setLong(0, v.timestamp * 1000)
            val sortedBucketsWithValues = v.buckets.toArray.map { h =>
              if (h._1.toLowerCase.equals("+inf")) (Double.PositiveInfinity, h._2) else (h._1.toDouble, h._2)
            }.sortBy(_._1)
            val hist = MutableHistogram(CustomBuckets(sortedBucketsWithValues.map(_._1)),
              sortedBucketsWithValues.map(_._2))
            row.setValues(v.timestamp * 1000, hist)
            row
          }
        }

        override def numRows: Option[Int] = Option(samples.size)

      }
      SerializedRangeVector(rv, builder, histRecSchema, "PromQlRemoteExec-hist")
      // TODO: Handle stitching with verbose flag
    }
    QueryResult(id, histResultSchema, rangeVectors)
  }

  def genAvgQueryResult(data: Data, id: String): QueryResult = {
    val rangeVectors = data.result.map { d =>

       // val samples = r.value

        val rv = new RangeVector {
          val row = new AvgAggTransientRow()

          override def key: RangeVectorKey = CustomRangeVectorKey(d.metric.map(m => m._1.utf8 -> m._2.utf8))

          override def rows(): RangeVectorCursor = {
            import NoCloseCursor._
            d.aggregateResponse.get.aggregateSampl.iterator.collect { case a: AvgSampl =>
              row.setLong(0, a.timestamp * 1000)
              row.setDouble(1, a.value)
              row.setLong(2, a.count)
              row
            }
          }
          override def numRows: Option[Int] = Option(d.aggregateResponse.get.aggregateSampl.size)
        }
      SerializedRangeVector(rv, builder, avgRecSchema, printTree(useNewline = false))
        }

        // TODO: Handle stitching with verbose flag
    QueryResult(id, avgResultSchema, rangeVectors)
  }

}

object PromRemoteExec extends StrictLogging {

  import com.softwaremill.sttp._
  import io.circe.generic.auto._

  // DO NOT REMOVE PromCirceSupport import below assuming it is unused - Intellij removes it in auto-imports :( .
  // Needed to override Sampl case class Encoder.
  import PromCirceSupport._
  implicit val backend = AsyncHttpClientFutureBackend()

  ShutdownHookThread(shutdown())

  def httpGet(httpEndpoint: String, httpTimeoutMs: Long, submitTime: Long, urlParams: Map[String, Any])
  (implicit scheduler: Scheduler):
  Future[Response[scala.Either[DeserializationError[io.circe.Error], SuccessResponse]]] = {
    val queryTimeElapsed = System.currentTimeMillis() - submitTime
    val readTimeout = FiniteDuration(httpTimeoutMs - queryTimeElapsed, TimeUnit.MILLISECONDS)
    val url = uri"$httpEndpoint?$urlParams"
    logger.debug("promQlExec url={}", url)
    sttp
      .get(url)
      .readTimeout(readTimeout)
      .response(asJson[SuccessResponse])
      .send()
  }

  def httpMetadataGet(httpEndpoint: String, httpTimeoutMs: Long, submitTime: Long, urlParams: Map[String, Any])
                     (implicit scheduler: Scheduler):
  Future[Response[scala.Either[DeserializationError[io.circe.Error], MetadataSuccessResponse]]] = {
    val queryTimeElapsed = System.currentTimeMillis() - submitTime
    val readTimeout = FiniteDuration(httpTimeoutMs - queryTimeElapsed, TimeUnit.MILLISECONDS)
    val url = uri"$httpEndpoint?$urlParams"
    logger.debug("promMetadataExec url={}", url)
    sttp
      .get(url)
      .readTimeout(readTimeout)
      .response(asJson[MetadataSuccessResponse])
      .send()
  }

  def shutdown(): Unit =
  {
    logger.info("Shutting PromQlExec http")
    backend.close()
  }
}

