package filodb.query.exec

import java.util.concurrent.TimeUnit

import com.softwaremill.sttp.asynchttpclient.future.AsyncHttpClientFutureBackend
import com.softwaremill.sttp.circe._
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.trace.Span
import monix.eval.Task
import monix.execution.Scheduler
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.sys.ShutdownHookThread

import filodb.core.DatasetRef
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.core.store.ChunkSource
import filodb.memory.format.RowReader
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._

trait RemoteExec extends LeafExecPlan {

  val params: PromQlQueryParams

  val queryEndpoint: String

  val requestTimeoutMs: Long

  val urlParams: Map[String, Any]

  def args: String = s"${params.toString}, queryEndpoint=$queryEndpoint, " +
    s"requestTimeoutMs=$requestTimeoutMs, limit=${queryContext.sampleLimit}"

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
      Task.fromFuture(sendHttpRequest(execPlan2Span, queryEndpoint, requestTimeoutMs))
    }
  }

  def sendHttpRequest(execPlan2Span: Span, httpEndpoint: String, httpTimeoutMs: Long)
                     (implicit sched: Scheduler): Future[QueryResponse]

  def getUrlParams(): Map[String, Any] = {
    var finalUrlParams = urlParams ++
      Map("start" -> params.startSecs,
        "end" -> params.endSecs,
        "time" -> params.endSecs,
        "step" -> params.stepSecs,
        "processFailure" -> params.processFailure,
        "processMultiPartition" -> params.processMultiPartition,
        "verbose" -> params.verbose)
    if (params.spread.isDefined) finalUrlParams = finalUrlParams + ("spread" -> params.spread.get)
    finalUrlParams
  }

}

case class PromQlMetricsRemoteExec(queryEndpoint: String,
                                   requestTimeoutMs: Long,
                                   queryContext: QueryContext,
                                   dispatcher: PlanDispatcher,
                                   dataset: DatasetRef,
                                   params: PromQlQueryParams,
                                   numberColumnRequired: Boolean = false) extends RemoteExec {

  private val columns = Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo(if (numberColumnRequired) "number" else "value", ColumnType.DoubleColumn))
  private val recSchema = SerializedRangeVector.toSchema(columns)
  private val resultSchema = ResultSchema(columns, 1)
  private val builder = SerializedRangeVector.newBuilder()

  override val urlParams = Map("query" -> params.promQl)

  override def sendHttpRequest(execPlan2Span: Span, httpEndpoint: String, httpTimeoutMs: Long)
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
    val rangeVectors = data.result.map { r =>
      val samples = r.values.getOrElse(Seq(r.value.get))

      val rv = new RangeVector {
        val row = new TransientRow()

        override def key: RangeVectorKey = CustomRangeVectorKey(r.metric.map (m => m._1.utf8 -> m._2.utf8))

        override def rows: Iterator[RowReader] = {
          samples.iterator.collect { case v: Sampl =>
            row.setLong(0, v.timestamp * 1000)
            row.setDouble(1, v.value)
            row
          }
        }

        override def numRows: Option[Int] = Option(samples.size)

      }
      SerializedRangeVector(rv, builder, recSchema, printTree(useNewline = false))
      // TODO: Handle stitching with verbose flag
    }
    span.finish()
    QueryResult(id, resultSchema, rangeVectors)
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

