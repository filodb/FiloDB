package filodb.query.exec

import java.util.concurrent.TimeUnit

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.sys.ShutdownHookThread

import com.softwaremill.sttp.{DeserializationError, Response, SttpBackend}
import com.softwaremill.sttp.asynchttpclient.future.AsyncHttpClientFutureBackend
import com.softwaremill.sttp.circe.asJson
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.trace.Span
import monix.eval.Task
import monix.execution.Scheduler

import filodb.core.query.{PromQlQueryParams, QuerySession}
import filodb.core.store.ChunkSource
import filodb.query.{BadQueryException, MetadataSuccessResponse, PromCirceSupport, QueryResponse, SuccessResponse}

trait RemoteExec extends LeafExecPlan with StrictLogging {

  def queryEndpoint: String

  def remoteExecHttpClient: RemoteExecHttpClient

  def requestTimeoutMs: Long

  def urlParams: Map[String, Any]

  def promQlQueryParams: PromQlQueryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]

  def args: String = s"${promQlQueryParams.toString}, ${queryContext.plannerParams}, queryEndpoint=$queryEndpoint, " +
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
        "processFailure" -> queryContext.plannerParams.processFailure,
        "processMultiPartition" -> queryContext.plannerParams.processMultiPartition,
        "histogramMap" -> "true",
        "skipAggregatePresent" -> queryContext.plannerParams.skipAggregatePresent,
        "verbose" -> promQlQueryParams.verbose)
    if (queryContext.plannerParams.spread.isDefined) finalUrlParams = finalUrlParams + ("spread" -> queryContext.
      plannerParams.spread.get)
    logger.debug("URLParams for RemoteExec:" + finalUrlParams)
    finalUrlParams
  }

}

/**
 * A zero-arg constructor class that knows how to create an HttpClient for Prom Remote Queries.
 */
trait RemoteExecSttpBackendFactory {

  /**
   * Returns an SttpBackend that can be used to create sttp for remote Http requests.
   * @param config the configuration for the http client
   */
  def create(config: Config): SttpBackend[Future, Nothing]

}

class AsyncHttpClientBackendFactory extends RemoteExecSttpBackendFactory {

  /**
   * Returns an AsyncHttpClientFutureBackend that can be used to create sttp for remote Http requests.
   *
   * @param config the configuration for the http client
   */
  override def create(config: Config): SttpBackend[Future, Nothing] = AsyncHttpClientFutureBackend()

}

/**
 * A default prom remote http client backend from DefaultPromRemoteHttpClientFactory.
 */
object DefaultSttpBackend {

  def apply(): SttpBackend[Future, Nothing] = new AsyncHttpClientBackendFactory().create(ConfigFactory.empty())

}

/**
 * A trait for remoteExec GET Queries.
 */
trait RemoteExecHttpClient extends StrictLogging {

  def httpGet(httpEndpoint: String, httpTimeoutMs: Long, submitTime: Long, urlParams: Map[String, Any])
             (implicit scheduler: Scheduler):
  Future[Response[scala.Either[DeserializationError[io.circe.Error], SuccessResponse]]]

  def httpMetadataGet(httpEndpoint: String, httpTimeoutMs: Long, submitTime: Long, urlParams: Map[String, Any])
                     (implicit scheduler: Scheduler):
  Future[Response[scala.Either[DeserializationError[io.circe.Error], MetadataSuccessResponse]]]

}

class RemoteHttpClient private(sttpBackend: SttpBackend[Future, Nothing]) extends RemoteExecHttpClient {

  import com.softwaremill.sttp._
  import io.circe.generic.auto._

  // DO NOT REMOVE PromCirceSupport import below assuming it is unused - Intellij removes it in auto-imports :( .
  // Needed to override Sampl case class Encoder.
  import PromCirceSupport._
  private implicit val backend = sttpBackend

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

object RemoteHttpClient {

  val default = RemoteHttpClient(DefaultSttpBackend())

  def apply(backend: SttpBackend[Future, Nothing]): RemoteHttpClient = new RemoteHttpClient(backend)

}
