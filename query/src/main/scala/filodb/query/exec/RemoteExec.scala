package filodb.query.exec

import java.util.concurrent.TimeUnit

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.sys.ShutdownHookThread

import com.softwaremill.sttp.{DeserializationError, Response, SttpBackendOptions}
import com.softwaremill.sttp.SttpBackendOptions.ProxyType.{Http, Socks}
import com.softwaremill.sttp.asynchttpclient.future.AsyncHttpClientFutureBackend
import com.softwaremill.sttp.circe.asJson
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.trace.Span
import monix.eval.Task
import monix.execution.Scheduler
import org.asynchttpclient.{AsyncHttpClientConfig, DefaultAsyncHttpClientConfig}
import org.asynchttpclient.proxy.ProxyServer

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
    if (queryEndpoint == null) {
      throw new BadQueryException("Remote Query endpoint can not be null in RemoteExec.")
    }

    // Please note that the following needs to be wrapped inside `runWithSpan` so that the context will be propagated
    // across threads. Note that task/observable will not run on the thread where span is present since
    // kamon uses thread-locals.
    val span = Kamon.currentSpan()
    // Dont finish span since this code didnt create it
    Kamon.runWithSpan(span, false) {
      Task.fromFuture(sendHttpRequest(span, requestTimeoutMs))
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
 * A trait for remoteExec GET Queries.
 */
trait RemoteExecHttpClient extends StrictLogging {

  def httpGet(applicationId: String, httpEndpoint: String,
              httpTimeoutMs: Long, submitTime: Long, urlParams: Map[String, Any])
             (implicit scheduler: Scheduler):
  Future[Response[scala.Either[DeserializationError[io.circe.Error], SuccessResponse]]]

  def httpMetadataGet(applicationId: String, httpEndpoint: String,
                      httpTimeoutMs: Long, submitTime: Long, urlParams: Map[String, Any])
                     (implicit scheduler: Scheduler):
  Future[Response[scala.Either[DeserializationError[io.circe.Error], MetadataSuccessResponse]]]

}

class RemoteHttpClient private(asyncHttpClientConfig: AsyncHttpClientConfig) extends RemoteExecHttpClient {

  import com.softwaremill.sttp._
  import io.circe.generic.auto._

  // DO NOT REMOVE PromCirceSupport import below assuming it is unused - Intellij removes it in auto-imports :( .
  // Needed to override Sampl case class Encoder.
  import PromCirceSupport._
  private implicit val backend = AsyncHttpClientFutureBackend.usingConfig(asyncHttpClientConfig)

  ShutdownHookThread(shutdown())

  def httpGet(applicationId: String, httpEndpoint: String,
              httpTimeoutMs: Long, submitTime: Long, urlParams: Map[String, Any])
             (implicit scheduler: Scheduler):
  Future[Response[scala.Either[DeserializationError[io.circe.Error], SuccessResponse]]] = {
    val queryTimeElapsed = System.currentTimeMillis() - submitTime
    val readTimeout = FiniteDuration(httpTimeoutMs - queryTimeElapsed, TimeUnit.MILLISECONDS)
    val url = uri"$httpEndpoint?$urlParams"
    logger.debug("promQlExec url={}", url)
    sttp
      .header(HeaderNames.UserAgent, applicationId)
      .get(url)
      .readTimeout(readTimeout)
      .response(asJson[SuccessResponse])
      .send()
  }

  def httpMetadataGet(applicationId: String, httpEndpoint: String,
                      httpTimeoutMs: Long, submitTime: Long, urlParams: Map[String, Any])
                     (implicit scheduler: Scheduler):
  Future[Response[scala.Either[DeserializationError[io.circe.Error], MetadataSuccessResponse]]] = {
    val queryTimeElapsed = System.currentTimeMillis() - submitTime
    val readTimeout = FiniteDuration(httpTimeoutMs - queryTimeElapsed, TimeUnit.MILLISECONDS)
    val url = uri"$httpEndpoint?$urlParams"
    logger.debug("promMetadataExec url={}", url)
    sttp
      .header(HeaderNames.UserAgent, applicationId)
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

  import scala.collection.JavaConverters._

  /**
   * A default prom remote http client backend from DefaultPromRemoteHttpClientFactory.
   */
  def configBuilder(): DefaultAsyncHttpClientConfig.Builder = {
    // A copy of private AsyncHttpClientBackend.defaultClient.
    var configBuilder = new DefaultAsyncHttpClientConfig.Builder()
      .setConnectTimeout(SttpBackendOptions.Default.connectionTimeout.toMillis.toInt)
    configBuilder = SttpBackendOptions.Default.proxy match {
      case None => configBuilder
      case Some(p) =>
        val proxyType: org.asynchttpclient.proxy.ProxyType =
          p.proxyType match {
            case Socks => org.asynchttpclient.proxy.ProxyType.SOCKS_V5
            case Http  => org.asynchttpclient.proxy.ProxyType.HTTP
          }

        configBuilder.setProxyServer(
          new ProxyServer.Builder(p.host, p.port)
            .setProxyType(proxyType) // Fix issue #145
            .setNonProxyHosts(p.nonProxyHosts.asJava)
            .build())
    }
    configBuilder
  }

  val defaultClient = RemoteHttpClient(configBuilder().build())

  def apply(asyncHttpClientConfig: AsyncHttpClientConfig): RemoteHttpClient =
    new RemoteHttpClient(asyncHttpClientConfig)

}
