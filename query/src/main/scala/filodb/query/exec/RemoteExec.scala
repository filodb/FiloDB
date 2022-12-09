package filodb.query.exec

import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.sys.ShutdownHookThread

import com.softwaremill.sttp.{DeserializationError, Response, SttpBackend, SttpBackendOptions}
import com.softwaremill.sttp.SttpBackendOptions.ProxyType.{Http, Socks}
import com.softwaremill.sttp.asynchttpclient.future.AsyncHttpClientFutureBackend
import com.softwaremill.sttp.circe.asJson
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.trace.Span
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.asynchttpclient.{AsyncHttpClientConfig, DefaultAsyncHttpClientConfig}
import org.asynchttpclient.proxy.ProxyServer

import filodb.core.{GlobalConfig, QueryTimeoutException}
import filodb.core.query.{PromQlQueryParams, QuerySession, QueryStats}
import filodb.core.store.ChunkSource
import filodb.query._

trait RemoteExec extends LeafExecPlan with StrictLogging {

  def queryEndpoint: String

  def remoteExecHttpClient: RemoteExecHttpClient

  def requestTimeoutMs: Long

  def urlParams: Map[String, String]

  def promQlQueryParams: PromQlQueryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]

  def args: String = s"${promQlQueryParams.toString}, ${queryContext.plannerParams}, queryEndpoint=$queryEndpoint, " +
    s"requestTimeoutMs=$requestTimeoutMs"

  def limit: Int = ???

  /**
   * Since execute is already overrided here, doExecute() can be empty.
   */
  override def doExecute(source: ChunkSource,
                querySession: QuerySession)
               (implicit sched: Scheduler): ExecResult = {
    if (queryEndpoint == null) {
      throw new BadQueryException("Remote Query endpoint can not be null in RemoteExec.")
    }

    // Please note that the following needs to be wrapped inside `runWithSpan` so that the context will be propagated
    // across threads. Note that task/observable will not run on the thread where span is present since
    // kamon uses thread-locals.
    val span = Kamon.currentSpan()
    val elapsedMillis = System.currentTimeMillis() - queryContext.submitTime
    val remainingMillis = queryContext.plannerParams.queryTimeoutMillis - elapsedMillis
    if (remainingMillis <= 0) {
      throw QueryTimeoutException(elapsedMillis, "RemoteExec::doExecute (before remote request)")
    }
    // Dont finish span since this code didnt create it
    val fut = Kamon.runWithSpan(span, false) {
      sendHttpRequest(span, requestTimeoutMs)
        .map {
          case QueryResult(_, resultSchema, result, _, _, _) =>
            ExecResult(Observable.fromIterable(result), Task.now(resultSchema))
          case QueryError(_, _, t) => throw t
        }
    }
    // FIXME: remote plans probably need their own dispatcher so they've got access
    //  to ExecPlanWithClientParams (and can therefore return partial results).
    Await.result(fut, FiniteDuration(remainingMillis, MILLISECONDS))
  }

  /**
   * Legacy execute() logic; does not invoke super.execute().
   * FIXME: this method should eventually be removed.
   */
  def executeLegacy(source: ChunkSource,
                    querySession: QuerySession)(implicit sched: Scheduler): Task[QueryResponse] = {
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

  override def execute(source: ChunkSource,
                       querySession: QuerySession)(implicit sched: Scheduler): Task[QueryResponse] = {
    // NOTE: this should no longer be overridden once executeLegacy is removed.
    if (GlobalConfig.systemConfig.getBoolean("filodb.query.enable-legacy-remote-execute")) {
      executeLegacy(source, querySession)
    } else {
      super.execute(source, querySession)
    }
  }

  def sendHttpRequest(execPlan2Span: Span, httpTimeoutMs: Long)
                     (implicit sched: Scheduler): Future[QueryResponse]

  def getUrlParams(): Map[String, String] = {
    var finalUrlParams = urlParams ++
      Map("start" -> promQlQueryParams.startSecs.toString,
        "end" -> promQlQueryParams.endSecs.toString,
        "time" -> promQlQueryParams.endSecs.toString,
        "step" -> promQlQueryParams.stepSecs.toString,
        "allowPartialResults" -> queryContext.plannerParams.allowPartialResults.toString,
        "processFailure" -> queryContext.plannerParams.processFailure.toString,
        "processMultiPartition" -> queryContext.plannerParams.processMultiPartition.toString,
        "histogramMap" -> "true",
        "skipAggregatePresent" -> queryContext.plannerParams.skipAggregatePresent.toString,
        "verbose" -> promQlQueryParams.verbose.toString)
    if (queryContext.plannerParams.spread.isDefined)
      finalUrlParams = finalUrlParams + ("spread" -> queryContext.plannerParams.spread.get.toString)
    logger.debug("URLParams for RemoteExec:" + finalUrlParams)
    finalUrlParams
  }

  def readQueryStats(queryStatsResponse: Option[Seq[QueryStatistics]]): QueryStats = {
    val queryStats = QueryStats()
    if (queryStatsResponse.isDefined && queryStatsResponse.get.nonEmpty) queryStatsResponse.get.foreach { stat =>
      queryStats.getTimeSeriesScannedCounter(stat.group).addAndGet(stat.timeSeriesScanned)
      queryStats.getDataBytesScannedCounter(stat.group).addAndGet(stat.dataBytesScanned)
      queryStats.getResultBytesCounter(stat.group).addAndGet(stat.resultBytes)
    }
    queryStats
  }
}

/**
 * A trait for remoteExec GET Queries.
 */
trait RemoteExecHttpClient extends StrictLogging {

  def httpPost(httpEndpoint: String,
              httpTimeoutMs: Long, submitTime: Long, urlParams: Map[String, String], traceInfo: Map[String, String])
             (implicit scheduler: Scheduler):
  Future[Response[scala.Either[DeserializationError[io.circe.Error], SuccessResponse]]]

  def httpMetadataPost(httpEndpoint: String,
                      httpTimeoutMs: Long, submitTime: Long, urlParams: Map[String, String],
                      traceInfo: Map[String, String])
                     (implicit scheduler: Scheduler):
  Future[Response[scala.Either[DeserializationError[io.circe.Error], MetadataSuccessResponse]]]

}

// scalastyle:off
import com.softwaremill.sttp._
import io.circe.generic.auto._

// DO NOT REMOVE PromCirceSupport import below assuming it is unused - Intellij removes it in auto-imports :( .
// Needed to override Sampl case class Encoder.
import PromCirceSupport._
class RemoteHttpClient private(asyncHttpClientConfig: AsyncHttpClientConfig)
                              (implicit backend: SttpBackend[Future, Nothing]
                                = AsyncHttpClientFutureBackend.usingConfig(asyncHttpClientConfig))
    extends RemoteExecHttpClient {

  ShutdownHookThread(shutdown())

  def httpPost(httpEndpoint: String,
              httpTimeoutMs: Long, submitTime: Long, urlParams: Map[String, String], traceInfo: Map[String, String])
             (implicit scheduler: Scheduler):
  Future[Response[scala.Either[DeserializationError[io.circe.Error], SuccessResponse]]] = {
    val queryTimeElapsed = System.currentTimeMillis() - submitTime
    val readTimeout = FiniteDuration(httpTimeoutMs - queryTimeElapsed, TimeUnit.MILLISECONDS)
    val url = uri"$httpEndpoint"
    logger.debug("promQlExec url={}  traceInfo={}", url, traceInfo)
    sttp
      .headers(traceInfo)
      .body(urlParams)
      .post(url)
      .readTimeout(readTimeout)
      .response(asJson[SuccessResponse])
      .send()
  }

  def httpMetadataPost(httpEndpoint: String,
                      httpTimeoutMs: Long, submitTime: Long, urlParams: Map[String, String],
                      traceInfo: Map[String, String])
                     (implicit scheduler: Scheduler):
  Future[Response[scala.Either[DeserializationError[io.circe.Error], MetadataSuccessResponse]]] = {
    val queryTimeElapsed = System.currentTimeMillis() - submitTime
    val readTimeout = FiniteDuration(httpTimeoutMs - queryTimeElapsed, TimeUnit.MILLISECONDS)
    val url = uri"$httpEndpoint"
    logger.debug("promMetadataExec url={} traceInfo={}", url, traceInfo)
    sttp
      .headers(traceInfo)
      .body(urlParams)
      .post(url)
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
  def apply(asyncHttpClientConfig: AsyncHttpClientConfig,
            sttpBackend: SttpBackend[Future, Nothing]): RemoteHttpClient =
    new RemoteHttpClient(asyncHttpClientConfig)(sttpBackend)

}
