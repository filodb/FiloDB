package filodb.query.exec

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
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

import filodb.core.query.{PromQlQueryParams, QuerySession, QueryStats, QueryWarnings}
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
   * Logs each time a transformer is added.
   * Transformations can always be pushed-down to the machines that serve the
   *   remote data; they should never be applied to RemoteExec plans directly.
   */
  override def addRangeVectorTransformer(mapper: RangeVectorTransformer): Unit = {
    super.addRangeVectorTransformer(mapper)
    logger.info("RangeVectorTransformer added to RemoteExec; plan=" + printTree())
  }

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
    // Dont finish span since this code didnt create it
    Kamon.runWithSpan(span, false) {
      val qResTask = sendRequest(span, requestTimeoutMs).map {
        // FIXME: QueryResponse should contain fields common to Result/Error
        case qr: QueryResult =>
          querySession.queryStats.add(qr.queryStats)
          qr
        case qe: QueryError =>
          querySession.queryStats.add(qe.queryStats)
          throw qe.t
      }.memoize
      val schemaTask = qResTask.map(_.resultSchema)
      val rvObs = Observable.fromTask(qResTask)
        .map(_.result)
        .flatMap(Observable.fromIterable(_))
      ExecResult(rvObs, schemaTask)
    }
  }

  def sendRequest(span: Span, timeoutMs: Long)
                 (implicit sched: Scheduler): Task[QueryResponse]

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
    finalUrlParams = finalUrlParams ++ getLimitsMap()
    if (queryContext.plannerParams.spread.isDefined)
      finalUrlParams = finalUrlParams + ("spread" -> queryContext.plannerParams.spread.get.toString)
    logger.debug("URLParams for RemoteExec:" + finalUrlParams)
    finalUrlParams
  }

  def getLimitsMap(): Map[String, String] = {
    var w = queryContext.plannerParams.warnLimits;
    var e = queryContext.plannerParams.enforcedLimits;
    var limitParams = Map(
      "warnExecPlanSamples" -> w.execPlanSamples.toString,
      "warnResultByteLimit" -> w.execPlanResultBytes.toString,
      "warnGroupByCardinality" -> w.groupByCardinality.toString,
      "warnJoinQueryCardinality" -> w.joinQueryCardinality.toString,
      "warnTimeSeriesSamplesScannedBytes" -> w.timeSeriesSamplesScannedBytes.toString,
      "warnTimeSeriesScanned" -> w.timeSeriesScanned.toString,
      "warnRawScannedBytes" -> w.rawScannedBytes.toString,
      "execPlanSamples" -> e.execPlanSamples.toString,
      "resultByteLimit" -> e.execPlanResultBytes.toString,
      "groupByCardinality" -> e.groupByCardinality.toString,
      "joinQueryCardinality" -> e.joinQueryCardinality.toString,
      "timeSeriesSamplesScannedBytes" -> e.timeSeriesSamplesScannedBytes.toString,
      "timeSeriesScanned" -> e.timeSeriesScanned.toString,
      "rawScannedBytes" -> e.rawScannedBytes.toString
    )
    limitParams
  }

  def readQueryStats(queryStatsResponse: Option[Seq[QueryStatistics]]): QueryStats = {
    val queryStats = QueryStats()
    if (queryStatsResponse.isDefined && queryStatsResponse.get.nonEmpty) queryStatsResponse.get.foreach { stat =>
      queryStats.getTimeSeriesScannedCounter(stat.group).addAndGet(stat.timeSeriesScanned)
      queryStats.getDataBytesScannedCounter(stat.group).addAndGet(stat.dataBytesScanned)
      queryStats.getResultBytesCounter(stat.group).addAndGet(stat.resultBytes)
      queryStats.getCpuNanosCounter(stat.group).addAndGet(stat.cpuNanos)
    }
    queryStats
  }

  def readQueryWarnings(queryWarningsResponse: Option[QueryWarningsResponse]): QueryWarnings = {
    val qws = queryWarningsResponse.getOrElse(QueryWarningsResponse())
    val queryWarnings = QueryWarnings(
      new AtomicInteger(qws.execPlanSamples),
      new AtomicLong(qws.execPlanResultBytes),
      new AtomicInteger(qws.groupByCardinality),
      new AtomicInteger(qws.joinQueryCardinality),
      new AtomicLong(qws.timeSeriesSamplesScannedBytes),
      new AtomicInteger(qws.timeSeriesScanned),
      new AtomicLong(qws.rawScannedBytes)
    )
    queryWarnings
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
