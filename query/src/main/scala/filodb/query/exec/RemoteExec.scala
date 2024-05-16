package filodb.query.exec

import java.util
import java.util.concurrent.{Callable, CompletableFuture, ExecutionException, ExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.stream.Collectors

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.sys.ShutdownHookThread
import scala.util.{Failure, Success}
import scala.util.control.Breaks.break

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

  // Executes tasks immediately on the current thread.
  val inProcessScheduler = Scheduler(new ExecutorService {
      var _shutdown = false
      def futureCompletedExceptionally[T](future: util.concurrent.Future[T]): Boolean = {
        try {
          future.get()
        } catch {
          case _: Throwable => return true
        }
        false
      }
      override def shutdown(): Unit = _shutdown = true
      override def shutdownNow(): util.List[Runnable] = util.List.of()
      override def isShutdown: Boolean = _shutdown
      override def isTerminated: Boolean = _shutdown
      override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
        val waitUntilMs = System.currentTimeMillis() + unit.toMillis(timeout)
        // looping in case thread is woken up early
        while (!_shutdown && System.currentTimeMillis() < waitUntilMs) {
          val waitMs = waitUntilMs - System.currentTimeMillis()
          _shutdown.wait(waitMs)
        }
        _shutdown
      }
      override def submit[T](task: Callable[T]): util.concurrent.Future[T] = {
        try {
          val res = task.call()
          CompletableFuture.completedFuture(res)
        } catch {
          case t: Throwable => CompletableFuture.failedFuture(t)
        }
      }

    override def submit[T](task: Runnable, result: T): util.concurrent.Future[T] = {
        try {
          execute(task)
          CompletableFuture.completedFuture(result)
        } catch {
          case t: Throwable => CompletableFuture.failedFuture(t)
        }
      }
      override def submit(task: Runnable): util.concurrent.Future[_] = {
        // Ignoring scalastyle null check; API requires null is the result of successful futures.
        submit(task, null) // scalastyle:ignore
      }
      override def execute(command: Runnable): Unit = command.run()
      override def invokeAll[T](tasks: util.Collection[_ <: Callable[T]]): util.List[util.concurrent.Future[T]] =
        tasks.stream().map(call => submit(call)).collect(Collectors.toList)
      override def invokeAll[T](tasks: util.Collection[_ <: Callable[T]], timeout: Long, unit: TimeUnit):
          util.List[util.concurrent.Future[T]] = {
        val endTimeMs = System.currentTimeMillis() + unit.toMillis(timeout)
        val res = new util.ArrayList[util.concurrent.Future[T]]
        for (c: Callable[T] <- tasks.asScala) {
          if (System.currentTimeMillis() > endTimeMs) {
            break
          }
          res.add(submit(c))
        }
        res
      }
      override def invokeAny[T](tasks: util.Collection[_ <: Callable[T]]): T =
        tasks.stream()
          .map(callable => submit(callable))
          .filter(future => !futureCompletedExceptionally(future))
          .findAny()
          .orElseThrow(() => new ExecutionException("no invokeAny task succeeded"))

      override def invokeAny[T](tasks: util.Collection[_ <: Callable[T]], timeout: Long, unit: TimeUnit): T = {
        val endTimeMs = System.currentTimeMillis() + unit.toMillis(timeout)
        for (c: Callable[T] <- tasks.asScala) {
          if (System.currentTimeMillis() > endTimeMs) {
            break
          }
          val future = submit(c)
          if (!futureCompletedExceptionally(future)) {
            return future.get()
          }
        }
        throw new ExecutionException("no invokeAny task succeeded")
      }
    })


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
  def doExecute(source: ChunkSource,
                querySession: QuerySession)
               (implicit sched: Scheduler): ExecResult = ???

  /**
   * Applies all {@link RangeVectorTransformer}s to a remote response.
   * FIXME: This is needed because RemoteExec plans override the default ExecPlan::execute()
   *   logic; if possible, this override should eventually be removed.
   */
  protected def applyTransformers(resp: QueryResponse,
                                  querySession: QuerySession,
                                  source: ChunkSource): QueryResponse = resp match {
    case qr: QueryResult =>
      val (obs, schema) =
        applyTransformers(Observable.fromIterable(qr.result), qr.resultSchema, querySession, source)(inProcessScheduler)
      val fut = obs.toListL.map(rvs => qr.copy(result = rvs, resultSchema = schema)).runToFuture(inProcessScheduler)
      assert(fut.isCompleted, "RemoteExec transformer application should have been complete, but was not")
      fut.value.get match {
        case Success(qRes) => qRes
        case Failure(error) => throw new RuntimeException("exception during RemoteExec transformer application", error)
      }
    case qe: QueryError => qe
  }

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
        .map(applyTransformers(_, querySession, source))
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
