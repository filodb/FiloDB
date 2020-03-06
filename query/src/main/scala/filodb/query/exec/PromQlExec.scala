package filodb.query.exec

import java.util.concurrent.TimeUnit

import com.softwaremill.sttp.asynchttpclient.future.AsyncHttpClientFutureBackend
import com.softwaremill.sttp.circe._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
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

case class PromQlExec(queryContext: QueryContext,
                      dispatcher: PlanDispatcher,
                      dataset: DatasetRef,
                      params: PromQlQueryParams) extends LeafExecPlan {

  protected def args: String = params.toString
  import PromQlExec._

  val builder = SerializedRangeVector.newBuilder()

  def limit: Int = ???

  /**
    * Sub classes should override this method to provide a concrete
    * implementation of the operation represented by this exec plan
    * node
    */
  def doExecute(source: ChunkSource, queryConfig: QueryConfig)
               (implicit sched: Scheduler): ExecResult = ???

  override def execute(source: ChunkSource,
                       queryConfig: QueryConfig)
                      (implicit sched: Scheduler): Task[QueryResponse] = {
    val execPlan2Span = Kamon.spanBuilder(s"execute-${getClass.getSimpleName}")
      .asChildOf(Kamon.currentSpan())
      .tag("query-id", queryContext.queryId)
      .start()

    val queryResponse = PromQlExec.httpGet(params, queryContext.submitTime).
      map { response =>
      response.unsafeBody match {
        case Left(error) => QueryError(queryContext.queryId, error.error)
        case Right(successResponse) => toQueryResponse(successResponse.data, queryContext.queryId, execPlan2Span)
      }
    }
    // Please note that the following needs to be wrapped inside `runWithSpan` so that the context will be propagated
    // across threads. Note that task/observable will not run on the thread where span is present since
    // kamon uses thread-locals.
    Kamon.runWithSpan(execPlan2Span, true) {
      Task.fromFuture(queryResponse)
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
    }
    span.finish()
    QueryResult(id, resultSchema, rangeVectors)
  }

}

object PromQlExec extends StrictLogging {

  import com.softwaremill.sttp._
  import io.circe.generic.auto._
  import net.ceedubs.ficus.Ficus._

  val columns: Seq[ColumnInfo] = Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
   ColumnInfo("value", ColumnType.DoubleColumn))
  val recSchema = SerializedRangeVector.toSchema(columns)
  val resultSchema = ResultSchema(columns, 1)

  // DO NOT REMOVE PromCirceSupport import below assuming it is unused - Intellij removes it in auto-imports :( .
  // Needed to override Sampl case class Encoder.
  import PromCirceSupport._
  implicit val backend = AsyncHttpClientFutureBackend()

  ShutdownHookThread(shutdown())

  def httpGet(promQlQueryParams: PromQlQueryParams, submitTime: Long)(implicit scheduler: Scheduler):
  Future[Response[scala.Either[DeserializationError[io.circe.Error], SuccessResponse]]] = {

    @transient lazy val config = ConfigFactory.parseString(promQlQueryParams.configString)
    val endpoint = config.as[Option[String]]("buddy.http.endpoint").get
    val queryTimeElapsed = System.currentTimeMillis() - submitTime
    val buddyHttpTimeout = config.as[Option[FiniteDuration]]("buddy.http.timeout").
                            getOrElse(60000.millis)
    val readTimeout = FiniteDuration(buddyHttpTimeout.toMillis - queryTimeElapsed, TimeUnit.MILLISECONDS)
    var urlParams = Map("query" -> promQlQueryParams.promQl,
                        "start" -> promQlQueryParams.startSecs,
                        "end" -> promQlQueryParams.endSecs,
                        "step" -> promQlQueryParams.stepSecs,
                        "processFailure" -> promQlQueryParams.processFailure)
    if (promQlQueryParams.spread.isDefined) urlParams = urlParams + ("spread" -> promQlQueryParams.spread.get)

    val url = uri"$endpoint?$urlParams"
    logger.debug("promqlexec url is {}", url)
    sttp
      .get(url)
      .readTimeout(readTimeout)
      .response(asJson[SuccessResponse])
      .send()
  }

  def shutdown(): Unit =
  {
    logger.info("Shutting PromQlExec http")
    backend.close()
  }
}

