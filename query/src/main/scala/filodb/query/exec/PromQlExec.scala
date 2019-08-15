package filodb.query.exec

import com.softwaremill.sttp.circe._
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.sys.ShutdownHookThread

import com.softwaremill.sttp.asynchttpclient.future.AsyncHttpClientFutureBackend

import filodb.core.DatasetRef
import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.Dataset
import filodb.core.query._
import filodb.core.store.ChunkSource
import filodb.memory.format.RowReader
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._

case class PromQlExec(id: String,
                      dispatcher: PlanDispatcher,
                      dataset: DatasetRef, params: PromQlInvocationParams,
                      submitTime: Long = System.currentTimeMillis())
                      extends LeafExecPlan {

  protected def args: String = params.toString
  import PromQlExec._

  val builder = SerializableRangeVector.toBuilder(recSchema)

  /**
    * Limit on number of samples returned by this ExecPlan
    */
  override def limit: Int = ???

  /**
    * Sub classes should override this method to provide a concrete
    * implementation of the operation represented by this exec plan
    * node
    */
  override protected def doExecute(source: ChunkSource, dataset: Dataset, queryConfig: QueryConfig)
                                  (implicit sched: Scheduler, timeout: FiniteDuration): Observable[RangeVector] = ???

  /**
    * Sub classes should implement this with schema of RangeVectors returned
    * from doExecute() abstract method.
    */
  override protected def schemaOfDoExecute(dataset: Dataset): ResultSchema = ???

  override def execute(source: ChunkSource,
                       dataset: Dataset,
                       queryConfig: QueryConfig)
                      (implicit sched: Scheduler,
                       timeout: FiniteDuration): Task[QueryResponse] = {

    val queryResponse = PromQlExec.httpGet(params).map { response =>

      response.unsafeBody match {
        case Left(error) => QueryError(id, error.error)
        case Right(successResponse) => toQueryResponse(successResponse.data, id)
      }

    }
    Task.fromFuture(queryResponse)
  }

  def toQueryResponse(data: Data, id: String): QueryResponse = {

    val rangeVectors = data.result.map { r =>

      val samples = r.values.getOrElse(Seq(r.value.get))

      val rv = new RangeVector {
        val row = new TransientRow()

        override def key: RangeVectorKey = CustomRangeVectorKey(r.metric.map (m => m._1.utf8 -> m._2.utf8))

        override def rows: Iterator[RowReader] = {
          samples.iterator.map { v =>
            row.setLong(0, v.timestamp * 1000)
            row.setDouble(1, v.value)
            row
          }
        }

        override def numRows: Option[Int] = Option(samples.size)

      }
      SerializableRangeVector(rv, builder, recSchema, printTree(useNewline = false))
    }
    QueryResult(id, resultSchema, rangeVectors)
  }

}

object PromQlExec extends  StrictLogging{

  import com.softwaremill.sttp._
  import io.circe.generic.auto._
  import net.ceedubs.ficus.Ficus._

  val columns: Seq[ColumnInfo] = Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
   ColumnInfo("value", ColumnType.DoubleColumn))
  val recSchema = SerializableRangeVector.toSchema(columns)
  val resultSchema = ResultSchema(columns, 1)

  // DO NOT REMOVE PromCirceSupport import below assuming it is unused - Intellij removes it in auto-imports :( .
  // Needed to override Sampl case class Encoder.
  import PromCirceSupport._
  implicit val backend = AsyncHttpClientFutureBackend()

  ShutdownHookThread(shutdown())

  def httpGet(params: PromQlInvocationParams)(implicit scheduler: Scheduler):
  Future[Response[scala.Either[DeserializationError[io.circe.Error], SuccessResponse]]] = {
    val endpoint = params.config.as[Option[String]]("buddy.http.endpoint").get
    val readTimeout = params.config.as[Option[FiniteDuration]]("buddy.http.timeout").getOrElse(60.seconds)
    var urlParams = Map("query" -> params.promQl, "start" -> params.start, "end" -> params.end, "step" -> params.step,
      "processFailure" -> params.processFailure)
    if (params.spread.isDefined)
      urlParams = urlParams + ("spread" -> params.spread.get)

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

