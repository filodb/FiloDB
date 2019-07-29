package filodb.query.exec

import com.softwaremill.sttp.akkahttp.AkkaHttpBackend
import com.softwaremill.sttp.circe._
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

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
                      dataset: DatasetRef, params: PromQlQueryParams,
                      submitTime: Long = System.currentTimeMillis()) extends LeafExecPlan {

  protected def args: String = ""

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

  val columns: Seq[ColumnInfo] = Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
    ColumnInfo("value", ColumnType.DoubleColumn))
  val recSchema = SerializableRangeVector.toSchema(columns)
  val builder = SerializableRangeVector.toBuilder(recSchema)

  override def execute(source: ChunkSource,
                       dataset: Dataset,
                       queryConfig: QueryConfig)
                      (implicit sched: Scheduler,
                       timeout: FiniteDuration): Task[QueryResponse] = {

    val queryResponse = PromQlExec.httpClient(params).map(response => {

      response.unsafeBody match {
        case Left(error) => QueryError(id, new IllegalStateException(error.message))
        case Right(response) => toQueryResponse(response.data, id)
      }

    })
    Task.fromFuture(queryResponse)
  }

  def toQueryResponse(data: Data, id: String): QueryResponse = {

    val schema = ResultSchema(columns, 1)
    val rangeVectors = data.result.map {
      r => {
        val rv = new RangeVector {
          override def key: RangeVectorKey = CustomRangeVectorKey(r.metric.map {
            m => {
              m._1.utf8 -> m._2.utf8
            }
          })

          override def rows: Iterator[RowReader] = r.values.map { v => {
            new TransientRow(v.timestamp, v.value)
          }
          }.iterator
        }
        SerializableRangeVector(rv, builder, recSchema, printTree(false))
      }
    }
    QueryResult(id, schema, rangeVectors)
  }
}

object PromQlExec extends  StrictLogging{

  import com.softwaremill.sttp._
  import io.circe.generic.auto._

  import scala.concurrent.ExecutionContext.Implicits.global
  // DO NOT REMOVE PromCirceSupport import below assuming it is unused - Intellij removes it in auto-imports :( .
  // Needed to override Sampl case class Encoder.
  import PromCirceSupport._
  implicit val backend = AkkaHttpBackend()

  def httpClient(params: PromQlQueryParams):
  Future[Response[scala.Either[DeserializationError[io.circe.Error], SuccessResponse]]] = {
    val urlParams = Map("query" -> params.promQl, "start" -> params.start, "end" -> params.end, "step" -> params.step,
      "processFailure:" -> params.processFailure)
    val endpoint = params.promEndPoint
    val url = uri"$endpoint?$urlParams"
    logger.debug("promqlexec url is {}", url)
    sttp.get(url).response(asJson[SuccessResponse]).send()
  }
}

