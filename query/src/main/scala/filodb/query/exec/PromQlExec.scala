package filodb.query.exec


import com.softwaremill.sttp.akkahttp.AkkaHttpBackend
import com.softwaremill.sttp.circe._
import io.circe.{Decoder, Encoder, HCursor, Json}
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration


import filodb.core.DatasetRef
import filodb.core.store.ChunkSource
import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.Dataset
import filodb.core.query._
import filodb.memory.format.RowReader
import filodb.query._
import filodb.memory.format.ZeroCopyUTF8String._


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

  override def execute(source: ChunkSource,
                       dataset: Dataset,
                       queryConfig: QueryConfig)
                      (implicit sched: Scheduler,
                       timeout: FiniteDuration): Task[QueryResponse] = {

    Task.fromFuture(PromQlExec.httpClient(params, id))

  }

}

object PromCirceSupport {
  // necessary to encode sample in promql response as an array with long and double value as string
  implicit val encodeSampl: Encoder[Sampl] = new Encoder[Sampl] {
    final def apply(a: Sampl): Json = Json.arr(Json.fromLong(a.timestamp), Json.fromString(a.value.toString))
  }

  implicit val decodeFoo: Decoder[Sampl] = new Decoder[Sampl] {
    final def apply(c: HCursor): Decoder.Result[Sampl] = {
      for {timestamp <- c.downArray.as[Long].right
           value <- c.downArray.right.as[String].right
      } yield {
        Sampl(timestamp, value.toDouble)
      }
    }
  }
}

object PromQlExec {
  import com.softwaremill.sttp._
  implicit val sttpBackend = AkkaHttpBackend()

  def toQueryResponse(data: Data, id: String): QueryResponse = {

    val schema = ResultSchema(Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
      ColumnInfo("value", ColumnType.DoubleColumn)), 1)
    val rangeVectors : Seq[RangeVector]= data.result.map {
      r => {

       new RangeVector {
          override def key: RangeVectorKey = CustomRangeVectorKey(r.metric.map {
            m => {
              m._1.utf8 -> m._2.utf8
            }
          })

         override def rows: Iterator[RowReader] = r.values.map { v => {
           new TransientRow(v.timestamp, v.value)
         }
         }.iterator

         override def numRows: Option[StatusCode] = Some(rows.size)
        }


      }
    }
    QueryResult(id, schema, rangeVectors)

  }

  def httpClient(params: PromQlQueryParams, id: String)(implicit scheduler: Scheduler): Future[QueryResponse] = {

    val urlParams = Map("query" -> params.promQl, "start" -> params.start, "end" -> params.end, "step" -> params.step)
    val url = uri"$params.promEndPoint?$urlParams"
    val queryResponse = sttp.get(url).response(asJson[SuccessResponse]).send()
    queryResponse.map(response => {

      response.unsafeBody match {
        case Left(error) => QueryError(id, new IllegalStateException(error.message))
        case Right(response) => toQueryResponse(response.data, id)
      }

    })

  }
}
