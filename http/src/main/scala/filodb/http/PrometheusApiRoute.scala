package filodb.http

import scala.concurrent.Future

import akka.actor.ActorRef
import akka.http.scaladsl.model.{HttpEntity, HttpResponse, MediaTypes, StatusCodes => Codes}
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.typesafe.scalalogging.StrictLogging
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.xerial.snappy.Snappy
import remote.RemoteStorage.ReadRequest

import filodb.coordinator.client.IngestionCommands.UnknownDataset
import filodb.coordinator.client.QueryCommands.{LogicalPlan2Query, QueryOptions, SpreadChange, StaticSpreadProvider}
import filodb.core.DatasetRef
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.prometheus.query.PrometheusModel.Sampl
import filodb.query.{LogicalPlan, QueryError, QueryResult}


class PrometheusApiRoute(nodeCoord: ActorRef, settings: HttpSettings)(implicit am: ActorMaterializer)
           extends FiloRoute with StrictLogging {

  import FailFastCirceSupport._
  import io.circe.generic.auto._

  import filodb.coordinator.client.Client._
  import filodb.prometheus.query.PrometheusModel._

  val spreadProvider = new StaticSpreadProvider(SpreadChange(0, settings.queryDefaultSpread))

  val queryOptions = QueryOptions(spreadProvider, settings.querySampleLimit)

  val route = pathPrefix( "promql" / Segment) { dataset =>
    // Path: /promql/<datasetName>/api/v1/query_range
    // Used to issue a promQL query for a time range with a `start` and `end` timestamp and at regular `step` intervals.
    // For more details, see Prometheus HTTP API Documentation
    // [Range Queries](https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries)
    path( "api" / "v1" / "query_range") {
      get {
        parameter('query.as[String], 'start.as[Double], 'end.as[Double],
                  'step.as[Int], 'verbose.as[Boolean].?) { (query, start, end, step, verbose) =>
          val logicalPlan = Parser.queryRangeToLogicalPlan(query, TimeStepParams(start.toLong, step, end.toLong))
          askQueryAndRespond(dataset, logicalPlan, verbose.getOrElse(false))
        }
      }
    } ~
    // Path: /promql/<datasetName>/api/v1/query
    // Used to issue a promQL query for a single time instant `time`.  Can also be used to query raw data by issuing
    // a PromQL range expression. For more details, see Prometheus HTTP API Documentation
    // [Instant Queries](https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries)
    path( "api" / "v1" / "query") {
      get {
        parameter('query.as[String], 'time.as[Double], 'verbose.as[Boolean].?) { (query, time, verbose) =>
          val logicalPlan = Parser.queryToLogicalPlan(query, time.toLong)
          askQueryAndRespond(dataset, logicalPlan, verbose.getOrElse(false))
        }
      }
    } ~
    // Path: /promql/<datasetName>/api/v1/read
    // Used to extract raw data for integration with other TSDB systems.
    //  * Input: ReadRequest Protobuf
    //  * Output: ReadResponse Protobuf
    // See [Prometheus Remote Proto Def](https://github.com/prometheus/prometheus/blob/master/prompb/remote.proto)
    // for more details
    /*  Important Note: This Prometheus API should NOT be used for extracting raw data out from FiloDB at scale. Current
    implementation includes the same 'limit' settings that apply in the Akka Actor interface.
    Currently, it is designed and intended only for functional testing of query engine. */
    path( "api" / "v1" / "read") {
      post {
        extractDataBytes { data =>
          val bytesFut = data.runFold(ByteString.empty) { case (acc, b) => acc ++ b }
          val fut = bytesFut.flatMap { bytes =>
            // Would have ideally liked to have the unencoding driven by content encoding headers,
            // but Akka doesnt support snappy out of the box. Elegant solution is a TODO for later.
            val readReq = ReadRequest.parseFrom(Snappy.uncompress(bytes.toArray))
            val asks = toFiloDBLogicalPlans(readReq).map { logicalPlan =>
              asyncAsk(nodeCoord, LogicalPlan2Query(DatasetRef.fromDotString(dataset), logicalPlan, queryOptions))
            }
            Future.sequence(asks)
          }
          onSuccess(fut) { case qr =>
            qr.find(!_.isInstanceOf[filodb.query.QueryResult]) match {
              case Some(qe: QueryError) => complete(toPromErrorResponse(qe))
              case Some(UnknownDataset) => complete(Codes.NotFound ->
                                           ErrorResponse("badQuery", s"Dataset $dataset is not registered"))
              case Some(a: Any)      => throw new IllegalStateException(s"Got $a as query response")
              case None              => val rrBytes = toPromReadResponse(qr.asInstanceOf[Seq[filodb.query.QueryResult]])
                                        // Would have ideally liked to have the encoding driven by akka directives,
                                        // but Akka doesnt support snappy out of the box.
                                        // Elegant solution is a TODO for later.
                                        val body = ByteString(Snappy.compress(rrBytes))
                                        val entity = HttpEntity.Strict(MediaTypes.`application/octet-stream`, body)
                                        complete(HttpResponse(entity = entity))
            }
          }
        }
      }
    }
  }

  private def askQueryAndRespond(dataset: String, logicalPlan: LogicalPlan, verbose: Boolean) = {
    val command = LogicalPlan2Query(DatasetRef.fromDotString(dataset), logicalPlan, queryOptions)
    onSuccess(asyncAsk(nodeCoord, command)) {
      case qr: QueryResult => complete(toPromSuccessResponse(qr, verbose))
      case qr: QueryError => complete(toPromErrorResponse(qr))
      case UnknownDataset => complete(Codes.NotFound ->
        ErrorResponse("badQuery", s"Dataset $dataset is not registered"))
    }
  }
}

// TODO extend and make more generic
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