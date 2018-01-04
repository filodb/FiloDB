package filodb.http

import akka.actor.ActorRef
import akka.http.scaladsl.model.{StatusCodes => Codes}
import akka.http.scaladsl.server.Directives._
import com.typesafe.config.{ConfigException, ConfigFactory}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport

import filodb.coordinator.{CurrentShardSnapshot, NodeClusterActor}
import filodb.core.{DatasetRef, ErrorResponse}
import filodb.core.store.IngestionConfig
import filodb.http.apiv1.{HttpSchema, HttpShardState}

class ClusterApiRoute(clusterProxy: ActorRef) extends FiloRoute {
  import FailFastCirceSupport._
  import io.circe.generic.auto._
  import HttpSchema._
  import filodb.coordinator.client.Client._
  import NodeClusterActor._

  val route = pathPrefix("api" / "v1" / "cluster") {
    // GET /api/v1/cluster/<dataset>/status - shard health status report
    path(Segment / "status") { dataset =>
      get {
        onSuccess(asyncAsk(clusterProxy, GetShardMap(DatasetRef.fromDotString(dataset)))) {
          case CurrentShardSnapshot(_, map) =>
            val statusList = map.shardValues.zipWithIndex.map { case ((ref, status), idx) =>
                               HttpShardState(idx, status.toString,
                                              if (ref == ActorRef.noSender) "" else ref.path.address.toString) }
            complete(httpList(statusList))
          case DatasetUnknown(_)            =>
            complete(Codes.NotFound -> httpErr("DatasetUnknown", s"Dataset $dataset is not registered"))
        }
      }
    } ~
    // POST /api/v1/cluster/<dataset> - set up dataset for streaming ingestion
    path(Segment) { dataset =>
      post {
        entity(as[String]) { sourceConfig =>
          try {
            val sourceConf = ConfigFactory.parseString(sourceConfig)
            val setupCmd = SetupDataset(IngestionConfig(sourceConf, noOpSource.streamFactoryClass))
            onSuccess(asyncAsk(clusterProxy, setupCmd)) {
              case DatasetVerified  => complete(httpList(Seq.empty[String]))
              case e: ErrorResponse => complete(Codes.Conflict -> httpErr(e.toString, e.toString))
            }
          } catch {
            case e: ConfigException =>
              complete(Codes.BadRequest -> httpErr(s"Parsing error: ${e.getClass.getName}", e.getMessage))
            case e: Exception =>
              complete(Codes.InternalServerError -> httpErr(e))
          }
        }
      }
    } ~
    // GET /api/v1/cluster - List the datasets registered for streaming ingestion
    pathEnd {
      get {
        complete {
          asyncTypedAsk[Seq[DatasetRef]](clusterProxy, ListRegisteredDatasets).map { refs =>
            httpList(refs.map(_.toString))
          }
        }
      }
    }
  }
}