package filodb.http

import akka.actor.ActorRef
import akka.http.scaladsl.model.{StatusCodes => Codes}
import akka.http.scaladsl.server.Directives._
import com.typesafe.scalalogging.StrictLogging
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport

import filodb.coordinator.{CurrentShardSnapshot, NodeClusterActor}
import filodb.core.{DatasetRef, ErrorResponse, Success => SuccessResponse}
import filodb.core.store.{AssignShardConfig, UnassignShardConfig}
import filodb.http.apiv1.{HttpSchema, HttpShardDetails, HttpShardState, HttpShardStateByAddress}

class ClusterApiRoute(clusterProxy: ActorRef) extends FiloRoute with StrictLogging {
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
    // GET /api/v1/cluster/<dataset>/statusByAddress - shard health status grouped by node address
    // Sample output as follows:
    // {{{
    //  {
    //     "status": "success",
    //     "data": [
    //         {
    //             "address": "akka.tcp://filo-standalone@23.13.16.45:91007",
    //             "shardList": [
    //                 {
    //                     "shard": 0,
    //                     "status": "ShardStatusActive"
    //                 },
    //                 {
    //                     "shard": 1,
    //                     "status": "ShardStatusRecovery(94)"
    //                 }
    //             ]
    //         }
    //     ]
    //  }
    // }}}
    path(Segment / "statusByAddress") { dataset =>
      get {
          onSuccess(asyncAsk(clusterProxy, GetShardMap(DatasetRef.fromDotString(dataset)))) {
            case CurrentShardSnapshot(_, map) =>
              val groupByAddressMap = map.shardValues.zipWithIndex.groupBy(_._1._1)
              val statusList = groupByAddressMap map { case (ref, statusTuple) =>
                HttpShardStateByAddress(if (ref == ActorRef.noSender) "" else ref.path.address.toString,
                  statusTuple.map { case ((ref2, status), idx) =>
                    HttpShardDetails(idx, status.toString)
                  })
              }
              complete(httpList(statusList.toSeq))
            case DatasetUnknown(_) =>
              complete(Codes.NotFound -> httpErr("DatasetUnknown", s"Dataset $dataset is not registered"))
          }
      }
    } ~
    // POST /api/v1/cluster/<dataset>/stopshards - shard reassignment request
    // Sample input as follows:
    // {{{
    //  {
    //    "shardList": [23, 24]
    //  }
    // }}}
    path(Segment / "stopshards") { dataset =>
      post {
        entity(as[UnassignShardConfig]) { shardConfig =>
          try onSuccess(asyncAsk(clusterProxy, StopShards(shardConfig, DatasetRef.fromDotString(dataset)))) {
            case SuccessResponse  => complete(httpList(Seq.empty[String]))
            case e: ErrorResponse => complete(Codes.BadRequest -> httpErr(e.toString, e.toString))
          } catch {
            case e: Exception => complete(Codes.InternalServerError -> httpErr(e))
          }
        }
      }
    } ~
    // POST /api/v1/cluster/<dataset>/startshards - shard reassignment request
    // Sample input as follows:
    // {{{
    //  {
    //    "address": "akka.tcp://filo-standalone@23.13.16.45:91007",
    //    "shardList": [23, 24]
    //  }
    // }}}
    path(Segment / "startshards") { dataset =>
      post {
        entity(as[AssignShardConfig]) { shardConfig =>
          try onSuccess(asyncAsk(clusterProxy, StartShards(shardConfig, DatasetRef.fromDotString(dataset)))) {
            case SuccessResponse  => complete(httpList(Seq.empty[String]))
            case e: ErrorResponse => complete(Codes.BadRequest -> httpErr(e.toString, e.toString))
          } catch {
            case e: Exception => complete(Codes.InternalServerError -> httpErr(e))
          }
        }
      }
    } ~
    // TODO: Need a route to list all spare filodb nodes too
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