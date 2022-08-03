package filodb.http

import scala.concurrent.duration.DurationInt

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport

import filodb.coordinator._
import filodb.coordinator.v2.{DatasetShardHealth, LocalShardsHealthRequest}
import filodb.core.DatasetRef
import filodb.http.apiv1.HttpSchema

final case class DatasetEvents(dataset: String, shardEvents: Seq[String])

class HealthRoute(coordinatorActor: ActorRef, v2ClusterEnabled: Boolean) extends FiloRoute {
  import FailFastCirceSupport._
  import io.circe.generic.auto._

  import filodb.coordinator.client.Client._
  import HttpSchema._

  private val healthyShardStatuses = Seq(ShardStatusActive, ShardStatusRecovery, ShardStatusAssigned)

  val route = {
    // Returns the local dataset/shard status as JSON
    path ("__health") {
      get {
        if (v2ClusterEnabled) {
          onSuccess(asyncAsk(coordinatorActor, LocalShardsHealthRequest, 60.seconds)) {
            case m: Seq[DatasetShardHealth]@unchecked =>
              if (m.forall(h => healthyShardStatuses.contains(h.status))) complete(m)
              else complete(StatusCodes.ServiceUnavailable -> m)
          }
        } else {
          onSuccess(asyncAsk(coordinatorActor, StatusActor.GetCurrentEvents, 60.seconds)) {
            case m: collection.Map[DatasetRef, Seq[ShardEvent]]@unchecked =>
              complete(httpList(m.toSeq.map { case (ref, ev) => DatasetEvents(ref.toString, ev.map(_.toString)) }))
          }
        }
      }
    }
  }
}
