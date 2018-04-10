package filodb.http

import akka.actor.ActorRef
import akka.http.scaladsl.server.Directives._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport

import filodb.coordinator.{ShardEvent, StatusActor}
import filodb.core.DatasetRef
import filodb.http.apiv1.HttpSchema

final case class DatasetEvents(dataset: String, shardEvents: Seq[String])

class HealthRoute(coordinatorActor: ActorRef) extends FiloRoute {
  import FailFastCirceSupport._
  import io.circe.generic.auto._
  import filodb.coordinator.client.Client._
  import HttpSchema._

  val route = {
    // Returns the local dataset/shard status as JSON
    path ("__health") {
      get {
        onSuccess(asyncAsk(coordinatorActor, StatusActor.GetCurrentEvents)) {
          case m: collection.Map[DatasetRef, Seq[ShardEvent]] @unchecked =>
            complete(httpList(m.toSeq.map { case (ref, ev) => DatasetEvents(ref.toString, ev.map(_.toString)) }))
        }
      }
    }
  }
}
