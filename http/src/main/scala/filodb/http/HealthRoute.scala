package filodb.http

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import com.typesafe.scalalogging.StrictLogging
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport

import filodb.coordinator._
import filodb.coordinator.v2.{DatasetShardHealth, LocalShardsHealthRequest}
import filodb.core.DatasetRef
import filodb.http.apiv1.HttpSchema

final case class DatasetEvents(dataset: String, shardEvents: Seq[String])

class HealthRoute(coordinatorActor: ActorRef, v2ClusterEnabled: Boolean, settings: HttpSettings)
  extends FiloRoute with StrictLogging {
  import FailFastCirceSupport._
  import io.circe.generic.auto._

  import filodb.coordinator.client.Client._
  import HttpSchema._

  private val healthyShardStatuses = Seq(ShardStatusActive.getClass,
                                         ShardStatusRecovery.getClass,
                                         ShardStatusAssigned.getClass)

  private val livenessShardEvents = Seq(
    IngestionStarted.getClass,
    RecoveryStarted.getClass,
    RecoveryInProgress.getClass)

  private val livenessShardStatuses = Seq(ShardStatusActive.getClass, ShardStatusRecovery.getClass)

  def checkIfAllShardsLiveClusterV1(shardEvents: collection.Map[DatasetRef, Seq[ShardEvent]]): Boolean = {
    shardEvents.values.flatten.forall(
      shardEvent => livenessShardEvents.contains(shardEvent.getClass))
  }

  def checkIfAllShardsLiveClusterV2(shardHealthSeq: Seq[DatasetShardHealth]): Boolean = {
    shardHealthSeq.forall(
      shardHealth => livenessShardStatuses.contains(shardHealth.status.getClass))
  }

  val route = {
    // Returns the local dataset/shard status as JSON
    path ("__health") {
      get {
        if (v2ClusterEnabled) {
          onSuccess(asyncAsk(coordinatorActor, LocalShardsHealthRequest, settings.queryAskTimeout)) {
            case m: Seq[DatasetShardHealth]@unchecked =>
              if (m.forall(h => healthyShardStatuses.contains(h.status.getClass))) complete(m)
              else {
                logger.error(s"Health check for this FiloDB node failed since some shards were not healthy: " +
                  s"$m")
                complete(StatusCodes.ServiceUnavailable -> m)
              }
          }
        } else {
          onSuccess(asyncAsk(coordinatorActor, StatusActor.GetCurrentEvents, settings.queryAskTimeout)) {
            case m: collection.Map[DatasetRef, Seq[ShardEvent]]@unchecked =>
              complete(httpList(m.toSeq.map { case (ref, ev) => DatasetEvents(ref.toString, ev.map(_.toString)) }))
          }
        }
      }
    } ~
    // Adding a simple liveness endpoint to check if the FiloDB instance is reachable or not
    path ("__liveness") {
      get {
        if (v2ClusterEnabled) {
          onSuccess(asyncAsk(coordinatorActor, LocalShardsHealthRequest, settings.queryAskTimeout)) {
            case m: Seq[DatasetShardHealth]@unchecked =>
              if (checkIfAllShardsLiveClusterV2(m)) {
                complete(httpLiveness("UP"))
              }
              else {
                logger.error(s"Liveness check for node failed ! ShardStatus: $m")
                complete(httpLiveness("DOWN"))
              }
          }
        } else {
          onSuccess(asyncAsk(coordinatorActor, StatusActor.GetCurrentEvents, settings.queryAskTimeout)) {
            case m: collection.Map[DatasetRef, Seq[ShardEvent]]@unchecked =>
              if (checkIfAllShardsLiveClusterV1(m)) {
                complete(httpLiveness("UP"))
              }
              else {
                logger.error(s"Liveness check for node failed ! ShardStatus: $m")
                complete(httpLiveness("DOWN"))
              }
          }
        }
      }
    }
  }
}
