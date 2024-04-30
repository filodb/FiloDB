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

  /**
   * @param shardEvents
   * @return
   */
  def checkIfAllShardsLiveClusterV1(shardEvents: collection.Map[DatasetRef, Seq[ShardEvent]]): Boolean = {
    val allShardEvents = shardEvents.values.flatten.toSeq
    // wait for the minimum amount of responses needed
    if (allShardEvents.length < numResponsesNeededForLivenessCheck) {
      return false
    }
    allShardEvents.forall {
      case IngestionStarted(_, _, _) | RecoveryStarted(_, _, _, _) | RecoveryInProgress(_, _, _, _) => true
      case _ => false
    }
  }

  /**
   * @param shardHealthSeq
   * @return
   */
  def checkIfAllShardsLiveClusterV2(shardHealthSeq: Seq[DatasetShardHealth]): Boolean = {
    val allShardStatus = shardHealthSeq.map(x => x.status)
    // wait for the minimum amount of responses needed
    if (allShardStatus.length < numResponsesNeededForLivenessCheck) {
      return false
    }
    allShardStatus.forall {
      case ShardStatusActive | ShardStatusRecovery(_) => true
      case _ => false
    }
  }

  /**
   * Gets the total number of shards from all the dataset configs and calculates the
   * number of responses needed for liveness check
   * @return [Int] number of responses needed
   */
  lazy val numResponsesNeededForLivenessCheck : Int = {
    var totalNumShards = 0
    settings.filoSettings.streamConfigs.foreach { config =>
      totalNumShards += config.getInt("num-shards")
    }
    totalNumShards / settings.filoSettings.minNumNodes.get
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
    // Adding a simple liveness endpoint to check if the FiloDB instance is reachable and if the kafka and cassandra
    // connections are initialized correctly on startup. This will help us in detecting any connection issues to
    // kafka and cassandra on startup.
    // NOTE: We don't wait for bootstrap to finish or normal ingestion to start or shards to be active. As soon as
    // ingestion or recovery is started, we consider the filodb pods to be live
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
