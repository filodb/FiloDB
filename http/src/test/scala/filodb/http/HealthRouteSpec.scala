package filodb.http

import akka.actor.ActorRef
import filodb.coordinator._
import filodb.coordinator.v2.DatasetShardHealth
import filodb.core.{AsyncTest, DatasetRef}
import org.scalatest.funspec.AnyFunSpec
class HealthRouteSpec extends AnyFunSpec with AsyncTest {

  val settings = new HttpSettings(PrometheusApiRouteSpec.config, new FilodbSettings())
  val healthRouteClusterV1 = new HealthRoute(ActorRef.noSender, false, settings)
  val healthRouteClusterV2 = new HealthRoute(ActorRef.noSender, true, settings)
  val dsRef = DatasetRef("prometheus")

  it("test checkIfAllShardsLiveClusterV1 all liveness scenarios") {

    var ingestionNotStartedForAll = collection.immutable.Map(dsRef ->
      Seq(
        IngestionStarted(dsRef, 0, ActorRef.noSender),
        IngestionStarted(dsRef, 1, ActorRef.noSender),
        ShardAssignmentStarted(dsRef, 2, ActorRef.noSender),
        RecoveryStarted(dsRef, 3, ActorRef.noSender, 0)
      ))

    ingestionNotStartedForAll match {
      case m: collection.Map[DatasetRef, Seq[ShardEvent]] => {
        healthRouteClusterV1.checkIfAllShardsLiveClusterV1(m) shouldEqual false
      }
      case _ => throw new IllegalArgumentException("Should not reach here")
    }

    ingestionNotStartedForAll = collection.immutable.Map(dsRef ->
      Seq(
        IngestionStarted(dsRef, 0, ActorRef.noSender),
        IngestionStopped(dsRef, 1),
        ShardAssignmentStarted(dsRef, 2, ActorRef.noSender),
        RecoveryStarted(dsRef, 3, ActorRef.noSender, 0)
      ))

    ingestionNotStartedForAll match {
      case m: collection.Map[DatasetRef, Seq[ShardEvent]] => {
        healthRouteClusterV1.checkIfAllShardsLiveClusterV1(m) shouldEqual false
      }
      case _ => throw new IllegalArgumentException("Should not reach here")
    }

    val ingestionStartedForAll = collection.immutable.Map(dsRef ->
      Seq(
        IngestionStarted(dsRef, 0, ActorRef.noSender),
        RecoveryInProgress(dsRef, 1, ActorRef.noSender, 10),
        IngestionStarted(dsRef, 2, ActorRef.noSender),
        RecoveryStarted(dsRef, 3, ActorRef.noSender, 0)
      ))

    ingestionStartedForAll match {
      case m: collection.Map[DatasetRef, Seq[ShardEvent]] => {
        healthRouteClusterV1.checkIfAllShardsLiveClusterV1(m) shouldEqual true
      }
      case _ => throw new IllegalArgumentException("Should not reach here")
    }
  }

  it("test checkIfAllShardsLiveClusterV2 all liveness scenarios") {

    var ingestionNotStartedForAll = Seq(
      DatasetShardHealth(dsRef, 0, ShardStatusAssigned),
      DatasetShardHealth(dsRef, 1, ShardStatusAssigned),
      DatasetShardHealth(dsRef, 2, ShardStatusRecovery(10)),
      DatasetShardHealth(dsRef, 3, ShardStatusActive)
    )

    ingestionNotStartedForAll match {
      case m: Seq[DatasetShardHealth]@unchecked => {
        healthRouteClusterV1.checkIfAllShardsLiveClusterV2(m) shouldEqual false
      }
      case _ => throw new IllegalArgumentException("Should not reach here")
    }

    ingestionNotStartedForAll = Seq(
      DatasetShardHealth(dsRef, 0, ShardStatusActive),
      DatasetShardHealth(dsRef, 1, ShardStatusDown),
      DatasetShardHealth(dsRef, 2, ShardStatusRecovery(10)),
      DatasetShardHealth(dsRef, 3, ShardStatusActive)
    )

    ingestionNotStartedForAll match {
      case m: Seq[DatasetShardHealth]@unchecked => {
        healthRouteClusterV1.checkIfAllShardsLiveClusterV2(m) shouldEqual false
      }
      case _ => throw new IllegalArgumentException("Should not reach here")
    }

    val ingestionStartedForAll = Seq(
      DatasetShardHealth(dsRef, 0, ShardStatusActive),
      DatasetShardHealth(dsRef, 1, ShardStatusActive),
      DatasetShardHealth(dsRef, 2, ShardStatusRecovery(10)),
      DatasetShardHealth(dsRef, 3, ShardStatusActive)
    )

    ingestionStartedForAll match {
      case m: Seq[DatasetShardHealth]@unchecked => {
        healthRouteClusterV1.checkIfAllShardsLiveClusterV2(m) shouldEqual true
      }
      case _ => throw new IllegalArgumentException("Should not reach here")
    }
  }
}
