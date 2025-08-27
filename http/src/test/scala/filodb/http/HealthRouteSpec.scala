package filodb.http

import akka.actor.ActorRef
import com.typesafe.config.ConfigFactory
import filodb.coordinator._
import filodb.coordinator.v2.DatasetShardHealth
import filodb.core.{AsyncTest, DatasetRef, TestData}
import org.scalatest.funspec.AnyFunSpec
class HealthRouteSpec extends AnyFunSpec with AsyncTest {

  val singleDatasetSourceConfigString =
    s"""
    filodb {
      memstore.groups-per-shard = 4
      min-num-nodes-in-cluster = 2
      inline-dataset-configs = [
        {
          dataset = "prometheus"
          schema = "gauge"
          num-shards = 8
          min-num-nodes = 2
          sourcefactory = "${classOf[NoOpStreamFactory].getName}"
          sourceconfig {
            shutdown-ingest-after-stopped = false
            ${TestData.sourceConfStr}
          }
        }
      ]
    }
  """

  val multipleDatasetSourceConfigString =
    s"""
    filodb {
      memstore.groups-per-shard = 4
      min-num-nodes-in-cluster = 2
      inline-dataset-configs = [
        {
          dataset = "prometheus_rules_1m"
          schema = "gauge"
          num-shards = 8
          min-num-nodes = 2
          sourcefactory = "${classOf[NoOpStreamFactory].getName}"
          sourceconfig {
            shutdown-ingest-after-stopped = false
            ${TestData.sourceConfStr}
          }
        },
        {
          dataset = "prometheus_rules_longterm"
          schema = "gauge"
          num-shards = 8
          min-num-nodes = 2
          sourcefactory = "${classOf[NoOpStreamFactory].getName}"
          sourceconfig {
            shutdown-ingest-after-stopped = false
            ${TestData.sourceConfStr}
          }
        }
      ]
    }
  """

  val singleDatasetConfig = ConfigFactory.parseString(singleDatasetSourceConfigString)
    .withFallback(ConfigFactory.parseResources("application_test.conf"))
    .withFallback(ConfigFactory.load("filodb-defaults.conf")).resolve()

  val multipleDatasetConfig = ConfigFactory.parseString(multipleDatasetSourceConfigString)
    .withFallback(ConfigFactory.parseResources("application_test.conf"))
    .withFallback(ConfigFactory.load("filodb-defaults.conf")).resolve()

  val singleDatasetSettings = new HttpSettings(PrometheusApiRouteSpec.config, new FilodbSettings(singleDatasetConfig))
  val multipleDatasetSettings = new HttpSettings(PrometheusApiRouteSpec.config,
    new FilodbSettings(multipleDatasetConfig))
  val healthRouteClusterV1 = new HealthRoute(ActorRef.noSender, false, singleDatasetSettings)
  val healthRouteClusterV2 = new HealthRoute(ActorRef.noSender, true, singleDatasetSettings)
  val healthRouteClusterV2Multiple = new HealthRoute(ActorRef.noSender, true, multipleDatasetSettings)
  val dsRefPrometheus = DatasetRef("prometheus")
  val dsRefPrometheusRules_1m = DatasetRef("prometheus_rules_1m")
  val dsRefPrometheusRules_longterm = DatasetRef("prometheus_rules_longterm")

  it("test checkIfAllShardsLiveClusterV1 all liveness scenarios") {

    val testNotEnoughResponses = collection.immutable.Map(dsRefPrometheus ->
      Seq(
        IngestionStarted(dsRefPrometheus, 0, ActorRef.noSender),
        IngestionStarted(dsRefPrometheus, 1, ActorRef.noSender)
      ))

    testNotEnoughResponses match {
      case m: collection.Map[DatasetRef, Seq[ShardEvent]] => {
        healthRouteClusterV1.checkIfAllShardsLiveClusterV1 (m) shouldEqual false
      }
      case _ => throw new IllegalArgumentException("Should not reach here")
    }

    var ingestionNotStartedForAll = collection.immutable.Map(dsRefPrometheus ->
      Seq(
        IngestionStarted(dsRefPrometheus, 0, ActorRef.noSender),
        IngestionStarted(dsRefPrometheus, 1, ActorRef.noSender),
        ShardAssignmentStarted(dsRefPrometheus, 2, ActorRef.noSender),
        RecoveryStarted(dsRefPrometheus, 3, ActorRef.noSender, 0)
      ))

    ingestionNotStartedForAll match {
      case m: collection.Map[DatasetRef, Seq[ShardEvent]] => {
        healthRouteClusterV1.checkIfAllShardsLiveClusterV1(m) shouldEqual false
      }
      case _ => throw new IllegalArgumentException("Should not reach here")
    }

    ingestionNotStartedForAll = collection.immutable.Map(dsRefPrometheus ->
      Seq(
        IngestionStarted(dsRefPrometheus, 0, ActorRef.noSender),
        IngestionStopped(dsRefPrometheus, 1),
        ShardAssignmentStarted(dsRefPrometheus, 2, ActorRef.noSender),
        RecoveryStarted(dsRefPrometheus, 3, ActorRef.noSender, 0)
      ))

    ingestionNotStartedForAll match {
      case m: collection.Map[DatasetRef, Seq[ShardEvent]] => {
        healthRouteClusterV1.checkIfAllShardsLiveClusterV1(m) shouldEqual false
      }
      case _ => throw new IllegalArgumentException("Should not reach here")
    }

    val ingestionStartedForAll = collection.immutable.Map(dsRefPrometheus ->
      Seq(
        IngestionStarted(dsRefPrometheus, 0, ActorRef.noSender),
        RecoveryInProgress(dsRefPrometheus, 1, ActorRef.noSender, 10),
        IngestionStarted(dsRefPrometheus, 2, ActorRef.noSender),
        RecoveryStarted(dsRefPrometheus, 3, ActorRef.noSender, 0)
      ))

    ingestionStartedForAll match {
      case m: collection.Map[DatasetRef, Seq[ShardEvent]] => {
        healthRouteClusterV1.checkIfAllShardsLiveClusterV1(m) shouldEqual true
      }
      case _ => throw new IllegalArgumentException("Should not reach here")
    }
  }

  it("test checkIfAllShardsLiveClusterV2 all liveness scenarios") {

    val testNotEnoughResponses = Seq(
      DatasetShardHealth(dsRefPrometheus, 2, ShardStatusRecovery(10)),
      DatasetShardHealth(dsRefPrometheus, 3, ShardStatusActive)
    )

    testNotEnoughResponses match {
      case m: Seq[DatasetShardHealth]@unchecked => {
        healthRouteClusterV2.checkIfAllShardsLiveClusterV2(m) shouldEqual false
      }
      case _ => throw new IllegalArgumentException("Should not reach here")
    }

    var ingestionNotStartedForAll = Seq(
      DatasetShardHealth(dsRefPrometheus, 0, ShardStatusAssigned),
      DatasetShardHealth(dsRefPrometheus, 1, ShardStatusAssigned),
      DatasetShardHealth(dsRefPrometheus, 2, ShardStatusRecovery(10)),
      DatasetShardHealth(dsRefPrometheus, 3, ShardStatusActive)
    )

    ingestionNotStartedForAll match {
      case m: Seq[DatasetShardHealth]@unchecked => {
        healthRouteClusterV2.checkIfAllShardsLiveClusterV2(m) shouldEqual false
      }
      case _ => throw new IllegalArgumentException("Should not reach here")
    }

    ingestionNotStartedForAll = Seq(
      DatasetShardHealth(dsRefPrometheus, 0, ShardStatusActive),
      DatasetShardHealth(dsRefPrometheus, 1, ShardStatusDown),
      DatasetShardHealth(dsRefPrometheus, 2, ShardStatusRecovery(10)),
      DatasetShardHealth(dsRefPrometheus, 3, ShardStatusActive)
    )

    ingestionNotStartedForAll match {
      case m: Seq[DatasetShardHealth]@unchecked => {
        healthRouteClusterV2.checkIfAllShardsLiveClusterV2(m) shouldEqual false
      }
      case _ => throw new IllegalArgumentException("Should not reach here")
    }

    val ingestionStartedForAll = Seq(
      DatasetShardHealth(dsRefPrometheus, 0, ShardStatusActive),
      DatasetShardHealth(dsRefPrometheus, 1, ShardStatusActive),
      DatasetShardHealth(dsRefPrometheus, 2, ShardStatusRecovery(10)),
      DatasetShardHealth(dsRefPrometheus, 3, ShardStatusActive)
    )

    ingestionStartedForAll match {
      case m: Seq[DatasetShardHealth]@unchecked => {
        healthRouteClusterV2.checkIfAllShardsLiveClusterV2(m) shouldEqual true
      }
      case _ => throw new IllegalArgumentException("Should not reach here")
    }
  }

  it("test checkIfAllShardsLiveClusterV2 for all liveness scenarios with multiple datasets") {

    val testNotEnoughResponses = Seq(
      DatasetShardHealth(dsRefPrometheusRules_1m, 2, ShardStatusRecovery(10)),
      DatasetShardHealth(dsRefPrometheusRules_1m, 3, ShardStatusActive),
      DatasetShardHealth(dsRefPrometheusRules_1m, 4, ShardStatusRecovery(30)),
      DatasetShardHealth(dsRefPrometheusRules_longterm, 2, ShardStatusActive)
    )

    testNotEnoughResponses match {
      case m: Seq[DatasetShardHealth]@unchecked => {
        healthRouteClusterV2Multiple.checkIfAllShardsLiveClusterV2(m) shouldEqual false
      }
      case _ => throw new IllegalArgumentException("Should not reach here")
    }

    val ingestionNotStartedForAll = Seq(
      DatasetShardHealth(dsRefPrometheusRules_1m, 0, ShardStatusAssigned),
      DatasetShardHealth(dsRefPrometheusRules_1m, 1, ShardStatusAssigned),
      DatasetShardHealth(dsRefPrometheusRules_1m, 2, ShardStatusRecovery(10)),
      DatasetShardHealth(dsRefPrometheusRules_1m, 3, ShardStatusActive),
      DatasetShardHealth(dsRefPrometheusRules_longterm, 0, ShardStatusActive),
      DatasetShardHealth(dsRefPrometheusRules_longterm, 2, ShardStatusAssigned),
      DatasetShardHealth(dsRefPrometheusRules_longterm, 1, ShardStatusRecovery(20)),
      DatasetShardHealth(dsRefPrometheusRules_longterm, 3, ShardStatusActive)
    )

    ingestionNotStartedForAll match {
      case m: Seq[DatasetShardHealth]@unchecked => {
        healthRouteClusterV2Multiple.checkIfAllShardsLiveClusterV2(m) shouldEqual false
      }
      case _ => throw new IllegalArgumentException("Should not reach here")
    }

    val ingestionStartedForAll = Seq(
      DatasetShardHealth(dsRefPrometheusRules_1m, 0, ShardStatusActive),
      DatasetShardHealth(dsRefPrometheusRules_1m, 1, ShardStatusActive),
      DatasetShardHealth(dsRefPrometheusRules_1m, 2, ShardStatusRecovery(10)),
      DatasetShardHealth(dsRefPrometheusRules_1m, 3, ShardStatusActive),
      DatasetShardHealth(dsRefPrometheusRules_longterm, 0, ShardStatusActive),
      DatasetShardHealth(dsRefPrometheusRules_longterm, 2, ShardStatusActive),
      DatasetShardHealth(dsRefPrometheusRules_longterm, 1, ShardStatusRecovery(20)),
      DatasetShardHealth(dsRefPrometheusRules_longterm, 3, ShardStatusActive)
    )

    ingestionStartedForAll match {
      case m: Seq[DatasetShardHealth]@unchecked => {
        healthRouteClusterV2Multiple.checkIfAllShardsLiveClusterV2(m) shouldEqual true
      }
      case _ => throw new IllegalArgumentException("Should not reach here")
    }
  }
}
