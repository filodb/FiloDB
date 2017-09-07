package filodb.coordinator

import scala.concurrent.ExecutionContext

import akka.actor.{ActorPath, PoisonPill}
import akka.cluster.Cluster
import monix.execution.Scheduler

import filodb.core.FutureUtils
import filodb.core.memstore.TimeSeriesMemStore
import filodb.core.reprojector.SegmentStateCache
import filodb.core.store.{ColumnStore, ColumnStoreScanner, MetaStore}

class SupervisorSpec extends AkkaSpec {

  import NodeProtocol._

  lazy val settings = new FilodbSettings(system.settings.config)
  import settings._

  /* Set all as lazy to test same startup as users. */
  private lazy val threadPool = FutureUtils.getBoundedTPE(QueueLength, PoolName, PoolSize, MaxPoolSize)

  implicit lazy val ec = Scheduler(ExecutionContext.fromExecutorService(threadPool): ExecutionContext)
  private lazy val readEc = ec

  private lazy val factory = StoreFactory(settings, ec, readEc)
  private lazy val columnStore: ColumnStore with ColumnStoreScanner = factory.columnStore
  private lazy val metaStore: MetaStore = factory.metaStore
  private lazy val stateCache = new SegmentStateCache(config, columnStore)
  private lazy val memStore = new TimeSeriesMemStore(config)
  private lazy val assignmentStrategy = new DefaultShardAssignmentStrategy
  private lazy val coordinatorProps = NodeCoordinatorActor.props(metaStore, memStore, columnStore, cluster.selfAddress, config)
  private lazy val guardianProps = NodeGuardian.props(settings, cluster, metaStore, memStore, columnStore, assignmentStrategy)
  private lazy val cluster = Cluster(system)

  "NodeGuardian" must {
    import ActorName.{NodeGuardianName => supervisor}
    "start Kamon metrics if not ClusterRole.Cli" in {
      factory.getClass should be(classOf[InMemoryStoreFactory])
      val guardian = system.actorOf(guardianProps, supervisor)
      guardian ! CreateTraceLogger(ClusterRole.Server)
      expectMsgPF() {
        case TraceLoggerRef(ref) =>
          ref.path.name should be(ActorName.TraceLoggerName)
          system stop ref
      }
      system stop guardian
    }
    "create the coordinator actor" in {
      val guardian = system.actorOf(guardianProps, "sguardian")
      guardian ! CreateCoordinator
      expectMsgPF() {
        case CoordinatorRef(ref) =>
          ref.path should be(ActorPath.fromString("akka://akka-test/user/sguardian/" + ActorName.CoordinatorName))
          ref ! PoisonPill // now kill it, should see it logged
      }
      system stop guardian
    }
    "create the cluster actor" in {
      val guardian = system.actorOf(guardianProps, "guardian")
      guardian ! CreateClusterSingleton("worker", withManager = true)
      expectMsgPF() {
        case ClusterSingletonRef(ref) =>
          ref.path should be(ActorPath.fromString("akka://akka-test/user/guardian/" + ActorName.NodeClusterProxyName))
      }
      system stop guardian
    }
    "stop gracefully" in {
      val guardian = system.actorOf(guardianProps)
      guardian ! CreateCoordinator
      expectMsgClass(classOf[CoordinatorRef])
      guardian ! CreateClusterSingleton("worker", withManager = true)
      expectMsgClass(classOf[ClusterSingletonRef])

      guardian ! GracefulShutdown
      expectMsgPF() {
        case ShutdownComplete(ref) =>
          ref should equal (guardian)
      }
      // TODO handle deadletter CurrentClusterState on start
      // while shutting test down during startup
    }
  }
}
