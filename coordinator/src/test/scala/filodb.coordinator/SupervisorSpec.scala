package filodb.coordinator

import akka.actor.{ActorPath, PoisonPill}
import filodb.core.store.MetaStore
import monix.execution.Scheduler

class SupervisorSpec extends AkkaSpec {

  import NodeProtocol._

  private val settings = new FilodbSettings(system.settings.config)

  /* Set all as lazy to test same startup as users. */
  // private lazy val threadPool = FutureUtils.getBoundedTPE(QueueLength, PoolName, PoolSize, MaxPoolSize)

  // implicit lazy val ec = Scheduler(ExecutionContext.fromExecutorService(threadPool): ExecutionContext)

  private lazy val factory = StoreFactory(settings, Scheduler.io("test"))
  private lazy val metaStore: MetaStore = factory.metaStore
  private lazy val memStore = factory.memStore
  private lazy val assignmentStrategy = DefaultShardAssignmentStrategy
  private lazy val guardianProps = NodeGuardian.props(settings, metaStore, memStore, assignmentStrategy)

  "NodeGuardian" must {
    "create the coordinator actor" in {
      val guardian = system.actorOf(guardianProps, "sguardian")
      guardian ! CreateCoordinator
      expectMsgPF() {
        case CoordinatorIdentity(ref) =>
          ref.path should be(ActorPath.fromString("akka://akka-test/user/sguardian/" + ActorName.CoordinatorName))
          ref ! PoisonPill // now kill it, should see it logged
      }
      system stop guardian
    }
    "create the cluster actor" in {
      val guardian = system.actorOf(guardianProps, "guardian")
      guardian ! CreateClusterSingleton("worker", None)
      expectMsgPF() {
        case ClusterSingletonIdentity(ref) =>
          ref.path should be(ActorPath.fromString(
            "akka://akka-test/user/guardian/" + ActorName.ClusterSingletonProxyName))
      }
      system stop guardian
    }
  }
}
