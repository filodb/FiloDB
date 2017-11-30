package filodb.coordinator

import scala.concurrent.duration.FiniteDuration

import akka.actor.{Actor, ActorRef, Address, Deploy, Props}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.cluster.ClusterEvent._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{FlightRecordingSupport, MultiNodeSpec, MultiNodeSpecCallbacks}
import akka.testkit.{ImplicitSender, TestLatch, TestProbe}
import com.typesafe.scalalogging.StrictLogging
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

trait ClusterMultiNodeSpec extends MultiNodeSpecCallbacks
  with WordSpecLike with Matchers with BeforeAndAfterAll { self: MultiNodeSpec =>

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()

  override def afterAll(): Unit = multiNodeSpecAfterAll()

}

trait MultiNodeClusterSpec extends Suite
  with ClusterMultiNodeSpec with FlightRecordingSupport
  with StrictLogging with ScalaFutures with ImplicitSender { self: MultiNodeSpec =>

  import NodeClusterActor.IngestionSource

  override def initialParticipants: Int = roles.size

  protected val defaultTimeout = filodbCluster.settings.DefaultTaskTimeout

  protected lazy val filodbCluster = FilodbCluster(system)

  protected lazy val cluster: Cluster = filodbCluster.cluster

  protected lazy val metaStore = filodbCluster.metaStore

  protected lazy val watcher = TestProbe()

  protected lazy val clusterSingleton: ActorRef = filodbCluster.clusterSingleton("worker", true, Some(watcher.ref))

  protected def address(role: RoleName): Address = node(role).address

  protected def noOpSource = IngestionSource(classOf[NoOpStreamFactory].getName)

  /** Returns true if node is up and healthy.
    * If unregistered, returns true.
    */
  protected def isAvailable(address: Address): Boolean =
    cluster.failureDetector.isAvailable(address)

  def createSubscribers(numSubscribers: Int): Seq[TestProbe] =
    for (n <- 0 to numSubscribers) yield TestProbe()

  override def beforeAll(): Unit = {
    metaStore.clearAllData().futureValue
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    filodbCluster.shutdown()
  }
}

trait MultiNodeClusterBehavior extends MultiNodeClusterSpec {
  self: MultiNodeSpec =>

  def startClusterNode(other: Option[RoleName] = None): Unit = {
    val role = other getOrElse myself
    val roleAddress = address(role)

    clusterSingleton
    filodbCluster.coordinatorActor // must start before join
    filodbCluster join roleAddress
    awaitAssert(cluster.state.members.map(_.address) should contain(roleAddress))
    awaitCond(filodbCluster.isJoined)
    info(s"${role.name} joined")
  }

  def awaitClusterUp(roles: RoleName*): Unit = {
    runOn(roles.head) {
      startClusterNode()
      watcher.expectMsgPF(defaultTimeout) {
        case NodeProtocol.PreStart(identity) =>
          identity.name shouldEqual ActorName.SingletonMgrName
          info(s"Prestart on $myself: $identity")
      }
    }
    enterBarrier(roles.head.name + "-started")

    if (roles.tail.contains(myself)) {
      startClusterNode(Some(roles.head))
    }
    if (roles.contains(myself)) {
      awaitMembersUp(numberOfMembers = roles.length)
    }
    enterBarrier(roles.map(_.name).mkString("-") + "-joined")
  }

  def isUp(m: Member, a: Address): Boolean = m.address == a && m.status == MemberStatus.Up

  def assertMemberUp(on: RoleName, up: RoleName, timeout: FiniteDuration = defaultTimeout): Unit =
    within(timeout) {
      runOn(on) {
        awaitCond(isAvailable(address(up)))
        awaitCond(cluster.state.members.exists(isUp(_, address(up))))
      }
      enterBarrier(s"${up.name}-up-on-${on.name}")
      info(s"MemberStatus.Up ${up.name} on ${on.name}")
    }

  def awaitMembersUp(numberOfMembers: Int, timeout: FiniteDuration = defaultTimeout): Unit =
    within(timeout) {
      awaitAssert(cluster.state.members.size shouldEqual numberOfMembers)
      awaitAssert(cluster.state.members.map(_.status) shouldEqual Set(MemberStatus.Up))
    }

  def awaitOnClusterLeave(on: RoleName,
                          leaving: RoleName,
                          event: Class[_ <: MemberEvent],
                          status: MemberStatus,
                          timeout: FiniteDuration = defaultTimeout): Unit =
    within(timeout) {
      runOn(on) {
        val addr = address(leaving)
        val exitingLatch = TestLatch()
        cluster.subscribe(system.actorOf(Props(new Actor {
          def receive: Actor.Receive = {
            case state: CurrentClusterState =>
              if (state.members.exists(m => m.address == addr && m.status == status))
                exitingLatch.countDown()
            case e: MemberEvent =>
              if (e.getClass == event && e.member.address == addr) {
                exitingLatch.countDown()
                watcher.expectMsgPF(defaultTimeout) {
                  case NodeProtocol.PreStart(identity) =>
                    identity.name shouldEqual ActorName.SingletonMgrName
                    identity.address shouldEqual address(on)
                    info(s"$identity restarted on $on.")
                }
              }
          }
        }).withDeploy(Deploy.local)), classOf[MemberEvent])
        enterBarrier("registered-listener")

        runOn(on) {
          cluster.leave(addr)
        }
        enterBarrier(s"${leaving.name}-change")

        exitingLatch.await
      }

      runOn(leaving) {
        enterBarrier("registered-listener")
        enterBarrier(s"${leaving.name}-change")
      }

      enterBarrier(s"$status-finished")
    }

  def awaitOnClusterJoin(on: RoleName,
                         joining: RoleName,
                         event: Class[_ <: MemberEvent],
                         status: MemberStatus,
                         timeout: FiniteDuration = defaultTimeout): Unit =
    within(timeout) {
      runOn(on) {
        val addr = address(joining)
        val exitingLatch = TestLatch()

        cluster.subscribe(system.actorOf(Props(new Actor {
          def receive: Actor.Receive = {
            case state: CurrentClusterState =>
              if (state.members.exists(m => m.address == addr && m.status == status))
                exitingLatch.countDown()
            case e: MemberEvent =>
              if (e.getClass == event && e.member.address == addr)
                exitingLatch.countDown()
          }
        }).withDeploy(Deploy.local)), classOf[MemberEvent])
        enterBarrier("registered-listener")

        runOn(on) {
          filodbCluster.coordinatorActor
          filodbCluster join addr
          awaitCond(filodbCluster.isJoined)
        }
        enterBarrier(s"changed")

        exitingLatch.await

        runOn(joining) {
          enterBarrier("registered-listener")
          enterBarrier(s"changed")
        }
      }

      enterBarrier(s"$status-finished")
    }

  def awaitAllReachable(): Unit =
    awaitAssert(cluster.state.unreachable shouldEqual Set.empty)

}
