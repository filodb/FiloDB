package filodb.coordinator.queryengine

import scala.concurrent.duration._

import akka.actor.{Actor, Props}
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures

import filodb.coordinator.{ActorSpecConfig, ActorTest, FilodbCluster}
import filodb.coordinator.client.QueryCommands
import filodb.core.MachineMetricsData
import filodb.core.query.ExecPlan
import filodb.core.store._

object DistributeSpec extends ActorSpecConfig

// This is really an end to end ingestion test, it's what a client talking to a FiloDB node would do
class DistributeSpec extends ActorTest(DistributeSpec.getNewSystem) with ScalaFutures {
  import MachineMetricsData._
  import monix.execution.Scheduler.Implicits.global

  implicit val askTimeout = Timeout(5.seconds)

  val fakeNode = system.actorOf(Props(new Actor {
    def receive = {
      case e: QueryCommands.ExecPlanQuery =>
        sender() ! QueryCommands.QueryError(10L, new RuntimeException("foo"))
    }
  }))

  private val cluster = FilodbCluster(system)
  private lazy val memStore = cluster.memStore

  describe("DistributeConcat") {
    it("should propagate QueryErrors from children as ChildQueryErrors") {
      val childPlan = new ExecPlan.LocalVectorReader(Seq(0), FilteredPartitionScan(ShardSplit(0), Nil), AllChunkScan)
      val plan = new Engine.DistributeConcat(Seq((fakeNode, childPlan)), 4, 100)
      val exc = Engine.execute(plan, dataset1, memStore, 100).runAsync.failed.futureValue
      exc shouldBe a[ChildQueryError]
      exc.asInstanceOf[ChildQueryError].source shouldEqual fakeNode
    }
  }
}
