package filodb.coordinator.queryengine

import scala.concurrent.duration._

import akka.actor.{Actor, Props}
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures

import filodb.coordinator._
import filodb.coordinator.client.QueryCommands
import filodb.core.MachineMetricsData
import filodb.core.metadata.DatasetOptions
import filodb.core.query.{ColumnFilter, ExecPlan, Filter}
import filodb.core.store._

object DistributeSpec extends ActorSpecConfig

/**
 * A test for DistributeConcat as well as various sharding / shard key hashing logic to do with distribution
 */
class DistributeSpec extends ActorTest(DistributeSpec.getNewSystem) with ScalaFutures {
  import MachineMetricsData._
  import monix.execution.Scheduler.Implicits.global
  import QueryCommands._

  implicit val askTimeout = Timeout(5.seconds)

  val fakeNode = system.actorOf(Props(new Actor {
    def receive = {
      case e: ExecPlanQuery =>
        sender() ! QueryError(10L, new RuntimeException("foo"))
    }
  }))

  private val cluster = FilodbCluster(system)
  private lazy val memStore = cluster.memStore

  describe("DistributeConcat") {
    it("should propagate QueryErrors from children as ChildQueryErrors") {
      val childPlan = new ExecPlan.LocalVectorReader(Seq(0), FilteredPartitionScan(ShardSplit(0), Nil), AllChunkScan)
      val plan = new Engine.DistributeConcat(Seq((fakeNode, childPlan)), 4, 100, System.currentTimeMillis())
      val exc = Engine.execute(plan, dataset1, memStore, 100).runAsync.failed.futureValue
      exc shouldBe a[ChildQueryError]
      exc.asInstanceOf[ChildQueryError].source shouldEqual fakeNode
    }
  }

  describe("validatePartQuery / shard key hashing") {
    val mapper = new ShardMapper(16)
    (0 to 15).foreach { i => mapper.updateFromEvent(IngestionStarted(dataset1.ref, i, fakeNode)) }

    val datasetWithShardCols = dataset1.copy(options = DatasetOptions.DefaultOptions.copy(
                                 shardKeyColumns = Seq("__name__", "job")))
    val options = QueryOptions(shardKeySpread = 1)

    it("should route to just a few shards if all filters matching and shard key columns defined") {
      val filters = Seq(ColumnFilter("__name__", Filter.Equals("jvm_heap_used")),
                        ColumnFilter("job",      Filter.Equals("prometheus")))
      val resp = Utils.validatePartQuery(datasetWithShardCols, mapper,
                                         FilteredPartitionQuery(filters), options)
      resp.isGood shouldEqual true
      resp.get should have length (2)
      resp.get.map(_.shard) shouldEqual Seq(14, 15)

      val resp2 = Utils.validatePartQuery(datasetWithShardCols, mapper,
                                         FilteredPartitionQuery(filters), options.copy(shardKeySpread = 2))
      resp2.isGood shouldEqual true
      resp2.get.map(_.shard) shouldEqual (12 to 15)
    }

    it("should return BadArgument if not all filters matching for all shard key columns") {
      val filters = Seq(ColumnFilter("__name__", Filter.Equals("jvm_heap_used")),
                        ColumnFilter("jehovah",  Filter.Equals("prometheus")))
      val resp = Utils.validatePartQuery(datasetWithShardCols, mapper,
                                         FilteredPartitionQuery(filters), options)
      resp.isGood shouldEqual false
      resp.swap.get shouldBe a[BadArgument]

      val filters2 = Seq(ColumnFilter("__name__", Filter.Equals("jvm_heap_used")),
                         ColumnFilter("job",      Filter.In(Set("prometheus"))))
      val resp2 = Utils.validatePartQuery(datasetWithShardCols, mapper,
                                         FilteredPartitionQuery(filters2), options)
      resp.isGood shouldEqual false
      resp.swap.get shouldBe a[BadArgument]
    }

    it("should route to all shards, governed by limit, if dataset does not define shard key columns") {
      val filters = Seq(ColumnFilter("__name__", Filter.Equals("jvm_heap_used")),
                        ColumnFilter("job",      Filter.Equals("prometheus")))
      val resp = Utils.validatePartQuery(dataset1, mapper,
                                         FilteredPartitionQuery(filters), options)
      resp.get.map(_.shard) shouldEqual (0 to 15)
    }

    it("should route to shards dictated by shardOverrides if provided") {
      val filters = Seq(ColumnFilter("__name__", Filter.Equals("jvm_heap_used")),
                        ColumnFilter("job",      Filter.Equals("prometheus")))
      val resp = Utils.validatePartQuery(datasetWithShardCols, mapper,
                                         FilteredPartitionQuery(filters),
                                         options.copy(shardOverrides = Some(Seq(5, 6))))
      resp.isGood shouldEqual true
      resp.get.map(_.shard) shouldEqual Seq(5, 6)
    }
  }
}
