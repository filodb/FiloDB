package filodb.core.cassandra

import akka.actor.{ActorSystem, ActorRef}
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import com.websudos.phantom.testing.SimpleCassandraTest
import org.scalatest.FunSpecLike
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.core.metadata.{Column, Dataset, Partition}
import filodb.core.messages._

object MetadataActorSpec {
  def getNewSystem = ActorSystem("test")
}

class MetadataActorSpec extends TestKit(MetadataActorSpec.getNewSystem)
with FunSpecLike with ImplicitSender with SimpleCassandraTest {
  val keySpace = "test"

  lazy val actor = system.actorOf(MetadataActor.props())

  import scala.concurrent.ExecutionContext.Implicits.global

  // First create the datasets table
  override def beforeAll() {
    super.beforeAll()
    val f = for { _ <- DatasetTableOps.create.future()
                  _ <- DatasetTableOps.truncate.future()
                  _ <- ColumnTable.create.future()
                  _ <- ColumnTable.truncate.future()
                  _ <- PartitionTable.create.future()
                  _ <- PartitionTable.truncate.future() } yield { 0 }
    Await.result(f, 3 seconds)
  }

  it("should return AlreadyExists when sending NewDataset message") {
    actor ! Dataset.NewDataset("gdelt")
    expectMsg(Success)
    actor ! Dataset.NewDataset("gdelt")
    expectMsg(AlreadyExists)
  }

  val monthYearCol = Column("monthYear", "gdelt", 1, Column.ColumnType.LongColumn)
  it("should be able to create a Column and get the Schema") {
    actor ! Column.NewColumn(monthYearCol)
    expectMsg(Success)
    actor ! Column.GetSchema("gdelt", 10)
    expectMsg(Column.TheSchema(Map("monthYear" -> monthYearCol)))
  }

  val p = Partition("gdelt", "1979-1984")
  it("should be able to create a Partition, add a shard, then get everything") {
    actor ! Partition.NewPartition(p)
    expectMsg(Success)
    actor ! Partition.AddShard(p, 0, 0 -> 1)
    expectMsg(Success)
    val p2 = p.addShard(0, 0 -> 1).get
    actor ! Partition.GetPartition("gdelt", "1979-1984")
    expectMsg(Partition.ThePartition(p2))
  }
}