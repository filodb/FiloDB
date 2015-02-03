package filodb.core.cassandra

import akka.actor.{ActorSystem, ActorRef}
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import com.websudos.phantom.testing.SimpleCassandraTest
import org.scalatest.FunSpecLike
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.core.metadata.{Column, Dataset}
import filodb.core.messages._

object MetadataActorSpec {
  def getNewSystem = ActorSystem("test")
}

class MetadataActorSpec extends TestKit(MetadataActorSpec.getNewSystem)
with FunSpecLike with ImplicitSender with SimpleCassandraTest {
  val keySpace = "test"

  lazy val actor = system.actorOf(MetadataActor.props())

  // First create the datasets table
  override def beforeAll() {
    super.beforeAll()
    // Note: This is a CREATE TABLE IF NOT EXISTS
    Await.result(DatasetTableOps.create.future(), 3 seconds)
    Await.result(DatasetTableOps.truncate.future(), 3 seconds)
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
}