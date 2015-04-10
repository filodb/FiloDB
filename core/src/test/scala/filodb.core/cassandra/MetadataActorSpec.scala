package filodb.core.cassandra

import akka.actor.{ActorSystem, ActorRef}
import scala.concurrent.duration._

import filodb.core.metadata.{Column, Dataset, Partition}
import filodb.core.messages._

object MetadataActorSpec {
  def getNewSystem = ActorSystem("test")
}

class MetadataActorSpec extends AllTablesTest(MetadataActorSpec.getNewSystem) {
  lazy val actor = system.actorOf(MetadataActor.props())

  override def beforeAll() {
    super.beforeAll()
    createAllTables()
  }

  before { truncateAllTables() }

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