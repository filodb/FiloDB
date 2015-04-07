package filodb.core.cassandra

import akka.actor.{ActorSystem, ActorRef}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import com.websudos.phantom.testing.SimpleCassandraTest
import org.scalatest.{FunSpecLike, Matchers, BeforeAndAfter, BeforeAndAfterAll}
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.core.metadata.{Column, Dataset, Partition}
import filodb.core.messages._

abstract class ActorTest(system: ActorSystem) extends TestKit(system)
with FunSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfter with ImplicitSender {
  override def afterAll() {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}

abstract class AllTablesTest(system: ActorSystem) extends ActorTest(system) with SimpleCassandraTest {
  val keySpace = "test"

  import scala.concurrent.ExecutionContext.Implicits.global

  lazy val metaActor = system.actorOf(MetadataActor.props())
  lazy val datastore = new CassandraDatastore(ConfigFactory.empty)

  def createAllTables(): Unit = {
    val f = for { _ <- DatasetTableOps.create.future()
                  _ <- ColumnTable.create.future()
                  _ <- PartitionTable.create.future()
                  _ <- DataTable.create.future() } yield { 0 }
    Await.result(f, 3 seconds)
  }

  def truncateAllTables(): Unit = {
    val f = for { _ <- DatasetTableOps.truncate.future()
                  _ <- ColumnTable.truncate.future()
                  _ <- PartitionTable.truncate.future()
                  _ <- DataTable.truncate.future() } yield { 0 }
    Await.result(f, 3 seconds)
  }

  val GdeltColumns = Seq("id" -> Column.ColumnType.LongColumn,
                         "sqlDate" -> Column.ColumnType.StringColumn,
                         "monthYear" -> Column.ColumnType.IntColumn,
                         "year" -> Column.ColumnType.IntColumn)

  val GdeltColNames = GdeltColumns.map(_._1)

  def createTable(datasetName: String,
                  partitionName: String,
                  columns: Seq[(String, Column.ColumnType)]): (Partition, Seq[Column]) = {
    metaActor ! Dataset.NewDataset(datasetName)
    expectMsg(Success)

    val partObj = Partition(datasetName, partitionName)
    metaActor ! Partition.NewPartition(partObj)
    expectMsg(Success)

    val columnSeq = columns.map { case (name, colType) => Column(name, datasetName, 0, colType) }
    columnSeq.foreach { column =>
      metaActor ! Column.NewColumn(column)
      expectMsg(Success)
    }

    (partObj, columnSeq)
  }
}