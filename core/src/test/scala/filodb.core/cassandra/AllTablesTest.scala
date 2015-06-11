package filodb.core.cassandra

import akka.actor.{ActorSystem, ActorRef}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import com.websudos.phantom.testing.SimpleCassandraTest
import org.scalatest.{FunSpecLike, Matchers, BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.Futures
import org.scalatest.time.{Millis, Span, Seconds}
import org.velvia.filo.IngestColumn
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

object AllTablesTest {
  val CassConfigStr = """
                   | max-outstanding-futures = 2
                   """.stripMargin
  val CassConfig = ConfigFactory.parseString(CassConfigStr)
}

abstract class AllTablesTest(system: ActorSystem) extends ActorTest(system)
with SimpleCassandraTest
with Futures {
  implicit val defaultPatience =
    PatienceConfig(timeout = Span(2, Seconds), interval = Span(10, Millis))

  val keySpace = "test"

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global

  lazy val datastore = new CassandraDatastore(AllTablesTest.CassConfig)

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

  val GdeltIngestColumns = Seq(IngestColumn("id", classOf[Long]),
                               IngestColumn("sqlDate", classOf[String]),
                               IngestColumn("monthYear", classOf[Int]),
                               IngestColumn("year", classOf[Int]))

  val GdeltColNames = GdeltColumns.map(_._1)

  def createTable(datasetName: String,
                  partitionName: String,
                  columns: Seq[(String, Column.ColumnType)]): (Partition, Seq[Column]) = {
    datastore.newDataset(datasetName).futureValue should equal (Success)

    val partObj = Partition(datasetName, partitionName)
    datastore.newPartition(partObj).futureValue should equal (Success)

    val columnSeq = columns.map { case (name, colType) => Column(name, datasetName, 0, colType) }
    columnSeq.foreach { column =>
      datastore.newColumn(column).futureValue should equal (Success)
    }

    (partObj, columnSeq)
  }
}