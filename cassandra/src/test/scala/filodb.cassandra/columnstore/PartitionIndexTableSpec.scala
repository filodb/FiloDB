package filodb.cassandra.columnstore

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.FlatSpec
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import filodb.cassandra.{AsyncTest, DefaultFiloSessionProvider, FiloCassandraConnector}
import filodb.core._

class PartitionIndexTableSpec extends FlatSpec with AsyncTest {

  import java.nio.ByteBuffer

  import monix.execution.Scheduler.Implicits.global

  import NamesTestData._

  val cassandraConfig = ConfigFactory.load("application_test.conf").getConfig("filodb.cassandra")
  val ingestionConsistencyLevel = ConsistencyLevel.valueOf(cassandraConfig.getString("ingestion-consistency-level"))

  val clusterConnector = new FiloCassandraConnector {
    def config: Config = cassandraConfig
    def ec: ExecutionContext = global
    val sessionProvider = new DefaultFiloSessionProvider(cassandraConfig)
  }

  val partitionIndexTable = new PartitionIndexTable(dataset.ref, clusterConnector, ingestionConsistencyLevel)

  // First create the datasets table
  override def beforeAll(): Unit = {
    super.beforeAll()
    partitionIndexTable.connector.createKeyspace(partitionIndexTable.keyspace)
    partitionIndexTable.initialize().futureValue(timeout)
  }

  before {
    partitionIndexTable.clearAll().futureValue(timeout)
  }

  val timeout = Timeout(30 seconds)

  "PartitionIndexTable" should "be able to write and query the partitionIndex table" in {
    val bytes = new Array[Byte](10)
    val byteBuffer = ByteBuffer.wrap(bytes)
    val segments = Seq(byteBuffer, byteBuffer)
    partitionIndexTable.writePartKeySegments(14, 5, segments, 1440).futureValue shouldEqual Success
    partitionIndexTable.writePartKeySegments(14, 6, Seq(byteBuffer), 1440).futureValue shouldEqual Success
    partitionIndexTable.getPartKeySegments(14, 5)
      .toListL.runAsync.futureValue.map(_.segmentId) shouldEqual Seq(0, 1)
    partitionIndexTable.getPartKeySegments(14, 5)
      .toListL.runAsync.futureValue.map(_.segment) shouldEqual segments
    partitionIndexTable.getPartKeySegments(14, 6)
      .toListL.runAsync.futureValue.map(_.segmentId) shouldEqual Seq(0)
  }

  it should "return empty result when trying to get nonexisting data" in {
    partitionIndexTable.getPartKeySegments(944, 20).toListL.runAsync.futureValue shouldEqual Seq.empty
  }

}