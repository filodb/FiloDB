package filodb.cassandra.columnstore

import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.config.{Config, ConfigFactory}
import filodb.cassandra.{AsyncTest, DefaultFiloSessionProvider, FiloCassandraConnector}
import filodb.core._
import org.scalatest.FlatSpec
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class PartitionIndexTableSpec extends FlatSpec with AsyncTest {

  import NamesTestData._
  import monix.execution.Scheduler.Implicits.global
  import java.nio.ByteBuffer

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
    partitionIndexTable.initialize().futureValue(timeout)
  }

  before {
    partitionIndexTable.clearAll().futureValue(timeout)
  }

  val timeout = Timeout(30 seconds)
  val curTimeStamp = System.currentTimeMillis().toInt

  "PartitionIndexTable" should "be able to write and query the partitionIndex table" in {
    val bytes = new Array[Byte](10)
    val byteBuffer = ByteBuffer.wrap(bytes)
    whenReady(partitionIndexTable.writePartitions(14, curTimeStamp, 1, byteBuffer, 1440), timeout) { response =>
      response should equal (Success)
    }
    whenReady(partitionIndexTable.writePartitions(14, curTimeStamp + 1, 2, byteBuffer, 1440), timeout) { response =>
      response should equal (Success)
    }
    partitionIndexTable.getPartitions(14, curTimeStamp).toListL.runAsync.futureValue.map(pr => {
      pr.timeBucket should be >= curTimeStamp
    }).size shouldEqual 1
  }

  it should "return NotFoundError when trying to get nonexisting data" in {
    partitionIndexTable.getPartitions(944, curTimeStamp).toListL.runAsync.futureValue shouldEqual Seq.empty
  }

}