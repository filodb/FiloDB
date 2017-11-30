package filodb.spark

import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.coordinator.NodeProtocol
import filodb.core._

trait SparkTestBase extends FunSpecLike with BeforeAndAfter with BeforeAndAfterAll
with Matchers with ScalaFutures {

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(250, Millis))

  def testDatasets: Seq[DatasetRef]
  def sc: SparkContext

  lazy val metaStore = FiloDriver.metaStore

  override def beforeAll(): Unit = {
    // TODO: so in theory when the dataset is setup, memStore.setup() should cause the ChunkSink
    // to initialize the tables
    // testDatasets.foreach { p => columnStore.initializeProjection(p).futureValue(defaultPatience) }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    sc.stop()
  }

  before {
    metaStore.clearAllData().futureValue(defaultPatience)
    FiloDriver.coordinatorActor ! NodeProtocol.ResetState
    try {
      testDatasets.foreach { d => FiloDriver.client.truncateDataset(d) }
    } catch {
      case e: Exception =>
    }
    FiloDriver.client.sendAllIngestors(NodeProtocol.ResetState)
    FiloExecutor.clusterActor ! NodeProtocol.ResetState
  }

  implicit lazy val ec = FiloDriver.ec
}
