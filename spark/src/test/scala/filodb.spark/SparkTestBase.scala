package filodb.spark

import org.apache.spark.SparkContext

import scala.concurrent.duration._
import scala.util.Try
import scalax.file.Path
import filodb.coordinator.NodeProtocol

import filodb.core.metadata.Projection
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

trait SparkTestBase extends FunSpecLike with BeforeAndAfter with BeforeAndAfterAll
with Matchers with ScalaFutures {

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(250, Millis))

  def testProjections: Seq[Projection]
  def sc: SparkContext

  lazy val metaStore = FiloDriver.metaStore
  lazy val columnStore = FiloDriver.columnStore

  override def beforeAll(): Unit = {
    testProjections.foreach { p => columnStore.initializeProjection(p).futureValue(defaultPatience) }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    sc.stop()
  }

  before {
    metaStore.clearAllData().futureValue(defaultPatience)
    FiloDriver.coordinatorActor ! NodeProtocol.ResetState
    try {
      testProjections.foreach { p => columnStore.clearProjectionData(p).futureValue(defaultPatience) }
    } catch {
      case e: Exception =>
    }
    FiloDriver.client.sendAllIngestors(NodeProtocol.ResetState)
    FiloDriver.clusterActor ! NodeProtocol.ResetState
  }

  after {
    FiloExecutor._config.map { config =>
      FiloExecutor.stateCache.clear()
      val walDir = config.getString("write-ahead-log.memtable-wal-dir")
      val path = Path.fromString(walDir)
      Try(path.deleteRecursively(continueOnFailure = false))
    }
  }

  implicit lazy val ec = FiloDriver.ec
}
