package filodb.spark

import org.apache.spark.SparkContext

import scala.concurrent.duration._
import _root_.filodb.core.metadata.Projection
import _root_.filodb.coordinator.NodeCoordinatorActor.Reset
import org.apache.spark.filodb.{FiloDriver, FiloExecutor}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import scala.util.Try
import scalax.file.Path

trait SparkTestBase extends FunSpecLike with BeforeAndAfter with BeforeAndAfterAll
with Matchers with ScalaFutures {

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(250, Millis))

  def testProjections: Seq[Projection]
  def sc: SparkContext

  lazy val metaStore = FiloDriver.metaStore
  lazy val columnStore = FiloDriver.columnStore

  override def beforeAll(): Unit = {
    metaStore.initialize().futureValue(defaultPatience)
    testProjections.foreach { p => columnStore.initializeProjection(p).futureValue(defaultPatience) }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    sc.stop()
  }

  before {
    metaStore.clearAllData().futureValue(defaultPatience)
    FiloDriver.coordinatorActor ! Reset
    try {
      testProjections.foreach { p => columnStore.clearProjectionData(p).futureValue(defaultPatience) }
    } catch {
      case e: Exception =>
    }
    FiloDriver.client.sendAllIngestors(Reset)
  }

  after {
    if (FiloExecutor._config.isDefined) FiloExecutor.stateCache.clear()
    val walDir = FiloExecutor._config.get.getString("write-ahead-log.memtable-wal-dir")
    val path = Path.fromString (walDir)
    Try(path.deleteRecursively(continueOnFailure = false))
  }

  implicit lazy val ec = FiloDriver.ec
}
