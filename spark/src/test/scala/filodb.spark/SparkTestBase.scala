package filodb.spark

import org.apache.spark.SparkContext
import scala.concurrent.duration._

import filodb.core.metadata.Projection
import filodb.coordinator.NodeCoordinatorActor.Reset

import org.scalatest.{FunSpecLike, BeforeAndAfter, BeforeAndAfterAll, Matchers}
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

  override def beforeAll() {
    metaStore.initialize("unittest").futureValue(defaultPatience)
    metaStore.initialize("unittest2").futureValue(defaultPatience)
    testProjections.foreach { p => columnStore.initializeProjection(p).futureValue(defaultPatience) }
  }

  override def afterAll() {
    super.afterAll()
    FiloDriver.shutdown()
    sc.stop()
  }

  before {
    metaStore.clearAllData("unittest").futureValue(defaultPatience)
    metaStore.clearAllData("unittest2").futureValue(defaultPatience)
    FiloDriver.coordinatorActor ! Reset
    try {
      testProjections.foreach { p => columnStore.clearProjectionData(p).futureValue(defaultPatience) }
    } catch {
      case e: Exception =>
    }
    FiloDriver.client.sendAllIngestors(Reset)
  }

  implicit lazy val ec = FiloDriver.ec
}
