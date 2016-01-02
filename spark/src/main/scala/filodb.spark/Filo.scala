package filodb.spark

import akka.actor.{ActorRef, ActorSystem, Props}
import com.typesafe.config.Config
import filodb.cassandra.FiloCassandraConnector
import filodb.core.metadata.Column
import filodb.core.store.Dataset

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

object Filo extends Serializable {

  lazy val system = ActorSystem("filo-spark")

  implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global

  lazy val connector = new FiloCassandraConnector(filoConfig.getConfig("cassandra"))
  lazy val metaStore = connector.metaStore
  lazy val columnStore = connector.columnStore

  // scalastyle:off
  var filoConfig: Config = null

  def init(config: Config) = {
    if (filoConfig == null) {
      filoConfig = config
    }
  }

  // scalastyle:on

  def getDatasetObj(dataset: String): Dataset =
    Filo.parse(metaStore.getDataset(dataset)) { ds => ds.get }

  def getSchema(dataset: String, version: Int): Seq[Column] =
    Filo.parse(metaStore.getSchema(dataset)) { schema => schema }

  def parse[T, B](cmd: => Future[T], awaitTimeout: FiniteDuration = 50000.seconds)(func: T => B): B = {
    func(Await.result(cmd, awaitTimeout))
  }

  def newActor(props: Props): ActorRef = system.actorOf(props)

  def memoryCheck(minFreeMB: Int) = () => getRealFreeMb > minFreeMB

  private def getRealFreeMb: Int =
    ((sys.runtime.maxMemory - (sys.runtime.totalMemory - sys.runtime.freeMemory)) / (1024 * 1024)).toInt


}
