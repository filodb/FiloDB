package filodb.spark

import com.typesafe.config.Config
import filodb.cassandra.FiloCassandraConnector

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

object Filo extends Serializable {


  implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global

  // The global config of filodb with cassandra, spark, etc. sections
  var config: Config = _

  lazy val connector = new FiloCassandraConnector(config.getConfig("cassandra"))
  lazy val metaStore = connector.metaStore
  lazy val columnStore = connector.columnStore

  def init(filoConfig: Config): Unit = {
    config = filoConfig
  }

  def parse[T, B](cmd: => Future[T], awaitTimeout: FiniteDuration = 5.seconds)(func: T => B): B = {
    func(Await.result(cmd, awaitTimeout))
  }

}
