package filodb.spark

import com.typesafe.config.{Config, ConfigFactory}
import filodb.cassandra.FiloCassandraConnector
import org.apache.spark.SparkContext

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object Filo {

  import collection.JavaConverters._
  implicit val context = scala.concurrent.ExecutionContext.Implicits.global

  // The global config of filodb with cassandra, columnstore, etc. sections
  var config: Config = _

  lazy val connector = new FiloCassandraConnector(config)
  lazy val metaStore = connector.metaStore
  lazy val columnStore = connector.columnStore

  def init(filoConfig: Config): Unit = {
    config = filoConfig
  }

  def init(context: SparkContext): Unit = init(configFromSpark(context))

  def configFromSpark(context: SparkContext): Config = {
    val conf = context.getConf
    val filoOverrides = conf.getAll.collect { case (k, v) if k.startsWith("filodb") =>
      k.replace("filodb.", "") -> v
    }
    ConfigFactory.parseMap(filoOverrides.toMap.asJava)
      .withFallback(ConfigFactory.load)
  }

  def parse[T, B](cmd: => Future[T], awaitTimeout: FiniteDuration = 5 seconds)(func: T => B): B = {
    func(Await.result(cmd, awaitTimeout))
  }

}
