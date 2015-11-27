package filodb.cassandra.columnstore

import com.typesafe.config.ConfigFactory
import com.websudos.phantom.testkit._
import java.nio.ByteBuffer
import filodb.coordinator.{Success, NotApplied}
import org.scalatest.BeforeAndAfter
import org.scalatest.time.{Millis, Seconds, Span}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

import filodb.core._
import filodb.core.metadata.{Column, ProjectionInfo$}
import filodb.core.Types

class CassandraColumnStoreSpec extends CassandraFlatSpec with BeforeAndAfter {
  import scala.concurrent.ExecutionContext.Implicits.global
  import com.websudos.phantom.dsl._
  import filodb.core.store._
  import Setup._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  val config = ConfigFactory.load("application_test.conf")
  val colStore = new CassandraColumnStore(config)
  implicit val keySpace = KeySpace(config.getString("cassandra.keyspace"))

}
