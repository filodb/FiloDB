package filodb.cassandra

import com.datastax.driver.core.{PoolingOptions, Session}
import com.websudos.phantom.connectors.{ContactPoint, KeySpace, VersionNumber}
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest._
import org.scalatest.concurrent.{AsyncAssertions, ScalaFutures}

trait CassandraTest extends FunSpec with ScalaFutures
with Matchers with Assertions
with AsyncAssertions
with BeforeAndAfterAll {
  self: BeforeAndAfterAll with Suite =>

  private[this] val connector = Defaults.defaultConnector

  implicit lazy val keySpace: KeySpace = KeySpace(Defaults.keySpaceString)

  implicit lazy val session: Session = connector.session

  def cassandraVersion: VersionNumber = connector.cassandraVersion

  def cassandraVersions: Set[VersionNumber] = connector.cassandraVersions

  override def beforeAll(): Unit ={
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra()
  }
}

object Defaults {

  final val keySpaceString = "unittest"
  final val defaultConnector = this.synchronized {

    ContactPoint.embedded
      .withClusterBuilder(
        _.withoutJMXReporting().withoutMetrics().withPoolingOptions(new PoolingOptions().setHeartbeatIntervalSeconds(0))
      ).keySpace("unittest")
  }

}
