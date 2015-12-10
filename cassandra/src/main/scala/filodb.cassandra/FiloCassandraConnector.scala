package filodb.cassandra

import com.datastax.driver.core.{AuthProvider, PlainTextAuthProvider, Session}
import com.typesafe.config.Config
import com.websudos.phantom.connectors.{ContactPoints, KeySpace, VersionNumber}
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.metastore.CassandraMetaStore
import net.ceedubs.ficus.Ficus._

import scala.concurrent.ExecutionContext

class FiloCassandraConnector(config: Config)(implicit val ec: ExecutionContext) {
  private[this] lazy val connector =
    ContactPoints(config.as[Seq[String]]("hosts"), config.getInt("port"))
      .withClusterBuilder(_.withAuthProvider(authProvider))
      .keySpace(keySpace.name)


  private[this] lazy val authEnabled = config.hasPath("username") && config.hasPath("password")

  private[this] lazy val authProvider =
    if (authEnabled) {
      new PlainTextAuthProvider(config.getString("username"), config.getString("password"))
    }
    else {
      AuthProvider.NONE
    }

  implicit lazy val keySpace = KeySpace(config.getString("keyspace"))

  implicit lazy val session: Session = connector.session

  def cassandraVersion: VersionNumber = connector.cassandraVersion

  def cassandraVersions: Set[VersionNumber] = connector.cassandraVersions

  lazy val columnStore = new CassandraColumnStore(keySpace, session)

  lazy val metaStore = new CassandraMetaStore(keySpace, session)
}
