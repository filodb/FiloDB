package filodb.cassandra

import com.datastax.driver.core.{AuthProvider, PlainTextAuthProvider, Session, VersionNumber}
import com.typesafe.config.Config
import com.websudos.phantom.connectors.{ContactPoints, KeySpace}
import net.ceedubs.ficus.Ficus._

trait FiloCassandraConnector {
  private[this] lazy val connector =
    ContactPoints(config.as[Seq[String]]("hosts"), config.getInt("port"))
      .withClusterBuilder(_.withAuthProvider(authProvider))
      .keySpace(keySpace.name)

  // Cassandra config with following keys:  keyspace, hosts, port, username, password
  def config: Config

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

  def cassandraVersion: VersionNumber =  connector.cassandraVersion

  def cassandraVersions: Set[VersionNumber] =  connector.cassandraVersions
}