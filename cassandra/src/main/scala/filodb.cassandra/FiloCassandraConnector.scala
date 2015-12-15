package filodb.cassandra

import java.net.InetAddress

import com.datastax.driver.core.{AuthProvider, PlainTextAuthProvider, Session}
import com.typesafe.config.Config
import com.websudos.phantom.connectors.{ContactPoints, KeySpace, VersionNumber}
import filodb.cassandra.cluster.LocalFirstDCOnly
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.metastore.CassandraMetaStore

import scala.concurrent.ExecutionContext

class FiloCassandraConnector(config: Config)(implicit val ec: ExecutionContext) {

  implicit lazy val keySpace = KeySpace(config.getString("keyspace"))

  implicit lazy val session: Session = connector.session

  def cassandraVersion: VersionNumber = connector.cassandraVersion

  def cassandraVersions: Set[VersionNumber] = connector.cassandraVersions

  lazy val columnStore = new CassandraColumnStore(keySpace, session)

  lazy val metaStore = new CassandraMetaStore(keySpace, session)

  private lazy val localDC = if (config.hasPath("localDC")) {
    Some(config.getString("localDC"))
  } else {
    None
  }
  // we use a local node first and local DC only load balancing policy.
  private lazy val lbp = new LocalFirstDCOnly(hosts, localDC)

  // this will throw exception for invalid hosts
  private[this] lazy val hosts =
    hostConfig.map(InetAddress.getByName(_)).toSet

  private[this] lazy val hostConfig = config.getString("hosts").split(',')

  private[this] lazy val connector =
    ContactPoints(hostConfig, config.getInt("port"))
      .withClusterBuilder(_.withAuthProvider(authProvider))
      .withClusterBuilder(_.withLoadBalancingPolicy(lbp))
      .keySpace(keySpace.name)


  private[this] lazy val authEnabled = config.hasPath("username") && config.hasPath("password")

  private[this] lazy val authProvider =
    if (authEnabled) {
      new PlainTextAuthProvider(config.getString("username"), config.getString("password"))
    }
    else {
      AuthProvider.NONE
    }


}
