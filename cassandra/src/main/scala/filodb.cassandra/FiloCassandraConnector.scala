package filodb.cassandra

import com.datastax.driver.core.{Session, VersionNumber}
import com.typesafe.config.Config
import com.websudos.phantom.connectors.{ContactPoints, KeySpace}
import net.ceedubs.ficus.Ficus._

trait FiloCassandraConnector {
  private[this] lazy val connector =
    ContactPoints(config.as[Seq[String]]("hosts"), config.getInt("port")).keySpace(keySpace.name)

  // Cassandra config with following keys:  keyspace, hosts, port
  def config: Config

  implicit lazy val keySpace = KeySpace(config.getString("keyspace"))

  implicit lazy val session: Session = connector.session

  def cassandraVersion: VersionNumber =  connector.cassandraVersion

  def cassandraVersions: Set[VersionNumber] =  connector.cassandraVersions
}