package filodb.cassandra

import com.datastax.driver.core._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

import filodb.core._

trait FiloCassandraConnector {
  private[this] lazy val cluster =
    Cluster.builder().addContactPoints(config.as[Seq[String]]("hosts") :_*)
                 .withPort(config.getInt("port"))
                 .withAuthProvider(authProvider)
                 .withSocketOptions(socketOptions)
                 .withQueryOptions(queryOptions)
                 .build

  // Cassandra config with following keys:  keyspace, hosts, port, username, password
  def config: Config

  private[this] lazy val authEnabled = config.hasPath("username") && config.hasPath("password")

  private[this] lazy val authProvider =
    if (authEnabled) {
      new PlainTextAuthProvider(config.getString("username"), config.getString("password"))
    } else {
      AuthProvider.NONE
    }

  private[this] lazy val socketOptions = {
    val opts = new SocketOptions
    config.as[Option[FiniteDuration]]("read-timeout").foreach { to =>
      opts.setReadTimeoutMillis(to.toMillis.toInt) }
    config.as[Option[FiniteDuration]]("connect-timeout").foreach { to =>
      opts.setConnectTimeoutMillis(to.toMillis.toInt) }
    opts
  }

  private[this] lazy val queryOptions = {
    val opts = new QueryOptions()
    config.as[Option[String]]("default-consistency-level").foreach { cslevel =>
      opts.setConsistencyLevel(ConsistencyLevel.valueOf(cslevel))
    }
    opts
  }

  implicit lazy val session: Session = cluster.connect()
  implicit def ec: ExecutionContext

  lazy val defaultKeySpace = config.getString("keyspace")

  def keySpaceName(ref: DatasetRef): String = ref.database.getOrElse(defaultKeySpace)

  // This is really intended for unit tests.  Production users should create keyspaces manually,
  // probably with something like NetworkTopologyStrategy.
  def createKeyspace(keyspace: String, replicationFactor: Int = 1): Unit = {
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = " +
                    s"{'class': 'SimpleStrategy', 'replication_factor' : $replicationFactor};")
  }

  import Util._

  def execCql(cql: String, notAppliedResponse: Response = NotApplied): Future[Response] =
    session.executeAsync(cql).toScalaFuture.toResponse(notAppliedResponse)

  def execStmt(statement: Statement, notAppliedResponse: Response = NotApplied): Future[Response] =
    session.executeAsync(statement).toScalaFuture.toResponse(notAppliedResponse)

  def shutdown(): Unit = {
    session.getCluster.close()
    session.close()
  }
}
