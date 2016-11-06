package filodb.cassandra

import com.datastax.driver.core._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

import filodb.core._

trait FiloCassandraConnector {
  private[this] lazy val cluster =
    Cluster.builder()
                 .addContactPoints(
                   Try(config.as[Seq[String]]("hosts")).getOrElse(config.getString("hosts").split(',').toSeq) :_*
                  )
                 .withPort(config.getInt("port"))
                 .withAuthProvider(authProvider)
                 .withSocketOptions(socketOptions)
                 .withQueryOptions(queryOptions)
                 .withCompression(cqlCompression)
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

  private[this] lazy val cqlCompression =
    Try(ProtocolOptions.Compression.valueOf(config.getString("cql-compression")))
      .getOrElse(ProtocolOptions.Compression.NONE)

  lazy val session: Session = cluster.connect()
  implicit def ec: ExecutionContext

  lazy val defaultKeySpace = config.getString("keyspace")

  def keySpaceName(ref: DatasetRef): String = ref.database.getOrElse(defaultKeySpace)

  def createKeyspace(keyspace: String): Unit = {
    val replOptions = config.getString("keyspace-replication-options")
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = $replOptions")
  }

  import Util._

  def execCql(cql: String, notAppliedResponse: Response = NotApplied): Future[Response] =
    session.executeAsync(cql).toScalaFuture.toResponse(notAppliedResponse)

  def execStmt(statement: Statement, notAppliedResponse: Response = NotApplied): Future[Response] =
    session.executeAsync(statement).toScalaFuture.toResponse(notAppliedResponse)

  def shutdown(): Unit = {
    session.close()
    session.getCluster.close()
  }
}
