package filodb.cassandra

import com.datastax.driver.core._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

import filodb.core._

trait FiloSessionProvider {
  // It is recommended this be implemented via lazy val.  Don't want to recreate a session every time.
  def session: Session
}

trait BaseCassandraOptions {
  def config: Config

  protected lazy val authEnabled = config.hasPath("username") && config.hasPath("password")

  protected lazy val authProvider =
    if (authEnabled) {
      new PlainTextAuthProvider(config.getString("username"), config.getString("password"))
    } else {
      AuthProvider.NONE
    }

  protected lazy val socketOptions = {
    val opts = new SocketOptions
    config.as[Option[FiniteDuration]]("read-timeout").foreach { to =>
      opts.setReadTimeoutMillis(to.toMillis.toInt) }
    config.as[Option[FiniteDuration]]("connect-timeout").foreach { to =>
      opts.setConnectTimeoutMillis(to.toMillis.toInt) }
    opts
  }

  protected lazy val queryOptions = {
    val opts = new QueryOptions()
    config.as[Option[String]]("default-consistency-level").foreach { cslevel =>
      opts.setConsistencyLevel(ConsistencyLevel.valueOf(cslevel))
    }
    opts
  }

  protected lazy val cqlCompression =
    Try(ProtocolOptions.Compression.valueOf(config.getString("cql-compression")))
      .getOrElse(ProtocolOptions.Compression.NONE)
}

/**
 * The default session provider creates a session from the config alone using
 * the default datastax driver.
 */
class DefaultFiloSessionProvider(val config: Config) extends FiloSessionProvider with BaseCassandraOptions {
  private[this] val cluster =
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

  val session = cluster.connect()
}


