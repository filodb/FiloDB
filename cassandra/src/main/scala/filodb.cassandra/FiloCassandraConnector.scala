package filodb.cassandra

import com.datastax.driver.core._
import com.typesafe.config.Config
import scala.concurrent.{ExecutionContext, Future}

import filodb.core._

trait FiloCassandraConnector {
  // Cassandra config with following keys:  keyspace, hosts, port, username, password
  def config: Config
  def sessionProvider: FiloSessionProvider

  lazy val session: Session = sessionProvider.session

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
