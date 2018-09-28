package filodb.cassandra

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Try

import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.{ConnectionException, NoHostAvailableException, QueryExecutionException}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.eval.Task

import filodb.core._

object FiloCassandraConnector {
  val cassRetriesScheduledCount = Kamon.counter("cassandra-retries-scheduled")
}

trait FiloCassandraConnector extends StrictLogging {

  import filodb.core.GlobalScheduler._

  // Cassandra config with following keys:  keyspace, hosts, port, username, password
  def config: Config
  def sessionProvider: FiloSessionProvider

  lazy val session: Session = sessionProvider.session

  lazy val baseRetryInterval = config.getDuration("retry-interval").toMillis.millis
  lazy val retryIntervalMaxJitter = config.getDuration("retry-interval-max-jitter").toMillis.toInt
  lazy val maxRetryAttempts = config.getInt("max-retry-attempts")

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

  def execStmtWithRetries(statement: Statement, notAppliedResponse: Response = NotApplied): Future[Response] =
    attemptExecute(statement, maxRetryAttempts).toResponse(notAppliedResponse)

  /**
    * Linear back-off is done between attempts with a configurable retry interval/count along with a random jitter
    */
  private def attemptExecute(statement: Statement, retryAttemptsLeft: Int): Future[ResultSet] = {
    session.executeAsync(statement).toScalaFuture.recoverWith {
      case e @ ( _: QueryExecutionException |
                 _: NoHostAvailableException |
                 _: ConnectionException ) =>
        logger.warn(s"CQL execution of $statement resulted in error. Will retry $retryAttemptsLeft times", e)
        if (retryAttemptsLeft > 0) {
          FiloCassandraConnector.cassRetriesScheduledCount.increment()
          scheduleRetry(baseRetryInterval + ThreadLocalRandom.current().nextInt(retryIntervalMaxJitter).millis) {
            attemptExecute(statement, retryAttemptsLeft - 1)
          }.runAsync.flatMap(r=>r)
        } else {
          throw StorageEngineException(e)
        }
      case e: Exception  => throw StorageEngineException(e)
    }
  }

  private def scheduleRetry[A](delay: FiniteDuration)(f: => A): Task[A] = {
    Task.create { (scheduler, callback) =>
      scheduler.scheduleOnce(delay) { callback(Try(f)) }
    }
  }

  def shutdown(): Unit = {
    session.close()
    session.getCluster.close()
  }
}
