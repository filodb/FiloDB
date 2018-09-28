package filodb.cassandra

import java.nio.ByteBuffer

import scala.concurrent.{Future, Promise}

import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.DriverException
import monix.reactive.Observable

import filodb.core._

/**
 * Utilities for dealing with Cassandra Java Driver
 */
object Util {
  import filodb.core.GlobalScheduler._

  implicit class ResultSetToResponse(f: Future[ResultSet]) {
    def toResponse(notAppliedResponse: Response = NotApplied): Future[Response] = {
      f.map { resultSet =>
        if (resultSet.wasApplied) Success else notAppliedResponse
      }.recover {
        case e: DriverException => throw StorageEngineException(e)
        case e: Exception       => throw StorageEngineException(e)
      }
    }
  }

  implicit class HandleErrors[T](f: Future[T]) {
    def handleErrors: Future[T] = f.recover {
      case e: DriverException          => throw StorageEngineException(e)
      // from invalid Enum strings, which should never happen, or some other parsing error
      case e: NoSuchElementException   => throw MetadataException(e)
      case e: IllegalArgumentException => throw MetadataException(e)
      case e: Exception                => throw StorageEngineException(e)
    }
  }

  implicit class HandleObservableErrors[T](o: Observable[T]) {
    def handleObservableErrors: Observable[T] = o.onErrorRecover {
      case e: DriverException          => throw StorageEngineException(e)
      // from invalid Enum strings, which should never happen, or some other parsing error
      case e: NoSuchElementException   => throw MetadataException(e)
      case e: IllegalArgumentException => throw MetadataException(e)
      case e: Exception                => throw StorageEngineException(e)
    }
  }

  import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

  implicit class CassandraFutureOps[A](lf: ListenableFuture[A]) {
    def toScalaFuture: Future[A] = {
      val promise = Promise[A]()
      Futures.addCallback(lf, new FutureCallback[A] {
        def onFailure(t: Throwable): Unit = promise failure t
        def onSuccess(result: A): Unit = promise success result
      })

      promise.future
    }
  }

  import collection.JavaConverters._

  implicit class ResultSetFutureOps(rsf: ResultSetFuture) {
    def toIterator: Future[Iterator[Row]] = rsf.toScalaFuture.map { rs => rs.iterator.asScala }
    def toOne: Future[Option[Row]] = rsf.toScalaFuture.map { rs => Option(rs.one()) }
  }

  def unloggedBatch(statements: Seq[Statement]): Statement = {
    val batch = new BatchStatement(BatchStatement.Type.UNLOGGED)
    statements.foreach { stmt => batch.add(stmt) }
    batch
  }

  val emptyBuffer = ByteBuffer.wrap(Array.empty[Byte])

  // Creates a writeable ByteBuffer, which is what the java driver expects
  def toBuffer(key: Array[Byte]): ByteBuffer = ByteBuffer.wrap(key)

  def toHex(bb: ByteBuffer): String = com.datastax.driver.core.utils.Bytes.toHexString(bb)
}