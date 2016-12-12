package filodb.cassandra

import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core._
import java.nio.ByteBuffer
import scala.concurrent.{Future, Promise}
import scodec.bits.ByteVector

import filodb.core._

/**
 * Utilities for dealing with Cassandra Java Driver
 */
object Util {
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit class ResultSetToResponse(f: Future[ResultSet]) {
    def toResponse(notAppliedResponse: Response = NotApplied): Future[Response] = {
      f.map { resultSet =>
        if (resultSet.wasApplied) Success else notAppliedResponse
      }.recover {
        case e: DriverException => throw StorageEngineException(e)
      }
    }
  }

  implicit class HandleErrors[T](f: Future[T]) {
    def handleErrors: Future[T] = f.recover {
      case e: DriverException          => throw StorageEngineException(e)
      // from invalid Enum strings, which should never happen, or some other parsing error
      case e: NoSuchElementException   => throw MetadataException(e)
      case e: IllegalArgumentException => throw MetadataException(e)
    }
  }

  import com.google.common.util.concurrent.{ListenableFuture, FutureCallback, Futures}

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

  // Creates a writeable ByteBuffer, which is what the java driver expects
  def toBuffer(bv: ByteVector): ByteBuffer = ByteBuffer.wrap(bv.toArray)

  def toHex(bb: ByteBuffer): String = com.datastax.driver.core.utils.Bytes.toHexString(bb)
}