package filodb.cassandra

import com.datastax.driver.core.exceptions.DriverException
import com.websudos.phantom.dsl._
import java.nio.ByteBuffer
import filodb.coordinator._

import scala.concurrent.Future

import filodb.core._

/**
 * Utilities for dealing with Cassandra I/O
 */
object Util {
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

  def toHex(bb: ByteBuffer): String = com.datastax.driver.core.utils.Bytes.toHexString(bb)
}
